from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
from time import time
from lstore.page import PageGroup, pageRange
from lstore import config

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table:Table = table
        self.rid_counter = 1 # counter to keep track of the number of records in the table
    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        rid_list = self.table.index.indices[config.PRIMARY_KEY_COLUMN][primary_key]
        if rid_list is None:
            return False
        rid = rid_list[0]

        page_range_num, base_page_num, record_num = self.table.page_directory[rid]
        base_page = self.table.page_ranges[page_range_num].base_pages[base_page_num]

        # # Follow indirection pointer to get the latest version
        # current_rid = base_page.pages[config.INDIRECTION_COLUMN].read(record_num) 
        
        # base_page.pages[config.RID_COLUMN].write(0, record_num) # set base page RID to 0
        # base_page.pages[config.TIMESTAMP_COLUMN].write(0, record_num) # set the timestamp to 0 as well
        # base_page.pages[config.INDIRECTION_COLUMN].write(0, record_num) # set the indirection to 0 too
        
        # then delete the RID, timestamp, and indirection
        zeroed = 0
        offset_number = record_num * config.VALUE_SIZE
        base_page.pages[config.RID_COLUMN].data[offset_number:offset_number + config.VALUE_SIZE] = zeroed.to_bytes(config.VALUE_SIZE, byteorder='little')
        base_page.pages[config.TIMESTAMP_COLUMN].data[offset_number:offset_number + config.VALUE_SIZE] = zeroed.to_bytes(config.VALUE_SIZE, byteorder='little')
        base_page.pages[config.INDIRECTION_COLUMN].data[offset_number:offset_number + config.VALUE_SIZE] = zeroed.to_bytes(config.VALUE_SIZE, byteorder='little')
        
        self.table.index.indices[config.PRIMARY_KEY_COLUMN][primary_key] = [None]

        # print(f"successfully deleted")

        return True            
    
    # is this function ever used? we might be able to remove it.
    def get_vals(self, rid):
        if rid in self.table.page_directory:
            return self.table.page_directory[rid]
        return None  # Handle missing RID properly

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # Validate column count and prevent insertion if column count does not match
        if len(columns) != self.table.num_columns:
            return False  
        

        primary_key = columns[self.table.key]  # Get primary key value
        # Check if primary key already exists
        if primary_key in self.table.index.indices[config.PRIMARY_KEY_COLUMN]:
            return False  # Prevent duplicate insertion
        
        # Generate metadata
        rid = self.rid_counter
        self.rid_counter += 1
        schema_encoding = int('0' * self.table.num_columns, 2) 
        timestamp = int(time())  # Store current timestamp
        indirection = 0 # initially set to 0
        
        # Convert into a full record format
        record = [indirection, rid, timestamp, schema_encoding] + list(columns)

        # Find available location to write to
        page_range_number, base_page_number, record_number = None, None, None
        
        # Iterate over page ranges to find a base page with capacity
        for pr_num, page_range in enumerate(self.table.page_ranges):
            for bp_num, base_page in enumerate(page_range.base_pages):
                # print(f"Checking capacity for Page Range {pr_num}, Base Page {bp_num}: {base_page.has_capacity()}")
                if base_page.has_capacity():
                    page_range_number, base_page_number = pr_num, bp_num
                    record_number = base_page.pages[0].num_records  # Use next available slot
                    
                    # DEBUG: Print the page range, base page, and record number
                    # print(f"using {pr_num}, {bp_num}, {record_number}")
                    break
            if page_range_number is not None:
                break

        # If no available space is found, create a new page range
        # Since base pages are created upon page range initialization, we only need to create a new page range
        if page_range_number is None:
            page_range_number = len(self.table.page_ranges)
            new_page_range = pageRange(num_columns=self.table.num_columns)  # Create a new page range
            self.table.page_ranges.append(new_page_range)
            base_page_number = 0 # First base page in the new page range
            record_number = 0  # First slot in the new page

            page_range = new_page_range  
        else:
            page_range = self.table.page_ranges[page_range_number]

        # Write the record
        if not page_range.base_pages[base_page_number].write(*record, record_number=record_number):
            return False  

        # Update page directory for hash table
        self.table.page_directory[rid] = (page_range_number, base_page_number, record_number)

        # Update index
        self.table.index.addRecord(primary_key, rid)
        return True

   
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        # just call self_version with 0 to get the current version
        return self.select_version(search_key, search_key_index, projected_columns_index, 0)

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        # fix relative version by taking absolute value
        # relative_version = abs(relative_version)
        records = []

        if search_key_index == self.table.key:
            rid_list = self.table.index.indices[config.PRIMARY_KEY_COLUMN][search_key]
            if rid_list is None:
                return False
            rid = rid_list[0]
            # print(f"rid is {rid}")
            # # Use page directory for fast lookup if searching by primary key
            # rid = None
            # for rid_key, (page_range_num, base_page_num, record_num) in self.table.page_directory.items():
            #     stored_primary_key = self.table.page_ranges[page_range_num].base_pages[base_page_num].pages[config.PRIMARY_KEY_COLUMN].read(record_num)                
                
            #     # gets the RID
            #     get_RID = self.table.page_ranges[page_range_num].base_pages[base_page_num].pages[config.RID_COLUMN].read(record_num)

            #     # only lets it through if the RID wasnt deleted
            #     if ((stored_primary_key == search_key) and (get_RID!=0)):
            #         rid = rid_key
            #         break
            # print(f"rid is {rid}")
            if rid is None or rid==0:
                return False 

            # Locate the record in the page directory using RID
            page_range_num, base_page_num, record_num = self.table.page_directory[rid]
            base_page = self.table.page_ranges[page_range_num].base_pages[base_page_num]

            # Follow indirection pointer to get the latest version
            current_rid = base_page.pages[config.INDIRECTION_COLUMN].read(record_num) 
            versions = [(base_page, record_num)]

            first_traverse = True
            # replace with a for loop that runs relative_version times
            while current_rid != 0 and current_rid in self.table.page_directory:
                prev_rid = current_rid # hold the last RID, 

                tail_page_range, tail_base_page, tail_record_num = self.table.page_directory[current_rid]
                tail_page = self.table.page_ranges[tail_page_range].tail_pages[tail_base_page]
                # if tail_page!=base_page and tail_record_num!=record_num:
                # print(f"\ninserting new version: {tail_page}, {tail_record_num}")
                if first_traverse:
                    versions.insert(0, (tail_page, tail_record_num)) # update version
                    first_traverse = False
                else: versions.insert(2, (tail_page, tail_record_num)) # update version


                current_rid = tail_page.pages[config.INDIRECTION_COLUMN].read(tail_record_num)  # Move to the next older version
                # print(f"indirection of {prev_rid} is {current_rid}")

            # print(f"versions: {versions[0][1]}, {versions[-1][1]}")
            # print(f"versions len: {len(versions)}")

            # Retrieve the Correct Version
            # if relative_version  0:
            #     # Retrieve the specified past version
            #     # print(f"relative version is {relative_version}, should be negative, length of versions is {len(versions)}")
            #     version_page, version_record_num = versions[relative_version]
            # print(f"rel ver: {relative_version}, len ver: {len(versions)}")
            version_page, version_record_num = versions[relative_version]
            # else:
            #     print(f"error: relative ver > 0")
            #     # # Retrieve the specified past version
            #     # print(f"relative version is {relative_version}, should be pos, length of versions is {len(versions)}")
            #     # version_page, version_record_num = versions[len(versions)-relative_version]

            # Read the final/latest version of the record
            # version_page, version_record_num = latest_version
            stored_values = [version_page.pages[i + 5].read(version_record_num) for i in range(self.table.num_columns - 1)]
            stored_primary_key = base_page.pages[config.PRIMARY_KEY_COLUMN].read(record_num)

            if relative_version==-2:
                version_page2, version_record_num2 = versions[-2]
                version_page1, version_record_num1 = versions[-1]
                version_page0, version_record_num0 = versions[0]
                stored2 = [version_page2.pages[i + 5].read(version_record_num2) for i in range(self.table.num_columns - 1)]
                stored1 = [version_page1.pages[i + 5].read(version_record_num1) for i in range(self.table.num_columns - 1)]
                stored0 = [version_page0.pages[i + 5].read(version_record_num0) for i in range(self.table.num_columns - 1)]
                # print(f"\nrel ver   = {relative_version}")
                # print(f"stored -2 = {stored2}")
                # print(f"stored -1 = {stored1}")
                # print(f"stored  0 = {stored0}")

            # Apply column projection
            projected_values = [stored_primary_key] + [
                stored_values[i] if projected_columns_index[i + 1] else None for i in range(self.table.num_columns - 1)
            ]
            records.append(Record(version_record_num, search_key, projected_values))
            # records.append(Record(search_key, search_key, projected_values))

        else:
            # Use the index to find all matching RIDs instead of scanning everything
            rid_list = self.table.index.locate(search_key_index, search_key)
            if not rid_list:
                return False  # No records found
            for rid in rid_list:
                if rid not in self.table.page_directory:
                    continue  # Skip invalid entries
                page_range_num, base_page_num, record_num = self.table.page_directory[rid]
                base_page = self.table.page_ranges[page_range_num].base_pages[base_page_num]
                # Follow indirection to get the latest version
                current_rid = base_page.pages[config.INDIRECTION_COLUMN].read(record_num)
                latest_version = (base_page, record_num)
                while current_rid != 0 and current_rid in self.table.page_directory:
                    tail_page_range, tail_base_page, tail_record_num = self.table.page_directory[current_rid]
                    tail_page = self.table.page_ranges[tail_page_range].tail_pages[tail_base_page]
                    latest_version = (tail_page, tail_record_num)
                    current_rid = tail_page.pages[config.INDIRECTION_COLUMN].read(tail_record_num)
                # Read the final/latest version of the record
                version_page, version_record_num = latest_version
                stored_values = [version_page.pages[i + 5].read(version_record_num) for i in range(self.table.num_columns - 1)]
                stored_primary_key = base_page.pages[config.PRIMARY_KEY_COLUMN].read(record_num)
                projected_values = [stored_primary_key] + [
                    stored_values[i] if projected_columns_index[i + 1] else None for i in range(self.table.num_columns - 1)
                ]
                records.append(Record(stored_primary_key, search_key, projected_values))

        return records if records else False

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        # print("Starting update function")
        
        # need RID to get base and tail pages
        rid_list = self.table.index.indices[config.PRIMARY_KEY_COLUMN][primary_key]
        if rid_list is None:
            return False
        rid = rid_list[0]
        # print(f"rid is {rid}")

        record = self.select(primary_key, 0, [1] * self.table.num_columns)
    
        # print(f"record is: {record[0].columns}")

        if record is None: # no match -> update fails
            # print("No matching record found")
            return False  
    

        # # Locate the record in the page directory using RID
        page_range_num, base_page_num, record_num = self.table.page_directory[rid]
        base_page = self.table.page_ranges[page_range_num].base_pages[base_page_num]
        base_page_indirection = base_page.pages[config.INDIRECTION_COLUMN].read(record_num) 

        # # Follow indirection pointer to get the latest version
        # current_rid = base_page.pages[config.INDIRECTION_COLUMN].read(record_num) 
        # base_page_indirection = current_rid
        # latest_version = (base_page, record_num)

        # while current_rid != 0 and current_rid in self.table.page_directory:
        #     tail_page_range, tail_base_page, tail_record_num = self.table.page_directory[current_rid]
        #     tail_page = self.table.page_ranges[tail_page_range].tail_pages[tail_base_page]
        #     latest_version = (tail_page, tail_record_num)  # Update latest version
        #     current_rid = tail_page.pages[config.INDIRECTION_COLUMN].read(tail_record_num)  # Move to the next older version

        # # Read the final/latest version of the record
        # version_page, version_record_num = latest_version
        # stored_values = [version_page.pages[i + 4].read(version_record_num) for i in range(self.table.num_columns)]
        # stored_primary_key = base_page.pages[4].read(record_num)



        stored_values = record[0].columns
        # Get the current values of the record
        updated_values = [columns[i] if columns[i] is not None else stored_values[i] for i in range(self.table.num_columns)]
        # print(f"updated is: {updated_values}")
        
        # might be num cols -1

        # print("Current values:", stored_values)
        # print("Updated values:", updated_values)

        # Create a new version of the record
        new_rid = self.rid_counter
        self.rid_counter += 1
        timestamp = int(time())
        schema_encoding = int(''.join(['1' if col is not None else '0' for col in columns]), 2)
        # print(f"base indir is {base_page_indirection}, latest indir is {current_rid}, new rid is {new_rid}")
        indirection = base_page_indirection

        # Convert into a full record format
        new_record = [indirection, new_rid, timestamp, schema_encoding] + updated_values

        # print("New record:", new_record)

        # Find available location to write the new version
        page_range = self.table.page_ranges[page_range_num]

        if not page_range.tail_pages:
            # print("creating a tail page")
            page_range.tail_pages.append(PageGroup(num_columns=self.table.num_columns))


        tailpage = page_range.tail_pages[-1]
        page = tailpage.pages[-1]
        # for page in tailpage.pages:
        if page.has_capacity()==False:
            # print(f"phys page is full, creating new page")
            page_range.tail_pages.append(PageGroup(num_columns=self.table.num_columns))

        # if not (page.pages[-1].has_capacity() for page in page_range.tail_pages):
        #     print("Adding new tail page")
        #     print(f"curr tail page num = {len(page_range.tail_pages)}")
        #     new_tail_page = PageGroup(num_columns=self.table.num_columns)
        #     page_range.tail_pages.append(new_tail_page)
        


        # if not page_range.tail_pages or not page_range.tail_pages[-1].pages[0].has_capacity():
        #     # print("Adding new tail page")
        #     page_range.tail_pages.append(PageGroup(num_columns=self.table.num_columns))

        # Last tail page and the slot in that page for the new record
        # tail_page = page_range.tail_pages[-1]
        # tail_record_num = tail_page.pages[0].num_records
        # print("DEBUG: type of page_range.tail_pages[-1] =", type(page_range.tail_pages[-1]))


        # run this line again to ensure that you have the most up to date range, in case a new page range was created
        page_range = self.table.page_ranges[page_range_num]
        tail_page_group = page_range.tail_pages[-1]
        tail_record_num = tail_page_group.pages[-1].num_records
        # print(f"new tail page num = {len(page_range.tail_pages)}")
        # print(f"new tail record num = {tail_record_num}")

        # print("Writing new version to tail page at record number:", tail_record_num)

        # Write the new version
        tail_page_group.write(*new_record, record_number=tail_record_num)

        # Update page directory for the new version
        self.table.page_directory[new_rid] = (page_range_num, len(page_range.tail_pages) - 1, tail_record_num)

        # Update the indirection column of the base record to point to the new version

        # print(f"DEBUG: Updating base record at {record_num} with new indirection RID: {new_rid}")
        # print(f"DEBUG: Previous indirection RID: {base_page.pages[config.INDIRECTION_COLUMN].read(record_num)}")
        # base_page.pages[config.INDIRECTION_COLUMN].write(new_rid, record_num)
        offset_number = record_num * config.VALUE_SIZE
        base_page.pages[config.INDIRECTION_COLUMN].data[offset_number:offset_number + config.VALUE_SIZE] = new_rid.to_bytes(config.VALUE_SIZE, byteorder='little')

        # update tail page indirection to be the next tail page
        # we grab the indirection of the base page
        # we set the indirection of the new tail page to the indirection of the old tail page


        #   
        # print(f"DEBUG: New indirection RID set: {base_page.pages[config.INDIRECTION_COLUMN].read(record_num)}")


        # print("Update successful")
        return True
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        return self.sum_version(start_range, end_range, aggregate_column_index, 0)
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        # ensure that start range is lower than end range
        if(start_range > end_range):
            start_range, end_range = end_range, start_range

        sum = 0
        range_not_empty = False
        
        for key in range(start_range, end_range+1):
            # same line from the increment function
            row = self.select_version(key, self.table.key, [1] * self.table.num_columns, relative_version=relative_version)
            
            # validate row
            if row is False:
                continue
            # at least one valid row
            range_not_empty = True

            val = row[0].columns[aggregate_column_index]
            # add the cell to the sum
            if val != None:
                sum += val

        if range_not_empty==False:
            return False
        else: return sum
    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[0].columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
