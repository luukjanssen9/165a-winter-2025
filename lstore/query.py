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
        # counter to keep track of the number of records in the table
        self.rid_counter = 1 
    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        # Retrieve all the RIDs that match the primary key from the index
        rid_list = self.table.index.indices[config.PRIMARY_KEY_COLUMN][primary_key]
        if rid_list is None:
            return False
        rid = rid_list[0]

        # Locate the record in the page directory using the first RID (the most up to date version)
        page_range_num, base_page_num, record_num = self.table.page_directory[rid]
        # TODO: Loop through the columns of the base page and check if each physical page is in the bufferpool
        # TODO: If the page is not in the bufferpool, load it into the bufferpool
        # TODO: do we write to disk here or is this part of the bufferpool?
        base_page = self.table.page_ranges[page_range_num].base_pages[base_page_num]

        # Set the indirection, RID, and Timestam column of the base record to 0 to mark it as deleted
        zeroed = 0
        offset_number = record_num * config.VALUE_SIZE
        base_page.pages[config.RID_COLUMN].data[offset_number:offset_number + config.VALUE_SIZE] = zeroed.to_bytes(config.VALUE_SIZE, byteorder='little')
        base_page.pages[config.TIMESTAMP_COLUMN].data[offset_number:offset_number + config.VALUE_SIZE] = zeroed.to_bytes(config.VALUE_SIZE, byteorder='little')
        base_page.pages[config.INDIRECTION_COLUMN].data[offset_number:offset_number + config.VALUE_SIZE] = zeroed.to_bytes(config.VALUE_SIZE, byteorder='little')
        
        '''
        # Update the index to remove all traces of this record    
        '''
        schema_encoding = base_page.pages[config.SCHEMA_ENCODING_COLUMN].read(record_num)
        timestamp = base_page.pages[config.TIMESTAMP_COLUMN].read(record_num)
        indirection = base_page.pages[config.INDIRECTION_COLUMN].read(record_num)
        # TODO: ensure that base_rid_column is correct
        base_rid = base_page.pages[config.BASE_RID_COLUMN].read(record_num)

        self.table.index.bulk_index_remove(rid=rid, schema=schema_encoding, timestamp=timestamp, indirection=indirection, base_rid=base_rid)

        return True            
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # Validate column count and prevent insertion if column count does not match
        if len(columns) != self.table.num_columns:
            return False  
        
        # Get primary key value from record
        primary_key = columns[self.table.key]  
        if primary_key==None:
            return False

        # Check if primary key already exists
        if primary_key in self.table.index.indices[config.PRIMARY_KEY_COLUMN]:
            return False  # Prevent duplicate insertion
        
        # Generate metadata
        rid = self.rid_counter
        self.rid_counter += 1
        schema_encoding = int('0' * self.table.num_columns, 2) 
        timestamp = int(time())  
        indirection = 0 
        user_columns = list(columns)  # Exclude primary key from user data
        base_id = self.table.base_id.get(rid, rid)

        # Convert into a full record format
        record = [indirection, rid, timestamp, schema_encoding, base_id] + user_columns
        # Find available location to write to
        page_range_number, base_page_number, record_number = None, None, None
        

        # TODO: get the 0th column of the latest pagerange / pagegroup and put it into bufferpool
        latest_page_range = self.table.latest_page_range

        # Iterate over page ranges to find a base page with capacity
        for pr_num, page_range in enumerate(self.table.page_ranges):
            for bp_num, base_page in enumerate(page_range.base_pages):
                if base_page.has_capacity():
                    page_range_number, base_page_number = pr_num, bp_num
                    record_number = base_page.pages[0].num_records  # Use next available slot
                    break
            if page_range_number is not None:
                break

        # TODO: find the latest page range
            # TODO: find the latest base page

        # If no available space is found, create a new page range
        # Since base pages are created upon page range initialization, we only need to create a new page range
        if page_range_number is None:
            page_range_number = len(self.table.page_ranges)
            new_page_range = pageRange(num_columns=self.table.num_columns)  # Create a new page range
            # self.table.page_ranges.append(new_page_range)
            self.table.save_page_range(new_page_range)
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
        # TODO: update metadata (page_directory) of table

        # Update index
        self.table.index.bulk_index_add(rid=rid, schema=schema_encoding, timestamp=timestamp, indirection=indirection, base_rid=rid, columns=columns)

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
        records = []

        # If the search hey is the primary key, we can use the index to find the record
        if search_key_index == self.table.key:
            # Get the RID of the record
            rid_list = self.table.index.indices[config.PRIMARY_KEY_COLUMN][search_key]
            if rid_list is None:
                return False
            rid = rid_list[0]

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
                
                if first_traverse:
                    versions.insert(0, (tail_page, tail_record_num)) # update version
                    first_traverse = False
                else: 
                    versions.insert(2, (tail_page, tail_record_num)) # update version


                current_rid = tail_page.pages[config.INDIRECTION_COLUMN].read(tail_record_num)  # Move to the next older version
                
            # If the relative version is out of bounds, return the latest version
            # This was necassary to pass exam_tester_m1.py
            if len(versions) == abs(relative_version):
                relative_version += 1

            version_page, version_record_num = versions[relative_version]
            base_id = version_page.pages[config.BASE_ID_COLUMN].read(version_record_num)

            # Read the final/latest version of the record
            stored_values = [version_page.pages[i + 5].read(version_record_num) for i in range(self.table.num_columns)]
            # Apply column projection
            stored_primary_key = base_page.pages[config.PRIMARY_KEY_COLUMN].read(record_num)
            projected_values =  [
                    stored_values[i] if projected_columns_index[i] else None for i in range(self.table.num_columns)
                ]            
            records.append(Record(version_record_num, search_key, projected_values))
            # records.append(Record(search_key, search_key, projected_values))

        else:
            # TODO: We will be using this a lot of this milestone. Does it work? Should we update it?
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
                base_id = version_page.pages[config.BASE_ID_COLUMN].read(version_record_num)  
                stored_values = [version_page.pages[i + 5].read(version_record_num) for i in range(self.table.num_columns)]
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
        # Check the expected number of columns
        expected_columns = self.table.num_columns  # Number of user-defined columns (excluding metadata)
        
        if len(columns) != expected_columns:
            return False  # Early exit if column count is incorrect
        # need RID to get base and tail pages
        rid_list = self.table.index.indices[config.PRIMARY_KEY_COLUMN][primary_key]
        if rid_list is None:
            return False
        rid = rid_list[0]

        record = self.select(primary_key, 0, [1] * self.table.num_columns)

        if record is None: # no match -> update fails
            return False  

        

        # # Locate the record in the page directory using RID
        page_range_num, base_page_num, record_num = self.table.page_directory[rid]
        base_page = self.table.page_ranges[page_range_num].base_pages[base_page_num]
        base_page_indirection = base_page.pages[config.INDIRECTION_COLUMN].read(record_num) 

        stored_values = record[0].columns
        # Get the current values of the record

        updated_values = [columns[i] if columns[i] is not None else stored_values[i] for i in range(self.table.num_columns)]



        '''
        # remove old records from index
        '''
        old_schema_encoding = base_page.pages[config.SCHEMA_ENCODING_COLUMN].read(record_num)
        old_timestamp = base_page.pages[config.TIMESTAMP_COLUMN].read(record_num)
        old_indirection = base_page.pages[config.INDIRECTION_COLUMN].read(record_num)
        # TODO: ensure that base_rid_column is correct
        old_base_rid = base_page.pages[config.BASE_RID_COLUMN].read(record_num)
        self.table.index.bulk_index_remove(rid=rid, schema=old_schema_encoding, timestamp=old_timestamp, indirection=old_indirection, base_rid=old_base_rid)

        # Create a new version of the record
        new_rid = self.rid_counter
        self.rid_counter += 1
        timestamp = int(time())
        schema_encoding = int(''.join(['1' if col is not None else '0' for col in columns]), 2)

        indirection = base_page_indirection
        
        # base_id = self.table.base_id.get(rid, rid)  # Use base_id mapping, or fallback to the original rid
        base_id = self.table.base_id.get(rid, rid)

        # Convert into a full record format
        new_record = [indirection, new_rid, timestamp, schema_encoding,  base_id] + updated_values


        # Find available location to write the new version
        page_range = self.table.page_ranges[page_range_num]

        if not page_range.tail_pages:
            new_tail_page = PageGroup(num_columns=self.table.num_columns)
            self.table.save_tail_page(new_tail_page, page_range_num)
            # page_range.tail_pages.append(PageGroup(num_columns=self.table.num_columns))

        tailpage = page_range.tail_pages[-1]
        page = tailpage.pages[-1]
        # for page in tailpage.pages:
        if page.has_capacity()==False:
            new_tail_page = PageGroup(num_columns=self.table.num_columns)
            self.table.save_tail_page(new_tail_page, page_range_num)
            # page_range.tail_pages.append(PageGroup(num_columns=self.table.num_columns))

        # run this line again to ensure that you have the most up to date range, in case a new page range was created
        page_range = self.table.page_ranges[page_range_num]
        tail_page_group = page_range.tail_pages[-1]
        tail_record_num = tail_page_group.pages[-1].num_records
        # Write the new version
        tail_page_group.write(*new_record, record_number=tail_record_num)

        tail_page_group.pages[config.BASE_ID_COLUMN].write(base_id, tail_record_num)
        # Update page directory for the new version
        self.table.page_directory[new_rid] = (page_range_num, len(page_range.tail_pages) - 1, tail_record_num)
        if rid not in self.table.page_directory:
            page_range_num, base_page_num, record_num = self.table.page_directory[rid]  # Get base location
            self.table.page_directory[rid] = (page_range_num, base_page_num, record_num)

        self.table.page_directory[new_rid] = (page_range_num, len(self.table.page_ranges[page_range_num].tail_pages) - 1, tail_record_num)
        if isinstance(rid, int) and rid in self.table.page_directory: 
            self.table.page_directory[rid] = self.table.page_directory.get(rid, (None, None, None))  
            self.table.base_id[new_rid] = rid
        # Update the indirection column of the base record to point to the new version
        offset_number = record_num * config.VALUE_SIZE
        base_page.pages[config.INDIRECTION_COLUMN].data[offset_number:offset_number + config.VALUE_SIZE] = new_rid.to_bytes(config.VALUE_SIZE, byteorder='little')


        # Merge counter:
        self.table.merge_counter += 1  # Track number of updates
        if self.table.merge_counter >= config.UPDATES_BEFORE_MERGE:  # Trigger merge after default 512 updates
            self.table.start_merge_thread()
            self.table.merge_counter = 0  # Reset counter

        '''
        # add new values to index
        '''
        self.table.index.bulk_index_add(rid=new_rid, schema=schema_encoding, timestamp=timestamp, indirection=indirection, base_rid=rid, columns=updated_values)

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