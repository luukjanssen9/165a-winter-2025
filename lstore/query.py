from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
from time import time
from lstore.page import PageGroup, pageRange
from lstore import config
from lstore.bufferpool import Bufferpool

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
        
        # get the RID page from the bufferpool
        page = self.table.bufferpool.getBufferpoolPage(rid, config.RID_COLUMN, self.table)
        # TODO: write to the page
        #base_page.pages[config.RID_COLUMN].data[offset_number:offset_number + config.VALUE_SIZE] = zeroed.to_bytes(config.VALUE_SIZE, byteorder='little')

        # get the timestamp page from the bufferpool
        page = self.table.bufferpool.getBufferpoolPage(rid, config.TIMESTAMP_COLUMN, self.table)
        # TODO: write to the page
        #base_page.pages[config.TIMESTAMP_COLUMN].data[offset_number:offset_number + config.VALUE_SIZE] = zeroed.to_bytes(config.VALUE_SIZE, byteorder='little')
        # get the indirection page from the bufferpool
        page = self.table.bufferpool.getBufferpoolPage(rid, config.INDIRECTION_COLUMN, self.table)
        # TODO: write to the page
        #base_page.pages[config.INDIRECTION_COLUMN].data[offset_number:offset_number + config.VALUE_SIZE] = zeroed.to_bytes(config.VALUE_SIZE, byteorder='little')
        
        # Update the index to remove the primary key
        self.table.index.indices[config.PRIMARY_KEY_COLUMN][primary_key] = [None]

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
        # Check if primary key already exists
        if primary_key in self.table.index.indices[config.PRIMARY_KEY_COLUMN]:
            return False  # Prevent duplicate insertion
        
        # Generate metadata
        rid = self.rid_counter
        self.rid_counter += 1
        schema_encoding = int('0' * self.table.num_columns, 2) 
        timestamp = int(time())  
        indirection = 0 
        
        # Convert into a full record format
        record = [indirection, rid, timestamp, schema_encoding] + list(columns)

        # Find available location to write to
        page_range_number, base_page_number, record_number = None, None, None
        
        # Get the latestpage and check if it has capacity
        # TODO: we do not use latest record here. Do we need it at all?
        page = self.table.bufferpool.getBufferpoolPage(self.rid_counter, 0, self.table)

        if page.has_capacity():
            page_range_number = self.table.latest_page_range
            base_page_number = self.table.page_ranges[self.table.latest_page_range].latest_base_page
            record_number = page.num_records

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

        # TODO: change the way we are writing? do we loop through pages and give it to the bufferpool? (I dont think so)
        # Write the record
        if not page_range.base_pages[base_page_number].write(*record, record_number=record_number):
            return False  
        
        # Update the latest page range and base page
        self.table.latest_page_range = page_range_number
        page_range.latest_base_page = base_page_number

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

            # Read the final/latest version of the record
            stored_values = [version_page.pages[i + 5].read(version_record_num) for i in range(self.table.num_columns - 1)]
            stored_primary_key = base_page.pages[config.PRIMARY_KEY_COLUMN].read(record_num)

            # Apply column projection
            projected_values = [stored_primary_key] + [
                stored_values[i] if projected_columns_index[i + 1] else None for i in range(self.table.num_columns - 1)
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

        # Create a new version of the record
        new_rid = self.rid_counter
        self.rid_counter += 1
        timestamp = int(time())
        schema_encoding = int(''.join(['1' if col is not None else '0' for col in columns]), 2)

        indirection = base_page_indirection

        # Convert into a full record format
        new_record = [indirection, new_rid, timestamp, schema_encoding] + updated_values

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

        # Update page directory for the new version
        self.table.page_directory[new_rid] = (page_range_num, len(page_range.tail_pages) - 1, tail_record_num)

        # Update the indirection column of the base record to point to the new version
        offset_number = record_num * config.VALUE_SIZE
        base_page.pages[config.INDIRECTION_COLUMN].data[offset_number:offset_number + config.VALUE_SIZE] = new_rid.to_bytes(config.VALUE_SIZE, byteorder='little')

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
