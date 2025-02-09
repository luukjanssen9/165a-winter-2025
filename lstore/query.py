from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
from time import time
from lstore.page import PageGroup, pageRange


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        self.rid_counter = 1 # counter to keep track of the number of records in the table
        

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        # TODO: delete record from page

        # TODO: update page directory
        pass
    
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
        for rid, (page_range_num, base_page_num, record_num) in self.table.page_directory.items():
            stored_primary_key = self.table.page_ranges[page_range_num].base_pages[base_page_num].pages[4].read(record_num)
            if stored_primary_key == primary_key:
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
                print(f"Checking capacity for Page Range {pr_num}, Base Page {bp_num}: {base_page.has_capacity()}")
                if base_page.has_capacity():
                    page_range_number, base_page_number = pr_num, bp_num
                    record_number = base_page.pages[0].num_records  # Use next available slot
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

        # TODO: Implement indexing
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
        pass

    
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
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        pass

    
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
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
