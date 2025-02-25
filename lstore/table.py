import os
from time import time
from lstore.index import Index
from lstore.page import Page
from lstore import config

INDIRECTION_COLUMN = 0 
RID_COLUMN = 1 
TIMESTAMP_COLUMN = 2 
SCHEMA_ENCODING_COLUMN = 3 

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    :param page_directory: dict #Dictionary of pages, returns page location of record given rid
    :param index: Index         #Index object for the table
    :param pages: list          #List of pages in the table
    """
    def __init__(self, name, path, num_columns, key, page_directory):
        self.name = name 
        self.path = path
        self.key = key
        self.num_columns = num_columns
        self.page_directory = page_directory # RID - > {page_range_number, base_page_number, record_number} 
        self.index = Index()
        self.page_ranges = []
        pass

    def __merge(self):
        print("merge is happening")
        pass

    def save_page_range(self, page_range):
        page_range_number = len(self.page_ranges)
        #0 index so the len gives the next number since we havent appended yet

        # check if table directory already exists
        if os.path.isdir(f"{self.path}/{page_range_number}"):
            print(f"error: A page range #{page_range_number} already exists")
            return None

        # create page range directory
        os.mkdir(f"{self.path}/{page_range_number}")

        # make folder with page_range name in db/table/
        for i in range(page_range.base_pages):
            # make folder with base page number (page_range.base_pages[i] is the i'th file)
            os.mkdir(f"{self.path}/{page_range_number}/b{i}")
            # TAIL PAGES: os.mkdir(f"{self.path}/{page_range_number}/t{i}")

            for j in range(page_range.base_pages[i].pages):
                # make file with column num (page_range.base_pages[i].pages[j] is the j'th file)
                path = f"{self.path}/{page_range_number}/b{i}"
                self.save_column(path=path, path_column=j, page=page_range.base_pages[i].pages[j])
                # TAIL PAGES: t{i}/col{j}
        self.page_ranges.append(page_range)

    def save_tail_page(self, tail_page, page_range_number):
        # ensure that page range exists
        if os.path.isdir(f"{self.path}/{page_range_number}")==False:
            print(f"error: Page range #{page_range_number} does not exist")
            return False
        
        num_folders = 0
        # count the number of tail pages that already exist by getting every folder inside the page range and subtracting the 16 base pages
        for dir in os.listdir(f"{self.path}/{page_range_number}"):
            if os.path.isdir(dir):
                num_folders += 1
        tail_page_number = num_folders-16 # remove 16 base pages

        os.mkdir(f"{self.path}/{page_range_number}/t{tail_page_number}")

        # create columns
        for i in range(tail_page.pages):
                path = f"{self.path}/{page_range_number}/t{tail_page_number}"
                self.save_column(path=path, path_column=i, page=tail_page.pages[i])
        # add the tail page to the page range in memory
        self.page_ranges[page_range_number].tail_pages.append(tail_page)

        # make sure to replace the `page_range.tail_pages.append()` with a call to this function. you can get page_range_number with `len(self.table.page_ranges)` before calling this func
        return True

    def save_column(self, path, path_column, page):
        file_path = f"{path}/col{path_column}" 
                             # ex: ./b{i}/col{j}
        metadata_path = f"{path}/met{path_column}"
                             # ex: ./b{i}/met{j}

        # write page.data to data file
        with open(file_path, 'wb+') as data_file:
            data_file.write(page.data)
            # data_file.close()

        # write page.num_records to metadata file
        with open(metadata_path, 'w+') as metadata_file:
            metadata_file.write(page.num_records)
            # metadara_file.close()
        
        # Create indices for all columns including the primary key
        for col in range(num_columns):
            self.index.create_index(col)

    def insert_record(self, rid, values):
        if len(values) != self.num_columns:
            raise ValueError("Invalid number of values")
        
        # Insert into table data structures (assuming some storage logic exists)
        record = Record(rid, values[self.key], values)
        self.page_directory[rid] = record  # Store record (simplified for this example)
        
        # Update indices for all indexed columns
        for col_index, value in enumerate(values):
            self.index.addRecord(value, rid, col_index)

    def delete_record(self, rid):
        if rid not in self.page_directory:
            print("Record not found")
            return False
        
        record = self.page_directory[rid]
        
        # Remove record from all indices
        for col_index, value in enumerate(record.columns):
            self.index.removeRecord(value, rid, col_index)
        
        # Remove record from storage
        del self.page_directory[rid]
        return True

    def update_record(self, rid, new_values):
        if rid not in self.page_directory:
            print("Record not found")
            return False
        
        record = self.page_directory[rid]
        
        # Update indices: Remove old values, add new ones
        for col_index, (old_value, new_value) in enumerate(zip(record.columns, new_values)):
            if old_value != new_value:
                self.index.removeRecord(old_value, rid, col_index)
                self.index.addRecord(new_value, rid, col_index)
        
        # Update record storage
        record.columns = new_values
        return True

    def locate(self, value, column):
        return self.index.locate(value, column)

    def locate_range(self, begin, end, column):
        return self.index.locate_range(begin, end, column)

