import os
from time import time
from lstore.index import Index
from lstore.page import Page

INDIRECTION_COLUMN = 0 # Each record also includes an indirection column that points to the latest tail record holding the latest update to the record
RID_COLUMN = 1 # Each record is assigned a unique identier called an RID, which is often the physical location where the record is actually stored.
TIMESTAMP_COLUMN = 2 # Not sure what this is for ?
SCHEMA_ENCODING_COLUMN = 3 # This is a bit vector with one bit per column that stores information about the updated state of each column


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

            # 
            # CREATE COLUMN FILES LAZILY!!! MORE EFFICIENT THAT WAY
            # 

            # for j in range(page_range.base_pages[i].pages):
            #     # make file with column num (page_range.base_pages[i].pages[j] is the j'th file)
            
            #     # wouldnt be mkdir, would me a w+ open probably
            #     os.mkdir(f"{self.path}/{page_range_number}/b{i}/col{j}")
            #     # TAIL PAGES: os.mkdir(f"{self.path}/{page_range_number}/t{i}/col{j}")
            
            #     # create metadata files for all columns
        self.page_ranges.append(page_range)