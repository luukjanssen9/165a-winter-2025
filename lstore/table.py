from lstore.index import Index
from time import time
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
    def __init__(self, name, num_columns, key):
        self.name = name 
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {} # RID - > {page_range_number, base_page_number, record_number} 
        self.index = Index(self)
        self.page_ranges = []
        pass

    def __merge(self):
        print("merge is happening")
        pass

