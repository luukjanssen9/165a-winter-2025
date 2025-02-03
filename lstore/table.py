from lstore.index import Index
from time import time
from lstore.page import Page

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
    def __init__(self, name, num_columns, key):
        self.name = name 
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.pages = []
        pass

    def __merge(self):
        print("merge is happening")
        pass

    def __insert(self, record):
        #TODO: find an available page or create a new one
        #__findAvailablePageOrCreateNewOne
                
        #TODO: write record to page

        #TODO: update page directory

        #TODO: update index
        pass

    def __findAvailablePageOrCreateNewOne():
        pass

    def __delete(self, key):
        # TODO: delete record from page

        # TODO: update page directory
        pass

    def __update(self, key, value):
        pass
 
