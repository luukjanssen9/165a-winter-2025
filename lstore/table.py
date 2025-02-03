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
        self.page_directory = {}
        self.index = Index(self)
        self.pages = []
        pass

    def __merge(self):
        print("merge is happening")
        pass

    def __insert(self, record):
        #TODO: find an available page or create a new one for each column
        for i in range(0, self.num_columns):
            page = self.__findAvailablePageOrCreateNewOne(record.columns[i])
            #TODO: write value to page
            page.write(record.columns[i])
            
        #TODO: update page directory
        # this means that if we give the rid to the page directory then it should be able to return all the pages for each individual column?       
        

        #TODO: update index
        pass

    def __findAvailablePageOrCreateNewOne(self, value):
        #TODO: find an available page or create a new one
        # pages are stored in self.pages
        # how do we find pages for a certain column?
        # do we loop through the values in the record and find pages associated with each value?
        # or do we loop through the columns and find pages associated with each column?

        # if there are no pages, create a new page
        page = Page()
        return page
        

    def __delete(self, key):
        # TODO: delete record from page

        # TODO: update page directory
        pass

    def __update(self, key, value):
        pass
 
