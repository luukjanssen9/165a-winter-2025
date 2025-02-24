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

            for j in range(page_range.base_pages[i].pages):
                # make file with column num (page_range.base_pages[i].pages[j] is the j'th file)
                path = f"{self.path}/{page_range_number}/b{i}"
                path_offset = f"col{j}"
                self.save_column(path=path, path_offset=path_offset, page=page_range.base_pages[i].pages[j])
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
                path_offset = f"col{i}"
                self.save_column(path=path, path_offset=path_offset, page=tail_page.pages[i])
        # add the tail page to the page range in memory
        self.page_ranges[page_range_number].tail_pages.append(tail_page)

        # make sure to replace the `page_range.tail_pages.append()` with a call to this function. you can get page_range_number with `len(self.table.page_ranges)` before calling this func
        return True

    def save_column(self, path, path_offset, page):
        file_path = f"{path}/{path_offset}" 
                             # ex: ./b{i}/col{j}
        metadata_path = path # ex: ./b{i}/

        # TODO: implement physical page saving by putting the full binary data into a file
        # should be a simple write to the path. the problem is how to write the page's binary data.
        # we might as well write the metadata while we're here, but i dont think we need to
        # the metadata should be a simple json that contains nothing but page.num_columns.
        # metadata file goes to the metadata_path i created above. the actual file goes to file_path.
        pass