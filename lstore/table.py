import os
import json
from time import time
from lstore.page import pageRange, PageGroup
from lstore.index import Index
from lstore.page import Page
from lstore import config
from lstore.bufferpool import Bufferpool

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
    def __init__(self, name, path, num_columns, key, page_directory, latest_page_range, bufferpool):
        self.name = name 
        self.path = path
        self.key = key
        self.num_columns = num_columns
        self.page_directory = page_directory # RID - > {page_range_number, base_page_number, record_number} 
        self.index = Index()
        self.page_ranges = []
        self.latest_page_range = latest_page_range
        self.bufferpool = bufferpool
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
                # TODO: should the following two lines be in the first for loop?
        self.page_ranges.append(page_range)
        self.latest_page_range+=1

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
        if self.page_ranges[page_range_number].latest_tail_page==None:
            self.page_ranges[page_range_number].latest_tail_page = 0
        else: self.page_ranges[page_range_number].latest_tail_page += 1

        # make sure to replace the `page_range.tail_pages.append()` with a call to this function. you can get page_range_number with `len(self.table.page_ranges)` before calling this func
        return True

    def save_column(self, path, path_column, page):
        file_path = f"{path}/col{path_column}.bin" 
        metadata_path = f"{path}/col{path_column}.json"

        # write page.data to data file
        with open(file_path, 'wb+') as data_file:
            data_file.write(page.data)
            # data_file.close()

        # turn metadata to json
        phys_page_metadata = {
            "num_records" : page.num_records
        }

        # write to metadata file
        with open(metadata_path, 'w+') as metadata_file:
            metadata_file.write(phys_page_metadata)
            # metadara_file.close()

    def open_page_ranges(self):
        
        # use os to see all page ranges in dir
        currdir = self.path
        range_pairs = []
        for dir in os.listdir(currdir):
            if dir.isdigit():  # Check if the entry is a number (folder name)
                folder_path = os.path.join(currdir, dir)
                json_path = os.path.join(currdir, f"{dir}.json")

                if os.path.isdir(folder_path) and os.path.isfile(json_path):
                    range_pairs.append((folder_path, json_path))

                # order pairs numerically
                range_pairs.sort(key=lambda x: int(os.path.basename(x[0])))

                # iterate through the pairs
                for path_folder, path_json in range_pairs:
                    # inside each page range, read the metadata
                    with open(path_json, "r") as range_metadata:
                        data = json.load(range_metadata)
                    
                    # get data from json
                    num_columns = data["num_columns"]
                    latest_bp = data["latest_base_page"]
                    latest_tp = data["latest_tail_page"]

                    # use the data to create a page range object
                    new_range = pageRange(num_columns=num_columns, latest_bp=latest_bp, latest_tp=latest_tp, new=False)
                    # then go into the page range folder and use os to see all base and tail pages
                    self.open_page_groups(new_range, path_folder)
                    # add page range to table
                    self.page_ranges.append(new_range)

    def open_page_groups(self, page_range, path_folder):
        base_pairs = []
        tail_pairs = []

        for dir in os.listdir(path_folder):
            if dir.startswith("b") and dir[1:].isdigit():  # Check if base
                num = int(dir[1:])  # get number
                folder_path = os.path.join(path_folder, dir)
                json_path = os.path.join(path_folder, f"b{num}.json")
                if os.path.isdir(folder_path) and os.path.isfile(json_path):
                    base_pairs.append((folder_path, json_path))

            elif dir.startswith("t") and dir[1:].isdigit():  # Check if tail
                num = int(dir[1:])  # get number
                folder_path = os.path.join(path_folder, dir)
                json_path = os.path.join(path_folder, f"t{num}.json")
                if os.path.isdir(folder_path) and os.path.isfile(json_path):
                    tail_pairs.append((folder_path, json_path))

        # order pairs numerically
        base_pairs.sort(key=lambda x: int(os.path.basename(x[0])[1:]))
        tail_pairs.sort(key=lambda x: int(os.path.basename(x[0])[1:]))
        
        # iterate through the base pairs
        for path_folder, path_json in base_pairs:
            # read the metadata, use it to create page group objects
            with open(path_json, "r") as base_page_metadata:
                data = json.load(base_page_metadata)
            
            # get data from json
            latest_record_number = data["latest_record_number"]

            # use the data to create a page group object
            new_base_page = PageGroup(num_columns=page_range.num_columns, type=config.BASE_PAGE, latest_record_number=latest_record_number)
            # then go into base page folder and use os to see all phys pages
            self.open_phys_pages(new_base_page, path_folder)
            # add base page to table
            page_range.base_pages.append(new_base_page)

        for path_folder, path_json in tail_pairs:
            # read the metadata, use it to create page group objects
            with open(path_json, "r") as tail_page_metadata:
                data = json.load(tail_page_metadata)
            
            # get data from json
            latest_record_number = data["latest_record_number"]

            # use the data to create a page group object
            new_tail_page = PageGroup(num_columns=page_range.num_columns, type=config.TAIL_PAGE, latest_record_number=latest_record_number)
            
            # TODO: i think bringing the phys pages to memory goes against the entire point of the buffer pool. the metadata that the page has isnt needed either, as the page groups already track what their next open page is.
            # # then go into tail page folder and use os to see all phys pages
            # self.open_phys_pages(new_tail_page, path_folder)
            
            # add base page to table
            page_range.tail_pages.append(new_tail_page)

    # def open_phys_pages(self, page_group, path_folder):
    #     # use os to see all phys page in dir
    #     page_pairs = []
    #     for dir in os.listdir(path_folder):
    #         if dir.startswith("col") and dir[3:].isdigit():  # Check if column file
    #             bin_path = os.path.join(path_folder, f"col{dir}.bin")
    #             json_path = os.path.join(path_folder, f"col{dir}.json")

    #             if os.path.isfile(bin_path) and os.path.isfile(json_path):
    #                 page_pairs.append((bin_path, json_path))

    #             # order pairs numerically
    #             page_pairs.sort(key=lambda x: int(os.path.basename(x[0])))

    #             # iterate through the pairs
    #             for path_folder, path_json in page_pairs:
    #                 # inside each page range, read the metadata
    #                 with open(path_json, "r") as page_metadata:
    #                     data = json.load(page_metadata)
                    
    #                 # get data from json
    #                 num_records = data["num_records"]

    #                 # use the data to create a page range object
    #                 new_page = Page(num_records=num_records)
    #                 # add page to page group
    #                 page_group.append(new_page)