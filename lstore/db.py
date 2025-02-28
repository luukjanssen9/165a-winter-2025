from lstore.table import Table
from lstore.bufferpool import Bufferpool
from lstore import config
import os
import json
import shutil
import threading
import time 
import pickle

class Database():

    def __init__(self):
        self.tables = []
        self.bufferpool = None
        self.isOpen = False
        self.path = None
        

    # Not required for milestone1
    def open(self, path):
        # check if database is already open
        if self.isOpen:
            print("error: Database is already open")
            return False
        
        # create directory for database or if it already exists, open it
        if not os.path.exists(path):
            os.mkdir(path)
        
        # Save the path
        self.path = path
        self.isOpen = True

        # initialize tables into memory
        for table in os.listdir(path):
            full_table_path = os.path.join(path, table) 
            if os.path.isdir(full_table_path):
                self.get_table(table)

        # create bufferpool
        self.bufferpool = Bufferpool(config.BUFFERPOOL_MAX_LENGTH) 

        # Start background merge process
        self.merge_thread = threading.Thread(target=self.run_merge, daemon=True)
        self.merge_thread.start()
        print("Merge thread has started!") 
        return True

    def close(self):
        if not self.isOpen:
            print("error: Database is not open")
            # return true because the database is closed and thats what we're checking.
            return True

        
        # loop through bufferpool and write dirty pages to disk

        for frameNum in range(len(self.bufferpool.frames)):
            if self.bufferpool.frames[frameNum] and self.bufferpool.frames[frameNum].dirty: 
                self.bufferpool.writeToDisk(frameNum) # TODO: idk if this syntax is correct.
        
        # Close all tables (write metadata to disk)
        for table in self.tables:
            self.close_table(table)

        # reset variables
        self.bufferpool = None
        self.isOpen = False
        self.path = None
        if self.merge_thread:
            self.merge_thread.join()  # Stop merge thread before closing
        return True

    def start_merge_thread(self):
        """Starts the background merge thread."""
        if not self.merge_thread:
            self.merge_thread = threading.Thread(target=self.run_merge, daemon=True)
            self.merge_thread.start()

    def run_merge(self):
        """Periodically calls merge to optimize database performance."""
        while self.isOpen:
            time.sleep(5)  # Adjust the interval as needed
            for table in self.tables:
                table.__merge()  # Call merge inside tables
    """
    # Creates a new table
    :param name: string         #Table name
    :param path: string         #Path of directory
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        # check if database is open
        if not self.isOpen:
            print("error: Database is not open")
            return None
        
        # define path
        newpath = f"{self.path}/{name}" 
        
        # check if table directory already exists
        if os.path.isdir(newpath):
            print(f"error: A table with the name \"{name}\" already exists")
            return None

        # create table directory
        os.mkdir(newpath)

        # create table object
        newTable = Table(name=name, path=newpath, num_columns=num_columns, key=key_index,page_directory={}, latest_page_range=None)
        newTable.bufferpool = self.bufferpool
        for table in self.tables:
            if table.name == newTable.name:
                print(f"error: A table with the name \"{table.name}\" already exists")
                return None
        self.tables.append(newTable)
        return newTable

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        # check if database is open
        if not self.isOpen:
            print("error: Database is not open")
            return False
        
        # TODO: delete file for table with associated files
        # do this by using os to find the table.path for the specified table, then deleting the entire folder associated with it.
        for table in self.tables:
            if table.name==name:
                # check if directory exists and delete it if so
                if os.path.isdir(f"{self.path}/{name}"):
                    shutil.rmtree(f"{self.path}/{name}")
                self.tables.remove(table)  
                return True
        # if you get this far without returning, then the table doesnt exist
        print(f"error: No table with the name \"{name}\" exists")
    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        # check if database is open
        if not self.isOpen:
            print("error: Database is not open")
            return None
        # check if table directory exists
        if not os.path.isdir(f"{self.path}/{name}"):
            # if it doesn't exist, there is an error
            print(f"error: No table with the name \"{name}\" exists")
            return None
        # return table object
        for table in self.tables:
            if table.name==name:
                return table
        # if you get this far, then the table isnt in memory yet. 
        with open(f'{self.path}/{name}.json', 'r+') as table_metadata:
            data = json.load(table_metadata)
            
        # get data from the json
        name = data["name"]
        path = data["path"]
        key = data["key"]
        num_columns = data["num_columns"]
        json_page_dir = data["page_directory"] 
        latest_page_range = data["latest_page_range"] 

        # properly format the page_dir using the json data
        page_directory = {}
        for i in json_page_dir:
            page_directory[i] = (json_page_dir[i]['page_range'], json_page_dir[i]['base_page'], json_page_dir[i]['record_number'])
        

        # get index from disk
        new_index = self.load_index(name)

        if new_index!=None:
            # create the table with the data from disk, and add it to memory
            new_table = Table(name=name, path=path, key=key, num_columns=num_columns, page_directory=page_directory, latest_page_range=latest_page_range, index=new_index)
        else: 
            # if there was no index found, just create a new one by falling back to the default value which creates a new index for this table
            new_table = Table(name=name, path=path, key=key, num_columns=num_columns, page_directory=page_directory, latest_page_range=latest_page_range)
        new_table.bufferpool = self.bufferpool  # Attach bufferpool to table
        new_table.open_page_ranges() # goes through everything inside and reads to memory
        self.tables.append(new_table)
        return new_table
    
    """
    # Close table with the passed name after writing its metadata to disk
    """
    def close_table(self, table):
        if self.write_table_metadata(table):
            # remove table from db
            self.tables.remove(table)
            return True
        else:
            print("writing table to disk failed")
            return False

    """
    # Write table metadata to disk
    """
    def write_table_metadata(self, table):
         # get page directory dict
        json_page_dir = {}
        for rid, (page_range, base_page, record_number) in table.page_directory.items(): 
            json_page_dir[rid] = {
            "page_range": page_range,
            "base_page": base_page,
            "record_number": record_number
        }
        
        # create metadata dict
        table_metadata = {
            "name" : table.name,
            "path" : table.path,
            "key" : table.key,
            "num_columns" : table.num_columns,
            "page_directory" : json_page_dir,
            "latest_page_range" : table.latest_page_range
        }
        table_metadata_path = f'{self.path}/{table.name}.json'


        # write metadata to disk
        with open(table_metadata_path, 'w') as json_output_file:
            json.dump(table_metadata, json_output_file, indent=4)
        if os.path.getsize(table_metadata_path) == 0:
            print(f"error: Failed to write table metadata for {table.name}")
            return False

        # write table's index to disk
        self.save_index(table)

        # write metadata for page ranges
        for i, page_range in enumerate(table.page_ranges):  
            page_range_metadata = {
                "latest_base_page": page_range.latest_base_page,  
                "latest_tail_page": page_range.latest_tail_page,
                "num_columns": page_range.num_columns
            }       
            page_range_path = f'{self.path}/{table.name}/{i}.json'

            # save the page range's metadata first
            with open(page_range_path, 'w') as page_range_output_file:
                json.dump(page_range_metadata, page_range_output_file, indent=4)
            
            # if 0 bytes are written then it failed
            if os.path.getsize(page_range_path) == 0:
                print(f"error: Failed to write page range metadata for {table.name}, range {i}")
                return False
                
            # then save all the page groups inside the page range
            # base pages
            for j, base_page in enumerate(page_range.base_pages):
                base_page_metadata = {"latest_record_number": base_page.latest_record_number}
                base_page_path = f'{self.path}/{table.name}/{i}/b{j}.json'

                with open(base_page_path, 'w') as base_page_output_file:
                    json.dump(base_page_metadata, base_page_output_file, indent=4)

                if os.path.getsize(base_page_path) == 0: 
                    print(f"error: Failed to write base page metadata for {table.name}, range {i}, base {j}")
                    return False
            # tail pages
            for k, tail_page in enumerate(page_range.tail_pages):
                tail_page_metadata = {"latest_record_number": tail_page.latest_record_number}

                tail_page_path = f'{self.path}/{table.name}/{i}/t{k}.json'

                with open(tail_page_path, 'w') as tail_page_output_file:
                    json.dump(tail_page_metadata, tail_page_output_file, indent=4)

                if os.path.getsize(tail_page_path) == 0:
                    print(f"error: Failed to write tail page metadata for {table.name}, range {i}, tail {k}")
                    return False
        return True

    """
    # save and load table indices
    """
    # save index to disk using pickle
    def save_index(self, table):
        index_path = f"{self.path}/{table.name}.index"
        with open(index_path, "wb+") as f:
            pickle.dump(table.index, f)

    # load index from disk using pickle
    def load_index(self, table_name):
        index_path = f"{self.path}/{table_name}.index"
        if os.path.exists(index_path):
            with open(index_path, "rb") as f:
                new_index = pickle.load(f)
                return new_index
        else:
            print(f"WARNING: Index does not exist for Table \"{table_name}\"")
            return None
