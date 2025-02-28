from lstore.table import Table
from lstore.bufferpool import Bufferpool
from lstore import config
import os
import json

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

        # initialize tables into memory
        for table in os.listdir(path):
            if os.path.isdir(table):
                self.get_table(path)

        # create bufferpool
        self.bufferpool = Bufferpool(config.BUFFERPOOL_MAX_LENGTH) 
        return True

    def close(self):
        # TODO: do we have to check if the database is open before closing it?
        if not self.isOpen:
            print("error: Database is not open")
            return False

        
        # loop through bufferpool and write dirty pages to disk
        for frameNum in range(self.bufferpool.frames):
            if self.bufferpool[frameNum].dirty:
                self.bufferpool.writeToDisk(frameNum) # TODO: idk if this syntax is correct.
        
        # Close all tables (write metadata to disk)
        for table in self.tables:
            self.close_table(table)

        # reset variables
        self.bufferpool = None
        self.isOpen = False
        self.path = None
        return True

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
        newTable = Table(name=name, path=newpath, num_columns=num_columns, page_directory=key_index, latest_page_range=None)
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
        for table in self.tables:
            if table.name==name:
                # check if file exists
                if os.path.isdir(f"{self.path}/{name}"):
                    # check if directory is empty
                    if not os.listdir(f"{self.path}/{name}"):
                        # loop through all files in the directory and remove them
                        for file in os.listdir(f"{self.path}/{name}"):
                            os.remove(f"{self.path}/{name}/{file}")
                        os.rmdir(f"{self.path}/{name}")
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
        with open(f'{self.path}/{name}.json', 'r') as table_metadata:
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
        
        # create the table with the data from disk, and add it to memory
        new_table = Table(name=name, path=path, key=key, num_columns=num_columns, page_directory=page_directory, latest_page_range=latest_page_range)
        new_table.open_page_ranges() # goes through everything inside and reads to memory
        self.tables.append(new_table)
        return new_table
    
    """
    # Close table with the passed name after writing its metadata to disk
    """
    def close_table(self, table):
        if self.write_table_metadata(table):
            # remove table from db
            self.tables.remove(table.name)
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
        for i in table.page_directory:
            json_page_dir[i] = {
                    "page_range" : table.page_directory[i]['page_range'],
                    "base_page" : table.page_directory[i]['base_page'],
                    "record_number" : table.page_directory[i]['record_number']
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

        # write metadata to disk
        with open(f'{self.path}/{table.name}.json', 'w+') as json_output_file:
            # if 0 bytes are written then it failed
            if json_output_file.write(table_metadata)==0:
                return False

        # write metadata for page ranges
        for i in table.page_ranges:
            page_range_metadata = {
                "latest_base_page" : table.page_ranges[i].latest_base_page,
                "latest_tail_page" : table.page_ranges[i].latest_tail_page,
                "num_columns" : table.page_ranges[i].num_columns
            }

            # save the page range's metadata first
            with open(f'{self.path}/{table.name}/{i}.json', 'w+') as page_range_output_file:
                # if 0 bytes are written then it failed
                if page_range_output_file.write(page_range_metadata)==0:
                    return False
                
            # then save all the page groups inside the page range
            # base pages
            for j in table.page_ranges[i].base_pages:
                base_page_metadata = {
                    "latest_record_number" : table.page_ranges[i].base_pages[j].latest_record_number
                }

                with open(f'{self.path}/{table.name}/{i}/b{j}.json', 'w+') as base_page_output_file:
                    # if 0 bytes are written then it failed
                    if base_page_output_file.write(base_page_metadata)==0:
                        return False
                    
            # tail pages
            for k in table.page_ranges[i].tail_pages:
                tail_page_metadata = {
                    "latest_record_number" : table.page_ranges[i].tail_pages[k].latest_record_number
                }

                with open(f'{self.path}/{table.name}/{i}/t{k}.json', 'w+') as tail_page_output_file:
                    # if 0 bytes are written then it failed
                    if tail_page_output_file.write(tail_page_metadata)==0:
                        return False

