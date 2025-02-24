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

        # TODO: Save table metadata to disk
        # idk if we need this since we already write to disk on close
        # self.write_table_metadata(table)

        # create table object
        newTable = Table(name=name, path=newpath, num_columns=num_columns, page_directory=key_index)
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

        # properly format the page_dir using the json data
        page_directory = {}
        for i in json_page_dir:
            page_directory[i] = (json_page_dir[i]['page_range'], json_page_dir[i]['base_page'], json_page_dir[i]['record_number'])
        
        # create the table with the data from disk, and add it to memory
        new_table = Table(name=name, path=path, key=key, num_columns=num_columns, page_directory=page_directory)
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
            "page_directory" : json_page_dir
        }

        # write metadata to disk
        with open(f'{self.path}/{table.name}.json', 'w+') as json_output_file:
            # if 0 bytes are written then it failed
            if json_output_file.write(table_metadata)==0:
                return False
            # successful because something was written
            else: return True