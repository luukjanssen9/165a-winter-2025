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
        # TODO: Loop through bufferpool and write all dirty pages to disk
        # TODO: write all table and page range metadata to disk

        # reset variables
        self.bufferpool = None
        self.isOpen = False
        self.path = None
        pass # TODO: should return True if successful

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        # check if database is open
        if not self.isOpen:
            print("error: Database is not open")
            return None
        # check if table directory already exists
        if os.path.isdir(f"{self.path}/{name}"):
            print(f"error: A table with the name \"{name}\" already exists")
            return None

        # create table directory
        os.mkdir(f"{self.path}/{name}")

        # TODO: Save table metadata to disk


        # create table object
        newTable = Table(name, num_columns, key_index)
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
        key = data["key"]
        num_columns = data["num_columns"]
        json_page_dir = data["page_directory"]  

        # properly format the page_dir using the json data
        page_directory = {}
        for i in json_page_dir:
            page_directory[i] = (json_page_dir[i]['page_range'], json_page_dir[i]['base_page'], json_page_dir[i]['record_number'])
        
        # create the table with the data from disk, and add it to memory
        new_table = Table(key=key, num_columns=num_columns, page_directory=page_directory)
        self.tables.append(new_table)
        return new_table