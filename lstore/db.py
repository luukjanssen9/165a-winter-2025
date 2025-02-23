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
            with open(f'{path}.json', 'r') as table_metadata:
                data = json.load(table_metadata)
            
            key = data["key"]
            num_columns = data["num_columns"]
            pagedir = data["page_directory"]
            page_directory = []
            for i in pagedir:
                # tbh idk what to do with these so im just leaving them here
                PR = pagedir[i]['page_range']
                BP = pagedir[i]['base_page']
                RN = pagedir[i]['record_number']
                page_directory.append("idk man something goes here") # TODO: fix this

            self.tables.append(Table(key=key, num_columns=num_columns, page_directory=page_directory)) # TODO: get metadata from disk

        # TODO: load page_directory into memory

        # create bufferpool
        self.bufferpool = Bufferpool(config.BUFFERPOOL_MAX_LENGTH) 
        return True

    def close(self):
        # TODO: Loop through bufferpool and write all dirty pages to disk

        # reset variables
        self.bufferpool = None
        self.isOpen = False
        self.path = None
        pass

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
            return
        
        # TODO: delete file for table with associated files
        for table in self.tables:
            if table.name==name:
                self.tables.remove(table)  
                return
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
       