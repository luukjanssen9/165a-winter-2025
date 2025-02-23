from lstore.table import Table
from lstore.bufferpool import Bufferpool
from lstore import config
import os

class Database():

    def __init__(self):
        self.tables = []
        self.bufferpool = None
        self.isOpen = False
        self.path = None
        

    # Not required for milestone1
    def open(self, path):
        # create directory for database or if it already exists, open it
        if not os.path.exists(path):
            os.mkdir(path)
        
        # Save the path
        self.path = path

        # create bufferpool
        self.bufferpool = Bufferpool(config.BUFFERPOOL_MAX_LENGTH) 
        pass

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
        # TODO: create directory for table with associated files
        # check if table directory already exists
        if os.path.isdir(f"{self.path}/{name}"):
            print(f"error: A table with the name \"{name}\" already exists")
            return None

        # create table directory
        os.mkdir(f"{self.path}/{name}")

        # TODO: what do we do with table object?
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
        # TODO: return file with the specified name 
        for table in self.tables:
            if table.name==name:
                return table
        print(f"error: No table with the name \"{name}\" exists")