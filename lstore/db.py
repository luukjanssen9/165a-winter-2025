from lstore.table import Table
from lstore.bufferpool import Bufferpool

class Database():

    def __init__(self):
        self.tables = []
        self.bufferpool = Bufferpool()

    # Not required for milestone1
    def open(self, path):
        # create bufferpool
        pass

    def close(self):
        # remove bufferpool, write anything thats dirty
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
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
        for table in self.tables:
            if table.name==name:
                self.tables.remove(table)  
                return
        print(f"error: No table with the name \"{name}\" exists")
    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if table.name==name:
                return table
        print(f"error: No table with the name \"{name}\" exists")