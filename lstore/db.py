from lstore.table import Table

class Database():

    def __init__(self):
        self.tables = []
        pass

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        if table.name not in self.tables:
            self.tables.append(table)
            return table
        else: print(f"error: A table with the name \"{table.name}\" already exists")

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for table in self.tables:
            if table.name==name:
                del table
    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if table.name==name:
                return table
