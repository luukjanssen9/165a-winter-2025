"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from collections import defaultdict
from lstore import config

class Index:

    def __init__(self, table):
        # the index is a dictionary of dictionaries
        self.indices = defaultdict(lambda: None)
        # each dictionary in the index dictionary contains the index of its row
        self.indices[config.PRIMARY_KEY_COLUMN] = defaultdict(lambda: None)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if column not in self.indices:
            print(f"no index exists for column {column}")
            return False
        else:
            return self.indices[column][value]


    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        if self.indices[column] == None:
            print("error: this column is not indexed")
            return False

        if begin>end: # ensure that begin is smaller than end
            begin, end = end, begin

        rid_list = []
        for key in range(begin, end+1):
            value = self.indices[column][key] # grab the value from the index
            rid_list.append(value)
        
        return rid_list

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number] is not None:
            print(f"column with number {column_number} already exists")
            return False
        
        # create an index for that column number
        self.indices[column_number] = defaultdict(lambda: None)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        if column_number == config.PRIMARY_KEY_COLUMN:
            print("error: cant drop primary key index")
            return False
        else: 
            self.indices[column_number] = None
            return True