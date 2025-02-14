"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from collections import defaultdict
from lstore import config

class Index:

    def __init__(self):
        # the index is a dictionary of dictionaries
        self.indices = defaultdict(lambda: None)
        # each dictionary in the index dictionary contains the index of its row
        self.indices[config.PRIMARY_KEY_COLUMN] = defaultdict(lambda: None)

    '''
    Add new record entry to index
    '''
    def addRecord(self, key, RID, key_col=config.PRIMARY_KEY_COLUMN):
        if key_col not in self.indices:
            print(f"no index exists for column {key_col}")
            return
        if key not in self.indices[key_col]:
            self.indices[key_col][key] = [RID]
        else:
            self.indices[key_col][key].append(RID)

    """
    # returns list of RIDs of all records with the given value on column "column"
    """
    def locate(self, key_val:int, key_col=config.PRIMARY_KEY_COLUMN):
        if key_col not in self.indices or self.indices[key_col] is None:
            print(f"no index exists for column {key_col}")
            return [] # [] ensures function always returns an iterable
        else:
            return self.indices[key_col].get(key_val, []) # If value is not found, return an empty list instead of None


    """
    # Returns list of RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin:int, end:int, key_col=config.PRIMARY_KEY_COLUMN):
        if self.indices == None:
            print("error: this column is not indexed")
            return []

        if begin > end: # ensure that begin is smaller than end
            begin, end = end, begin

        all_rids = []
        for key in range(begin, end+1):
            rids = self.indices[key_col].get(key, None)  # Use `.get()` to prevent `KeyError`
            if rids is not None:
                all_rids.extend(rids)
        
        return all_rids

    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        if self.indices[column_number] is not None:
            print(f"column with number {column_number} already exists")
            return False
        
        # create an index for that column number
        self.indices[column_number] = defaultdict(lambda: None)
        return True

    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
        if column_number == config.PRIMARY_KEY_COLUMN:
            print("error: cant drop primary key index")
            return False
        if self.indices[column_number] == None:
            return False
        self.indices[column_number] = None
        return True