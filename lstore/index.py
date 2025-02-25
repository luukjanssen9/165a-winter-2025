"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from collections import defaultdict
from lstore import config

class Index:
    def __init__(self):
        # Dictionary of dictionaries: using default dick instead of none
        self.indices = defaultdict(lambda: defaultdict(list))
        # Ensure primary key index is created
        self.create_index(config.PRIMARY_KEY_COLUMN)

    '''
    Add new record entry to index
    '''
    def addRecord(self, key, RID, key_col=config.PRIMARY_KEY_COLUMN):
        if key_col not in self.indices:
            print(f"no index exists for column {key_col}")
            return
        self.indices[key_col][key].append(RID)

    '''
    Remove a record entry from index (used when deleting or updating records)
    '''
    def removeRecord(self, key, RID, key_col=config.PRIMARY_KEY_COLUMN):
        if key_col not in self.indices:
            return
        if key in self.indices[key_col]:
            self.indices[key_col][key].remove(RID)
            if not self.indices[key_col][key]:  # Remove empty entries
                del self.indices[key_col][key]

    """
    Returns list of RIDs of all records with the given value on column "column"
    """
    def locate(self, key_val, key_col=config.PRIMARY_KEY_COLUMN):
        return self.indices[key_col].get(key_val, [])  # Return empty list if key is not found

    """
    Returns list of RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, key_col=config.PRIMARY_KEY_COLUMN):
        if key_col not in self.indices:
            return []

        if begin > end:
            begin, end = end, begin

        all_rids = []
        for key in range(begin, end + 1):
            all_rids.extend(self.indices[key_col].get(key, []))
        
        return all_rids

    """
    Create index on specific column and populate it with existing data
    """
    def create_index(self, column_number, table=None):
        if column_number in self.indices:
            print(f"Index already exists for column {column_number}")
            return False
        
        self.indices[column_number] = defaultdict(list)

        # scan table reference to populate the index
        if table:
            for rid, record in table.records.items():  # I assume table.records stores data
                self.addRecord(record[column_number], rid, column_number)
        
        return True

    """
    Drop index of a specific column
    """
    def drop_index(self, column_number):
        if column_number == config.PRIMARY_KEY_COLUMN:
            print("Error: Can't drop primary key index")
            return False
        if column_number not in self.indices:
            return False
        del self.indices[column_number]
        return True
