"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from collections import defaultdict
import pickle
import os
from sortedcontainers import SortedDict  # better for range queries
from lstore import config

class Index:
    def __init__(self, table_name):
        self.indices = defaultdict(lambda: SortedDict())  # Changed from defaultdict(list) to SortedDict
        self.table_name = table_name
        self.index_file = f"{table_name}_index.pkl"
        
        # Load index from disk if it exists
        self.load_index()

    '''
    Add new record entry to index
    '''
    def addRecord(self, key, rid, key_col=config.PRIMARY_KEY_COLUMN):
        if key_col not in self.indices:
            print(f"Warning: No index exists for column {key_col}. Skipping index update.")
            return
        if key in self.indices[key_col]:
            self.indices[key_col][key].append(rid)
        else:
            self.indices[key_col][key] = [rid]

    '''
    Remove a record entry from index
    '''
    def removeRecord(self, key, rid, key_col=config.PRIMARY_KEY_COLUMN):
        if key_col not in self.indices:
            return
        if key in self.indices[key_col]:
            self.indices[key_col][key].remove(rid)
            if not self.indices[key_col][key]:  # Remove empty lists
                del self.indices[key_col][key]

    '''
    Update a record's index when the value changes
    '''
    def updateRecord(self, old_key, new_key, rid, key_col=config.PRIMARY_KEY_COLUMN):
        self.removeRecord(old_key, rid, key_col)
        self.addRecord(new_key, rid, key_col)

    '''
    Locate RIDs with exact value
    '''
    def locate(self, key_val, key_col=config.PRIMARY_KEY_COLUMN):
        return self.indices[key_col].get(key_val, [])

    '''
    Locate RIDs within a range
    '''
    def locate_range(self, begin, end, key_col=config.PRIMARY_KEY_COLUMN):
        if key_col not in self.indices:
            return []
        return [rid for key in self.indices[key_col].irange(begin, end) for rid in self.indices[key_col][key]]

    '''
    Create index on a specific column
    '''
    def create_index(self, column_number, table=None):
        if column_number in self.indices:
            print(f"Index already exists for column {column_number}")
            return False
        self.indices[column_number] = SortedDict()
    
        if table:
            for rid in table.page_directory.keys():  # Iterates over all RIDs
                value = table.getBufferpoolPage(rid, column_number, table)  # Fetches value
                if value is not None:
                    self.addRecord(value, rid, column_number)
    
        self.save_index()
        return True


    '''
    Drop index for a column
    '''
    def drop_index(self, column_number):
        if column_number == config.PRIMARY_KEY_COLUMN:
            print("Error: Can't drop primary key index")
            return False
        if column_number not in self.indices:
            return False
        del self.indices[column_number]
        self.save_index()
        return True

    '''
    Here on uses pickle
    Save index to disk
    '''
    def save_index(self):
        with open(self.index_file, "wb") as f:
            pickle.dump(self.indices, f)

    '''
    Load index from disk
    '''
    def load_index(self):
        if os.path.exists(self.index_file):
            with open(self.index_file, "rb") as f:
                self.indices = pickle.load(f)

    '''
    # bulk index handling
    '''
    # for insert
    def bulk_index_add(self, rid, schema, timestamp, indirection, base_rid, columns):
        self.addRecord(config.SCHEMA_ENCODING_COLUMN, schema, rid)
        self.addRecord(config.TIMESTAMP_COLUMN, timestamp, rid)
        self.addRecord(config.INDIRECTION_COLUMN, indirection, rid)
        # TODO: ENSURE THAT BASE RID COLUMN IS CORRECTLY NAMED
        self.addRecord(config.BASE_RID_COLUMN, base_rid, rid)
        for i in range(len(columns)):
            # TODO: might be i+5 instead of i+6
            self.addRecord(key=i+6, rid=rid, key_col=columns[i])

    # for delete
    def bulk_index_remove(self, rid, schema, timestamp, indirection, base_rid, columns):
        self.removeRecord(config.SCHEMA_ENCODING_COLUMN, schema, rid)
        self.removeRecord(config.TIMESTAMP_COLUMN, timestamp, rid)
        self.removeRecord(config.INDIRECTION_COLUMN, indirection, rid)
        # TODO: ENSURE THAT BASE RID COLUMN IS CORRECTLY NAMED
        self.removeRecord(config.BASE_RID_COLUMN, base_rid, rid)
        for i in range(len(columns)):
            # TODO: might be i+5 instead of i+6
            self.removeRecord(key=i+6, rid=rid, key_col=columns[i])