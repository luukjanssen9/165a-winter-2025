from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.rollback_log = {}
        self.table = None

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        # use grades_table for aborting
        if self.table == None:
            self.table = table

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            if query.__name__ == "update":
                primary_key = args[0]
                original_values = self.table.select(primary_key, 0, [1] * self.table.num_columns) 
    
                if original_values:
                    self.rollback_log[primary_key] = original_values[0].columns

            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        for primary_key, original_values in self.rollback_log.items():
            if original_values:
                success = self.table.update(primary_key, *original_values[0].columns)  
            if not success:
                print(f"Error: Rollback failed for primary key {primary_key}")
        self.rollback_log.clear() 
        return False

    
    def commit(self):
        # TODO: commit to database
        for query, args in self.queries:
            if query.__name__ == "update":
                primary_key = args[0]
                if primary_key in self.rollback_log:
                    self.table.flush_to_disk(primary_key)  
        self.rollback_log.clear()
        return True

