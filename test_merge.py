from lstore.db import Database
from lstore.table import Table
from lstore.query import Query

db = Database()
db.open("test_db")  # Create test database directory

if not db.isOpen:
    print("ERROR: Database is not open. Call db.open(path) first.")
    exit()
table = db.create_table("Students", 5, 0)  # (name, num_columns, primary_key_index)
table = db.get_table("Students")  # Now it should work!

if table is None:
    print("ERROR: Table creation failed. Check db.create_table().")
    exit()
query = Query(table)

print("Inserting Records...")
query.insert(101, 20, 90, 85, 75)
query.insert(102, 22, 95, 80, 70)
query.insert(103, 21, 85, 88, 95)

print("Updating Records...")
query.update(101, None, 11, None, None, None)  # Update Age
query.update(102, None, 15, None, None, None)  # Update Age Again

print("\n⚡ Running Manual Merge...")
table.test_merge()

print("\nFetching Merged Records...")
record1 = query.select(101, 0, [1, 1, 1, 1, 1])  # Select all columns for ID 101
record2 = query.select(102, 0, [1, 1, 1, 1, 1])

def print_record(record):
    return record.columns

print("Merged Record 101:", print_record(record1[0]))  
print("Merged Record 102:", print_record(record2[0]))

db.close()
