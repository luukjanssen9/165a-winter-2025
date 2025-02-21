from lstore.table import Table
from lstore.query import Query
from lstore.config import *
from time import time

def test_query_update():
    # Create a table with 5 columns (including primary key)
    table = Table('TestTable', 5, 0)  # Assuming primary key is at index 0
    query = Query(table)

    # Insert some records
    query.insert(1, 10, 20, 30, 40)
    query.insert(2, 15, 25, 35, 45)
    query.insert(3, 20, 30, 40, 50)

    # Test updating an existing record
    result = query.update(1, None, 100, None, 300, None)
    print("DEBUG: Update function returned:", result)  # Check if it exits properly
    assert result == True, "Update failed for existing record"

    # Verify the record is updated
    print("DEBUG: About to call select()")
    record = query.select(1, 0, [1, 1, 1, 1, 1])[0]
    print("DEBUG: select() returned:", record)
    assert record.columns == [1, 100, 20, 300, 40], "Record update verification failed"

    # Test updating a non-existent record
    result = query.update(4, None, 100, None, 300, None)
    assert result == False, "Update should fail for non-existent record"

    # Test updating only some columns of an existing record
    result = query.update(2, None, None, 200, None, 400)
    assert result == True, "Update failed for partial columns"

    # Verify the record is updated
    record = query.select(2, 0, [1, 1, 1, 1, 1])[0]
    assert record.columns == [2, 15, 200, 35, 400], "Partial columns update verification failed"

    # Test updating all columns of an existing record
    result = query.update(3, 5, 10, 15, 20, 25)
    assert result == True, "Update failed for all columns"

    # Verify the record is updated
    record = query.select(3, 0, [1, 1, 1, 1, 1])[0]
    assert record.columns == [3, 10, 15, 20, 25], "All columns update verification failed"

    print("All tests passed!")
    return

if __name__ == '__main__':
    test_query_update()