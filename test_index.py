from lstore.db import Database
from lstore.query import Query

def test_index():
    # Initialize database and create table
    db = Database()
    table = db.create_table("Students", 5, 0)  # 5 columns, primary key at column index 0
    query = Query(table)

    # Insert records
    query.insert(1, 10, 20, 30, 40)
    query.insert(2, 50, 60, 70, 80)
    query.insert(3, 90, 100, 110, 120)

    # Manually add index for column 1 (since create_index is optional)
    table.index.indices[1] = {}  # Create empty index for column 1
    table.index.indices[1][10] = [1]  # Manually map value 10 to RID 1
    table.index.indices[1][50] = [2]
    table.index.indices[1][90] = [3]

    # Test locate() function
    result1 = table.index.locate(1, 10)  # Column 1, value 10
    assert result1 == [1], f"Locate failed for value 10 in column 1. Got: {result1}"

    result2 = table.index.locate(1, 50)  # Column 1, value 50
    assert result2 == [2], f"Locate failed for value 50 in column 1. Got: {result2}"

    result3 = table.index.locate(1, 90)  # Column 1, value 90
    assert result3 == [3], f"Locate failed for value 90 in column 1. Got: {result3}"

    result4 = table.index.locate(1, 999)  # Non-existent value
    assert result4 == [], f"Locate should return empty list for missing value. Got: {result4}"

    # Test locate_range()
    result_range = table.index.locate_range(10, 90, 1)
    assert result_range == [[1], [2], [3]], f"Locate range failed. Got: {result_range}"

    print("test_index passed!")

# Run test
test_index()