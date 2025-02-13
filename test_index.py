def test_indexing():
    from lstore.db import Database
    from lstore.query import Query

    print("\n Running test_indexing...")

    #Initialize database and create table
    db = Database()
    table = db.create_table("Students", 5, 0)  # 5 columns, primary key at column index 0
    query = Query(table)

    # Insert records (Index should be created automatically)
    query.insert(1, 10, 20, 30, 40)
    query.insert(2, 50, 60, 70, 80)
    query.insert(3, 90, 100, 110, 120)

    # Ensure the index was created
    assert table.index.indices[0] is not None, "Index for primary key was not created automatically."
    print("Index was created automatically.")

    # Test locate() function for primary key
    rid1 = table.index.locate(0, 1)  # Primary key 1
    assert rid1 == [1], f"Locate failed for primary key 1. Got: {rid1}"

    rid2 = table.index.locate(0, 2)  # Primary key 2
    assert rid2 == [2], f"Locate failed for primary key 2. Got: {rid2}"

    rid3 = table.index.locate(0, 3)  # Primary key 3
    assert rid3 == [3], f"Locate failed for primary key 3. Got: {rid3}"

    print("Index correctly tracks primary keys.")

    # Test locate() function for column 1 (non-primary key)
    rid4 = table.index.locate(1, 10)  # Column 1, value 10
    assert rid4 == [1], f"Locate failed for value 10 in column 1. Got: {rid4}"

    rid5 = table.index.locate(1, 50)  # Column 1, value 50
    assert rid5 == [2], f"Locate failed for value 50 in column 1. Got: {rid5}"

    rid6 = table.index.locate(1, 90)  # Column 1, value 90
    assert rid6 == [3], f"Locate failed for value 90 in column 1. Got: {rid6}"

    rid7 = table.index.locate(1, 999)  # Non-existent value
    assert rid7 == [], f"Locate should return empty list for missing value. Got: {rid7}"

    print("Locate() works correctly.")

    # Test locate_range()
    result_range = table.index.locate_range(10, 90, 1)
    assert result_range == [[1], [2], [3]], f"Locate range failed. Got: {result_range}"

    print("locate_range() works correctly.")
    print("test_indexing passed successfully!")

test_indexing()