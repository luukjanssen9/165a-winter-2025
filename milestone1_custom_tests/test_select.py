def test_select():
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    table = db.create_table("Students", 5, 0)
    query = Query(table)

    # Insert records
    query.insert(1, 10, 20, 30, 40)
    query.insert(2, 50, 60, 70, 80)

    # Select record with primary key 1
    result = query.select(1, 0, [1, 1, 1, 1, 1])  # Get all columns

    assert result is not False, "Select failed"
    assert result[0].columns == [1, 10, 20, 30, 40], "Incorrect select output"

    print("Select test passed!")

test_select()

def test_select_multiple_records():
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    table = db.create_table("Students", 5, 0)  # 5 columns, primary key at index 0
    query = Query(table)

    # Insert multiple records
    query.insert(1, 10, 20, 30, 40)
    query.insert(2, 50, 60, 70, 80)

    # Select first record
    result1 = query.select(1, 0, [1, 1, 1, 1, 1])
    assert result1 is not False, "Select failed for primary key 1"
    assert result1[0].columns == [1, 10, 20, 30, 40], "Incorrect result for primary key 1"

    # Select second record
    result2 = query.select(2, 0, [1, 1, 1, 1, 1])
    assert result2 is not False, "Select failed for primary key 2"
    assert result2[0].columns == [2, 50, 60, 70, 80], "Incorrect result for primary key 2"

    print("Select test for multiple records passed!")

test_select_multiple_records()

def test_select_non_existent():
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    table = db.create_table("Students", 5, 0)
    query = Query(table)

    # Insert a record
    query.insert(1, 10, 20, 30, 40)

    # Attempt to select a non-existent record
    result = query.select(999, 0, [1, 1, 1, 1, 1])  # 999 does not exist
    assert result == False, "Select should fail for non-existent primary key"

    print("Select test for non-existent record passed!")

test_select_non_existent()

def test_select_column_projection():
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    table = db.create_table("Students", 5, 0)
    query = Query(table)

    # Insert a record
    query.insert(1, 10, 20, 30, 40)

    # Select with projection (include only columns 0, 2, and 4)
    result = query.select(1, 0, [1, 0, 1, 0, 1])
    assert result is not False, "Select failed for column projection"
    assert result[0].columns == [1, None, 20, None, 40], "Incorrect column projection"

    print("Select test for column projection passed!")

test_select_column_projection()

