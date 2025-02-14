def test_select_version_latest():
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    table = db.create_table("Students", 5, 0)
    query = Query(table)

    # Insert a record
    query.insert(1, 10, 20, 30, 40)

    # Select latest version (same as select)
    result = query.select_version(1, 0, [1, 1, 1, 1, 1], 0)
    assert result is not False, "Select version failed"
    assert result[0].columns == [1, 10, 20, 30, 40], "Incorrect latest version result"

    print("Select latest version test passed!")

test_select_version_latest()

def test_select_version_old():
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    table = db.create_table("Students", 5, 0)
    query = Query(table)

    # Insert and update the record
    query.insert(1, 10, 20, 30, 40)
    query.update(1, None, None, 99, None, None)  # Update column index 2
    query.update(1, None, None, 100, None, None)  # Update column index 2
    query.update(1, None, None, 101, None, None)  # Update column index 2

    # Select the previous version
    resultprev3 = query.select_version(1, 0, [1, 1, 1, 1, 1], -3)
    resultprev2 = query.select_version(1, 0, [1, 1, 1, 1, 1], -2)
    resultprev = query.select_version(1, 0, [1, 1, 1, 1, 1], -1)
    resultcurr = query.select_version(1, 0, [1, 1, 1, 1, 1], 0)
    print(f"pre3: {resultprev3[0].columns[2]}")
    print(f"pre2: {resultprev2[0].columns[2]}")
    print(f"prev: {resultprev[0].columns[2]}")
    print(f"curr: {resultcurr[0].columns[2]}")



    assert resultprev is not False, "Select previous version failed"
    assert resultprev3[0].columns == [1, 10, 20, 30, 40], "Incorrect previous version result"

    print(" Select previous version test passed!")

test_select_version_old()

# TODO: need update() to work before testing this
# def test_select_version_too_old():
#     from lstore.db import Database
#     from lstore.query import Query

#     db = Database()
#     table = db.create_table("Students", 5, 0)
#     query = Query(table)

#     # Insert and update the record
#     query.insert(1, 10, 20, 30, 40)
#     query.update(1, None, None, 99, None, None)  # First update
#     query.update(1, None, None, 77, None, None)  # Second update

#     # Try selecting an older version than exists
#     result = query.select_version(1, 0, [1, 1, 1, 1, 1], 3)  # No third version
#     assert result == False, "Select should fail for too old version"

#     print("Select too old version test passed!")

# test_select_version_too_old()
