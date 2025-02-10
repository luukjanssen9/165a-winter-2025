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

    # Select the previous version
    result = query.select_version(1, 0, [1, 1, 1, 1, 1], 1)
    assert result is not False, "Select previous version failed"
    assert result[0].columns == [1, 10, 20, 30, 40], "Incorrect previous version result"

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
