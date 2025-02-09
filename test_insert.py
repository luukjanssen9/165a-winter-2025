def test_insert_basic():
    from lstore.db import Database
    from lstore.query import Query

    # Step 1: Create Database and Table
    db = Database()
    table = db.create_table("Students", 5, 0)  # 5 columns, column 0 as primary key
    query = Query(table)

    # Step 2: Insert a Record
    result = query.insert(1, 10, 20, 30, 40)  # 5 values matching column count

    if not result:
        print("Insert failed. Possible cause: No space in page group.")
    
    print(f"Page Directory: {table.page_directory}")

    assert result, "Insert should return True"
    assert 1 in table.page_directory, "Inserted RID should exist in page directory"

test_insert_basic()


def test_insert_multiple():
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    table = db.create_table("Students", 5, 0)  # 5 columns, column 0 as primary key
    query = Query(table)

    # Insert multiple records
    query.insert(1, 10, 20, 30, 40)  # Insert record 1
    query.insert(2, 50, 60, 70, 80)  # Insert record 2

    # Ensure both records exist in the page directory
    assert 1 in table.page_directory, "First record's RID not found"
    assert 2 in table.page_directory, "Second record's RID not found"

    # Ensure primary keys are correctly stored
    page_range_0, base_page_0, record_num_0 = table.page_directory[1]  # Record with RID 1
    page_range_1, base_page_1, record_num_1 = table.page_directory[2]  # Record with RID 2

    # Read stored primary key values
    stored_values_0 = table.page_ranges[page_range_0].base_pages[base_page_0].pages[4]
    stored_values_1 = table.page_ranges[page_range_1].base_pages[base_page_1].pages[4]

    # Check if the primary key is correctly stored in the 4th column
    assert stored_values_0.read(record_num_0) == 1, "First primary key mismatch"
    assert stored_values_1.read(record_num_1) == 2, "Second primary key mismatch"

test_insert_multiple()


def test_insert_column_mismatch():
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    table = db.create_table("Students", 5, 0)
    query = Query(table)

    # Insert with missing column (only 4 values instead of 5)
    result = query.insert(1, 10, 20, 30)
    assert result == False, "Insert should fail due to missing columns"

    # Insert with extra column (6 values instead of 5)
    result = query.insert(1, 10, 20, 30, 40, 50)
    assert result == False, "Insert should fail due to extra columns"


test_insert_column_mismatch()

def test_insert_duplicate_primary_key():
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    table = db.create_table("Students", 5, 0)
    query = Query(table)

    # Insert first record
    result1 = query.insert(1, 10, 20, 30, 40)
    assert result1 == True, "First insert should succeed"

    # Insert duplicate primary key (should fail)
    result2 = query.insert(1, 50, 60, 70, 80)
    assert result2 == False, "Insert should fail due to duplicate primary key"

test_insert_duplicate_primary_key()

def test_insert_page_capacity():
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    table = db.create_table("Students", 5, 0)
    query = Query(table)

    # Fill up a page (assuming 512 records per page)
    for i in range(512):
        result = query.insert(i + 1, 10, 20, 30, 40)  # Insert starting from RID 1 to 512
        assert result == True, f"Insert failed at RID {i + 1}"

    # Insert one more (should go into a new page)
    result = query.insert(513, 50, 60, 70, 80)
    assert result == True, "Insert failed when starting a new page"


test_insert_page_capacity()

def test_insert_metadata():
    from lstore.db import Database
    from lstore.query import Query

    # Step 1: Create a database and a table
    db = Database()
    table = db.create_table("Students", 5, 0)  # 5 columns
    query = Query(table)

    # Step 2: Insert a record
    query.insert(1, 10, 20, 30, 40)

    # Step 3: Retrieve metadata and column data
    rid = 1  # First record's RID (adjusted from 0 to 1 for proper indexing)
    page_range, base_page, record_number = table.page_directory[rid]
    stored_values = table.page_ranges[page_range].base_pages[base_page].pages

    # Assertions for metadata
    assert stored_values[0].read(record_number) == 0, "Indirection mismatch"
    assert stored_values[1].read(record_number) == rid, "RID mismatch"
    assert isinstance(stored_values[2].read(record_number), int), "Timestamp missing or invalid"
    assert stored_values[3].read(record_number) == 0, "Schema encoding mismatch"

    # Assertions for actual data
    assert stored_values[4].read(record_number) == 1, "Primary key mismatch"
    assert stored_values[5].read(record_number) == 10, "Column 1 mismatch"
    assert stored_values[6].read(record_number) == 20, "Column 2 mismatch"
    assert stored_values[7].read(record_number) == 30, "Column 3 mismatch"
    assert stored_values[8].read(record_number) == 40, "Column 4 mismatch"


test_insert_metadata()