def test_insert_basic():
    from lstore.db import Database
    from lstore.query import Query

    # Step 1: Create Database and Table
    db = Database()
    table = db.create_table("Students", 5, 0)  # 5 columns, column 0 as primary key
    query = Query(table)

    # Step 2: Insert a Record
    result = query.insert(1, 10, 20, 30, 40)  # 5 values matching column count

    # Step 3: Assertions
    assert result == True, "Insert failed"
    assert 0 in table.page_directory, "Inserted record's RID not found in page directory"

    # Step 4: Verify Data Storage
    page_range_number, base_page_number, record_number = table.page_directory[0]
    stored_values = table.page_ranges[page_range_number].base_pages[base_page_number].pages

    assert stored_values[0].read(record_number) == 1, "Primary key mismatch"
    assert stored_values[1].read(record_number) == 10, "Column 1 mismatch"
    assert stored_values[2].read(record_number) == 20, "Column 2 mismatch"
    assert stored_values[3].read(record_number) == 30, "Column 3 mismatch"
    assert stored_values[4].read(record_number) == 40, "Column 4 mismatch"



def test_insert_multiple():
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    table = db.create_table("Students", 5, 0)
    query = Query(table)

    # Insert multiple records
    query.insert(1, 10, 20, 30, 40)
    query.insert(2, 50, 60, 70, 80)

    # Ensure both records exist
    assert 0 in table.page_directory, "First record's RID not found"
    assert 1 in table.page_directory, "Second record's RID not found"

    # Ensure primary keys are correctly stored
    page_range_0, base_page_0, record_num_0 = table.page_directory[0]
    page_range_1, base_page_1, record_num_1 = table.page_directory[1]

    stored_values_0 = table.page_ranges[page_range_0].base_pages[base_page_0].pages
    stored_values_1 = table.page_ranges[page_range_1].base_pages[base_page_1].pages

    assert stored_values_0[0].read(record_num_0) == 1, "First primary key mismatch"
    assert stored_values_1[0].read(record_num_1) == 2, "Second primary key mismatch"


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


def test_insert_page_capacity():
    from lstore.db import Database
    from lstore.query import Query

    db = Database()
    table = db.create_table("Students", 5, 0)
    query = Query(table)

    # Fill up a page (assuming 512 records per page)
    for i in range(512):
        result = query.insert(i, 10, 20, 30, 40)
        assert result == True, f"Insert failed at RID {i}"

    # Insert one more (should go into a new page)
    result = query.insert(513, 50, 60, 70, 80)
    assert result == True, "Insert failed when starting a new page"
