def test_table_initialization():
    from lstore.table import Table

    # Create a table with 5 columns and primary key at index 0
    table = Table("Students", 5, 0)

    # Assertions
    assert table.name == "Students", "Table name mismatch"
    assert table.num_columns == 5, "Incorrect number of columns"
    assert table.key == 0, "Primary key index mismatch"
    assert isinstance(table.page_directory, dict), "Page directory should be a dictionary"
    assert len(table.page_ranges) == 0, "Page ranges should be empty initially"
