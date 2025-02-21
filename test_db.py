def test_create_table():
    from lstore.db import Database
    db = Database()
    
    # Create a table
    table = db.create_table("Students", 5, 0)
    
    # Assertions
    assert table is not None, "Table creation failed"
    assert table.name == "Students", "Table name mismatch"
    assert table.num_columns == 5, "Column count mismatch"
    assert table.key == 0, "Primary key index mismatch"

def test_get_table():
    from lstore.db import Database
    db = Database()
    
    db.create_table("Grades", 4, 1)
    table = db.get_table("Grades")

    assert table is not None, "Table retrieval failed"
    assert table.name == "Grades", "Table name mismatch"

def test_drop_table():
    from lstore.db import Database
    db = Database()
    
    db.create_table("Tests", 3, 1)
    db.drop_table("Tests")
    
    table = db.get_table("Tests")
    assert table is None, "Drop table failed"
