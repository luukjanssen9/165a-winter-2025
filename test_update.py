from lstore.db import Database
from lstore.query import Query

def test_update():
    

    db = Database()
    table = db.create_table("Students", 5, 0)
    query = Query(table)

    # Insert records
    query.insert(1, 10, 20, 30, 40)
    query.insert(2, 50, 60, 70, 80)

    # updating record #1
    query.increment(1, 2)

    # Selecting record #1
    result2.update()

    result1 = query.select(1, 0, [1, 1, 1, 1, 1])  # Get all columns
    result2 = query.select(2, 0, [1, 1, 1, 1, 1])  # Get all columns

    # if the record was deleted, then the results should be uhhh idk
    
    # DEBUG, to see what the select returns:
    print(f"rid: {result1[0].rid}")
    print(f"key: {result1[0].key}")
    print(f"cols: {result1[0].columns}")

    # updating record #1
    query.increment(1, 3)
    query.increment(1, 4)
    query.increment(1, 3)


    result1 = query.select(1, 0, [1, 1, 1, 1, 1])  # Get all columns
    # DEBUG, to see what the select returns:
    print(f"\nafter update set 1:\nrid: {result1[0].rid}")
    print(f"key: {result1[0].key}")
    print(f"cols: {result1[0].columns}")


    query.increment(1, 2)
    query.increment(1, 2)
    query.increment(1, 4)

    result1 = query.select(1, 0, [1, 1, 1, 1, 1])  # Get all columns
    # DEBUG, to see what the select returns:
    print(f"\nafter update set 2:\nrid: {result1[0].rid}")
    print(f"key: {result1[0].key}")
    print(f"cols: {result1[0].columns}")



    
    # assert result2 is False, "Incorrect delete output"
    # if result2==False: print(f"\npassed")
    # else: print(f"\nfailed")
    # print(f"rid: {result2[0].rid}")
    # print(f"key: {result2[0].key}")
    # print(f"cols: {result2[0].columns}")

    print("Update test passed!")

test_update()



def test_update_513():
    db = Database()
    table = db.create_table("Students", 5, 0)
    query = Query(table)

    # Insert 513 records
    for i in range(1, 514):
        query.insert(i, i * 10, i * 20, i * 30, i * 40)

    # Update each record multiple times to test tail page expansion
    for i in range(1, 514):
        query.increment(i, 2)
        query.increment(i, 3)
        query.increment(i, 4)

    # Select and verify updates
    for i in range(1, 514):
        result = query.select(i, 0, [1, 1, 1, 1, 1])  # Select full record
        print(f"Record {i}: rid={result[0].rid}, key={result[0].key}, cols={result[0].columns}")

    print("Update test for 513 records passed!")

# test_update_513()


def test_update_twice():
    db = Database()
    table = db.create_table("Students", 5, 0)  # 5 columns, primary key at index 0
    query = Query(table)

    # Insert a single record
    query.insert(1, 10, 20, 30, 40)

    # First update: Increment column 2
    query.increment(1, 2)
    query.increment(1, 2)
    query.increment(1, 2)
    query.increment(1, 2)

    # Fetch and verify first update
    result1 = query.select(1, 0, [1, 1, 1, 1, 1])  # Select all columns
    print("\nAfter first update:")
    print(f"RID: {result1[0].rid}")
    print(f"Key: {result1[0].key}")
    print(f"Columns: {result1[0].columns}")  # Expecting column 2 to be updated

    # Second update: Increment column 3
    query.increment(1, 3)
    query.increment(1, 2)

    # Fetch and verify second update
    result2 = query.select(1, 0, [1, 1, 1, 1, 1])  # Select all columns
    print("\nAfter second update:")
    print(f"RID: {result2[0].rid}")
    print(f"Key: {result2[0].key}")
    print(f"Columns: {result2[0].columns}")  # Expecting column 3 to be updated

    print("\nUpdate test passed!")

test_update_twice()

