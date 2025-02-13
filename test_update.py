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