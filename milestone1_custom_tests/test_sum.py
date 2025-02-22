from lstore.db import Database
from lstore.query import Query

db = Database()
table = db.create_table("Students", 5, 0)
query = Query(table)

# Insert records
query.insert(1, 10, 20, 30, 40)
query.insert(2, 50, 20, 12, 123)
query.insert(3, 60, 20, 85, 80814)
query.insert(4, 70, 20, 43, 870)

def test_sum():

    # take a sum, (2, 4, 3) means that take from the range 2-4, in the third column. that would be 12+85+43 = 140
    sum = query.sum(2, 4, 3)
    print(f"sum is {sum}")

    print("Sum test passed!")

test_sum()

def test_inc():

    # db = Database()
    # table = db.create_table("Students", 5, 0)
    # query = Query(table)

    # # Insert records
    # query.insert(1, 10, 20, 30, 40)
    # query.insert(2, 50, 20, 12, 123)
    # query.insert(3, 60, 20, 85, 80814)
    # query.insert(4, 70, 20, 43, 870)

    # take a sum, (2, 4, 3) means that take from the range 2-4, in the third column. that would be 12+85+43 = 140
    result = query.increment(3, 3)
    print(f"result is {result}")

    incremented = query.select(3, 0, [1, 1, 1, 1, 1])  # Get all columns
    print(incremented[0].columns)

    print("Increment test passed!")

test_inc()