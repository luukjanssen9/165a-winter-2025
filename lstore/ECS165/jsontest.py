import json
import os

path = "Grades"
with open(f'{path}.json', 'r') as table_metadata:
    data = json.load(table_metadata)
    
key = data["key"]
num_columns = data["num_columns"]
pagedir = data["page_directory"]

print(f"key is {key}, there are {num_columns} columns")
for i in pagedir:
    print(f"info is {pagedir[i]['page_range']}, {pagedir[i]['base_page']}, {pagedir[i]['record_number']}")