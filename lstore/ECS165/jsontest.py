import json
import os

currdir = "."
for thing in os.listdir(currdir):
    print(f"{thing}: {os.path.isdir(thing)}")

path = "Grades"
with open(f'{path}.json', 'r') as table_metadata:
    data = json.load(table_metadata)

key = data["key"]
num_columns = data["num_columns"]
json_page_dir = data["page_directory"]

page_directory = {}
for i in json_page_dir:
    page_directory[i] = (json_page_dir[i]['page_range'], json_page_dir[i]['base_page'], json_page_dir[i]['record_number'])
            

print(f"key is {key}, there are {num_columns} columns")
print(page_directory)