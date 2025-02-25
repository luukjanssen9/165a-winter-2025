import json
import os

# THIS SEGMENT TESTS TO SEE WHAT IN THE DIRECTORY IS A FOLDER VS A FILE
# TRUE = FOLDER, FAKSE = FILE
# currdir = "."
# for thing in os.listdir(currdir):
#     print(f"{thing}: {os.path.isdir(thing)}")

# YOU CAN TEST THE SYSTEM BY SWITCHING PATH1 AND PATH2
# IF IT WORKS, THEN THE CODE SHOULD RUN TO COMPLETION AND COPY ONE OUTPUT TO THE OTHER
# THEN YOU CAN SWAP THE FILE NAMES AND HAVE IT GO THE OTHER WAY
# YOU CAN ALSO EDIT A VALUE IN ONE FILE AND WATCH IT CHANGE IN THE OTHER FILE
path1 = "output"
path2 = "Grades"
with open(f'{path1}.json', 'r') as table_metadata:
    data = json.load(table_metadata)

key = data["key"]
num_columns = data["num_columns"]
json_page_dir = data["page_directory"]

page_directory = {}
for i in json_page_dir:
    page_directory[i] = {
        "page_range" : json_page_dir[i]['page_range'],
        "base_page" : json_page_dir[i]['base_page'],
        "record_number" : json_page_dir[i]['record_number']
    }

# print(f"key is {key}, there are {num_columns} columns")
# print(page_directory)

# OUTPUT TO JSON
output = {
    "key" : key,
    "num_columns" : num_columns,
    "page_directory" : page_directory
}

outputjson = json.dumps(output, indent=4)
# print(f"output is {outputjson}")
with open(f'{path2}.json', 'w') as json_output_file:
    json_output_file.write(outputjson)