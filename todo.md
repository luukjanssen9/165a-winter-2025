# THINGS TO DO:
## Milestone 1 Parts:

### Data Model: 
store the schema and instance for each table in columnar form.

### Bufferpool Management: 
maintain data in memory.

### Query Interface: 
to offer data manipulation and querying capabilities such as select, insert, update, and delete of a single key along with a simple aggregation query, namely, to return the summation of a single column for a range of keys.


## Things to do: Implement the APIs
- Database class
    -  create table function
    -  drop table function
- Query class
    -  select function
    -  insert function
    -  update function
    -  delete function
    -  sum function
- Table class
    -  "core of our relational storage functionality" (idk, it doesnt tell us what to implement specifically)
- Index class
    -  create_index function
    -  drop_index function
- Page class
    -  keep in mind that base pages and tail pages should be indentical from the hardware's POV

- config.py
    -  "centralized storage for all the configuration options and the constant values used in the code" 
- be able to run main.py
    -  used for the autograder

## Extra things to implement for an A+:
- indexing functionality (primary and/or secondary indexes) (using hash tables or trees)
- experimental analysis/graphs
- extended query capabilities
