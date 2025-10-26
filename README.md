# BraddPortfolio
This project brings together all the components we’ve built in previous assignments to create a fully functioning in-memory database system that can process SQL queries and manage data efficiently.
The main goal is to extend the SQL compiler so that it can create and use indexes, improving query performance when dealing with large datasets. 
Essentially, the project revolves around creating a B+ tree index structure on an attribute of a given table and integrating it into the query execution pipeline through a new operator called ScanIndex. 
Once implemented, users should be able to interact with the database using a command-line interface, similar to SQLite, and perform commands like create index, select, and other SQL operations.

the project requires integrating ScanIndex into the query compiler so that it is automatically used when an indexed attribute appears in a query’s predicate. 
This involves modifying both the SQL parser and query execution components. Additionally, the command-line interface must support various commands such as exit, schema, and index, allowing users to view table structures, see available indexes, or terminate the program. 
The database should be able to execute all SQL commands from previous projects while also supporting the creation and use of indexes for TPC-H benchmark tables.

Overall, this project is designed to test our understanding of database internals, file management, and query optimization. 
By implementing the Create Year command and ScanIndex operator, we learn how indexing improves query performance and how relational database systems handle data organization behind the scenes.
It’s a culmination of everything we’ve built throughout the semester — from record and file management to relational operators and query execution. 
The end result is a lightweight database system that behaves much like a simplified version of SQLite, capable of executing complex queries efficiently through the use of indexes.
