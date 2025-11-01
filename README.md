## What is hey?

Hey is a simple, high-performance ORM for Go. <br>
For example: INSERT, DELETE, UPDATE, SELECT ...

## hey's mission
1. Support as many SQL general syntax as possible.
2. Write less or no original strings in business code, such as "username", "SUM(salary)", "id = ?", "SELECT id, name FROM your_table_name" ...
3. Try to avoid using reflection when scanning and querying data to reduce time consumption.
4. Through the template code of the table structure, when the database table structure changes, your code can immediately perceive it.
5. When you implement a business, focus more on the business rather than on building SQL statements.
6. Allows the use of custom caches to reduce query request pressure on relational databases.
7. It (hey) can help you build complex SQL statements, especially complex query SQL statements.
8. Allows you to define a set of commonly used template SQL statements and use them in complex SQL statements.
9. Allows you to efficiently build the SQL statements you need.

## INSTALL
```shell
go get github.com/cd365/hey/v6@latest
```
