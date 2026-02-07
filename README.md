## What is hey?
Hey is a Go-based SQL builder and ORM.<br>
It has no third-party library dependencies and does not restrict you to using any drivers.<br>
It supports common databases such as PostgreSQL, MySQL, SQLite...<br>
When constructing regular SQL, you only need to set the identifier and its corresponding value.<br>
During the development phase, you can view the dynamically generated SQL statements at any time.<br>
Log the executed SQL statements and transactions(Use it as an ORM).<br>


## hey's mission
1. Support as many SQL general syntax as possible.
2. Write less or no original strings in business code, such as "username", "SUM(salary)", "id = ?", "SELECT id, name FROM your_table_name" ...
3. Try to avoid using reflection when scanning and querying data to reduce time consumption.
4. Through the template code of the table structure, when the database table structure changes, your code can immediately perceive it.
5. When you implement a business, focus more on the business rather than on building SQL statements.
6. It (hey) can help you build complex SQL statements, especially complex query SQL statements.
7. Allows you to define a set of commonly used template SQL statements and use them in complex SQL statements.
8. Allows you to efficiently build the SQL statements you need.


## Special requirements:
1. All SQL parameters should be passed using parameter placeholders "?" and a list of parameter values.
2. Parameter placeholders "?" are prohibited in SQL statement comments.
3. Parameter placeholders "?" are prohibited in SQL statement string constants.


## INSTALL
```shell
go get github.com/cd365/hey/v7@latest
```

## EXAMPLE
Currently, only PostgreSQL is used in this case; the usage methods for other databases are similar.<br>
[All](https://github.com/cd365/hey/tree/master/_examples)<br>
[PostgreSQL](https://github.com/cd365/hey/blob/master/_examples/pgsql)<br>
