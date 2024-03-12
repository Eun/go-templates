# database-iteration
This is a simple rest api that exposes a list endpoint to iterate
over a database table, it uses immutable next page and sync tokens.

A system versioned table is used to store history, that makes it possible to
provide the immutable next page and sync tokens.

In this particular case, the history of the table will be cleaned every 24 hours.

More details can be found in [main.go](main.go)

Technologies used:
- MariaDB
