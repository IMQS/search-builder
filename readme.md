# IMQS Search Server

This was created in order to serve up domain-specific searches for IMQS. Examples of what we're searching for are
Water Meter ID, Stand ID, Municipal Account Number.

This server works by reading a set of fields from databases, splitting them up into tokens, and indexing
those tokens in one giant index table.

The goal is not yet to do natural language search, nor to search documents. We'll likely use something like Lucene
when we get around to that.

# Run

To run the search server:

    go run imqssearch.go -c=example-search.json run

# Test

To run unit tests:

Postgres must be installed on localhost. Create a user called unit_test_user, with
password unit_test_password. The user must be able to create databases.

There were changes made that broke the unit tests which is not easily fixable; for the meantime make
sure that the following is added to a db-aliases file close to you *just for testing*, do not push.


```json
{
  "index": {
    "driver": "postgres",
    "host": "localhost",
    "port": "5432",
    "alias": "",
    "name": "unit_test_search_index",
    "username": "unit_test_user",
    "password": "unit_test_password",
    "modtrack": "install"
  },
  "generic": {
    "driver": "postgres",
    "host": "localhost",
    "port": "5432",
    "alias": "",
    "name": "unit_test_search_generic",
    "username": "unit_test_user",
    "password": "unit_test_password",
    "modtrack": "install"
  },
  "db1": {
    "driver": "postgres",
    "host": "localhost",
    "port": "5432",
    "alias": "",
    "name": "unit_test_search_src1",
    "username": "unit_test_user",
    "password": "unit_test_password",
    "modtrack": "install"
  },
  "db2": {
    "driver": "postgres",
    "host": "localhost",
    "port": "5432",
    "alias": "",
    "name": "unit_test_search_src2",
    "username": "unit_test_user",
    "password": "unit_test_password",
    "modtrack": "install"
  }
}
```

The following should now work.

```bash
	go test ./...
```


# Docker Build

sudo docker build --build-arg SSH_KEY="`cat ~/.ssh/id_rsa`" -t imqs/search:master .
