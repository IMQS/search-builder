# Search builder

This is the top-level project for the Search service.

This is basically just a bunch of git sub-module references and a build script.

To run the search server:

	env
	go run src/github.com/IMQS/search/cmd/main.go -c=examples\imqs-search.json run

To run unit tests:

Postgres must be installed on localhost. Create a user called search_test, with
password search_test. The user must be able to create databases.

	env
	go test github.com/IMQS/search/search -db_postgres
