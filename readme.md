# Search builder

This is the top-level project for the Search service.

This is basically just a bunch of git sub-module references and a build script.

To build an index and then run the search server:

	env
	go run src/github.com/IMQS/search/cmd/main.go -c=examples\imqs-search.json rebuild WaterDemandMeterWater WaterDemandStand
	go run src/github.com/IMQS/search/cmd/main.go -c=examples\imqs-search.json run
