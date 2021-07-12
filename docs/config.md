Adding an  extra category to the search bar

To start of here is an example config

		` "ImqsServerMirror": {
			"Tables": { //(1)
				"AssetComponent": {
						"Categories": [ //(2)
							"Assets"
						],
						"FriendlyName": "Facility Name", //(3)
						"Fields": [ //(4)
							{
								"FriendlyName": "ComponentID", //(5)
								"Field": "ComponentID", //(6)
								"Order": "1" //(7)
							},
							{
								"FriendlyName": "Facility Name",
								"Field": "AssetFacilityName",
								"Order": "2"
							}
						]
					},`

1. In order to search on a new table it has to be entered into the config under the rightful DB it belongs to.

# DBs contains tables:
1. Tables:All of the tables  that need be searchable from the DB it is slotted under are listed 

## Each table contains:
2. Categories:Here you type the Category that your search belongs to e.g Assets,Water,Esri Electricity
3. FriendlyName:The friendly name of the column you would like to search on
4. Fields:These are the fields you are going to be searching on

## Within fields you will find the following
5. FriendlyName:The friendly name of the column you would like to search on
6. Field:This is the field name as seen in the schema 
7. Order(Optional):The order in which the column are to be displayed
