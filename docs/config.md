How to configure search bar to be able to search on more categories.
Directions:
*First decide what you what to search on e.g Facility Name
*Then go find the appropriate table in the DB that contains the column you want to search on
*Then go into your docker-compose folder and open the search.json file
*Inside search.json you will notice the DatabaseNames e.g ImqsServerMain followed by all the tables
	we want to search on below it.The same goes for ImqsServerMirror,ImqsServerTemp etc.
*Find withing the search.json file the db that your table with your column in it belongs to.If it
	isnt listed you may add a new entry for your db.
*Underneath the dbName you will notice a "Tables" tag.
*It is in here that you type your table name e.g AssetComponent
*Then comes the "Categories" label,under here you type the Category that your search belongs to e.g Assets,Water,Esri Electricity
*After the caories tag is "Friendly Name",this is where you type the friendly name of the column you wish to search on.
*Next is "Fields" these are the fields you are going to be searching on
	-Each entry in "Fields" has 2-3 attibutes:
		-"FriendlyName":This is the friendly name of your field
		-"Field":This is the field name as seen in the schema 
		-"Order"(Optional):The order in which the column are to be displayed
		-"IsHumanID"(Optional):This is a boolean value
*Lastly underneath "Fields" you can add "Relations" 
	-Relations has 4 attributes
		-"Type":E.g OneToMany ,OneToOne etc.
		-"Field":This is the field they are joined on in the primary table e.g "ID"
		-"ForeignTable":The name of the foreign table
		-'ForeignField":The name of the column in the foreign table that the join is made on
