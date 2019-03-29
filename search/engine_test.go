package search

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"

	"github.com/IMQS/log"
)

// Run these tests with
//  go test github.com/IMQS/search/search -db_postgres -cpu 2
//  go test github.com/IMQS/search/search -db_postgres -cpu 2 -race

var db_postgres = flag.Bool("db_postgres", false, "Run tests against Postgres index")

func conx_index_postgres() *ConfigDatabase {
	return &ConfigDatabase{
		Driver:   "postgres",
		Host:     "localhost",
		Database: "unit_test_search_index",
		User:     "unit_test_user",
		Password: "unit_test_password",
	}
}

func conx_src_postgres(num int) *ConfigDatabase {
	conx := &ConfigDatabase{
		Driver:   "postgres",
		Host:     "localhost",
		User:     "unit_test_user",
		Password: "unit_test_password",
	}
	conx.Database = fmt.Sprintf("unit_test_search_src%v", num)
	return conx
}

func isDBTest() bool {
	return *db_postgres
}

func addField(c *Config, dbname, tablename, fieldname, friendlyname string, minimumTokenLength int, excludeEntireStringFromResult bool) {
	item := &ConfigField{
		Field:                         fieldname,
		FriendlyName:                  friendlyname,
		IsHumanID:                     false,
		MinimumTokenLength:            minimumTokenLength,
		ExcludeEntireStringFromResult: excludeEntireStringFromResult,
	}
	c.Databases[dbname].Tables[tablename].Fields = append(c.Databases[dbname].Tables[tablename].Fields, item)
}

func addRelationship(c *Config, dbname, table1, field1 string, reltype RelType, table2, field2 string) {
	tab, ok := c.Databases[dbname].Tables[table1]
	if !ok {
		panic("table " + dbname + "." + table1 + " does not exist in config")
	}
	tab.Relations = append(tab.Relations, &ConfigRelation{
		Type:         reltype,
		Field:        field1,
		ForeignTable: table2,
		ForeignField: field2,
	})
}

func initDatabases() error {
	// Connect to the "postgres" DB, where we can drop & create new databases
	cfg_postgres := conx_src_postgres(1)
	cfg_postgres.Database = "postgres"
	root, err := sql.Open(cfg_postgres.Driver, cfg_postgres.DSN())
	if err != nil {
		return fmt.Errorf("Unable to connect to database %v: %v", cfg_postgres.DSN(), err)
	}
	defer root.Close()

	root.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %v", conx_index_postgres().Database))

	drop_and_recreate := func(num int) error {
		if _, err := root.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %v", conx_src_postgres(num).Database)); err != nil {
			return fmt.Errorf("Unable to drop database %v: %v", conx_src_postgres(num).Database, err)
		}
		if _, err := root.Exec(fmt.Sprintf("CREATE DATABASE %v OWNER = unit_test_user", conx_src_postgres(num).Database)); err != nil {
			return fmt.Errorf("Unable to create database %v: %v", conx_src_postgres(num).Database, err)
		}
		return nil
	}
	if err := drop_and_recreate(1); err != nil {
		return err
	}
	if err := drop_and_recreate(2); err != nil {
		return err
	}
	return nil
}

func ensureExec(t testing.TB, db *sql.DB, query string) {
	_, err := db.Exec(query)
	if err != nil {
		t.Fatalf("Error executing query %.40v: %v", query, err)
	}
}

func connectToDatabases(t testing.TB) (*sql.DB, *sql.DB) {
	db1_cfg := conx_src_postgres(1)
	db1, err := sql.Open(db1_cfg.Driver, db1_cfg.DSN())
	if err != nil {
		t.Fatalf("Unable to connect to database %v: %v", db1_cfg.DSN(), err)
	}

	db2_cfg := conx_src_postgres(2)
	db2, err := sql.Open(db2_cfg.Driver, db2_cfg.DSN())
	if err != nil {
		t.Fatalf("Unable to connect to database %v: %v", db2_cfg.DSN(), err)
	}
	return db1, db2
}

func setupCorrectness(t testing.TB, e *Engine) {
	db1, db2 := connectToDatabases(t)
	defer db1.Close()
	defer db2.Close()

	setupDb1 := `
	DROP TABLE IF EXISTS meters;
	CREATE TABLE meters (rowid BIGSERIAL PRIMARY KEY, meterid VARCHAR);
	INSERT INTO meters (meterid) VALUES ('abc 123');
	INSERT INTO meters (meterid) VALUES ('abc 456');
	INSERT INTO meters (meterid) VALUES ('xyz 123');
	INSERT INTO meters (meterid) VALUES ('aaa 789x');
	INSERT INTO meters (meterid) VALUES ('xyz 150');    -- Purposefully similar to a pipe

	DROP TABLE IF EXISTS sausages;
	CREATE TABLE sausages (rowid BIGSERIAL PRIMARY KEY, title VARCHAR, meat VARCHAR, farm UUID);
	INSERT INTO sausages (title, meat, farm) VALUES ('wild pork',    'pork',    '850E97AC-BB00-479E-B2B6-17273F934035');
	INSERT INTO sausages (title, meat, farm) VALUES ('wild chicken', 'chicken', '850E97AC-BB00-479E-B2B6-17273F934035');
	INSERT INTO sausages (title, meat, farm) VALUES ('stock pork',   'pork',    '07EFD697-8697-40E7-AC05-62A23BC6A059');
	INSERT INTO sausages (title, meat, farm) VALUES ('stock beef',   'beef',    '07EFD697-8697-40E7-AC05-62A23BC6A059');
	INSERT INTO sausages (title, meat, farm) VALUES ('stock horse',  'horse',   '07EFD697-8697-40E7-AC05-62A23BC6A059');
	INSERT INTO sausages (title, meat, farm) VALUES ('unknown mix',  'chicken', 'BFCF4C71-FA23-4E2F-B8E0-8429AC9560F8');

	DROP TABLE IF EXISTS farms;
	CREATE TABLE farms (rowid BIGSERIAL PRIMARY KEY, id UUID, type VARCHAR);
	INSERT INTO farms (id, type) VALUES ('850E97AC-BB00-479E-B2B6-17273F934035', 'organic free range');
	INSERT INTO farms (id, type) VALUES ('07EFD697-8697-40E7-AC05-62A23BC6A059', 'feed lot');
	INSERT INTO farms (id, type) VALUES ('BFCF4C71-FA23-4E2F-B8E0-8429AC9560F8', 'organic');

	DROP TABLE IF EXISTS sku;
	CREATE TABLE sku (rowid BIGSERIAL PRIMARY KEY, shopname VARCHAR, sausage VARCHAR);
	INSERT INTO sku (shopname, sausage) VALUES ('healthy hipster', 'wild pork');
	INSERT INTO sku (shopname, sausage) VALUES ('healthy hipster', 'wild chicken');
	INSERT INTO sku (shopname, sausage) VALUES ('cheap grub', 'stock pork');
	INSERT INTO sku (shopname, sausage) VALUES ('cheap grub', 'stock beef');
	INSERT INTO sku (shopname, sausage) VALUES ('cheap grub', 'stock horse');
	INSERT INTO sku (shopname, sausage) VALUES ('cheap grub', 'unknown mix');
	INSERT INTO sku (shopname, sausage) VALUES ('piglets', 'wild pork');
	INSERT INTO sku (shopname, sausage) VALUES ('piglets', 'stock pork');
	`
	ensureExec(t, db1, setupDb1)

	setupDb2 := `
	DROP TABLE IF EXISTS roads;
	CREATE TABLE roads (rowid BIGSERIAL PRIMARY KEY, streetname VARCHAR);
	INSERT INTO roads (streetname) VALUES ('idle crescent');
	INSERT INTO roads (streetname) VALUES ('inkvis street');

	DROP TABLE IF EXISTS address;
	CREATE TABLE address (rowid BIGSERIAL PRIMARY KEY, address VARCHAR, postal VARCHAR);
	INSERT INTO address (address, postal) VALUES('6 Omega Crescent', '2309');
	INSERT INTO address (address, postal) VALUES('8 Idle crescent', '2309');
	INSERT INTO address (address, postal) VALUES('20 Omega Crescent', '4309');
	INSERT INTO address (address, postal) VALUES('55 Idle Crescent', '9876');

	DROP TABLE IF EXISTS pipes;
	CREATE TABLE pipes (rowid BIGSERIAL PRIMARY KEY, id VARCHAR, pipename VARCHAR, diameter DOUBLE PRECISION, sparecap DOUBLE PRECISION);
	INSERT INTO pipes (id, pipename, diameter, sparecap) VALUES('p01', 'XYZ', 150, 150); -- Purposefully similar to a meter
	INSERT INTO pipes (id, pipename, diameter, sparecap) VALUES('p02', 'I-74E', 200, 150);
	INSERT INTO pipes (id, pipename, diameter, sparecap) VALUES('p03', 'AAA', 200, 200);
	INSERT INTO pipes (id, pipename, diameter, sparecap) VALUES('p04', 'xyx', 170, 333);
	INSERT INTO pipes (id, pipename, diameter, sparecap) VALUES('p05', 'xyx', 180, 333);
	INSERT INTO pipes (id, pipename, diameter, sparecap) VALUES('p06', 'xyz', 190, 447);
	INSERT INTO pipes (id, pipename, diameter, sparecap) VALUES('p07', 'xyz', 197, 448);

	DROP TABLE IF EXISTS pipe_memo;
	CREATE TABLE pipe_memo (rowid BIGSERIAL PRIMARY KEY, id VARCHAR, material VARCHAR);
	INSERT INTO pipe_memo (id, material) VALUES('p01', 'PVC');
	INSERT INTO pipe_memo (id, material) VALUES('p02', 'alu');
	INSERT INTO pipe_memo (id, material) VALUES('p03', 'alu');
	INSERT INTO pipe_memo (id, material) VALUES('p04', 'PVC');
	INSERT INTO pipe_memo (id, material) VALUES('p05', 'alu');

	DROP TABLE IF EXISTS pipe_result;
	CREATE TABLE pipe_result (rowid BIGSERIAL PRIMARY KEY, id VARCHAR, flow VARCHAR);
	INSERT INTO pipe_result (id, flow) VALUES('p01', 10);
	INSERT INTO pipe_result (id, flow) VALUES('p02', 20);
	INSERT INTO pipe_result (id, flow) VALUES('p03', 30);
	INSERT INTO pipe_result (id, flow) VALUES('p04', 40);
	INSERT INTO pipe_result (id, flow) VALUES('p05', 50);
	INSERT INTO pipe_result (id, flow) VALUES('p06', 60);

	DROP TABLE IF EXISTS empty;
	CREATE TABLE empty (rowid BIGSERIAL PRIMARY KEY, color VARCHAR);
	`
	ensureExec(t, db2, setupDb2)
	cfg := &Config{}
	cfg.Databases = make(map[string]*ConfigDatabase)
	cfg.Databases["index"] = conx_index_postgres()
	cfg.Databases["db1"] = conx_src_postgres(1)
	cfg.Databases["db2"] = conx_src_postgres(2)

	cfg.Databases["db1"].Tables = map[string]*ConfigTable{
		"meters": &ConfigTable{
			IndexField:   ROWID,
			FriendlyName: "meters",
			Keywords:     "meter meters",
		},
		"sausages": &ConfigTable{
			IndexField:   ROWID,
			FriendlyName: "sausages",
		},
		"farms": &ConfigTable{
			IndexField:   ROWID,
			FriendlyName: "farms",
		},
		"sku": &ConfigTable{
			IndexField:   ROWID,
			FriendlyName: "sku",
		},
	}

	cfg.Databases["db2"].Tables = map[string]*ConfigTable{
		"roads": &ConfigTable{
			IndexField:   ROWID,
			FriendlyName: "roads",
			Keywords:     "roads road",
		},
		"address": &ConfigTable{
			IndexField:   ROWID,
			FriendlyName: "address",
			Keywords:     "addresses address stand",
		},
		"pipes": &ConfigTable{
			IndexField:   ROWID,
			FriendlyName: "pipes",
			Keywords:     "pipe",
		},
		"pipe_memo": &ConfigTable{
			IndexField:   ROWID,
			FriendlyName: "pipe memo",
		},
		"pipe_result": &ConfigTable{
			IndexField:   ROWID,
			FriendlyName: "pipe result",
		},
		"empty": &ConfigTable{
			IndexField:   ROWID,
			FriendlyName: "always empty",
		},
	}
	defLen := defaultMinimumTokenLength
	// Add indexed fields
	addField(cfg, "db1", "meters", "meterid", "meter id", defLen, false)
	addField(cfg, "db1", "sausages", "meat", "meat", defLen, false)
	addField(cfg, "db1", "farms", "type", "type", defLen, false)
	addField(cfg, "db1", "sku", "shopname", "shopname", defLen, false)
	addField(cfg, "db2", "roads", "streetname", "street name", defLen, false)
	addField(cfg, "db2", "address", "address", "address", 1, false) // address min token length = 1
	addField(cfg, "db2", "address", "postal", "postal", defLen, false)
	addField(cfg, "db2", "pipes", "pipename", "pipe name", defLen, false)
	addField(cfg, "db2", "pipes", "diameter", "diameter", defLen, false)
	addField(cfg, "db2", "pipes", "sparecap", "space capacity", defLen, false)
	addField(cfg, "db2", "pipe_memo", "material", "material", defLen, false)
	addField(cfg, "db2", "pipe_result", "flow", "flow", defLen, false)
	addField(cfg, "db2", "empty", "color", "color", defLen, false)

	// setup relationships
	addRelationship(cfg, "db1", "sausages", "farm", RelManyToOne, "farms", "id")
	addRelationship(cfg, "db1", "sku", "sausage", RelManyToOne, "sausages", "title")
	addRelationship(cfg, "db2", "pipes", "id", RelOneToOne, "pipe_memo", "id")
	addRelationship(cfg, "db2", "pipes", "id", RelOneToOne, "pipe_result", "id")
	addRelationship(cfg, "db2", "pipe_memo", "id", RelOneToOne, "pipe_result", "id")

	// Test the config validity checks.
	// Add an invalid relationship, which will cause a conflict, because we're adding a reverse
	// relationship which is ManyToOne, but tehre is already a ManyToOne in the opposite direction.
	addRelationship(cfg, "db1", "sausages", "title", RelManyToOne, "sku", "sausage")
	if err := cfg.populateOppositeRelationships(); err == nil {
		t.Error("Expected populateOppositeRelationships to return an error")
	}
	// remove the invalid relationship
	sausageTab := cfg.Databases["db1"].Tables["sausages"]
	sausageTab.Relations = sausageTab.Relations[0 : len(sausageTab.Relations)-1]

	// Config is now good again

	cfg.populateOppositeRelationships()
	cfg.computeTotalOrder()

	//dumpTotalOrder(&e.Config)
	//fmt.Printf("Relationship subjugate graph:\n%s", e.Config.dumpRelationshipGraph())

	// Test some properties of the relationships
	sausages_rel := cfg.tableConfigFromName(MakeTableName("db1", "sausages")).subjugateRelations(cfg.Databases["db1"])
	farms_rel := cfg.tableConfigFromName(MakeTableName("db1", "farms")).subjugateRelations(cfg.Databases["db1"])
	sku_rel := cfg.tableConfigFromName(MakeTableName("db1", "sku")).subjugateRelations(cfg.Databases["db1"])
	//dumpRelations("sausages", sausages_rel)
	//dumpRelations("farms", farms_rel)
	//dumpRelations("sku", sku_rel)
	verifyRelationList(t, "sausages", sausages_rel, "farms")
	verifyRelationList(t, "farms", farms_rel, "")
	verifyRelationList(t, "sku", sku_rel, "sausages")
	if sausages_rel[0].foreignTable.totalOrder == 0 {
		t.Error("Expected totalOrder to be non-zero. It COULD be legitimately zero, but there is a 1/(2^32) chance of that, so make sure.")
	}

	pipes_rel := cfg.tableConfigFromName(MakeTableName("db2", "pipes")).subjugateRelations(cfg.Databases["db2"])
	pipe_memo_rel := cfg.tableConfigFromName(MakeTableName("db2", "pipe_memo")).subjugateRelations(cfg.Databases["db2"])
	pipe_result_rel := cfg.tableConfigFromName(MakeTableName("db2", "pipe_result")).subjugateRelations(cfg.Databases["db2"])
	//dumpRelations("pipes", pipes_rel)
	//dumpRelations("pipe_memo", pipe_memo_rel)
	//dumpRelations("pipe_result", pipe_result_rel)
	//dumpRelationsConfig("pipes", e.Config.Databases["db2"].Tables["pipes"].Relations)
	//dumpRelationsConfig("pipe_result", e.Config.Databases["db2"].Tables["pipe_result"].Relations)

	if !(relationListCheck(t, "pipes", pipes_rel, "pipe_memo,pipe_result") == nil ||
		relationListCheck(t, "pipe_memo", pipe_memo_rel, "pipes,pipe_result") == nil ||
		relationListCheck(t, "pipe_result", pipe_result_rel, "pipes,pipe_memo") == nil) {
		t.Fatalf("Expected one of pipes, pipe_memo, pipe_result to have both others as children")
	}
	e.Config = cfg
}

// This builds upon setupCorrectness, by removing some of the config there
func setupRemoveTableFromIndex(t testing.TB, e *Engine) {
	config := e.GetConfig()
	db1 := config.Databases["db1"]
	db2 := config.Databases["db2"]
	pipes := db2.Tables["pipes"]
	db1.Tables = map[string]*ConfigTable{}
	db2.Tables = map[string]*ConfigTable{
		"pipes": pipes,
	}
	pipes.Relations = []*ConfigRelation{}
}

func setupPerformance(t testing.TB, e *Engine) {
	db1, db2 := connectToDatabases(t)
	defer db1.Close()
	defer db2.Close()

	numRec := int64(100000)

	setupDb1 := `
	CREATE TABLE stands (rowid BIGSERIAL PRIMARY KEY, code VARCHAR);
	INSERT INTO stands (code) (select left(md5(generate_series(1,$NUMREC)::text),5));
	`
	setupDb1 = strings.Replace(setupDb1, "$NUMREC", strconv.FormatInt(numRec, 10), -1)
	ensureExec(t, db1, setupDb1)

	config := &Config{}
	config.Databases = make(map[string]*ConfigDatabase)
	config.Databases["index"] = conx_index_postgres()
	config.Databases["db1"] = conx_src_postgres(1)

	config.Databases["db1"].Tables = map[string]*ConfigTable{
		"stands": &ConfigTable{
			IndexField:   ROWID,
			FriendlyName: "Stands",
			Keywords:     "stand",
		},
	}

	addField(config, "db1", "stands", "code", "Stand Code", 2, false)
	//dump, _ := json.MarshalIndent(e, "", "\t")
	//fmt.Printf("e: %v\n", string(dump))
	e.Config = config
}

func setupUpdateConfig(t testing.TB, e *Engine) {
	db1, _ := connectToDatabases(t)
	defer db1.Close()

	setupDb1 := `
	DROP TABLE IF EXISTS sausages;
	CREATE TABLE sausages (rowid BIGSERIAL PRIMARY KEY, title VARCHAR, meat VARCHAR, farm UUID);
	INSERT INTO sausages (title, meat, farm) VALUES ('wild pork',    'pork',    '850E97AC-BB00-479E-B2B6-17273F934035');
	INSERT INTO sausages (title, meat, farm) VALUES ('wild chicken', 'chicken', '850E97AC-BB00-479E-B2B6-17273F934035');
	INSERT INTO sausages (title, meat, farm) VALUES ('stock pork',   'pork',    '07EFD697-8697-40E7-AC05-62A23BC6A059');
	INSERT INTO sausages (title, meat, farm) VALUES ('stock beef',   'beef',    '07EFD697-8697-40E7-AC05-62A23BC6A059');
	INSERT INTO sausages (title, meat, farm) VALUES ('stock horse',  'horse',   '07EFD697-8697-40E7-AC05-62A23BC6A059');
	INSERT INTO sausages (title, meat, farm) VALUES ('unknown mix',  'chicken', 'BFCF4C71-FA23-4E2F-B8E0-8429AC9560F8');
	`
	ensureExec(t, db1, setupDb1)

	config := &Config{}
	config.Databases = make(map[string]*ConfigDatabase)
	config.Databases["index"] = conx_index_postgres()
	config.Databases["db1"] = conx_src_postgres(1)
	config.Databases["db1"].Tables = map[string]*ConfigTable{}
	e.Config = config
}

func setup(t testing.TB, setupFunc func(testing.TB, *Engine)) *Engine {
	e := &Engine{}
	e.ErrorLog = log.New(log.Stdout)

	e.ConfigFile = "engine-test-search.json"
	// We use to clear the index database here, but removed that once we made
	// the engine automatically clear out items that were no longer indexed
	setupFunc(t, e)
	err := e.GetConfig().postJSONLoad()
	if err != nil {
		t.Fatalf("Error on postJSONLoad: %v", err)
	}
	err = e.Initialize(true)
	if err != nil {
		t.Fatalf("Error on e.Initialize: %v", err)
	}
	err = e.AutoRebuild()
	if err != nil {
		t.Fatalf("Error on e.AutoRebuild: %v", err)
	}
	return e
}

func dumpTotalOrder(c *Config) {
	for dbName, db := range c.Databases {
		for tabName, tab := range db.Tables {
			fmt.Printf("%v.%v = %v\n", dbName, tabName, tab.totalOrder)
		}
	}
}

func dumpRelationsConfig(table string, relations []*ConfigRelation) {
	fmt.Printf("Config relations from %v\n", table)
	for _, rel := range relations {
		fmt.Printf("%v > %v.%v\n", rel.Field, rel.ForeignTable, rel.ForeignField)
	}
}

func dumpRelations(table string, relations []*bakedRelation) {
	fmt.Printf("Relations from %v\n", table)
	for _, rel := range relations {
		fmt.Printf("  %v (order = %v)\n", rel.relation.ForeignTable, rel.foreignTable.totalOrder)
	}
}

func relationListCheck(t testing.TB, table string, relations []*bakedRelation, expect string) error {
	expectList := strings.Split(expect, ",")
	if len(expectList) == 1 && expectList[0] == "" {
		expectList = []string{}
	}
	if len(expectList) != len(relations) {
		return fmt.Errorf("Relationship list length mismatch. Expected %v", expect)
	}
	for _, rel := range relations {
		found := false
		for _, e := range expectList {
			if e == rel.relation.ForeignTable {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("Unexpected relationship from %v to %v", table, rel.relation.ForeignTable)
		}
	}
	return nil
}

func verifyRelationList(t testing.TB, table string, relations []*bakedRelation, expect string) {
	if err := relationListCheck(t, table, relations, expect); err != nil {
		dumpRelations(table, relations)
		t.Fatal(err)
	}
}

// This ensures that all key:value pairs inside 'expect' are present in 'actual'.
// 'actual' is allowed to have other items in it, that are not specified in 'expect'.
// expect is of the form key1:value1,key2:value2,...
func valuesEqual(expect string, actual map[string]string) bool {
	if expect == "" {
		return true
	}
	pairs := strings.Split(expect, ",")
	for _, p := range pairs {
		kv := strings.Split(p, ":")
		if len(kv) != 2 {
			panic("invalid key:value list " + expect)
		}
		if actual[kv[0]] != kv[1] {
			fmt.Printf("Values different (%v): '%v' vs '%v'", kv[0], actual[kv[0]], kv[1])
			return false
		}
	}
	return true
}

func formatResult(res *FindResultRow) string {
	s := fmt.Sprintf("%v %v %v", res.Table, res.Row, res.Rank)
	if len(res.Values) != 0 {
		s += " "
		for k, v := range res.Values {
			s += k + ":" + v + ","
		}
		return s[0 : len(s)-1]
	}
	return s
}

func formatResultList(res []*FindResultRow) string {
	if len(res) == 0 {
		return "<empty>"
	}
	s := []string{}
	for _, r := range res {
		s = append(s, "\""+formatResult(r)+"\"")
	}
	return strings.Join(s, ", ")
}

const (
	qFlagSendJoins = iota + 1
)

// Run a query and verify the results.
// For an explanation of what goes into "items", see existing examples, as well as the error message
// that this function returns when "items" is malformed.
func expectQuery(t *testing.T, e *Engine, flags int, queryString string, items ...string) {
	query := &Query{
		SendRelatedRecords: flags&qFlagSendJoins != 0,
		Query:              queryString,
	}
	config := e.GetConfig()
	res, err := e.Find(query, config)
	expected := ""
	if items != nil {
		quotedItems := make([]string, len(items))
		for i := 0; i < len(items); i++ {
			quotedItems[i] = "\"" + items[i] + "\""
		}
		expected = strings.Join(quotedItems, ", ")
	}

	if err != nil {
		t.Errorf("Error searching for %v: %v", queryString, err)
	}
	same := false
	rows := res.Rows

	if query.SendRelatedRecords {
		// This is nice for dumping the result to the console
		//jv, _ := json.MarshalIndent(res, "", "  ")
		//fmt.Printf("Rows From Join:\n%v\n", string(jv))
	}

	if len(rows) == len(items) {
		same = true
		for i := 0; i < len(rows); i++ {
			same = false
			parts := strings.Split(items[i], " ")
			if len(parts) < 3 {
				t.Fatal("In your expect string, you must provide 'db.table rowid rank key1:value1,key2:value2...'")
			}
			if len(parts) == 3 {
				// Add a dummy empty 'values'
				parts = append(parts, "")
			} else {
				// recombine all the 'values' parts into one string
				parts[3] = strings.Join(parts[3:], " ")
			}
			if rows[i].Table == TableFullName(parts[0]) &&
				strconv.FormatInt(rows[i].Row, 10) == parts[1] &&
				strconv.FormatInt(int64(rows[i].Rank), 10) == parts[2] &&
				valuesEqual(parts[3], rows[i].Values) {
				same = true
			}
		}
	}
	//t.Logf("%v -> %v", queryString, res)
	if !same {
		t.Errorf("Incorrect result:\nQuery: %v\nResult:   %v\nExpected: %v", queryString, formatResultList(rows), expected)
	}
}

func findString(e *Engine, query string) (*FindResult, error) {
	q := &Query{
		SendRelatedRecords: true,
		Query:              query,
	}
	config := e.GetConfig()
	return e.Find(q, config)
}

func TestRank(t *testing.T) {
	if !isDBTest() {
		return
	}
	e := setup(t, setupCorrectness)

	// expect schema is: db.table rowid rank key1:value1,key2:value2...

	// This was a rather subtle bug. The default Postgres collation is not the same as "C" collation,
	// where strings are lexicographically compared on their unicode code point values. '9' + 1 ends up
	// being a colon, and in the default Postgres collation, a colon is LESS than 9.
	expectQuery(t, e, 0, "aaa 789", "db1.meters 4 11 meterid:aaa 789x")

	// Queries without filters
	expectQuery(t, e, 0, "abc 123", "db1.meters 1 15 meterid:abc 123")
	expectQuery(t, e, 0, "abc", "db1.meters 1 5", "db1.meters 2 5")
	expectQuery(t, e, 0, "ink", "db2.roads 2 3 streetname:inkvis street")
	expectQuery(t, e, 0, "roads ink", "db2.roads 2 5 streetname:inkvis street")
	expectQuery(t, e, 0, "6 Omega Crescent", "db2.address 1 15 address:6 Omega Crescent,postal:2309", "db2.address 3 5 address:20 Omega Crescent,postal:4309")
	expectQuery(t, e, 0, "pipe 150", "db2.pipes 1 7 pipename:XYZ,diameter:150,sparecap:150", "db2.pipes 2 7 pipename:I-74E,diameter:200,sparecap:150")
	expectQuery(t, e, 0, "pipe diameter 150", "db2.pipes 1 9 pipename:XYZ,diameter:150,sparecap:150")
	expectQuery(t, e, 0, "74e", "db2.pipes 2 5 pipename:I-74E,diameter:200,sparecap:150")
	expectQuery(t, e, 0, "i-74e", "db2.pipes 2 10 pipename:I-74E,diameter:200,sparecap:150")
	expectQuery(t, e, 0, "nonexistent")
	expectQuery(t, e, 0, "150", "db1.meters 5 5 meterid:xyz 150", "db2.pipes 1 5 sparecap:150,pipename:XYZ,diameter:150", "db2.pipes 2 5 pipename:I-74E,diameter:200,sparecap:150")

	// Invalid filtered query (field does not exist)
	_, err := findString(e, "idle <fields:db2.address.notafield>")
	if err == nil || strings.Index(err.Error(), msgFieldNotConfigured) != 0 {
		t.Errorf("Expected error %v, but got %v", msgFieldNotConfigured, err)
	}
	_, err = findString(e, "idle <pairs:db2.address.notafield=b>")
	if err == nil || strings.Index(err.Error(), msgFieldNotConfigured) != 0 {
		t.Errorf("Expected error %v, but got %v", msgFieldNotConfigured, err)
	}

	// Queries with Filters (aka special instructions)

	// The same query is executed above, but without the filter. Here we only produce one record, instead of the three above.
	expectQuery(t, e, 0, "150 <fields:db2.pipes.diameter>", "db2.pipes 1 5 pipename:XYZ,diameter:150,sparecap:150")

	// Test <pairs> instructions
	expectQuery(t, e, 0, "aa <pairs:db2.pipes.diameter=200>", "db2.pipes 3 3 sparecap:200,pipename:AAA,diameter:200")
	expectQuery(t, e, 0, "xy <pairs:db2.pipes.diameter=150>", "db2.pipes 1 3 pipename:XYZ,diameter:150,sparecap:150")
	expectQuery(t, e, 0, "xy <pairs:db2.pipes.diameter=150,db2.pipes.diameter=170>", "db2.pipes 1 3 pipename:XYZ,diameter:150,sparecap:150", "db2.pipes 4 3 pipename:xyx,diameter:170,sparecap:333")
	expectQuery(t, e, 0, "xy <pairs:db2.pipes.diameter=~19>", "db2.pipes 6 3 pipename:xyz,diameter:190,sparecap:447", "db2.pipes 7 3 pipename:xyz,diameter:197,sparecap:448")
	expectQuery(t, e, 0, "xy <pairs:db2.pipes.diameter=~19,db2.pipes.sparecap=~47>", "db2.pipes 6 3 pipename:xyz,diameter:190,sparecap:447", "db2.pipes 7 3 pipename:xyz,diameter:197,sparecap:448")
	expectQuery(t, e, 0, "aa <pairs:db2.pipes.diameter=~200>", "db2.pipes 3 3 sparecap:200,pipename:AAA,diameter:200")
	expectQuery(t, e, 0, "Aa <pairs:db2.pipes.diameter=~20>", "db2.pipes 3 3 sparecap:200,pipename:AAA,diameter:200")
	expectQuery(t, e, 0, "Aa <pairs:db2.pipes.diameter=20>")
	expectQuery(t, e, 0, "150 <fields:db2.pipes.sparecap,db2.pipes.diameter> <pairs:db2.pipes.diameter=~2>", "db2.pipes 2 5 pipename:I-74E,diameter:200,sparecap:150")
	expectQuery(t, e, 0, "<pairs:db2.pipes.diameter=~2>", "db2.pipes 2 0 pipename:I-74E,diameter:200,sparecap:150", "db2.pipes 3 0 pipename:AAA,diameter:200,sparecap:200")
	expectQuery(t, e, 0, "<pairs:db2.pipes.diameter=2>")

	// Test the conditions specified in doc.go under "Ambiguity caused by 'fields' and 'pairs' instructions"

	// We have explicitly specified that we want to search only in field 'sparecap', so we get zero results.
	expectQuery(t, e, 0, "aa <fields:db2.pipes.sparecap> <pairs:db2.pipes.diameter=200>")

	// Now we explicitly specify 'pipename', and we get back a single result
	expectQuery(t, e, 0, "aa <fields:db2.pipes.pipename> <pairs:db2.pipes.diameter=200>", "db2.pipes 3 3 sparecap:200,pipename:AAA,diameter:200")

	// Mention an explicit field in a different table to our 'pairs' constraint. This has no effect - we still search
	// all fields belonging to the pairs constraint. The rule here is that, since no fields were explicitly mentioned
	// for the 'pairs' table, we search all fields of the 'pairs' table.
	expectQuery(t, e, 0, "aa <fields:db1.meters.meterid> <pairs:db2.pipes.diameter=200>", "db2.pipes 3 3 sparecap:200,pipename:AAA,diameter:200")

	// Only a <pairs> instruction, and no tokens
	expectQuery(t, e, 0, "<pairs:db2.pipes.diameter=200>", "db2.pipes 2 0 pipename:I-74E,diameter:200,sparecap:150", "db2.pipes 3 0 diameter:200,sparecap:200,pipename:AAA")

	// Test relational queries
	expectQuery(t, e, 0, "xyx PVC", "db2.pipes 4 10 pipename:xyx,diameter:170,sparecap:333")
	expectQuery(t, e, 0, "xyx ALU", "db2.pipes 5 10 pipename:xyx,diameter:180,sparecap:333")
	expectQuery(t, e, 0, "ALU xyx", "db2.pipes 5 10 pipename:xyx,diameter:180,sparecap:333")
	expectQuery(t, e, 0, "xyx ALU 40")
	expectQuery(t, e, 0, "xyx PVC 40", "db2.pipe_result 4 15 flow:40")

	// Relation queries that also send back related records
	expectQuery(t, e, qFlagSendJoins, "xyx PVC", "db2.pipes 4 10 pipename:xyx,diameter:170,sparecap:333", "db2.pipe_result 4 0 flow:40", "db2.pipe_memo 4 0 material:PVC")

	// Same as above, but don't include tokens from the resulting table.
	// This stresses a slightly different code path to the above case.
	expectQuery(t, e, qFlagSendJoins, "I-74E", "db2.pipes 2 10 pipename:I-74E,diameter:200,sparecap:150", "db2.pipe_result 2 0 flow:20", "db2.pipe_memo 2 0 material:alu")

	e.Close()
}

func TestRowCountMetadata(t *testing.T) {
	if !isDBTest() {
		return
	}
	e := setup(t, setupCorrectness)
	config := e.GetConfig()

	config.updateHttpApiConfig(e)
	rc := config.Databases["db2"].Tables["pipes"].RowCount
	// If RowCount was not just 0 or 1, we'd expect a value of 5 here.
	if rc != 1 {
		t.Errorf("RowCount of 'pipes' is not correct. expected %v. actual = %v.", 1, rc)
	}

	rc = config.Databases["db2"].Tables["empty"].RowCount
	if rc != 0 {
		t.Errorf("RowCount of 'empty' is not correct. expected %v. actual = %v.", 0, rc)
	}

	e.Close()
}

func TestRemoveFromIndex(t *testing.T) {
	if !isDBTest() {
		return
	}
	e := setup(t, setupCorrectness)

	// A query copied from TestRank
	expectQuery(t, e, 0, "abc 123", "db1.meters 1 15 meterid:abc 123")

	// Wipe most of our config (except for "pipes")
	setupRemoveTableFromIndex(t, e)
	e.AutoRebuild()

	// Same query as above, but due to removing config for db1, this should no longer yield any results
	expectQuery(t, e, 0, "abc 123")

	// This is the one piece of data that we do leave configured
	expectQuery(t, e, 0, "i-74e", "db2.pipes 2 10 pipename:I-74E,diameter:200,sparecap:150")

	e.Close()
}

func setupBuildIndex(t testing.TB, e *Engine) {
	db1, db2 := connectToDatabases(t)
	defer db1.Close()
	defer db2.Close()
	setupDb1 := `
	DROP TABLE IF EXISTS drainage;
	CREATE TABLE drainage (rowid BIGSERIAL PRIMARY KEY, descr VARCHAR, wwtp VARCHAR, system VARCHAR);
	INSERT INTO drainage (descr, wwtp, system) VALUES ('Monaghan X5 PS', 'Monaghan WWTW', 'Monaghan');
	INSERT INTO drainage (descr, wwtp, system) VALUES ('Monaghan X4 PS', 'Monaghan WWTW', 'Monaghan');
	INSERT INTO drainage (descr, wwtp, system) VALUES ('Monaghan X3 PS', 'Monaghan WWTW', 'Monaghan');
	INSERT INTO drainage (descr, wwtp, system) VALUES ('Monaghan X2 PS', 'Monaghan WWTW', 'Monaghan');
	INSERT INTO drainage (descr, wwtp, system) VALUES ('Monaghan X1 PS', null, 'Monaghan');
	`
	ensureExec(t, db1, setupDb1)

	config := &Config{}
	config.Databases = make(map[string]*ConfigDatabase)
	config.Databases["index"] = conx_index_postgres()
	config.Databases["db1"] = conx_src_postgres(1)

	config.Databases["db1"].Tables = map[string]*ConfigTable{
		"drainage": &ConfigTable{
			IndexField:   ROWID,
			FriendlyName: "Drainage",
			Keywords:     "Sewer Drainage",
		},
	}

	addField(config, "db1", "drainage", "descr", "Description", 2, false)
	addField(config, "db1", "drainage", "wwtp", "WWTP", 2, false)
	addField(config, "db1", "drainage", "system", "System", 2, false)

	e.Config = config
}
func TestBuildIndex(t *testing.T) {
	if !isDBTest() {
		return
	}
	e := setup(t, setupBuildIndex)

	testTable := []TableFullName{"db1.drainage"}

	err := e.BuildIndex(testTable, false)
	if err != nil {
		t.Errorf("Error indexing table: %v", err)
	}

	e.Close()
}
func TestConcurrency(t *testing.T) {
	if !isDBTest() {
		return
	}
	e := setup(t, setupCorrectness)

	done := make(chan bool)

	run := func() {
		for i := 0; i < 50; i++ {
			expectQuery(t, e, 0, "abc 123", "db1.meters 1 15 meterid:abc 123")
			expectQuery(t, e, 0, "ink", "db2.roads 2 3 streetname:inkvis street")
		}

		done <- true
	}

	nthread := 2

	for i := 0; i < 20; i++ {
		for j := 0; j < nthread; j++ {
			go run()
		}

		for j := 0; j < nthread; j++ {
			<-done
		}

		// Clear all cached state, so that we are more likely to catch race conditions that create
		// this cached state as the program runs.
		e.clearInMemoryCaches()
	}

	e.Close()
}

var updateConfigTestData = `[{"name": "sausages", "table": {"friendlyName": "sausages", "feyWords": "sausages",
	"keywords": "sausages","fields": [{"friendlyName": "title","field": "title","isHumanID": true},{"friendlyName": "meat",
	"field": "meat","isHumanID": true},{"friendlyName": "farm","field": "farm","isHumanID": true}]}}]`

func TestUpdateConfig(t *testing.T) {
	if !isDBTest() {
		return
	}
	e := setup(t, setupUpdateConfig)

	dbParam := httprouter.Param{Key: "database", Value: "db1"}
	reqParams := make([]httprouter.Param, 0)
	reqParams = append(reqParams, dbParam)

	configData := bytes.NewReader([]byte(updateConfigTestData))

	updateRequest := httptest.NewRequest("PUT", "http://localhost/search/config", configData)
	updateW := httptest.NewRecorder()

	// Update config
	httpConfigSet(e, updateW, updateRequest, reqParams)
	updateResponse := updateW.Result()

	// Check response status code, 200 means everything went well
	if updateResponse.StatusCode != http.StatusOK {
		t.Errorf("Error updating config, expected %v, actual = %v", http.StatusOK, updateResponse.StatusCode)
	}
	fmt.Println("Search config PUT status code", updateResponse.StatusCode)

	getRequest := httptest.NewRequest("GET", "http://localhost/search/config", nil)
	getW := httptest.NewRecorder()

	// Get config
	httpConfigGet(e, getW, getRequest, nil)
	getResponse := getW.Result()

	if _, err := ioutil.ReadAll(getResponse.Body); err != nil {
		t.Errorf("Error getting body of updated config: %v", err)
	}

	contentType := getResponse.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Error getting updated config content type, expected %v, actual = %v", "application/json", contentType)
	}
	eTag := getResponse.Header.Get("ETag")
	if len(eTag) <= 0 {
		t.Errorf("Error getting updated config")
	}

	fmt.Println("Search config GET status code", getResponse.StatusCode)

	updatedConfig := e.GetConfig()
	rc := updatedConfig.Databases["db1"].Tables["sausages"].RowCount
	if rc != 1 {
		t.Errorf("RowCount of 'sausages' is not correct. expected %v. actual = %v.", 1, rc)
	}

	e.Close()
}
func TestIndexPerformance(t *testing.T) {
	if !isDBTest() {
		return
	}
	t.Logf("Populating test database")
	e := setup(t, setupPerformance)
	//b.ResetTimer()
	n := 2000
	start := time.Now()
	timeQueryParse := time.Duration(0)
	timeDBQuery := time.Duration(0)
	timeDBFetch := time.Duration(0)
	timeAuxFetch := time.Duration(0)
	timeKeywords := time.Duration(0)
	timeSort := time.Duration(0)
	timeOther := time.Duration(0)
	rowsProcessed := int64(0)
	t.Logf("Performing %v queries", n)
	for i := 0; i < n; i++ {
		res, err := findString(e, strconv.FormatInt(int64(i%1000), 10))
		timeQueryParse += res.TimeQueryParse
		timeDBQuery += res.TimeDBQuery
		timeDBFetch += res.TimeDBFetch
		timeAuxFetch += res.TimeAuxFetch
		timeKeywords += res.TimeKeywords
		timeSort += res.TimeSort
		timeOther += res.TimeTotal - res.TimeDBQuery - res.TimeDBFetch - res.TimeAuxFetch - res.TimeKeywords - res.TimeSort
		rowsProcessed += res.RawDBRowCount
		if err != nil {
			t.Fatalf("Find error during benchmark: %v", err)
		}
	}
	msPerQuery := time.Now().Sub(start).Seconds() * 1000 / float64(n)
	msPerQueryParse := timeQueryParse.Seconds() * 1000 / float64(n)
	msPerQueryDBQuery := timeDBQuery.Seconds() * 1000 / float64(n)
	msPerQueryDBFetch := timeDBFetch.Seconds() * 1000 / float64(n)
	msPerQueryAuxFetch := timeAuxFetch.Seconds() * 1000 / float64(n)
	msPerQueryKeywords := timeKeywords.Seconds() * 1000 / float64(n)
	msPerQuerySort := timeSort.Seconds() * 1000 / float64(n)
	msPerQueryOther := timeOther.Seconds() * 1000 / float64(n)
	t.Logf("%.2f ms per query (%.2f parse, %.2f DB Query, %.2f DB Fetch, %.2f Aux Fetch, %.2f keywords, %.2f sort, %.2f other)\n", msPerQuery, msPerQueryParse, msPerQueryDBQuery, msPerQueryDBFetch, msPerQueryAuxFetch, msPerQueryKeywords, msPerQuerySort, msPerQueryOther)
	t.Logf("DB rows processed per query: %.0f\n", float64(rowsProcessed)/float64(n))
	if msPerQuery > 5 {
		t.Errorf("Expected around 2.5 milliseconds per query, but instead, average query time is %.3f milliseconds", msPerQuery)
	}

	e.Close()
}

func TestConfig(t *testing.T) {
	var rel ConfigRelation
	json.Unmarshal([]byte(`{"Type": "ManyToOne" }`), &rel)
	if rel.Type != RelManyToOne {
		t.Error("ConfigRelation failed to parse")
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	if !isDBTest() {
		fmt.Printf("Not testing anything, because no backend database specified. See top of engine_test.go\n")
		os.Exit(1)
	}

	if err := initDatabases(); err != nil {
		fmt.Printf("Error setting up databases:\n%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(m.Run())
	}
}
