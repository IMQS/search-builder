package server

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	serviceconfig "github.com/IMQS/serviceconfigsgo"
	"github.com/pierrec/xxHash/xxHash32"
)

/*
Sample config

The database called "index" is special, and must be present.

If you don't specify logfiles, then stderr is used for the Error log,
and stdout is used for the Access log.

{
	"DisableAutoIndexRebuild": false,
	"HTTP": {
		"Bind": "",
		"Port": 2007
	},
	"Log": {
		"ErrorFile": "/var/log/imqs-search/error.log",
		"AccessFile": "/var/log/imqs-search/access.log"
	},
	"Databases": {
		"index": {
			"Driver":        "postgres",
			"Host":          "127.0.0.1",
			"Database":      "searchindex",
			"User":          "imqs",
			"Password":      "password"
			"MaxIdleConns":  4,                            -- Applies only to index DB
			"MaxOpenConns":  16                            -- Applies only to index DB
		},
		"main": {
			"Driver":        "postgres",
			"Host":          "127.0.0.1",
			"Database":      "main",
			"User":          "imqs",
			"Password":      "password",
			"Tables": {
				"WaterDemandMeterWater": {
					"Name": "Water Meter",
					"Keywords": "Water Meter Bill",
					"Fields": [
						{
							"Field": "Meter_ID",
							"FriendlyName": "Meter ID",
							"HumanID": true
						}
					]

					---  Everything below here does not affect the search engine; ---
					---  It is used by our front-end only.                        ---

					"Categories": [ "Water", "Billing" ]
					"DisplayedFields": [ "Meter_ID", "Stand_ID", "Owner" ],
					"Types": {
						"WiFi": "Type=WiFi",
						"Plastic": "Type=Pla",
						"Copper": "Type=Cu"
					}
				},
				"WaterPipe_04": {
					"Name": "Water Pipes",
					"Fields": [
						{
							"Field": "Diameter",
							"FriendlyName": "Diameter",
							"Order": "3"
						}
					],
					"Relations": [
						{
							"Type": "OneToOne",
							"Field": "ID",
							"ForeignTable": "WaterPipeMemo_04",
							"ForeignField": "ID"
						}
					]
				},
				"WaterPipeMemo_04": {
					"Name": "Water Pipes",
					"Fields": [
						{
							"Field": "Material",
							"FriendlyName": "Material",
							"Order": "1"
						}
					]
				}
			}
		}
	}
}
*/

const (
	indexDatabaseName   = "index"
	genericDatabaseName = "ImqsServerGeneric"
)

// Use load() and store() to read and write to an atomicTriState variable.
type atomicTriState uint32

const (
	atomicTriState_Nil   atomicTriState = 0
	atomicTriState_False                = 1
	atomicTriState_True                 = 2
)

func (a *atomicTriState) store(state atomicTriState) {
	atomic.StoreUint32((*uint32)(a), uint32(state))
}

func (a *atomicTriState) load() atomicTriState {
	return atomicTriState(atomic.LoadUint32((*uint32)(a)))
}

type RelType int

const (
	RelOneToOne RelType = iota
	RelOneToMany
	RelManyToOne
)

const (
	serviceConfigFileName = "search.json"
	serviceConfigVersion  = 1
	serviceName           = "ImqsSearch"
)

func (r RelType) Opposite() RelType {
	switch r {
	case RelOneToOne:
		return RelOneToOne
	case RelOneToMany:
		return RelManyToOne
	case RelManyToOne:
		return RelOneToMany
	}
	panic("Unimplemented RelType.Opposite")
}

func (r *RelType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("Relationship type must be a string. %v", err)
	}

	if rt, ok := map[string]RelType{"OneToOne": RelOneToOne, "OneToMany": RelOneToMany, "ManyToOne": RelManyToOne}[s]; !ok {
		return fmt.Errorf("Invalid relationship type '%v'. Valid types are OneToOne, OneToMany, ManyToOne", s)
	} else {
		*r = rt
		return nil
	}
}

type ConfigHttp struct {
	Bind string
	Port string
}

type ConfigLog struct {
	ErrorFile  string
	AccessFile string
}

type ConfigField struct {
	Field                         string
	FriendlyName                  string
	Order                         string
	IsHumanID                     bool
	MinimumTokenLength            int
	ExcludeEntireStringFromResult bool `json:",omitempty"`

	// Cached state
	tokenizedName []string
}

type ConfigRelation struct {
	Type         RelType
	Field        string
	ForeignTable string
	ForeignField string
}

func (c *ConfigRelation) Equals(other *ConfigRelation) bool {
	return c.Type == other.Type && c.Field == other.Field && c.ForeignTable == other.ForeignTable && c.ForeignField == other.ForeignField
}

func (c *ConfigRelation) signature() string {
	return fmt.Sprintf("%v:%v:%v:%v", c.Type, c.Field, c.ForeignTable, c.ForeignField)
}

type bakedRelation struct {
	relation        *ConfigRelation
	foreignTable    *ConfigTable
	useInSearchJoin bool // True if this relation is walked when performing result row ranking
}

type bakedRelationByOrder []*bakedRelation

func (a bakedRelationByOrder) Len() int      { return len(a) }
func (a bakedRelationByOrder) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a bakedRelationByOrder) Less(i, j int) bool {
	return a[i].foreignTable.totalOrder < a[j].foreignTable.totalOrder
}

type ConfigTable struct {
	FriendlyName string
	Keywords     string
	IndexField   string
	Fields       []*ConfigField
	Relations    []*ConfigRelation

	// Non-functional state. Maintained purely for the sake of the front-end.
	Categories      []string          `json:",omitempty"`
	DisplayedFields []string          `json:",omitempty"`
	Types           map[string]string `json:",omitempty"`
	// Originally, this was intended to be the exact row count of the table.
	// However, doing an actual row count is slow on a cold DB, and can make
	// our startup sequence so long that the service manager believes we've timed out.
	// For this reason, we only do an EXISTS query, so that we know only whether the
	// table has zero rows or non-zero rows. The consumer of this API is only concerned
	// with RowCount != 0, so we're OK in that sense. However, it is unfortunate that
	// we are lying here. In the interest of backward compatibility, I'm keeping this
	// member's name RowCount.
	// This is populated by updateRowCounts()
	RowCount int64

	// Cached state
	tokenizedKeywords []string
	hasGeometry       atomicTriState
	totalOrder        uint32 // Total ordering for the sake of the table relationship graph. Only unique among tables of a particular database.
}

// Returns the index of the given field, or -1 if the field does not exist
func (c *ConfigTable) fieldIndex(fieldName string) int {
	for i := 0; i < len(c.Fields); i++ {
		if c.Fields[i].Field == fieldName {
			return i
		}
	}
	return -1
}

// Returns the named field, or nil if it does not exist
func (c *ConfigTable) findField(fieldName string) *ConfigField {
	for i := 0; i < len(c.Fields); i++ {
		if c.Fields[i].Field == fieldName {
			return c.Fields[i]
		}
	}
	return nil
}

// Finds the relationship that points back to the given table, or nil if no such relationship exists
func (c *ConfigTable) findRelationship(table string) *ConfigRelation {
	for _, rel := range c.Relations {
		if rel.ForeignTable == table {
			return rel
		}
	}
	return nil
}

// Return the list of subjugate relations for this table. The subjugates are those specified
// in the main documentation under "tuple ordering".
func (c *ConfigTable) subjugateRelations(db *ConfigDatabase) []*bakedRelation {
	subs := []*bakedRelation{}
	for _, rel := range c.Relations {
		foreignTable := db.Tables[rel.ForeignTable]
		include := rel.Type == RelManyToOne || rel.Type == RelOneToOne
		if include {
			subs = append(subs, &bakedRelation{
				relation:        rel,
				foreignTable:    foreignTable,
				useInSearchJoin: !(rel.Type == RelOneToOne && foreignTable.totalOrder < c.totalOrder),
			})
		}
	}

	// Important that we sort by total order, so that we have a stable ordering
	sort.Sort(bakedRelationByOrder(subs))

	return subs
}

type ConfigDatabase struct {
	Driver   string `json:",omitempty"`
	Host     string `json:",omitempty"`
	Database string `json:",omitempty"`
	User     string `json:",omitempty"`
	Password string `json:",omitempty"`
	Port     uint16 `json:",omitempty"`
	Tables   map[string]*ConfigTable

	// These two settings apply only to the index database, because it doesn't make sense
	// to throttle the single connections that we make to our source databases.
	MaxIdleConns int `json:",omitempty"`
	MaxOpenConns int `json:",omitempty"`
}

func (c *ConfigDatabase) DSN() string {
	conStr := fmt.Sprintf("host=%v user=%v password=%v dbname=%v sslmode=disable", c.Host, c.User, c.Password, c.Database)
	if c.Port != 0 {
		conStr += fmt.Sprintf(" port=%v", c.Port)
	}
	return conStr
}

func (c *ConfigDatabase) tableNames() []string {
	names := []string{}
	for name, _ := range c.Tables {
		names = append(names, name)
	}
	return names
}

func (c *ConfigDatabase) tablesOrderedByName() []string {
	names := c.tableNames()
	sort.Strings(names)
	return names
}

type configHttpAPI struct {
	Databases map[string]*ConfigDatabase
}

type Config struct {
	DisableAutoIndexRebuild bool // This was introduced for servers running on hard drives (i.e. not solid state), so that index rebuilds can be scheduled at off-peak times
	HTTP                    ConfigHttp
	Log                     ConfigLog
	Databases               map[string]*ConfigDatabase

	// We cache this, so that we only need to generate it once.
	// This is generated by postJSONLoad(), and thereafter updated
	// by StartStateWatcher(). Always use httpAPIConfigLock when
	// accessing httpAPIConfig
	httpAPIConfig     string
	httpAPIConfigHash string // This is used as the HTTP ETag
	httpAPIConfigLock sync.RWMutex
}

func (c *Config) tableConfigHash(table TableFullName) string {
	db := c.Databases[table.DBOnly()]
	tab := c.tableConfigFromName(table)

	// This is altered whenever system-level changes are made,
	// which do not make their way into the signature elements below.
	seed := "0"

	fields := sort.StringSlice{}
	for _, f := range tab.Fields {
		fields = append(fields, fmt.Sprintf("%v:%v:%v", f.Field, f.MinimumTokenLength, f.ExcludeEntireStringFromResult))
	}
	fields.Sort()
	fieldsBlob := strings.Join(fields, "|")

	relationBlob := ""
	relations := tab.subjugateRelations(db)
	for _, rel := range relations {
		relationBlob += fmt.Sprintf("%v:%v ", rel.foreignTable.totalOrder, rel.relation.signature())
	}

	hash := sha1.Sum([]byte(seed + ";" + fieldsBlob + ";" + relationBlob))
	// Only use the first 8 bytes of the hash. This is purely here to produce shorter stamps.
	// Since we only need to distinguish between 2 different states (is Has Signature Changed?),
	// 8 bytes is plenty fine.
	return hex.EncodeToString(hash[:8])
}

// Returns (DBName, DB) which owns the given table, or ("", nil) otherwise.
func (c *Config) getDatabaseFromTable(table *ConfigTable) (string, *ConfigDatabase) {
	for dbName, db := range c.Databases {
		for _, tab := range db.Tables {
			if tab == table {
				return dbName, db
			}
		}
	}
	return "", nil
}

func (c *Config) tableConfigFromName(table TableFullName) *ConfigTable {
	db := table.DBOnly()
	tab := table.TableOnly()
	if c.Databases[db] == nil {
		// This situation (and also Tables[tab] = nil) occurs when a table is removed from our index
		return nil
	}
	return c.Databases[db].Tables[tab]
}

func (c *Config) getTableHumanIDsFromName(table TableFullName) []string {
	fields := c.tableConfigFromName(table).Fields
	humanIDs := []string{}
	for _, field := range fields {
		if field.IsHumanID {
			humanIDs = append(humanIDs, field.FriendlyName)
		}
	}
	return humanIDs
}

// Returns the set of all tables and fields that are referenced. The tables
// are "full table names", ie "database.table", but the fields are just the pure
// field names, ie "field". This is used to flush garbage out of search_nametables,
// because that table has a limited size of 64k.
func (c *Config) allTableAndFieldNames() map[string]bool {
	all := map[string]bool{}
	for dbName, db := range c.Databases {
		for tableName, table := range db.Tables {
			all[string(MakeTableName(dbName, tableName))] = true
			for _, field := range table.Fields {
				all[field.Field] = true
			}
		}
	}
	return all
}

func (c *Config) allTables() []TableFullName {
	all := []TableFullName{}
	for dbName, db := range c.Databases {
		for tableName, _ := range db.Tables {
			all = append(all, MakeTableName(dbName, tableName))
		}
	}
	return all
}

func (c *Config) databasesOrderedByName() []string {
	names := []string{}
	for name, _ := range c.Databases {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (c *Config) dumpRelationshipGraph() string {
	s := ""
	dbNames := c.databasesOrderedByName()
	for _, dbName := range dbNames {
		db := c.Databases[dbName]
		s += fmt.Sprintf("%v\n", dbName)
		tabNames := db.tablesOrderedByName()
		for _, tabName := range tabNames {
			tab := db.Tables[tabName]
			s += fmt.Sprintf("  %v (%v)\n", tabName, tab.totalOrder)
			for _, sub := range tab.subjugateRelations(db) {
				s += fmt.Sprintf("    %v -> %v.%v (%v)\n", sub.relation.Field, sub.relation.ForeignTable, sub.relation.ForeignField, sub.useInSearchJoin)
			}
		}
	}
	return s
}

func (c *Config) postJSONLoad() error {
	parser := NewDefaultParser()
	for _, vD := range c.Databases {
		for _, vT := range vD.Tables {
			if len(vT.Fields) > maxFieldsPerTable {
				return errTooManyIndexedFields
			}
			for _, field := range vT.Fields {
				if field.MinimumTokenLength == 0 {
					field.MinimumTokenLength = defaultMinimumTokenLength
				}
				field.tokenizedName = parser.Tokenize(field.FriendlyName)
			}
			vT.tokenizedKeywords = parser.Tokenize(vT.FriendlyName + " " + vT.Keywords)
		}
	}

	if err := c.populateOppositeRelationships(); err != nil {
		return err
	}

	// Verify that fields are not specified more than once.
	for _, db := range c.Databases {
		for tabName, tab := range db.Tables {
			seenField := map[string]bool{}
			for _, field := range tab.Fields {
				if seenField[field.Field] {
					return fmt.Errorf("Field is specified more than once: %v.%v", tabName, field.Field)
				}
				seenField[field.Field] = true
			}
		}
	}

	c.computeTotalOrder()

	return nil
}

// Refresh the Configuration that we send back over the HTTP API. This is subtly different
// from the configuration that we read off disk. It has passwords removed, and it has
// some more metadata baked in.
func (c *Config) updateHttpApiConfig(e *Engine) {
	err := c.updateRowCounts(e)
	if err != nil {
		e.ErrorLog.Errorf("Error updating row counts: %v", err)
	}

	c.httpAPIConfigLock.Lock()
	defer c.httpAPIConfigLock.Unlock()

	c.httpAPIConfig, err = c.generateConfigForHttpAPI()
	if err != nil {
		e.ErrorLog.Errorf("Error generating config for HTTP API: %v", err)
	}
	hash := sha1.Sum([]byte(c.httpAPIConfig))
	c.httpAPIConfigHash = hex.EncodeToString(hash[:8])
}

func (c *Config) populateOppositeRelationships() error {
	for _, db := range c.Databases {
		for tabName, tab := range db.Tables {
			for _, rel := range tab.Relations {

				// We cannot verify the field names, because the relationship field names do not need to be indexed.
				// In fact, they are typically not indexed, because they're often internal IDs.

				xTab, ok := db.Tables[rel.ForeignTable]
				if !ok {
					return fmt.Errorf("Related table %s not found (in relationships of %s)", rel.ForeignTable, tabName)
				}

				opposite := &ConfigRelation{
					Type:         rel.Type.Opposite(),
					Field:        rel.ForeignField,
					ForeignTable: tabName,
					ForeignField: rel.Field,
				}

				xRel := xTab.findRelationship(tabName)
				if xRel != nil {
					// Verify the other side
					if !xRel.Equals(opposite) {
						return fmt.Errorf("Table relationships conflict between %s and %s", tabName, rel.ForeignTable)
					}
				} else {
					// Add the other side
					xTab.Relations = append(xTab.Relations, opposite)
				}
			}
		}
	}

	// We must perform the "max one edge between nodes" check after doing all the fixups,
	// so that we have a clean bi-directional graph to work with.
	for _, db := range c.Databases {
		for tabName, tab := range db.Tables {
			if len(tab.Relations)+1 > maxRowsInTuple {
				return fmt.Errorf("Table %s has too many relationships (%v). Max relationships = %v", tabName, len(tab.Relations), maxRowsInTuple-1)
			}
			hasRelationTo := map[string]bool{}
			for _, rel := range tab.Relations {
				if hasRelationTo[rel.ForeignTable] {
					return fmt.Errorf("Table %s has more than one relationship to %s (there may be only one relationship between each table)", tabName, rel.ForeignTable)
				}
				hasRelationTo[rel.ForeignTable] = true
			}
		}
	}

	return nil
}

// Compute a total ordering across all tables within a database.
// We don't need to make the order unique across databases, because tables cannot be related to
// tables from another database. We could in theory do that, but so far we have not had the need.
// Why do we seed the ordering numbers with xxHash32(table_name)? This is an attempt at producing
// stable-ish numbers for tables. By having stable table orderings, we can avoid unnecessary
// index rebuilds due to configuration changes.
func (c *Config) computeTotalOrder() {
	for _, db := range c.Databases {
		// It is important that we sort by name. If one simply walks the ConfigDatabase.Table map,
		// then you get different iteration orders on each program run, because Go hash maps use
		// a hash seed that is pseudo-randomly generated at program startup. By sorting, we get
		// stable ordering, even in the face of collisions in our xxHash.
		sortedTableNames := db.tablesOrderedByName()
		id2tab := map[uint32]*ConfigTable{}
		for _, tabName := range sortedTableNames {
			tab := db.Tables[tabName]
			id := xxHash32.Checksum([]byte(tabName), 1)
			for id2tab[id] != nil {
				id++
			}
			id2tab[id] = tab
		}
		for id, tab := range id2tab {
			tab.totalOrder = id
		}
	}
}

func (c *Config) LoadFile(filename string) error {
	err := serviceconfig.GetConfig(filename, serviceName, serviceConfigVersion, serviceConfigFileName, c)
	if err != nil {
		return err
	}
	for _, db := range c.Databases {
		for _, table := range db.Tables {
			if table.IndexField == "" {
				table.IndexField = ROWID
			}
		}
	}
	// We don't run postJSONLoad here, because if there is a problem with the config, then we'd
	// like to at least be able to emit log messages, if that's at all possible.
	return nil
}

func (c *Config) updateRowCounts(e *Engine) error {
	cfg := e.GetConfig()
	start := time.Now()
	for dbName, dbConfig := range c.Databases {
		// if len(dbName) != 0 {
		db, err := e.getSrcDB(dbName, cfg)
		if err != nil {
			return err
		}
		// See long explanation above the "RowCount" member for why we do an EXISTS query,
		// and not an actual COUNT query
		for tableName, _ := range dbConfig.Tables {
			exists := false
			if err := db.QueryRow(fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %v)", escapeSqlIdent(db, tableName))).Scan(&exists); err != nil {
				e.ErrorLog.Errorf("Row count failed on %v.%v: %v", dbName, tableName, err)
			} else {
				if exists {
					dbConfig.Tables[tableName].RowCount = 1
				} else {
					dbConfig.Tables[tableName].RowCount = 0
				}
			}
			// }
		}
	}
	if time.Now().Sub(start) > 15*time.Second {
		e.ErrorLog.Warnf("Row count took %.1f seconds", time.Now().Sub(start).Seconds())
	}
	return nil
}

// Returns a JSON string of our config, which is suitable for consumption by an HTTP API.
// The returned config has no passwords or other such information inside it.
func (c *Config) generateConfigForHttpAPI() (string, error) {
	cfg := configHttpAPI{}
	cfg.Databases = map[string]*ConfigDatabase{}

	for dbName, db := range c.Databases {
		if dbName == indexDatabaseName {
			continue
		}
		dbCopy := &ConfigDatabase{}
		dbCopy.Tables = db.Tables
		cfg.Databases[dbName] = dbCopy
	}

	json, err := json.MarshalIndent(&cfg, "", "\t")
	if err != nil {
		return "", err
	}
	return string(json), nil
}
