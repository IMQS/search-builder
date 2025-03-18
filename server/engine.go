package server

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/migration"
	"github.com/IMQS/log"
	serviceconfig "github.com/IMQS/serviceconfigsgo"
)

const (
	msgTableNotConfigured = "Requested table is not configured as part of the search index:"
	msgFieldNotConfigured = "Requested field is not configured as part of the search index:"
	msgKeywordNotMatched  = "Keyword not matched by any table:"

	// This is an arbitrary constant. It has performance implications, because we have statically
	// allocated buffers that can hold maxRowsInTuple x 64-bit, for every intermediate result. This
	// is why we impose this limit. The number of relations from any one table to any other table
	// has a maximum of maxRowsInTuple - 1 (because one slot is used for the primary table's srcrow).
	// This works in conjunction with maxBytesInRowTuple. Both constraints must be satisfied.
	maxRowsInTuple = 5

	// This is a constant very similar to the above maxRowsInTuple.
	// This can store 4x5-byte rows, and one 4-byte row.
	// A 5-byte varint can store a value up to 17,179,869,184
	// A 4-byte varint can store a value up to 134,217,728
	// This works in conjunction with maxRowsInTuple. Both constraints must be satisfied.
	maxBytesInRowTuple = 24
)

var (
	errUnterminatedInstruction = errors.New("Query instruction not terminated")
	errInvalidPairsInstruction = errors.New("Invalid 'pairs' instruction")
	errTableNotConfigured      = errors.New(msgTableNotConfigured)
	errTooManyTokensInQuery    = errors.New("Query is too complicated")
	errTooManyIndexedFields    = fmt.Errorf("Too many indexed fields. Maximum is %v", maxFieldsPerTable)
	errRowTupleTooLarge        = fmt.Errorf("Too many bytes in row tuple. Maximum is %v", maxBytesInRowTuple)
	errFieldNotFoundInConfig   = errors.New("Field not found in config")
)

type tableID uint16
type fieldID uint16
type fieldFullID uint32 // Composed of (tableID << 16) | fieldID

func makeFieldFullID(tableID tableID, fieldID fieldID) fieldFullID {
	return fieldFullID((uint32(tableID) << 16) | uint32(fieldID))
}

func makeFieldFullID_Low(tableID tableID) fieldFullID {
	return makeFieldFullID(tableID, 0)
}

func makeFieldFullID_High(tableID tableID) fieldFullID {
	return makeFieldFullID(tableID, 0xffff)
}

func (f fieldFullID) extractTableID() tableID {
	return tableID(f >> 16)
}

func (f fieldFullID) extractFieldID() fieldID {
	return fieldID(f & 0xffff)
}

// Engine for the search service
type Engine struct {
	// The config can be modified by API calls, but whenever it is modified, it is done so by creating a completely new instance, acquiring the write lock,	and replacing the old instance.
	// This minimizes lock contention, so that long running jobs (eg indexing) can proceed while, for example, the user modifies the configuration from the front-end.
	Config     *Config
	ConfigLock sync.RWMutex

	IndexDB      *sql.DB
	ErrorLog     *log.Logger
	AccessLog    *log.Logger
	ConfigFile   string
	ConfigString string // ConfigString takes precedence over ConfigFile. ConfigString was created for use by unit tests

	// Cache of search_nametable
	nameToID     map[string]uint16
	nameToIDLock sync.Mutex
	idToName     map[uint16]string
	idToNameLock sync.Mutex

	tableIDToTable     map[tableID]*ConfigTable
	tableIDToTableLock sync.Mutex

	// Cache of all source databases that we have touched.
	srcDBPool     map[string]*sql.DB
	srcDBPoolLock sync.Mutex

	// This tracks the state of whether our index is stale.
	// It is updated once a minute by StartStateWatcher().
	isIndexOutOfDate_Atomic uint32

	stateWatchTicker  *time.Ticker
	autoRebuildTicker *time.Ticker

	// Tracks the number of find operations currently in progress.
	// This is used in conjunction with maxFindOpsInProgress to track down DB connection leaks.
	numFindOpsInProgress uint32
	maxFindOpsInProgress uint32

	// This is a cache of stale tables that is used when producing query results, so that we
	// can inform the caller whether he may be receiving stale results. This is not used
	// when determining which tables to re-index.
	staleTables     map[TableFullName]bool
	staleTablesLock sync.RWMutex
}

func pickLogFile(filename, defaultFilename string) string {
	if filename != "" {
		return filename
	}
	return defaultFilename
}

func (e *Engine) initLogging() {
	config := e.GetConfig()

	isWindows := runtime.GOOS == "windows"
	e.ErrorLog = log.New(pickLogFile(config.Log.ErrorFile, log.Stderr), !isWindows)
	e.AccessLog = log.New(pickLogFile(config.Log.AccessFile, log.Stdout), !isWindows)
	if config.VerboseLogging {
		e.ErrorLog.Level = log.Trace
		e.AccessLog.Level = log.Trace
	}
}

// Initialize sets up the service engine
func (e *Engine) Initialize(isTest bool) error {
	var err error
	e.isIndexOutOfDate_Atomic = 0
	e.initLogging()
	if err = e.openIndexDB(); err != nil {
		return fmt.Errorf("Could not open index database: %v", err.Error())
	}

	if !isTest {
		// Now that we have loaded the file-based config, connect to the database
		// and read the extra config stored inside the database and merge with the current config
		if err := e.reloadMergedConfig(); err != nil {
			return err
		}
	}

	config := e.GetConfig()
	// It's important that we run postJSONLoad after setting up our logging. That way, the user
	// gets to see config errors in the logs.
	if err := config.postJSONLoad(); err != nil {
		return err
	}

	e.nameToID = make(map[string]uint16)
	e.idToName = make(map[uint16]string)
	e.tableIDToTable = make(map[tableID]*ConfigTable)
	e.srcDBPool = make(map[string]*sql.DB)

	// The actual intervals that these jobs use are internal to the job functions.
	// Five seconds is just the baseline. Actual intervals vary, but are always
	// much larger than 5 seconds.
	e.autoRebuildTicker = time.NewTicker(5 * time.Second)
	e.stateWatchTicker = time.NewTicker(5 * time.Second)

	// The following step can take quite a while, because it does a SELECT COUNT(*) on
	// every table that is indexed.
	start := time.Now()
	config.updateHttpApiConfig(e)
	e.ErrorLog.Infof("Row count scan took %.2f seconds", time.Now().Sub(start).Seconds())
	return nil
}

func (e *Engine) Close() {
	e.autoRebuildTicker.Stop()
	e.stateWatchTicker.Stop()
	if e.ErrorLog != nil {
		e.ErrorLog.Close()
		e.ErrorLog = nil
	}
	if e.AccessLog != nil {
		e.AccessLog.Close()
		e.AccessLog = nil
	}
	if e.IndexDB != nil {
		e.IndexDB.Close()
		e.IndexDB = nil
	}
	e.clearInMemoryCaches()
}

// openIndexDB opens a connection to the index db, after performing the migrations
// that and customizing the DB Connection limits
func (e *Engine) openIndexDB() error {
	_rootConfig := e.GetConfig()
	genericConf, err := serviceconfig.GetDBAlias(_rootConfig.getGenericDbAlias())
	if err != nil {
		return fmt.Errorf("Could not find generic database: %v", err)
	}

	conf, err := serviceconfig.GetDBAlias(e.Config.IndexDBAlias)
	if err != nil {
		return fmt.Errorf("Could not find index database: %v", err)
	}

	migrations := createMigrations(genericConf)

	if _rootConfig.PurgeDatabaseOnStartup {
		e.ErrorLog.Info("Dropping index database")
		if err = e.dropIndexDB(); err != nil {
			return fmt.Errorf("Could not drop index database: %v", err)
		}
	}

	if e.IndexDB, err = migration.Open(conf.Driver, conf.DSN(), migrations); err == nil {
		cfg := e.GetConfig().Databases[conf.Name]
		e.setDBConnectionLimits(cfg, conf, e.IndexDB)
		return nil
	}

	// Got the following error on 'prefix', when 'searchindex' DB did not exist:
	// Error initializing search engine: Error connecting to search index database:
	// Could not get DB version: WSARecv tcp 127.0.0.1:60064:
	// An existing connection was forcibly closed by the remote host.
	if isDBNotExistError(err, conf.Name) {
		if eCreate := createDB(conf); eCreate != nil {
			return err
		}
		e.IndexDB, err = migration.Open(conf.Driver, conf.DSN(), migrations)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("While connecting to the searchindex DB: %v", err)
	}

	if e.IndexDB == nil {
		return errors.New("Fatal: Unreachable code. A valid connection to the index db should have been established at this point")
	}

	cfg := e.GetConfig().Databases[conf.Name]
	e.setDBConnectionLimits(cfg, conf, e.IndexDB)
	return nil
}

func (e *Engine) setDBConnectionLimits(cfg *ConfigDatabase, conx *serviceconfig.DBConfig, db *sql.DB) {
	if cfg != nil && cfg.MaxIdleConns != 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg != nil && cfg.MaxOpenConns != 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
}

func (e *Engine) Find(query *Query, config *Config) (*FindResult, error) {
	// track the number of simultaneous find operations, as well as high water mark
	numFindOps := atomic.AddUint32(&e.numFindOpsInProgress, 1)
	defer atomic.AddUint32(&e.numFindOpsInProgress, ^uint32(0))
	if atomicMaxUint32(&e.maxFindOpsInProgress, numFindOps) {
		e.ErrorLog.Debugf("Max simultaneous find ops in progress: %v", atomic.LoadUint32(&e.maxFindOpsInProgress))
	}

	// something is wrong if we go above 50 pending requests -> switch to detailed logging
	if numFindOps > 50 {
		e.ErrorLog.Error("Max simultaneous find ops are above threshold: switching to debug log level")
		e.ErrorLog.Level = log.Debug
	}

	start := time.Now()
	res, err := e.findWithManualJoin(query, config)
	if err != nil {
		return nil, err
	}
	e.ErrorLog.Debug("engine.go: e.findWithManualJoin done")

	auxStart := time.Now()
	err = e.addValuesToResults(res.Rows, config)
	if err != nil {
		return nil, err
	}
	e.ErrorLog.Debug("engine.go: e.addValuesToResults done")

	res.TimeAuxFetch = time.Now().Sub(auxStart)
	res.TimeTotal = time.Now().Sub(start)
	return res, err
}

func (e *Engine) Vacuum() error {
	// Log profusely, because this is likely to be a performance hotspot for the server
	e.ErrorLog.Info("Starting VACUUM ANALYZE")
	start := time.Now()
	_, err := e.IndexDB.Exec("VACUUM ANALYZE search_index")
	if err != nil {
		e.ErrorLog.Errorf("Error running VACUUM ANALYZE: %v", err)
	} else {
		e.ErrorLog.Infof("VACUUM ANALYZE completed in %v seconds", time.Now().Sub(start).Seconds())
	}
	return err
}

// reloadMergedConfig - Merges file-based config with database search_config
func (e *Engine) reloadMergedConfig() error {
	cfg := &Config{}

	if e.ConfigString != "" {
		// ConfigString was created for unit tests, so that we can synthesize the config in code, and
		// still stress the config loading and merging code path.
		if err := cfg.LoadString(e.ConfigString); err != nil {
			return err
		}
	} else {
		// Load original config file from disk (or config service)
		if err := cfg.LoadFile(e.ConfigFile); err != nil {
			return err
		}
	}

	tableRows, err := e.IndexDB.Query("SELECT dbname, tablename, config FROM search_config")
	if err != nil {
		return err
	}
	defer tableRows.Close()

	for tableRows.Next() {
		var dbName string
		var tableName string
		var configString string

		if err := tableRows.Scan(&dbName, &tableName, &configString); err != nil {
			return err
		}

		dbTable := new(ConfigTable)
		if err = json.Unmarshal([]byte(configString), &dbTable); err != nil {
			return err
		}

		if _, ok := cfg.Databases[dbName]; !ok {
			cfg.Databases[dbName] = &ConfigDatabase{}
		}
		if cfg.Databases[dbName].Tables == nil {
			cfg.Databases[dbName].Tables = map[string]*ConfigTable{}
		}
		if dbTable.IndexField == "" {
			dbTable.IndexField = ROWID
		}
		cfg.Databases[dbName].Tables[tableName] = dbTable
	}

	e.ConfigLock.Lock()
	e.Config = cfg
	e.ConfigLock.Unlock()

	return nil
}

func (e *Engine) LoadConfigFromFile() error {
	cfg := &Config{}
	err := cfg.LoadFile(e.ConfigFile)
	if err != nil {
		return err
	}
	// No need for a lock here. This function is only called once at start up.
	e.Config = cfg
	return nil
}

/*
GetConfig - returns the latest search for engine
*/
func (e *Engine) GetConfig() *Config {
	e.ConfigLock.RLock()
	c := e.Config
	e.ConfigLock.RUnlock()
	return c
}

func (e *Engine) updateConfig(databaseName string, tables []GenericTable) error {
	txn, err := e.IndexDB.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	stmt, err := txn.Prepare("INSERT INTO search_config (dbname, tablename, longlived_name, config) VALUES ($1, $2, $3, $4) ON CONFLICT (dbname, longlived_name) DO UPDATE SET config = EXCLUDED.config ,tablename = $2;")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, table := range tables {
		if len(table.Table.Fields) > 32 {
			return fmt.Errorf("Number of fields to be configured  %v is greater than the limit 32", len(table.Table.Fields))
		}
		s, err := json.Marshal(table.Table)
		if err != nil {
			return err
		}

		if _, err := stmt.Exec(databaseName, table.Name, table.LongLivedName, string(s)); err != nil {
			return err
		}
	}

	return txn.Commit()
}

func (e *Engine) deleteConfigTable(database, table string) error {
	if _, err := e.IndexDB.Exec(`DELETE FROM search_config WHERE tablename = $1`, table); err != nil {
		e.ErrorLog.Errorf("Error in 'deleteConfigTable' for table %v: %v", table, err)
		return err
	}
	e.ErrorLog.Infof("Config deleted for table %v, database: %v", table, database)
	return nil
}

/*
getConfigTable - returns the table name and config
*/
func (e *Engine) getConfigTable(table string) (string, *ConfigTable) {
	var currentTableName string
	var tableConfig string

	configTable := new(ConfigTable)
	tableRow := e.IndexDB.QueryRow("SELECT tablename, config FROM search_config WHERE longlived_name = $1", table)

	err := tableRow.Scan(&currentTableName, &tableConfig)
	if err != nil {
		e.ErrorLog.Errorf("Unable to get config for table %v", table)
		return "", nil
	}

	err = json.Unmarshal([]byte(tableConfig), &configTable)
	if err != nil {
		e.ErrorLog.Errorf("Unable to get config for table %v", table)
		return "", nil
	}

	return currentTableName, configTable
}

/*
getSrcTableColumns - returns a list of column names from the source table
*/
func (e *Engine) getSrcTableColumns(database, table string) ([]string, error) {
	alias, err := serviceconfig.GetDBAlias(database)
	if err != nil {
		return nil, fmt.Errorf("Could not get alias: %v", err)
	}

	db, err := sql.Open(alias.Driver, alias.DSN())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf(`SELECT * FROM "%v" LIMIT 1`, table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return rows.Columns()
}

/*
updateConfigTable - updates config tablename using longLivedName
*/
func (e *Engine) updateConfigTable(database, tableName, longLivedName string) error {
	// If table is not configured for search just return
	// Nothing to update or check
	currentTableName, currentTable := e.getConfigTable(longLivedName)
	if currentTableName == "" && currentTable == nil {
		return nil
	}

	columnNames, err := e.getSrcTableColumns(database, tableName)
	if err != nil {
		e.ErrorLog.Errorf("updateConfigTable: unable to get source column names for table %v, %v", tableName, err)
		return err
	}

	sort.Strings(columnNames)
	// Build a new list of only fields contained in the new list of column names
	newFields := make([]*ConfigField, 0)
	for _, field := range currentTable.Fields {
		index := sort.SearchStrings(columnNames, field.Field)
		if index < len(columnNames) && columnNames[index] == field.Field {
			newFields = append(newFields, field)
		}
	}

	if len(newFields) > 0 {
		currentTable.Fields = newFields
		jsonConfig, err := json.Marshal(currentTable)
		if err != nil {
			e.ErrorLog.Errorf("updateConfigTable: unable to marshal into json, table %v, %v", tableName, err)
			return err
		}

		_, err = e.IndexDB.Exec(`UPDATE search_config SET tablename = $1, config = $2 WHERE longlived_name = $3;`, tableName, jsonConfig, longLivedName)
		if err != nil {
			e.ErrorLog.Errorf("updateConfigTable: unable to update search_config for table %v, %v", tableName, err)
			return err
		}

		e.ErrorLog.Infof("Config updated for table %v", longLivedName)
	} else {
		_, err := e.IndexDB.Exec(`DELETE FROM search_config WHERE longlived_name = $1`, longLivedName)
		if err != nil {
			e.ErrorLog.Errorf("updateConfigTable: unable to delete for table %v: %v", tableName, err)
			return err
		}

		e.ErrorLog.Infof("Config for table %v has been removed, since none of the originally indexed fields no longer exist", tableName)
	}

	return nil
}

/*
refreshJSONConfig - updates front end search config after database config update
*/
func (e *Engine) refreshJSONConfig() error {
	if err := e.reloadMergedConfig(); err != nil {
		return err
	}

	config := e.GetConfig()
	if err := config.postJSONLoad(); err != nil {
		return err
	}

	// Set index out of date to true after config update.
	atomic.StoreUint32(&e.isIndexOutOfDate_Atomic, 1)

	// Update front-end config with the new and updated engine config
	config.updateHttpApiConfig(e)

	return nil
}

func (e *Engine) isIndexOutOfDate() bool {
	return atomic.LoadUint32(&e.isIndexOutOfDate_Atomic) != 0
}

func (e *Engine) getTableID(name TableFullName, createIfNotExist bool) (tableID, error) {
	id, err := e.getIDFromName(string(name), createIfNotExist)
	return tableID(id), err
}

// This is just the pure field name, NOT the full field name
func (e *Engine) getFieldID(name string, createIfNotExist bool) (fieldID, error) {
	id, err := e.getIDFromName(name, createIfNotExist)
	return fieldID(id), err
}

// Lookup a string in 'search_nametable', and optionally create a new ID if it doesn't already exist
func (e *Engine) getIDFromName(name string, createIfNotExist bool) (uint16, error) {
	e.ErrorLog.Debugf("engine.go: getIDFromName: %v", name)

	e.nameToIDLock.Lock()
	defer e.nameToIDLock.Unlock()
	if id, ok := e.nameToID[name]; ok {
		return id, nil
	}
	e.ErrorLog.Debugf("engine.go: getIDFromName (not found in cache): %v", name)

	fetch_from_db := func() (uint16, error) {
		var id uint32
		err := e.IndexDB.QueryRow("SELECT id FROM search_nametable WHERE name = $1", name).Scan(&id)
		if err == nil {
			if id > 32767 {
				// This restriction is strictly only for tables, because tables go into the upper 16 bits like so (table << 16 | field).
				// That 32-bit integer is then inserted into the DB as an INTEGER type, which is signed. For this reason, we cannot allow
				// the top bit of the table ID to be set. Here we enforce that restriction upon field IDs too, but I don't see that being
				// a problem in practice. This system is likely to have other scaling issues if you're indexing over 30k fields.
				msg := fmt.Sprintf("id in search_nametable is too high (%v). About to panic.", id)
				e.ErrorLog.Error(msg)
				e.ErrorLog.Close()
				panic(msg)
			}
			// Insert into cache
			e.nameToID[name] = uint16(id)
			return uint16(id), nil
		}
		return 0, err
	}

	id, err := fetch_from_db()
	if err == nil {
		return id, err
	}

	if err == sql.ErrNoRows && createIfNotExist {
		_, err = e.IndexDB.Exec("INSERT INTO search_nametable (name, id) VALUES ($1, (SELECT coalesce(max(id), 0)+1 from search_nametable))", name)
		if err != nil {
			return 0, err
		}
		return fetch_from_db()
	}

	return 0, err
}

func (e *Engine) getNameFromTableID(id tableID) (string, error) {
	return e.getNameFromID(uint16(id))
}

func (e *Engine) getNameFromFieldID(id fieldID) (string, error) {
	return e.getNameFromID(uint16(id))
}

func (e *Engine) getNameFromID(id uint16) (string, error) {
	e.ErrorLog.Debugf("engine.go: getNameFromID: %v", id)

	e.idToNameLock.Lock()
	defer e.idToNameLock.Unlock()
	if name, ok := e.idToName[id]; ok {
		return name, nil
	}
	e.ErrorLog.Debugf("engine.go: getNameFromID (not in cache): %v", id)

	var name string
	if err := e.IndexDB.QueryRow("SELECT name FROM search_nametable WHERE id = $1", id).Scan(&name); err != nil {
		return "", err
	}
	e.idToName[id] = name
	return name, nil
}

func (e *Engine) getTableFromID(tabID tableID) (*ConfigTable, error) {
	e.ErrorLog.Debugf("engine.go: getTableFromID: %v", tabID)

	e.tableIDToTableLock.Lock()
	defer e.tableIDToTableLock.Unlock()
	tableConfig, exist := e.tableIDToTable[tabID]
	if exist {
		return tableConfig, nil
	}
	e.ErrorLog.Debugf("engine.go: getTableFromID (not found in cache): %v", tabID)

	name, err := e.getNameFromTableID(tabID)
	if err != nil {
		return nil, err
	}

	tableConfig = e.GetConfig().tableConfigFromName(TableFullName(name))
	if tableConfig == nil {
		return nil, errTableNotConfigured
	}
	e.ErrorLog.Debugf("engine.go: getTableFromID (found tableConfig): %v", tabID)

	e.tableIDToTable[tabID] = tableConfig
	return tableConfig, nil
}

func (e *Engine) getTableFromName(name TableFullName, createIfNotExist bool) (*ConfigTable, error) {
	tabID, err := e.getIDFromName(string(name), createIfNotExist)
	if err != nil {
		return nil, err
	}
	return e.getTableFromID(tableID(tabID))
}

// Remove table metadata (because it is no longer configured as part of the index)
func (e *Engine) deleteTableInfo(tableNames []TableFullName) error {
	if len(tableNames) == 0 {
		return nil
	}
	names := ""
	for _, table := range tableNames {
		names += escapeSQLLiteral(e.IndexDB, string(table)) + ", "
	}
	names = names[0 : len(names)-2]

	stmt := fmt.Sprintf("DELETE FROM search_src_tables WHERE tablename IN (%v)", names)
	_, err := e.IndexDB.Exec(stmt)
	return err
}

func (e *Engine) getSrcDB(dbName string, config *Config) (*sql.DB, error) {
	e.ErrorLog.Debugf("engine.go: getSrcDB: %v", dbName)

	e.srcDBPoolLock.Lock()
	defer e.srcDBPoolLock.Unlock()
	db, exist := e.srcDBPool[dbName]
	if exist {
		return db, nil
	}
	e.ErrorLog.Debugf("engine.go: getSrcDB (not found in cache): %v", dbName)

	var (
		err error
		cfg = config.Databases[dbName]
		dba = dbName
	)
	if dba == "index" {
		dba = config.getIndexDbAlias()
	}
	if cfg.DBConfig, err = serviceconfig.GetDBAlias(dba); err != nil {
		return nil, fmt.Errorf("Could not find DB Config: %v", err)
	}

	db, err = sql.Open(cfg.DBConfig.Driver, cfg.DBConfig.DSN())
	if err != nil {
		return nil, err
	}
	e.srcDBPool[dbName] = db
	e.ErrorLog.Debugf("engine.go: getSrcDB (found db): %v", dbName)

	return db, nil
}

func (e *Engine) dropIndexDB() error {
	alias, err := serviceconfig.GetDBAlias(e.GetConfig().getIndexDbAlias())
	if err != nil {
		return fmt.Errorf("Could not get dbAlias while dropping database: index")
	}
	cp := alias
	return dropDB(cp)
}

func (e *Engine) clearInMemoryCaches() {
	e.nameToID = make(map[string]uint16)
	e.idToName = make(map[uint16]string)
	e.tableIDToTable = make(map[tableID]*ConfigTable)

	for _, db := range e.srcDBPool {
		db.Close()
	}
	e.srcDBPool = make(map[string]*sql.DB)
}

// Atomically set *value to max(*value, newPossibleMax)
// Returns true if we raised the max value
func atomicMaxUint32(value *uint32, newPossibleMax uint32) bool {
	for {
		old := atomic.LoadUint32(value)
		if old >= newPossibleMax {
			return false
		}
		if atomic.CompareAndSwapUint32(value, old, newPossibleMax) {
			return true
		}
	}
}
