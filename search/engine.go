package search

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IMQS/log"
)

const (
	msgTableNotConfigured = "Requested table is not configured as part of the search index:"
	msgFieldNotConfigured = "Requested field is not configured as part of the search index:"
	msgKeywordNotMatched  = "Keyword not matched by any table:"
)

var errUnterminatedInstruction = errors.New("Query instruction not terminated")
var errInvalidPairsInstruction = errors.New("Invalid 'pairs' instruction")
var errTableNotConfigured = errors.New(msgTableNotConfigured)
var errTooManyTokensInQuery = errors.New("Query is too complicated")
var errTooManyIndexedFields = errors.New(fmt.Sprintf("Too many indexed fields. Maximum is %v", maxFieldsPerTable))
var errRowTupleTooLarge = errors.New(fmt.Sprintf("Too many bytes in row tuple. Maximum is %v", maxBytesInRowTuple))
var errFieldNotFoundInConfig = errors.New("Field not found in config")

// This is an arbitrary constant. It has performance implications, because we have statically
// allocated buffers that can hold maxRowsInTuple x 64-bit, for every intermediate result. This
// is why we impose this limit. The number of relations from any one table to any other table
// has a maximum of maxRowsInTuple - 1 (because one slot is used for the primary table's srcrow).
// This works in conjunction with maxBytesInRowTuple. Both constraints must be satisfied.
const maxRowsInTuple = 5

// This is a constant very similar to the above maxRowsInTuple.
// This can store 4x5-byte rows, and one 4-byte row.
// A 5-byte varint can store a value up to 17,179,869,184
// A 4-byte varint can store a value up to 134,217,728
// This works in conjunction with maxRowsInTuple. Both constraints must be satisfied.
const maxBytesInRowTuple = 24

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

/* Search Engine
 */
type Engine struct {
	// The config can be modified by API calls, but whenever it is modified, it is done so by creating a completely new instance, acquiring the write lock,	and replacing the old instance.
	// This minimizes lock contention, so that long running jobs (eg indexing) can proceed while, for example, the user modifies the configuration from the front-end.
	Config     *Config
	ConfigLock sync.RWMutex

	IndexDB    *sql.DB
	ErrorLog   *log.Logger
	AccessLog  *log.Logger
	ConfigFile string

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

func (e *Engine) Initialize(isTest bool) error {
	e.isIndexOutOfDate_Atomic = 0
	config := e.GetConfig()

	e.ErrorLog = log.New(pickLogFile(config.Log.ErrorFile, log.Stderr))
	e.AccessLog = log.New(pickLogFile(config.Log.AccessFile, log.Stdout))

	indexCfg, haveIndex := config.Databases[IndexDatabaseName]
	if !haveIndex {
		return errors.New("No 'index' database specified")
	}
	indexDB, err := OpenIndexDB(indexCfg)
	if err != nil {
		return fmt.Errorf("Error connecting to search index database: %v", err)
	}
	e.IndexDB = indexDB

	if isTest == false {
		// Now that we have loaded the file-based config, connect to the database
		// and read the extra config stored inside the database and merge with the current config
		if err := e.ReloadMergedConfig(); err != nil {
			return err
		}
	}

	config = e.GetConfig()
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

func (e *Engine) Find(query *Query, config *Config) (*FindResult, error) {
	// track the number of simultaneous find operations, as well as high water mark
	numFindOps := atomic.AddUint32(&e.numFindOpsInProgress, 1)
	defer atomic.AddUint32(&e.numFindOpsInProgress, ^uint32(0))
	if atomicMaxUint32(&e.maxFindOpsInProgress, numFindOps) {
		e.ErrorLog.Infof("Max simultaneous find ops in progress: %v", atomic.LoadUint32(&e.maxFindOpsInProgress))
	}
	start := time.Now()
	res, err := e.findWithManualJoin(query, config)
	if err != nil {
		return nil, err
	}
	auxStart := time.Now()
	err = e.addValuesToResults(res.Rows, config)
	if err != nil {
		return nil, err
	}
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

// Merge file-based config with one stored on the database.
func (e *Engine) ReloadMergedConfig() error {
	cfg := &Config{}
	// Reload the original config file from disk
	err := cfg.LoadFile(e.ConfigFile)
	if err != nil {
		return err
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
		err := tableRows.Scan(&dbName, &tableName, &configString)
		if err != nil {
			return err
		}

		dbTable := new(ConfigTable)
		err = json.Unmarshal([]byte(configString), &dbTable)
		if err != nil {
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

func (e *Engine) GetConfig() *Config {
	e.ConfigLock.RLock()
	c := e.Config
	e.ConfigLock.RUnlock()
	return c
}

func (e *Engine) updateConfig(databaseName string, configData io.Reader) error {
	type Table struct {
		Name  string
		Table ConfigTable
	}

	tables := make([]Table, 0)
	if err := json.NewDecoder(configData).Decode(&tables); err != nil {
		return err
	}

	// We update the database in a transaction for atomicity, and we follow the pattern described on
	// https://godoc.org/github.com/lib/pq
	txn, err := e.IndexDB.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	stmt, err := txn.Prepare("INSERT INTO search_config (dbname, tablename, config) VALUES ($1, $2, $3) ON CONFLICT (dbname, tablename) DO UPDATE SET config = EXCLUDED.config;")
	if err != nil {
		return err
	}

	for _, table := range tables {
		if len(table.Table.Fields) > 32 {
			return fmt.Errorf("Number of fields to be configured  %v is greater than the limit 32", len(table.Table.Fields))
		}
		s, err := json.Marshal(table.Table)
		if err != nil {
			return err
		}
		_, err = stmt.Exec(databaseName, table.Name, string(s))
		if err != nil {
			return err
		}
	}

	if err = stmt.Close(); err != nil {
		return err
	}

	if err = txn.Commit(); err != nil {
		return err
	}
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
	e.nameToIDLock.Lock()
	defer e.nameToIDLock.Unlock()
	if id, ok := e.nameToID[name]; ok {
		return id, nil
	}

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
	e.idToNameLock.Lock()
	defer e.idToNameLock.Unlock()
	if name, ok := e.idToName[id]; ok {
		return name, nil
	}

	var name string
	if err := e.IndexDB.QueryRow("SELECT name FROM search_nametable WHERE id = $1", id).Scan(&name); err != nil {
		return "", err
	}
	e.idToName[id] = name
	return name, nil
}

func (e *Engine) getTableFromID(tabID tableID) (*ConfigTable, error) {
	e.tableIDToTableLock.Lock()
	defer e.tableIDToTableLock.Unlock()
	tableConfig, exist := e.tableIDToTable[tabID]
	if exist {
		return tableConfig, nil
	}

	name, err := e.getNameFromTableID(tabID)
	if err != nil {
		return nil, err
	}

	tableConfig = e.GetConfig().tableConfigFromName(TableFullName(name))
	if tableConfig == nil {
		return nil, errTableNotConfigured
	}

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
		names += escapeSqlLiteral(e.IndexDB, string(table)) + ", "
	}
	names = names[0 : len(names)-2]

	stmt := fmt.Sprintf("DELETE FROM search_src_tables WHERE tablename IN (%v)", names)
	_, err := e.IndexDB.Exec(stmt)
	return err
}

func (e *Engine) getSrcDB(dbName string, config *Config) (*sql.DB, error) {
	e.srcDBPoolLock.Lock()
	defer e.srcDBPoolLock.Unlock()
	db, exist := e.srcDBPool[dbName]
	if exist {
		return db, nil
	}
	cfg := config.Databases[dbName]
	db, err := sql.Open(cfg.Driver, cfg.DSN())
	if err != nil {
		return nil, err
	}
	e.srcDBPool[dbName] = db
	return db, nil
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
