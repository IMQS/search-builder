package server

import (
	//"crypto/sha1"
	"database/sql"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jasonlvhit/gocron"
	"github.com/lib/pq"
)

// Composition of Database.Table
type TableFullName string

func MakeTableName(dbName, tableName string) TableFullName {
	return TableFullName(dbName + "." + tableName)
}

func (t TableFullName) Split() (dbName string, tableName string) {
	parts := strings.Split(string(t), ".")
	if len(parts) == 2 {
		dbName = parts[0]
		tableName = parts[1]
	}
	return
}

func (t TableFullName) DBOnly() string {
	db, _ := t.Split()
	return db
}

func (t TableFullName) TableOnly() string {
	_, table := t.Split()
	return table
}

type tableFullNameSlice []TableFullName

func (a tableFullNameSlice) Len() int      { return len(a) }
func (a tableFullNameSlice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a tableFullNameSlice) Less(i, j int) bool {
	return a[i] < a[j]
}

// Composition of Database.Table.Field
type fieldFullName string

func makeFieldFullName(table TableFullName, field string) fieldFullName {
	return fieldFullName(string(table) + "." + field)
}

func (t fieldFullName) split() (tableName TableFullName, fieldName string) {
	parts := strings.Split(string(t), ".")
	if len(parts) == 3 {
		tableName = MakeTableName(parts[0], parts[1])
		fieldName = parts[2]
	}
	return
}

func (t fieldFullName) table() TableFullName {
	parts := strings.Split(string(t), ".")
	if len(parts) == 3 {
		return MakeTableName(parts[0], parts[1])
	}
	return ""
}

type tokenFieldPair struct {
	token   string
	fieldID fieldID
}

type indexedRow struct {
	//srcrow  int64
	srcrows []byte
	//tokens []string
	tokens []tokenFieldPair
}

// Pack rows using varint (aka Protobuf) encoding into buf, and returns the number of bytes written.
// If buf is not large enough, then we panic.
func packRows(buf []byte, rows []int64) int {
	bytes := 0
	for _, r := range rows {
		bytes += binary.PutVarint(buf[bytes:], r)
	}
	return bytes
}

// Unpacks the next int64 value from a packed row buffer
// Panics if it encounters an invalid encoding.
// Returns the unpacked value, and the number of bytes read.
func unpackNextRow(buf []byte) (int64, int) {
	val, n := binary.Varint(buf)
	if n <= 0 {
		panic("Invalid packed row encoding")
	}
	return val, n
}

// Unpacks all int64s in 'buf', and places them in 'target'.
// Returns the number of int64s decoded
func unpackRows(buf []byte, target []int64) int {
	i := 0
	for pos := 0; pos < len(buf); {
		inc := 0
		target[i], inc = unpackNextRow(buf[pos:])
		pos += inc
		i++
	}
	return i
}

// Rebuild the index for the given tables.
// If ignoreErrors is true, and the rebuild for a table fails, then we log the error and proceed onto the next table.
// If ignoreErrors is falss, and the rebuild for a table fails, then we return that error immediately.
func (e *Engine) BuildIndex(tableNames []TableFullName, ignoreErrors bool) error {
	config := e.GetConfig()
	seenTable := map[TableFullName]bool{}

	doTable := func(tname TableFullName) error {
		// Allow the caller to specify a table more than once
		if seenTable[tname] {
			return nil
		}
		seenTable[tname] = true

		e.ErrorLog.Infof("Rebuild index on %v", tname)

		table := config.tableConfigFromName(tname)

		// Open the source database
		db, err := e.getSrcDB(tname.DBOnly(), config)
		if err != nil {
			return fmt.Errorf("During BuildIndex.getSrcDB: %v", err)
		}

		tabID, err := e.getTableID(tname, true)
		if err != nil {
			return fmt.Errorf("During BuildIndex.getTableID: %v", err)
		}

		// Wipe existing index for table
		err = e.deleteIndexOnTable(tname, tabID, config)
		if err != nil {
			return fmt.Errorf("During BuildIndex.deleteIndexOnTable: %v", err)
		}

		// Scan the table
		err = e.buildIndexOnTable(db, tabID, tname, table, config)
		if err != nil {
			return fmt.Errorf("During BuildIndex.buildIndexOnTable: %v", err)
		}
		return nil
	}

	for _, tname := range tableNames {
		err := doTable(tname)
		if err != nil {
			if ignoreErrors {
				e.ErrorLog.Errorf("Index build failed on table %v: %v", tname, err)
			} else {
				return fmt.Errorf("Index build failed on table %v: %v", tname, err)
			}
		}
	}

	return nil
}

// Remove tables from the index
func (e *Engine) deleteIndexOnTables(tableNames []TableFullName, config *Config) error {
	// config := e.GetConfig()
	for _, table := range tableNames {
		tabID, err := e.getTableID(table, false)
		if err != nil {
			return err
		}
		err = e.deleteIndexOnTable(table, tabID, config)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) deleteIndexOnTable(name TableFullName, tabID tableID, config *Config) error {
	// Clear cache
	cfg := config.tableConfigFromName(name)
	if cfg != nil {
		cfg.hasGeometry.store(atomicTriState_Nil)
	}

	_, err := e.IndexDB.Exec(fmt.Sprintf("DELETE FROM search_index WHERE srcfield >= %v AND srcfield <= %v", makeFieldFullID_Low(tabID), makeFieldFullID_High(tabID)))
	return err
}

type subjugateJoinMap map[string]int64

// Index all of the data from this table.
func (e *Engine) buildIndexOnTable(sourceDB *sql.DB, tabID tableID, tableName TableFullName, config *ConfigTable, engineConfig *Config) error {
	esc := func(ident string) string {
		return escapeSQLIdent(sourceDB, ident)
	}

	// First phase is to select the rowids for all of the subjugate (related) tables. We are effectively "pre-joining"
	// tables up here, so that the find operation doesn't need to do that work. We could use an SQL JOIN here,
	// but that has the problem of potentially explosive joins. It's plausible that we see a dataset with gigantic
	// join products. For example, the primary table has 10 records with id "A", and the first related table has
	// 100 records with id "A", and the third table has 100 records with id "A". That produces 10 * 100 * 100 = 100000
	// records. That means we're fetching 100k records from the DB, where we should have fetched 10, which is just
	// to expensive to tolerate.
	// Instead, we join ourselves, using a hash table.
	// Before fetching records from the primary table, we fetch all of the rowids and joining fields, from the
	// related tables. Then, as we iterate over the records of the primary table, we splice in the joined records.
	_, sourceDBConfig := engineConfig.getDatabaseFromTable(config)
	subjugates := config.subjugateRelations(sourceDBConfig)
	joins := make([]subjugateJoinMap, len(subjugates))
	for isub, sub := range subjugates {
		joins[isub] = make(subjugateJoinMap)
		pick := fmt.Sprintf("SELECT %v, %v FROM %v", esc(config.IndexField), esc(sub.relation.ForeignField), esc(sub.relation.ForeignTable))
		rows, err := sourceDB.Query(pick)
		if err != nil {
			return fmt.Errorf("%v, while scanning relationship from %v.%v -> %v.%v", err, tableName, sub.relation.Field, sub.relation.ForeignTable, sub.relation.ForeignField)
		}
		defer rows.Close()
		for rows.Next() {
			var srcrow int64
			var key []byte
			if err := rows.Scan(&srcrow, &key); err != nil {
				return fmt.Errorf("%v, while scanning relationship from %v.%v -> %v.%v", err, tableName, sub.relation.Field, sub.relation.ForeignTable, sub.relation.ForeignField)
			}
			joins[isub][string(key)] = srcrow
		}
	}

	// Prepare the SELECT statement etc, for the main table read
	// Our SELECT statement reads:

	// SELECT rowid, <field1, field2...> <subID1, subID2,...> FROM table

	// field1, field2... are the fields inside 'table' that need to be indexed
	// subID1, subID2... are the fields which relate 'table' to our subjugate relations

	fieldIDs := make([]fieldID, len(config.Fields))
	for i, field := range config.Fields {
		id, err := e.getFieldID(field.Field, true)
		if err != nil {
			return err
		}
		fieldIDs[i] = id
	}

	pick := fmt.Sprintf("SELECT %v,", esc(config.IndexField))

	for _, f := range config.Fields {
		pick += fmt.Sprintf("%v,", esc(f.Field))
	}

	for _, sub := range subjugates {
		pick += fmt.Sprintf("%v,", esc(sub.relation.Field))
	}

	parser := NewDefaultParser()
	pick = pick[:len(pick)-1]
	pick += fmt.Sprintf(" FROM %v", esc(tableName.TableOnly()))

	// Useful when debugging, and to avoid premature SSD death!
	//pick += fmt.Sprintf(" LIMIT 10000")

	//fmt.Printf("Query: '%v'\n", pick)

	rows, err := sourceDB.Query(pick)
	if err != nil {
		return err
	}
	defer rows.Close()

	results := make([]interface{}, 0)
	results = append(results, new(sql.NullInt64))
	for i := 0; i < len(config.Fields); i++ {
		results = append(results, new(sql.NullString))
	}
	for i := 0; i < len(subjugates); i++ {
		buf := []byte{}
		results = append(results, &buf)
	}

	queue := make([]indexedRow, 0)

	fmt.Printf("Scanning %v fields in table %v ", len(config.Fields), tableName)

	rownum := 0
	rowBuf := [maxRowsInTuple]int64{}

	// Scan rows
	for rows.Next() {
		if rownum%10000 == 0 {
			fmt.Printf(".")
		}
		if err := rows.Scan(results...); err != nil {
			return err
		}
		if !results[0].(*sql.NullInt64).Valid {
			continue
		}
		row := indexedRow{}

		// Scan indexed fields
		for i := 1; i < 1+len(config.Fields); i++ {
			field := config.Fields[i-1]
			res := results[i].(*sql.NullString)
			if res.Valid {
				parser = field.ToDefaultParser()
				tokens := parser.Tokenize(res.String)
				for _, t := range tokens {
					row.tokens = append(row.tokens, tokenFieldPair{t, fieldIDs[i-1]})
				}
			}
		}

		// Set primary srcrow
		rowBuf[0] = results[0].(*sql.NullInt64).Int64
		rowPackedBuf := [maxRowsInTuple * binary.MaxVarintLen64]byte{}

		// Scan joins
		base := 1 + len(config.Fields)
		for i := 0; i < len(subjugates); i++ {
			id := results[base+i].(*[]byte)
			rowBuf[1+i] = joins[i][string(*id)]
		}
		packedLen := packRows(rowPackedBuf[:], rowBuf[:1+len(subjugates)])
		if packedLen > maxBytesInRowTuple {
			return errRowTupleTooLarge
		}
		row.srcrows = rowPackedBuf[:packedLen]

		if len(row.tokens) != 0 {
			queue = append(queue, row)
		}
		rownum++
	}
	rows.Close()

	err = flushWriteQueue(e.IndexDB, tabID, queue)
	if err != nil {
		return err
	}

	modStamps, err := e.computeTableModStamps(engineConfig)
	if err != nil {
		return err
	}

	_, err = e.IndexDB.Exec(`INSERT INTO search_src_tables (tablename, modstamp) SELECT $1, ''
				WHERE NOT EXISTS (SELECT * FROM search_src_tables WHERE tablename = $2);`, string(tableName), string(tableName))
	if err != nil {
		return err
	}
	e.ErrorLog.Infof("Updating search_src_tables.modstamp[%v] to %v", string(tableName), modStamps[tableName])
	_, err = e.IndexDB.Exec("UPDATE search_src_tables SET modstamp = $1 WHERE tablename = $2", modStamps[tableName], string(tableName))
	return err
}

// StartAutoVacuum uses gocron to setup a 2 am task that performs the engine's
// vacuum as a preliminary measure to investigate issues with the search service.
// We run into issues when some of our users try to search 'WaterDemandStand'
// One of the suspicions is that the size of this dataset (roughly 740 000
// records), combined with the concurrency model that Postgres uses is leaving
// ghost tuples all over the place.
func (e *Engine) StartAutoVacuum() {
	gocron.Every(1).Day().At("02:00").Do(e.Vacuum)
}

// StartAutoRebuilder launches an auto rebuild loop that never exits.
// This function is never run if Config.DisableAutoIndexRebuild = true.
// Our pause time between checks starts at 1 minute.
// When we encounter a failure, we double our pause time, up to a maximum
// of 30 minutes. As soon as we have success again, we take our pause time back
// down to 1 minute.
func (e *Engine) StartAutoRebuilder() {
	e.ErrorLog.Info("Starting Auto Index Rebuilder")
	last_attempt := time.Now().Add(-time.Hour)
	min_pause := time.Minute
	max_pause := 30 * time.Minute
	pause := min_pause
	for range e.autoRebuildTicker.C {
		if time.Now().Sub(last_attempt) > pause {
			err := e.AutoRebuild()
			if err != nil {
				e.ErrorLog.Errorf("Auto rebuild failed: %v", err)
				pause *= 2
				if pause > max_pause {
					pause = max_pause
				}
			} else {
				pause = min_pause
			}
			last_attempt = time.Now()
		}
	}
}

// StartStateWatcher launches a never-ending loop that wakes up once a minute
// and determines whether we have any stale indexes that need to be rebuilt.
// This is used to inform the front-end that we may be serving up stale results.
// This runs completely independently of the auto rebuilder, but it shares the
// same logic for determining which tables need to be reindexed.
func (e *Engine) StartStateWatcher() {
	config := e.GetConfig()
	last_stale := time.Now().Add(-time.Hour)
	last_rowcount := time.Now() // This runs at startup, so we don't need to run it immediately again
	stale_interval := 60 * time.Second
	rowcount_interval := 5 * time.Minute
	for range e.stateWatchTicker.C {
		if time.Now().Sub(last_stale) > stale_interval {
			rebuild, _, erase, _, err := e.determineStaleTables(config)
			if err != nil {
				e.ErrorLog.Errorf("StateWatcher: determineStaleTables failed: %v", err)
			} else {
				stale := uint32(0)
				if len(rebuild) != 0 || len(erase) != 0 {
					stale = 1
				}
				e.refreshStaleIndexes(rebuild)
				atomic.StoreUint32(&e.isIndexOutOfDate_Atomic, stale)
			}
			last_stale = time.Now()
		}
		if time.Now().Sub(last_rowcount) > rowcount_interval {
			config.updateHttpApiConfig(e)
			last_rowcount = time.Now()
		}
	}
}

// AutoRebuild detect tables that have changed, and rebuild the index on them
// This should be called perhaps once every thirty seconds.
func (e *Engine) AutoRebuild() error {
	config := e.GetConfig()
	rebuild, rebuild_log, erase, erase_log, err := e.determineStaleTables(config)
	if err != nil {
		return err
	}

	// Don't confuse the following 'erase' steps with the implicit erasure that occurs inside BuildIndex.
	// Here, we erase all traces of a table, because it was removed from the config.
	// In the more regular BuildIndex case below, we only remove the indexed tokens, because the table
	// is either new, or has been reconfigured, or has new data.
	if len(erase) != 0 {
		e.ErrorLog.Infof("Auto rebuild: erasing tables from index [%v]", strings.Join(erase_log, ", "))
		if err = e.deleteIndexOnTables(erase, config); err != nil {
			return err
		}
		e.ErrorLog.Infof("Auto rebuild: delete table info")
		if err = e.deleteTableInfo(erase); err != nil {
			return err
		}
	}

	e.refreshStaleIndexes(rebuild)
	if len(rebuild) != 0 {
		// Sort the tables, so that we get consistent tableID values for unit tests
		sort.Sort(tableFullNameSlice(rebuild))
		sort.Strings(rebuild_log)
		e.ErrorLog.Infof("Auto rebuild: reindex [%v]", strings.Join(rebuild_log, ", "))
		if err = e.BuildIndex(rebuild, true); err != nil {
			return err
		}
		e.ErrorLog.Infof("Auto rebuild done")
	}

	if err := e.trimUnusedNames(config); err != nil {
		return err
	}

	return nil
}

// Return the number of tables that need to be reindexed.
func (e *Engine) determineStaleTables(config *Config) (rebuildList []TableFullName, rebuildLogs []string, eraseList []TableFullName, eraseLogs []string, err error) {
	indexedStamps, err := e.readTableModStamps()
	if err != nil {
		return
	}

	currentStamps, err := e.computeTableModStamps(config)
	if err != nil {
		return
	}

	// Find tables that are either new, or have modified content, or modified configuration
	for table, stamp := range currentStamps {
		if indexedStamps[table] != stamp {
			rebuildList = append(rebuildList, table)
			rebuildLogs = append(rebuildLogs, fmt.Sprintf("%v (%v -> %v)", table, indexedStamps[table], stamp))
		}
	}

	// Find tables that were previously indexed, but have now been removed from the config file
	for table, _ := range indexedStamps {
		if cfg := config.tableConfigFromName(table); cfg == nil {
			eraseList = append(eraseList, table)
			eraseLogs = append(eraseLogs, string(table))
		}
	}

	return
}

func (e *Engine) trimUnusedNames(config *Config) error {
	// Find names which are no longer referenced. It is important that we keep search_nametable trimmed,
	// because our 16-bit IDs have limited range. It's conceivable that machine-generated table names and/or
	// field names are continuously added and removed from the search index. If left unchecked, this would
	// eventually break the search engine.
	isUsed := config.allTableAndFieldNames()
	trimSet := []string{}
	rows, err := e.IndexDB.Query("SELECT name FROM search_nametable")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		if !isUsed[name] {
			trimSet = append(trimSet, name)
		}
	}

	if len(trimSet) == 0 {
		return nil
	}

	// Wipe DB
	trimSetSql := ""
	for _, s := range trimSet {
		trimSetSql += escapeSQLLiteral(e.IndexDB, s) + ", "
	}
	trimSetSql = trimSetSql[0 : len(trimSetSql)-2]
	e.ErrorLog.Infof("Trim unused names from search_nametable (%v)", trimSetSql)
	sql := fmt.Sprintf("DELETE FROM search_nametable WHERE name IN (%v)", trimSetSql)
	_, err = e.IndexDB.Exec(sql)

	// Wipe caches
	e.nameToIDLock.Lock()
	defer e.nameToIDLock.Unlock()
	e.nameToID = make(map[string]uint16)

	e.idToNameLock.Lock()
	defer e.idToNameLock.Unlock()
	e.idToName = make(map[uint16]string)

	return err
}

func (e *Engine) readTableModStamps() (map[TableFullName]string, error) {
	rows, err := e.IndexDB.Query("SELECT tablename, modstamp FROM search_src_tables")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	stamps := map[TableFullName]string{}
	for rows.Next() {
		var table sql.NullString
		var stamp sql.NullString
		if err = rows.Scan(&table, &stamp); err != nil {
			return nil, err
		}
		if stamp.Valid {
			stamps[TableFullName(table.String)] = stamp.String
		}
	}
	return stamps, nil
}

// Compute a hash on the modification stamps of all tables, incorporating
// information from both pgstats and modtracker.
// The information from pgstats is unreliable. See https://imqssoftware.atlassian.net/wiki/display/AR/Postgres+stats+are+unreliable for more.
// For this reason, we incorporate modtracker stamps, where available.
func (e *Engine) computeTableModStamps(config *Config) (map[TableFullName]string, error) {
	stamps := map[TableFullName]string{}

	// Make sure that we include all tables, even if they don't exist in the modtracker or pgStats
	allTables := config.allTables()

	pgStats, err := e.computeTableModStamps_PgStat(config)
	if err != nil {
		return nil, err
	}

	modtrack, err := e.computeTableModStamps_ModTracker(config)
	if err != nil {
		return nil, err
	}

	// Union all of the table info, combining pgStats and modtracker data
	for _, table := range allTables {
		stamps[table] = fmt.Sprintf("%v-%v", config.tableConfigHash(table), pgStats[table]+modtrack[table])
		//fmt.Printf("combined %v: %v\n", table, stamps[table])
	}
	return stamps, nil
}

// Compute table modification stamps based on modtracker tables. modtrack_tables maintains
// a mapping between a table name, and an integer. The integer is incremented whenever
// the table is changed. There are no triggers or anything magical to make this happen.
// It is the responsibility of the code that modifies the database, to increment the
// modtracker number.
func (e *Engine) computeTableModStamps_ModTracker(config *Config) (map[TableFullName]int64, error) {
	// config := e.GetConfig()
	stamps := map[TableFullName]int64{}

	//fmt.Printf("Full modtrack scan (%v databases)\n", len(config.Databases))

	for dbName, dbConfig := range config.Databases {
		if len(dbConfig.Tables) == 0 {
			continue
		}
		db, err := e.getSrcDB(dbName, config)
		if err != nil {
			return nil, err
		}

		query := "SELECT tablename, createcount, stamp FROM modtrack_tables"
		isIndexedTable := map[string]bool{}
		for _, table := range dbConfig.tableNames() {
			isIndexedTable[table] = true
		}

		rows, err := db.Query(query)
		if err != nil {
			if strings.Contains(err.Error(), `"modtrack_tables" does not exist`) {
				//fmt.Printf("DB %v has no modtrack table\n", dbName)
				continue
			}
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var table string
			var createcount, stamp int64
			if err = rows.Scan(&table, &createcount, &stamp); err != nil {
				return nil, err
			}
			if !isIndexedTable[table] {
				continue
			}
			// the constant of 1 million is arbitrarily chosen, but should be sufficient
			stamps[MakeTableName(dbName, table)] = createcount*1000000 + stamp
			//fmt.Printf("modtrack %v: %v\n", MakeTableName(dbName, table), stamp)
		}
	}
	//fmt.Printf("modtrack scan finished\n")
	return stamps, nil

}

// Compute a hash on the modification stamps of all tables, by reading
// statistics on the tables. These statistics are maintained by Postgres.
// If we needed to support other databases, then we'd need comparable solutions there.
// A fallback technique would be to use the modtracker system that Albion maintains,
// or something similar.
// Returns a map from table name to (ninsert + nupdate + ndelete)
func (e *Engine) computeTableModStamps_PgStat(config *Config) (map[TableFullName]int64, error) {
	// SELECT relid, n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_user_tables WHERE relname = 'WaterDemandMeterWater'

	stamps := map[TableFullName]int64{}
	// config := e.GetConfig()
	for dbName, dbConfig := range config.Databases {
		if len(dbConfig.Tables) == 0 {
			continue
		}
		db, err := e.getSrcDB(dbName, config)
		if err != nil {
			return nil, err
		}

		query := "SELECT relname, n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_user_tables WHERE relname IN ("
		params := []interface{}{}
		for tableName, _ := range dbConfig.Tables {
			params = append(params, tableName)
			query += fmt.Sprintf("$%v, ", len(params))
		}
		query = query[0:len(query)-2] + ")"

		rows, err := db.Query(query, params...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var table string
			var nInsert, nUpdate, nDelete int64
			if err = rows.Scan(&table, &nInsert, &nUpdate, &nDelete); err != nil {
				return nil, err
			}
			stamp := nInsert + nUpdate + nDelete
			stamps[MakeTableName(dbName, table)] = stamp
			//fmt.Printf("pgstat %v: %v\n", MakeTableName(dbName, table), stamp)
		}
	}
	return stamps, nil
}

func flushWriteQueue(db *sql.DB, tabID tableID, rows []indexedRow) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	//dumpWriteQueue(tabID, rows)
	//ensureWriteQueueHasNoDuplicates(rows)

	st, err := tx.Prepare(pq.CopyIn("search_index", "token", "srcfield", "srcrows"))
	if err != nil {
		return err
	}

	rownum := 0

	for _, row := range rows {
		if rownum%10000 == 0 {
			fmt.Printf(".")
		}
		for _, tokFieldPair := range row.tokens {
			srcfield := makeFieldFullID(tabID, tokFieldPair.fieldID)
			_, err = st.Exec(tokFieldPair.token, srcfield, row.srcrows)
			if err != nil {
				st.Close()
				tx.Rollback()
				return err
			}
		}
		rownum++
	}

	_, err = st.Exec()
	if err != nil {
		fmt.Printf("(error on Exec: %v)\n", err)
		st.Close()
		tx.Rollback()
		return err
	}

	err = st.Close()
	if err != nil {
		fmt.Printf("(error on Close: %v)\n", err)
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		fmt.Printf("(error on Commit: %v)\n", err)
		return err
	}

	fmt.Printf("done\n")
	return nil
}

func ensureWriteQueueHasNoDuplicates(rows []indexedRow) {
	all := map[string]bool{}
	for _, row := range rows {
		for _, tok := range row.tokens {
			srcrow, _ := unpackNextRow(row.srcrows)
			key := fmt.Sprintf("%v.%v.%v", tok.token, tok.fieldID, srcrow)
			if all[key] {
				panic(fmt.Sprintf("duplicate key %v", key))
			}
			all[key] = true
		}
	}
}

func dumpWriteQueue(tabID tableID, rows []indexedRow) {
	fmt.Printf("\n")
	for _, row := range rows {
		srcrows := [maxRowsInTuple]int64{}
		unpackRows(row.srcrows, srcrows[:])
		rowtext := "["
		for _, r := range srcrows {
			rowtext += fmt.Sprintf("%v,", r)
		}
		rowtext = rowtext[:len(rowtext)-1] + "]"
		for _, tok := range row.tokens {
			fmt.Printf("%v %v.%v %v\n", tok.token, tabID, tok.fieldID, rowtext)
		}
	}
}
