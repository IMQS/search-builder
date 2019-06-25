package server

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

/*
We initially tried to get the DB to perform the joins across the different tokens.
That turned out to produce generally terrible results, as you'd often end up with
full table scans. On 2016-02-04, I removed that old code path.

This is an example of the kind of statement that we generated, which produced the bad results:

select t0.srctab, t0.srcrow from (
	(select srctab, srcrow from search_index where (token >= '85' and token < '86')) as t0
	INNER JOIN
	(select srctab, srcrow from search_index where (token >= '363' and token < '365')) as t1
	ON t0.srctab = t1.srctab AND t0.srcrow = t1.srcrow
	INNER JOIN
	(select srctab, srcrow from search_index where (token >= '66' and token < '67')) as t2
	ON t0.srctab = t2.srctab AND t0.srcrow = t2.srcrow
)
*/

const defaultFindResultNum = 10
const maxFindResultNum = 1000
const maxFieldsPerTable = 32 // Must be divisible by 32, because we store touched fields in a bitmap of 32-bit integers
const maxTokensInQuery = 24  // Must be divisible by 4 (because we pack 4 stokenState values into each byte)

type Query struct {
	SendRelatedRecords bool
	MaxResultNum       int // If zero, then use defaultFindResultNum. Hard clamped to maxFindResultNum.
	Query              string
}

func (q *Query) effectiveMaxResultNum() int {
	if q.MaxResultNum <= 0 {
		return defaultFindResultNum
	}
	if q.MaxResultNum > maxFindResultNum {
		return maxFindResultNum
	}
	return q.MaxResultNum
}

type FindResult struct {
	TimeQueryParse time.Duration
	TimeDBQuery    time.Duration
	TimeDBFetch    time.Duration
	TimeTotal      time.Duration
	TimeAuxFetch   time.Duration
	TimeKeywords   time.Duration
	TimeRelWalk    time.Duration
	TimeSort       time.Duration
	RawDBRowCount  int64
	NumValidRows   int
	Rows           []*FindResultRow
	StaleIndex     bool
}

type FindResultRelatedRow struct {
	SrcTab tableID
	RowKey int64
}

type FindResultRow struct {
	Table          TableFullName
	SrcTab         tableID
	RowKey         int64
	Rank           uint16
	Values         map[string]string      // Arbitrary field:value pairs from this row that we decide to return with the search results
	RelatedRecords []FindResultRelatedRow `json:",omitempty"`
}

func (r *FindResultRow) hashKey() string {
	return resultRowHashKey(r.SrcTab, r.RowKey)
}

func (r *FindResultRow) copyFromResultRow(src *resultRow) {
	r.RowKey = src.srcRowKey()
	r.SrcTab = src.srctab
	r.Rank = src.rank
}

func (r *FindResultRow) populateTableName(e *Engine) error {
	var tableName, err = e.getNameFromTableID(r.SrcTab)
	if err != nil {
		return err
	}
	r.Table = TableFullName(tableName)
	return nil
}

// We store this in tokenStateArray using 2 bits per entry
type tokenState uint8

// Note - it is important that these are in increasing order of "goodness", for the sake of raiseTokenState
const (
	tokenStateNotFound     tokenState = 0
	tokenStateKeywordMatch            = 1
	tokenStatePrefixMatch             = 2
	tokenStateExactMatch              = 3
)

type tokenStateArray [maxTokensInQuery / 4]uint8

func (t *tokenStateArray) get(item int) tokenState {
	return tokenState(load2Bit(t[:], item))
}

func (t *tokenStateArray) set(item int, state tokenState) {
	store2bit(t[:], item, uint8(state))
}

const (
	_                                 = iota
	resultRowFlag_HasPairMatch uint16 = 1 << iota // This row has at least one matching <pair> instruction
)

/* Intermediate search result.

40 bytes per object.

It is a tricky decision here, how we choose to store 'srcrows'. Remember this is the tuple
of rows, where the first element is the primary row where the token was found, and all
the remaining tuples are the records that join onto that primary row, via our relationship
system.
Firstly, do we store it in packed varints, or do we store them as plain int64s?
Secondly, do we use a slice to store them, or do we store them in a static array.
Slice overhead is ptr + 2 * int, so on 64-bit that's 24 bytes.
So let's compare storage of different options, all capable of storing 5 records (ie one primary and 4 joined):

int64 array: 40
int64 slice: 64 (24 + 40)
[24]byte:    24            (assume an average of 4.8 bytes per varint)
[]byte:      48 (24 + 24)
*/
type resultRow struct {
	srctab  tableID                        // [ 2 bytes]
	tokens  tokenStateArray                // [ 6 bytes] For each token in the query, record tokenState
	srcrows [maxBytesInRowTuple]byte       // [24 bytes]
	fields  [maxFieldsPerTable / 32]uint32 // [ 4 bytes] For each indexed field of 'srctab', record whether it was matched
	flags   uint16                         // [ 2 bytes] resultRowFlag_*
	rank    uint16                         // [ 2 bytes] Computed by computeRank()
}

// Unpack and return the first int64 in srcrows
func (r *resultRow) srcRowKey() int64 {
	val, _ := unpackNextRow(r.srcrows[:])
	return val
}

// Sets token state to max(existing, new state)
func (r *resultRow) raiseTokenState(token int, state tokenState) {
	if r.tokens.get(token) < state {
		r.tokens.set(token, state)
	}
}

func (r *resultRow) isFieldMatched(field int) bool {
	return getBit(r.fields[:], field)
}

func (r *resultRow) setFieldMatched(field int) {
	setBit(r.fields[:], field, true)
}

func (r *resultRow) isValidResult(tokens []string, queryHasPairs bool) bool {
	if queryHasPairs && (r.flags&resultRowFlag_HasPairMatch) == 0 {
		return false
	}

	// We only reject a result if it has an unmatched token of length >= 2.
	// In other words, a missing result for a single-character token is OK (it merely has a lower rank).

	// We are intimately using the knowledge here that the token parser includes the entire
	// string as the first token. For that reason, we don't mandate that the first field is necessary.

	for i := 1; i < len(tokens); i++ {
		if r.tokens.get(i) == tokenStateNotFound && len(tokens[i]) >= 2 {
			return false
		}
	}
	return true
}

func (r *resultRow) computeRank(tokens []string) {
	// Defining exactly how to rank search results is really the hardest part of search.
	// We layout some very simple criteria here for ourselves, because we really
	// don't know what else to do.
	// Two prefix matches must outweigh one exact match       (3+3 > 5)
	// Two keyword matches should outweigh one prefix match   (2+2 > 3)
	rank := uint16(0)
	for i := 0; i < len(tokens); i++ {
		switch r.tokens.get(i) {
		case tokenStateKeywordMatch:
			rank += 2
		case tokenStatePrefixMatch:
			rank += 3
		case tokenStateExactMatch:
			rank += 5
		}
	}
	r.rank = rank
}

func (r *resultRow) mergeRelatedIntoSelf(ntokens int, related *resultRow) {
	for i := 0; i < ntokens; i++ {
		r.raiseTokenState(i, related.tokens.get(i))
	}
}

func (r *resultRow) debugRank(tokens []string, queryHasPairs bool) {
	fmt.Printf("%v %v %v\n", r.srctab, r.srcRowKey(), r.isValidResult(tokens, queryHasPairs))
	for i := 0; i < len(tokens); i++ {
		switch r.tokens.get(i) {
		case tokenStateKeywordMatch:
			fmt.Printf("Keyword match on %v\n", tokens[i])
		case tokenStatePrefixMatch:
			fmt.Printf("Prefix match on %v\n", tokens[i])
		case tokenStateExactMatch:
			fmt.Printf("Exact match on %v\n", tokens[i])
		}
	}
}

func (r *resultRow) hashKey() string {
	return resultRowHashKey(r.srctab, r.srcRowKey())
}

func resultRowHashKey(tab tableID, row int64) string {
	return fmt.Sprintf("%v %v", tab, row)
}

type resultRowByRank []*resultRow

func (a resultRowByRank) Len() int      { return len(a) }
func (a resultRowByRank) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a resultRowByRank) Less(i, j int) bool {
	iRank := a[i].rank
	jRank := a[j].rank
	if iRank == jRank {
		if a[i].srctab == a[j].srctab {
			return a[i].srcRowKey() > a[j].srcRowKey()
		} else {
			return a[i].srctab > a[j].srctab
		}
	}
	return iRank < jRank
}

type findResultRowByTable []*FindResultRow

func (a findResultRowByTable) Len() int      { return len(a) }
func (a findResultRowByTable) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a findResultRowByTable) Less(i, j int) bool {
	if a[i].Table != a[j].Table {
		return a[i].Table < a[j].Table
	}
	// The row ordering here is tightly coupled to the use of ORDER BY in our
	// SQL statement that retrieves records for this table
	return a[i].RowKey < a[j].RowKey
}

type cachedSubjugate struct {
	foreignTableID  tableID
	foreignTable    *ConfigTable
	relation        *ConfigRelation
	useInSearchJoin bool
}

type cachedSubjugateList []*cachedSubjugate

// This is similar to fieldFullID, but whereas fieldFullID uses the field ID stored inside search_nametable,
// here we store the field index inside the config. This is a temporary data structure. We use this because
// we are guaranteed to have a low number of contiguous integers that we can use to refer to a field. This
// makes the encoding of this information amenable to storage inside a bitmap (concretely: resultRow.fields).
type tableFieldIndexPair uint32

func makeTableFieldIndexPair(tableID tableID, fieldIndex int) tableFieldIndexPair {
	return tableFieldIndexPair((uint32(tableID) << 16) | uint32(fieldIndex))
}

func lexIncrement(token string) string {
	runes := stringToRunes(token)
	runes[len(runes)-1]++
	return runesToString(runes)
}

// Cached lookup table so that we can quickly determine a field's index, given it's fullFieldID
type fieldIDToIndexCache map[fieldFullID]int

func (cache fieldIDToIndexCache) getFieldIndex(e *Engine, fieldID fieldFullID) (int, error) {
	if fieldIndex, ok := cache[fieldID]; ok {
		return fieldIndex, nil
	}

	config, err := e.getTableFromID(fieldID.extractTableID())
	if err != nil {
		return -1, err
	}

	fieldName, err := e.getNameFromFieldID(fieldID.extractFieldID())
	if err != nil {
		return -1, err
	}

	if fieldIndex := config.fieldIndex(fieldName); fieldIndex == -1 {
		// This condition is benign is the index is out of date. What this implies is that a field which
		// was once indexed, is now no longer specified in the config.
		if e.isIndexOutOfDate() {
			return -1, errFieldNotFoundInConfig
		} else {
			tableName, _ := e.getNameFromTableID(fieldID.extractTableID())
			return -1, fmt.Errorf("Field not found during 'find': %v.%v, ID = %v.%v", tableName, fieldName, fieldID.extractTableID(), fieldID.extractFieldID())
		}
	} else {
		cache[fieldID] = fieldIndex
		return fieldIndex, nil
	}
}

// Returns true if the error can be ignored
func (cache fieldIDToIndexCache) isBenignError(err error) bool {
	return err == errFieldNotFoundInConfig
}

/* Perform the joins across different query tokens in Go code

This function has the words "manual join" in it's name, because it is distinct from
getting the database to perform the join (using classic SQL expressions for joining
results together). Initial experiments determined that using the DB to perform the
joins was in many cases far slower than doing it ourselves, "manually". We have
subsequently implemented features that would be hard to perform if the database
was to perform the join, so we can't really go back now, even if the database
performance problem was to be magically solved.

Rough outline of this function's operation:

1. Preparse the query, extracting special instructions such as <field:...> or <pairs:...>
2. Parse the remaining query string into search tokens. Place them in an array of strings called "tokens".
3. For each token, perform a DB query. Add all results of these queries into an intermediate array of 'resultRow' objects.
4. Iterate through all of the 'pairs' instructions, and execute the necessary DB index query for each pair.
   Merge these results into the existing set of 'resultRow' objects.
4. Perform keyword matching. For example, if one of the search tokens is "diameter", and we find a record
   inside a table with the keyword "diameter", then mark that token as being satisfied, despite it not
   existing inside the database data.
5. Discard invalid resultRow objects. An example of an invalid row is one where not all tokens were found.
6. Sort results according to their rank.
7. Return the best N results.

On the topic of searching by prefix vs exact match, when tokens are short:
On a sample dataset, searching for the single token "2":
prefix match: Total query runtime: 11302 ms. 579762 rows retrieved.
exact match: Total query runtime: 263 ms. 15299 rows retrieved.
Bottom line: searching for single character tokens is too expensive if we treat them as a prefix.
A common search concept is the "trigram", which is three characters in sequence. We should really
move away from being able to search for single characters, and instead have intelligent indexing
which recognizes things such as street addresses, and combines the single digit house number
with the street, before indexing.
*/
func (e *Engine) findWithManualJoin(query *Query, config *Config) (*FindResult, error) {
	res := &FindResult{}
	// Process special instructions
	startParse := time.Now()
	err, preprocessedQuery, special := queryPreparse(query.Query)
	if err != nil {
		return nil, err
	}
	res.StaleIndex = e.anyStaleIndex(special.fields)

	if err = special.realize(e); err != nil {
		return nil, err
	}

	parser := NewDefaultParser()
	tokens := parser.Tokenize(preprocessedQuery)
	if len(tokens) > maxTokensInQuery {
		return nil, errUnterminatedInstruction
	}

	includeFieldsExp := buildIncludeFieldExpression(&special)

	res.TimeQueryParse = time.Now().Sub(startParse)

	tx, err := e.IndexDB.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	// all rows that have at least one token match
	foundRows := map[string]*resultRow{}

	// Cached lookup table so that we can quickly determine a field's index, given it's fullFieldID
	fieldIDToFieldIndex := make(fieldIDToIndexCache)

	// Query the index for the tokens
	for p, part := range tokens {
		var rows *sql.Rows
		startQuery := time.Now()
		// See comment at top of this function for some runtime numbers of prefix search vs full match
		if len(part) == 1 {
			rows, err = tx.Query(fmt.Sprintf("SELECT token,srcfield,srcrows FROM search_index WHERE (token = $1) %v", includeFieldsExp), part)
		} else {
			rows, err = tx.Query(fmt.Sprintf("SELECT token,srcfield,srcrows FROM search_index WHERE (token >= $1 AND token < $2) %v", includeFieldsExp), part, lexIncrement(part))
		}
		if err != nil {
			return nil, fmt.Errorf("When querying tokens in index: %v", err)
		}
		defer rows.Close()
		res.TimeDBQuery += time.Now().Sub(startQuery)

		startFetch := time.Now()
		for rows.Next() {
			var token string
			var srcfield fieldFullID
			var srcrowsPacked []byte
			if err := rows.Scan(&token, &srcfield, &srcrowsPacked); err != nil {
				return nil, fmt.Errorf("When scanning tokens in index: %v", err)
			}
			res.RawDBRowCount++

			outRow := mergeTokenIntoFoundRows(foundRows, srcfield, srcrowsPacked, true)

			if token == part {
				outRow.raiseTokenState(p, tokenStateExactMatch)
			} else {
				outRow.raiseTokenState(p, tokenStatePrefixMatch)
			}

			if fieldIndex, err := fieldIDToFieldIndex.getFieldIndex(e, srcfield); err != nil {
				if !fieldIDToFieldIndex.isBenignError(err) {
					return nil, err
				}
			} else {
				outRow.setFieldMatched(fieldIndex)
			}
		}
		res.TimeDBFetch += time.Now().Sub(startFetch)
	}

	// Query the index for the pair instructions (eg Type=Valve).
	pairInstructions, err := special.generatePairInstructionSets(e)
	if err != nil {
		return nil, err
	}
	for _, instruction := range pairInstructions {
		var rows *sql.Rows
		var tokenORList string
		startQuery := time.Now()

		if instruction.isFuzzy {
			tokenORList = buildFuzzyTokenORList(e, instruction.tokens)
		} else {
			tokenORList = buildTokenORList(e, instruction.tokens)
		}

		rows, err = tx.Query(fmt.Sprintf("SELECT srcrows FROM search_index WHERE %v AND (srcfield = $1)", tokenORList), int32(instruction.fieldID))
		if err != nil {
			return nil, fmt.Errorf("When querying pair tokens in index: %v", err)
		}
		defer rows.Close()
		res.TimeDBQuery += time.Now().Sub(startQuery)

		startFetch := time.Now()
		for rows.Next() {
			var srcrowsPacked []byte
			if err := rows.Scan(&srcrowsPacked); err != nil {
				return nil, fmt.Errorf("When scanning pair tokens in index: %v", err)
			}
			res.RawDBRowCount++
			outRow := mergeTokenIntoFoundRows(foundRows, instruction.fieldID, srcrowsPacked, len(tokens) == 0)
			if outRow != nil {
				outRow.flags |= resultRowFlag_HasPairMatch
			}
		}
		res.TimeDBFetch += time.Now().Sub(startFetch)
	}

	startKeywords := time.Now()
	e.addKeywordMatchesToResults(tokens, foundRows, config)
	res.TimeKeywords = time.Now().Sub(startKeywords)

	startRelations := time.Now()
	tabToSubjugates := map[tableID]cachedSubjugateList{}
	e.mixRelatedRows(tokens, tabToSubjugates, foundRows, config)
	res.TimeRelWalk = time.Now().Sub(startRelations)

	startSort := time.Now()
	finalRows := []*resultRow{}
	for _, row := range foundRows {
		//row.debugRank(tokens, len(special.pairs) != 0)
		if row.isValidResult(tokens, len(special.pairs) != 0) {
			row.computeRank(tokens)
			finalRows = append(finalRows, row)
		}
	}
	// It is tempting here to say "If there are no search tokens, then don't sort."
	// However, for the sake of testing consistency, we sort always.
	sort.Sort(sort.Reverse(resultRowByRank(finalRows)))
	res.TimeSort = time.Now().Sub(startSort)

	numResults := len(finalRows)
	if numResults > query.effectiveMaxResultNum() {
		numResults = query.effectiveMaxResultNum()
	}

	for i := 0; i < numResults; i++ {
		src := finalRows[i]
		dst := &FindResultRow{}
		dst.copyFromResultRow(src)
		if err := dst.populateTableName(e); err != nil {
			return nil, err
		}
		res.Rows = append(res.Rows, dst)
		if query.SendRelatedRecords {
			e.addRelatedRowsReference(tabToSubjugates, src, dst)
		}
	}

	res.NumValidRows = numResults

	if query.SendRelatedRecords {
		// Mark all rows that are in the result set
		rowIsInResult := map[string]bool{}
		for _, prim := range res.Rows {
			rowIsInResult[prim.hashKey()] = true
		}

		// Add all related rows that are not already in the result set
		for _, prim := range res.Rows {
			for _, rel := range prim.RelatedRecords {
				key := resultRowHashKey(rel.SrcTab, rel.RowKey)
				if !rowIsInResult[key] {
					dst := &FindResultRow{}
					if src, ok := foundRows[key]; !ok {
						// This path is hit when the related record was not part of the original result
						// set that we pulled out of the index. i.e. it is simply a related table, but
						// wasn't involved in the query.
						dst.SrcTab = rel.SrcTab
						dst.RowKey = rel.RowKey
					} else {
						// This path is hit when the related record was found via one of it's tokens. In this
						// case the related record has a rank, so we include it. But we probably shouldn't, because
						// it doesn't matter. We could probably get rid of this code path - I don't see it
						// adding real value.
						dst.copyFromResultRow(src)
					}
					if err := dst.populateTableName(e); err != nil {
						return nil, err
					}
					res.Rows = append(res.Rows, dst)
					rowIsInResult[key] = true
				}
			}
		}
	}

	return res, nil
}

func mergeTokenIntoFoundRows(foundRows map[string]*resultRow, srcfield fieldFullID, srcrowsPacked []byte, addIfNotExisting bool) *resultRow {
	primaryRow, _ := unpackNextRow(srcrowsPacked)
	outHash := resultRowHashKey(srcfield.extractTableID(), primaryRow)
	outRow, exist := foundRows[outHash]
	if !exist && addIfNotExisting {
		outRow = &resultRow{
			srctab: srcfield.extractTableID(),
		}
		copy(outRow.srcrows[:], srcrowsPacked)
		foundRows[outHash] = outRow
	}
	return outRow
}

func (e *Engine) dumpResultRow(srcrow int64) {
	fmt.Printf("Found %v\n", srcrow)
}

func buildTokenORList(e *Engine, tokens []string) string {
	sql := "("
	for _, token := range tokens {
		sql += "token = " + escapeSqlLiteral(e.IndexDB, token) + " OR "
	}
	sql = sql[0:len(sql)-4] + ")"
	return sql
}

func buildFuzzyTokenORList(e *Engine, tokens []string) string {
	sql := "("
	for _, token := range tokens {
		sql += "token >= " + escapeSqlLiteral(e.IndexDB, token) + " AND token < " + escapeSqlLiteral(e.IndexDB, lexIncrement(token)) + " OR "
	}
	sql = sql[0:len(sql)-4] + ")"
	return sql
}

func buildIncludeFieldExpression(special *specialInstructions) string {
	includeFields := []string{}
	for _, fieldID := range special.fieldIDs {
		includeFields = append(includeFields, strconv.Itoa(int(fieldID)))
	}
	includeFieldsExp := ""
	if len(includeFields) != 0 {
		includeFieldsExp = " AND (srcfield IN (" + strings.Join(includeFields, ",") + "))"
	}
	return includeFieldsExp
}

// Give scores for metadata. For example, if the query is "water 300", and you've found
// pipes with diameter = 300, then mark the token "water" as matched.
func (e *Engine) addKeywordMatchesToResults(tokens []string, rows map[string]*resultRow, config *Config) error {

	// When thinking about this code, I like to keep the following example in mind:
	// The query is "pipe diameter 300"
	// There are three tokens: PIPE  DIAMETER  300
	// Only 300 is matched by a token in the index.
	// We now need to figure out how PIPE and DIAMETER are satisfied.
	// PIPE is satisfied because the record where we found the 300 was inside a table called "WaterPipes".
	// The table name WaterPipes is not sufficient, but what DOES make it sufficient, is that the table
	// WaterPipes has the word "pipe" in it's Keywords. This means that any record inside resultRow that
	// belongs to WaterPipes will have it's PIPE token marked as satisfied (ie as matched by a prefix).
	// The verification of DIAMETER is a little more complex. DIAMETER does not appear as a keyword inside
	// the WaterPipes table config. Instead, DIAMETER is special because it matches the friendly name of
	// a field in WaterPipes called "Diameter". What's more, when we look at the bitmap of matched fields
	// inside resultRow, we observe that the field called Diameter was indeed matched by at least one token.
	// Because of this intersection, we grant the DIAMETER token as being satisfied. Think of a failure
	// case, where 300 matched a field called SIZE. In this case, the bitmap would not match the particular
	// field, so the token DIAMETER would not be satisfied.

	// Phase 1: Scan through all tokens.
	// For each token, scan through all configs, and discover:
	//   a) Which tables match that token. A table matches a token via it's keywords, name, or friendly name.
	//   b) Which fields match that token. A field matches a token via it's name or friendly name.

	// For each token, store a list of tables that match that token. Simply matching a row from
	// one of those blessed tables means that token is satisfied.
	blessedTables := make([]map[tableID]bool, len(tokens))

	// For each token, store a list of fields that match that token. In order for this to be applicable,
	// the resultRow.fields bitmap must already have it's bit set for the potentially matching field.
	blessedFields := make([]map[tableFieldIndexPair]bool, len(tokens))

	for p, part := range tokens {
		blessedTables[p] = make(map[tableID]bool)
		blessedFields[p] = make(map[tableFieldIndexPair]bool)
		for dbName, db := range config.Databases {
			for tableName, table := range db.Tables {
				tabID, err := e.getTableID(MakeTableName(dbName, tableName), true)
				if err != nil {
					return err
				}
				for _, tableToken := range table.tokenizedKeywords {
					if tableToken == part {
						blessedTables[p][tabID] = true
						//fmt.Printf("blessedTables[%v][%v] = true (part = %v)\n", p, tabID, part)
					}
				}
				for fieldIndex, field := range table.Fields {
					for _, fieldToken := range field.tokenizedName {
						if fieldToken == part {
							blessedFields[p][makeTableFieldIndexPair(tabID, fieldIndex)] = true
						}
					}
				}
			}
		}
	}

	// Phase 2: Scan through result rows, and raise the match state of each token, if that token
	// matches a table or a field.

	// Cache the number of fields per table
	tableFieldCount := map[tableID]int{}

	for _, row := range rows {
		for p, _ := range tokens {
			if row.tokens.get(p) == tokenStateNotFound {
				if blessedTables[p][row.srctab] {
					//fmt.Printf("Raising token[%v] to KeywordMatch\n", p)
					row.raiseTokenState(p, tokenStateKeywordMatch)
				} else {
					nFields, ok := tableFieldCount[row.srctab]
					if !ok {
						config, err := e.getTableFromID(row.srctab)
						if err != nil {
							return err
						}
						nFields = len(config.Fields)
					}
					for i := 0; i < nFields; i++ {
						if row.isFieldMatched(i) && blessedFields[p][makeTableFieldIndexPair(row.srctab, i)] {
							row.raiseTokenState(p, tokenStateKeywordMatch)
							break
						}
					}
				}
			}
		}
	}

	return nil
}

func (e *Engine) addValuesToResults(results []*FindResultRow, config *Config) error {
	sorted := make(findResultRowByTable, len(results))
	copy(sorted, results)
	sort.Sort(sorted)

	queue := []*FindResultRow{}
	processQueue := func() error {
		if len(queue) == 0 {
			return nil
		}
		db, err := e.getSrcDB(queue[0].Table.DBOnly(), config)
		if err != nil {
			return err
		}
		esc := func(ident string) string {
			return escapeSqlIdent(db, ident)
		}

		config := config.tableConfigFromName(queue[0].Table)

		// Check if table has geometry column. If so, also send back bounding box
		// TODO: remove assumption that geometry column is named "Geometry"
		hasGeom := false
		hasGeomMaybe := config.hasGeometry.load()
		if hasGeomMaybe != atomicTriState_Nil {
			hasGeom = hasGeomMaybe == atomicTriState_True
		} else {
			sql := fmt.Sprintf("SELECT %v FROM %v LIMIT 1", esc("Geometry"), esc(queue[0].Table.TableOnly()))
			_, err := db.Query(sql)
			if err == nil {
				config.hasGeometry.store(atomicTriState_True)
			} else {
				config.hasGeometry.store(atomicTriState_False)
			}
		}

		// Return all indexed fields for this table
		fields := config.Fields
		query := "SELECT "
		for _, field := range fields {
			query += fmt.Sprintf("%v,", esc(field.Field))
		}
		if hasGeom {
			query += fmt.Sprintf("BOX2D(%v) AS %v,", esc("Geometry"), esc("Bounds"))
		}
		query = query[0 : len(query)-1]
		query += fmt.Sprintf(" FROM %v WHERE %v in (", esc(queue[0].Table.TableOnly()), esc(config.IndexField))
		for _, q := range queue {
			query += fmt.Sprintf("%v,", q.RowKey)
		}
		query = query[0 : len(query)-1]
		query += fmt.Sprintf(") ORDER BY %v", esc(config.IndexField)) // The ORDER BY here is tightly coupled to the sort order of findResultRowByTable
		// fmt.Printf("Executing (%v)\n", query)
		rows, err := db.Query(query)
		if err != nil {
			return fmt.Errorf("When reading indexed fields of %v: %v", queue[0].Table, err)
		}

		if hasGeom {
			fields = append(fields, &ConfigField{Field: "Bounds", FriendlyName: "Bounds"})
		}

		for irow := 0; rows.Next(); irow++ {
			values := [maxFieldsPerTable + 1]sql.NullString{} // Because "Bounds" is not one of the indexed fields, we need to increase capacity to maxFieldsPerTable + 1
			values_if := [maxFieldsPerTable + 1]interface{}{}
			for i := 0; i < len(fields); i++ {
				values_if[i] = &values[i]
			}
			if err := rows.Scan(values_if[:len(fields)]...); err != nil {
				return err
			}
			queue[irow].Values = make(map[string]string)
			for j := 0; j < len(fields); j++ {
				queue[irow].Values[fields[j].Field] = values[j].String
			}
		}

		queue = []*FindResultRow{}
		return nil
	}

	var lastTable TableFullName
	for i, _ := range sorted {
		if sorted[i].Table != lastTable {
			if err := processQueue(); err != nil {
				return err
			}
		}
		queue = append(queue, sorted[i])
	}
	// Process the final table
	return processQueue()
}

/* Walk the relationship graph and allow related records to boost each other's results.
I suspect there are some performance wins to be had here by bucketing our resultRow
list so that they're grouped by srctab. This code feels extremely branchy, in the
unpredictable-branch sense, because we're skipping from one type of record to another.
However, in the absence of benchmarking, I don't want to make any "optimizations" yet.
*/
func (e *Engine) mixRelatedRows(tokens []string, tabToSubjugates map[tableID]cachedSubjugateList, rows map[string]*resultRow, config *Config) error {
	for _, row := range rows {
		// Get the list of subjugates (we cache them the first time we see a new root table)
		subs := tabToSubjugates[row.srctab]
		if subs == nil {
			table, err := e.getTableFromID(row.srctab)
			if err != nil {
				return err
			}

			dbName, db := config.getDatabaseFromTable(table)
			rels := table.subjugateRelations(db)
			for _, rel := range rels {
				foreignTableID, err := e.getTableID(MakeTableName(dbName, rel.relation.ForeignTable), true)
				if err != nil {
					return err
				}
				subs = append(subs, &cachedSubjugate{
					foreignTableID:  foreignTableID,
					relation:        rel.relation,
					foreignTable:    rel.foreignTable,
					useInSearchJoin: rel.useInSearchJoin,
				})
			}
			//fmt.Printf("%v.%v subs: %v\n", dbName, table.FriendlyName, subs)
			tabToSubjugates[row.srctab] = subs
		}

		// overlay the matched tokens of our subjugates into our own result
		_, vpos := unpackNextRow(row.srcrows[:])
		for _, sub := range subs {
			subRowNum, inc := unpackNextRow(row.srcrows[vpos:])
			vpos += inc
			if !sub.useInSearchJoin {
				// As a performance optimization, we don't include this row in our result, because
				// this row is related to us OneToOne, but when we visit HIM, then HE will raise
				// HIS rank because of his relation to us. This is probably a trivial optimization,
				// and I'm not sure it's really worth keeping track of "useInSearchJoin", for total
				// code size sake.
				continue
			}
			//fmt.Printf("Reading sub %v/%v. vpos = %v. subRowNum = %v. len(row.srcrows) = %v\n", isub+1, len(subs), vpos, subRowNum, len(row.srcrows))
			subResultRow := rows[resultRowHashKey(sub.foreignTableID, subRowNum)]
			if subResultRow != nil {
				//fmt.Printf("Merging related row %v -----------------------\n", subRowNum)
				row.mergeRelatedIntoSelf(len(tokens), subResultRow)
			}
		}
	}
	return nil
}

func (e *Engine) addRelatedRowsReference(tabToSubjugates map[tableID]cachedSubjugateList, src *resultRow, res *FindResultRow) {
	subs := tabToSubjugates[src.srctab]
	_, vpos := unpackNextRow(src.srcrows[:])
	for i := 0; i < len(subs); i++ {
		relRow, inc := unpackNextRow(src.srcrows[vpos:])
		res.RelatedRecords = append(res.RelatedRecords, FindResultRelatedRow{subs[i].foreignTableID, relRow})
		vpos += inc
	}
}

func (e *Engine) anyStaleIndex(fields []fieldFullName) bool {
	e.staleTablesLock.RLock()
	defer e.staleTablesLock.RUnlock()
	for _, field := range fields {
		if e.staleTables[field.table()] {
			return true
		}
	}
	return false
}

func (e *Engine) refreshStaleIndexes(rebuildList []TableFullName) {
	e.staleTablesLock.Lock()
	e.staleTables = make(map[TableFullName]bool)
	for _, staleTable := range rebuildList {
		e.staleTables[staleTable] = true
	}
	e.staleTablesLock.Unlock()
}
