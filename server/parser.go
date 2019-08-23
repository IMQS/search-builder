package server

import (
	"bytes"
	"fmt"
	"strings"
	"unicode/utf8"
)

const defaultMinimumTokenLength = 2

// Split a source string into zero or more tokens.
type Parser interface {
	// The result set may not contain duplicates
	Tokenize(src string) []string
}

// The default IMQS parser
type DefaultParser struct {
	MinimumTokenLength            int
	ExcludeEntireStringFromResult bool
}

var defaultSplitters []rune
var ignoredWords map[string]bool

type states int

const (
	state_space states = iota
	state_word_ignored_prefix
	state_word_active
)

type fieldValuePair struct {
	isFuzzy bool
	field   fieldFullName
	value   string
}

type pairInstruction struct {
	isFuzzy bool
	fieldID fieldFullID
	tokens  []string
}

type specialInstructions struct {
	fields []fieldFullName
	pairs  []*fieldValuePair

	// fieldIDs is a cache of 'fields', and is built up by realize()
	fieldIDs []fieldFullID
}

func (s *specialInstructions) realize(e *Engine) error {
	e.ErrorLog.Debug("parser.go: realize")

	if err := s.fixPairFieldAmbiguity(e); err != nil {
		return err
	}

	// ahem -- this is not right - we actually need to perform the pair searches afterwards, in a slightly separate phase
	//s.ensurePairFieldsAreSearched()

	// translate 'fields' into 'fieldIDs'
	s.fieldIDs = make([]fieldFullID, len(s.fields))
	for ifield, f := range s.fields {
		fieldID, err := s.lookupFieldID(e, f)
		if err != nil {
			return err
		}
		s.fieldIDs[ifield] = fieldID
	}

	return nil
}

// Ensure that all tables that are touched by 'pairs' instructions are covered by 'fields'
// This procedure, and thinking behind it, are documenting in 'doc.go'. Search for "ambiguity".
func (s *specialInstructions) fixPairFieldAmbiguity(e *Engine) error {
	e.ErrorLog.Debug("parser.go: fixPairFieldAmbiguity")

	seenTable := map[TableFullName]bool{}
	for _, pair := range s.pairs {
		pairTable := pair.field.table()
		if seenTable[pairTable] {
			continue
		}
		seenTable[pairTable] = true

		tableExistsInFields := false
		for _, field := range s.fields {
			if pairTable == field.table() {
				tableExistsInFields = true
				break
			}
		}

		if !tableExistsInFields {
			if err := s.addAllFieldsFromTable(e, pairTable); err != nil {
				return err
			}
		}
	}
	return nil
}

/* Make sure that if a field=value pair exists in the query, then that field
also exists in the list of searched fields. This is for efficiency, so that
we don't need to do a row fetch from the original source table. Instead,
we have that information readily available from the search index.
*/
func (s *specialInstructions) ensurePairFieldsAreSearched() {
	for _, pair := range s.pairs {
		if !s.hasField(pair.field) {
			s.fields = append(s.fields, pair.field)
		}
	}
}

func (s *specialInstructions) addAllFieldsFromTable(e *Engine, table TableFullName) error {
	e.ErrorLog.Debugf("parser.go: addAllFieldsFromTable: %v", table)

	config, err := e.getTableFromName(table, true)
	if err != nil {
		return err
	}
	for _, field := range config.Fields {
		full := makeFieldFullName(table, field.Field)
		if !s.hasField(full) {
			s.fields = append(s.fields, full)
		}
	}
	return nil
}

func (s *specialInstructions) hasField(field fieldFullName) bool {
	for _, f := range s.fields {
		if f == field {
			return true
		}
	}
	return false
}

func (s *specialInstructions) lookupFieldID(e *Engine, fieldFull fieldFullName) (fieldFullID, error) {
	tableName, fieldNameOnly := fieldFull.split()
	config, err := e.getTableFromName(tableName, true)
	if err != nil {
		return 0, err
	}
	tableID, err := e.getTableID(tableName, true)
	if err != nil {
		return 0, err
	}
	for _, cfgField := range config.Fields {
		if cfgField.Field == fieldNameOnly {
			fieldID, err := e.getFieldID(fieldNameOnly, true)
			if err != nil {
				return 0, err
			}
			return makeFieldFullID(tableID, fieldID), nil
		}
	}
	return 0, fmt.Errorf("%v '%v'", msgFieldNotConfigured, fieldFull)
}

func (s *specialInstructions) generatePairInstructionSets(e *Engine) ([]pairInstruction, error) {
	fields := map[fieldFullName]bool{}
	for _, pair := range s.pairs {
		fields[pair.field] = true
	}

	instructions := []pairInstruction{}
	for field, _ := range fields {
		ins := pairInstruction{}
		fieldID, err := s.lookupFieldID(e, field)
		if err != nil {
			return nil, err
		}
		ins.fieldID = fieldID
		for _, pair := range s.pairs {
			if pair.field == field {
				ins.isFuzzy = pair.isFuzzy
				ins.tokens = append(ins.tokens, pair.value)
			}
		}
		instructions = append(instructions, ins)
	}
	return instructions, nil
}

func NewDefaultParser() *DefaultParser {
	return &DefaultParser{
		MinimumTokenLength:            1,
		ExcludeEntireStringFromResult: false,
	}
}

func isSplitter(c rune) bool {
	for _, r := range defaultSplitters {
		if r == c {
			return true
		}
	}
	return false
}

func stringToRunes(str string) []rune {
	runes := make([]rune, 0, utf8.RuneCountInString(str))
	for _, r := range str {
		runes = append(runes, r)
	}
	return runes
}

func runesToString(runes []rune) string {
	totalLen := 0
	for _, r := range runes {
		totalLen += utf8.RuneLen(r)
	}
	s := make([]byte, totalLen)
	out := 0
	for _, r := range runes {
		rlen := utf8.RuneLen(r)
		utf8.EncodeRune(s[out:out+rlen], r)
		out += rlen
	}
	//fmt.Printf("r2s [%v]\n", string(s))
	return string(s)
}

func canonicalizeString(input string) string {
	return strings.TrimSpace(strings.ToLower(input))
}

func (p *DefaultParser) Tokenize(src string) []string {
	// the 8 and 128 defaults here are an attempt at reducing GC pressure. no measurements performed.
	tokens_default := [8]string{}
	tokens := tokens_default[:0]
	token_buf := [128]rune{}
	token := token_buf[:0]
	state := state_space

	if p.MinimumTokenLength < 1 {
		panic("MinimumTokenLength must be at least 1")
	}

	if !p.ExcludeEntireStringFromResult {
		// The fact that the entire query string ends up being the first token is intimately used by resultRow.isValidResult().
		everything := canonicalizeString(src)
		if len(everything) != 0 {
			tokens = append(tokens, everything)
		}
	}

	emit := func() {
		allow := true
		if len(token) < p.MinimumTokenLength {
			allow = false
		}
		var tokenStr string
		if allow {
			// Remove duplicates. This is for efficiency sake - the indexer also does its own duplicate removal
			tokenStr = canonicalizeString(runesToString(token))
			if _, exist := ignoredWords[tokenStr]; exist {
				allow = false
			}
			if allow {
				for _, t := range tokens {
					if t == tokenStr {
						allow = false
						break
					}
				}
			}
		}

		if allow {
			tokens = append(tokens, tokenStr)
		}
		token = token_buf[:0]
	}

	ignoreCount := 0 // number of consecutive ignore characters (zeroes)

	for _, ch := range src {
		// Incorporate alphanumeric test so that we can frequently avoid the lookup inside the splitter list
		is_alnum := (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')
		is_break := !is_alnum && isSplitter(ch)
		is_ignored_prefix := ch == '0'
		switch state {
		case state_space:
			if !is_break {
				if is_ignored_prefix {
					state = state_word_ignored_prefix
				} else {
					state = state_word_active
					token = append(token, ch)
					ignoreCount = 0
				}
			}
		case state_word_ignored_prefix:
			if !is_break && !is_ignored_prefix {
				state = state_word_active
				token = append(token, ch)
				ignoreCount = 0
			}
		case state_word_active:
			if !is_break {
				token = append(token, ch)
				if is_ignored_prefix {
					ignoreCount++
					if ignoreCount >= 4 {
						// If we have four trailing ignored characters, then terminate our token, and switch to 'ignored prefix' state
						token = token[0 : len(token)-4]
						emit()
						state = state_word_ignored_prefix
					}
				} else {
					ignoreCount = 0
				}
			} else {
				emit()
				state = state_space
			}
		}
	}
	if state == state_word_active {
		emit()
	}

	return tokens
}

func extractInstruction(rawQuery string, instruction string) (err error, remain string, list []string) {
	start := strings.Index(rawQuery, "<"+instruction+":")
	if start == -1 {
		return nil, rawQuery, nil
	}

	content_start := start + len(instruction) + 2

	item := bytes.Buffer{}
	isEscaped := false
	var i int
	var c rune
	for i, c = range rawQuery[content_start:] {
		if isEscaped {
			item.WriteRune(c)
			isEscaped = false
			continue
		}

		if c == '|' {
			isEscaped = true
		} else if c == ',' {
			list = append(list, item.String())
			item.Reset()
		} else if c == '>' {
			list = append(list, item.String())
			item.Reset()
			break
		} else {
			item.WriteRune(c)
		}
	}

	if item.Len() != 0 {
		return errUnterminatedInstruction, "", nil
	}

	remain = rawQuery[0:start] + rawQuery[content_start+i+1:]
	return
}

/* Pre-Parse a query, extracting special instructions.
See the main documentation inside doc.go for specifications on these special instructions.
*/
func queryPreparse(rawQuery string) (err error, tokens string, instructions specialInstructions) {
	list := []string{}

	// fields
	if err, rawQuery, list = extractInstruction(rawQuery, "fields"); err != nil {
		return
	}
	for _, s := range list {
		instructions.fields = append(instructions.fields, fieldFullName(s))
	}

	// pairs
	if err, rawQuery, list = extractInstruction(rawQuery, "pairs"); err != nil {
		return
	}
	for _, s := range list {
		isFuzzy := false
		parts := strings.Split(s, "=")
		if len(parts) != 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
			err = errInvalidPairsInstruction
			return
		}

		if string(parts[1][0]) == "~" {
			isFuzzy = true
			parts[1] = parts[1][1:]
			if len(parts[1]) == 0 {
				err = errInvalidPairsInstruction
				return
			}
		}
		pair := &fieldValuePair{
			isFuzzy: isFuzzy,
			field:   fieldFullName(parts[0]),
			value:   canonicalizeString(parts[1]),
		}
		instructions.pairs = append(instructions.pairs, pair)
	}

	tokens = rawQuery
	return
}

func init() {
	for _, r := range " \t\r\n`~!@#$%^&*()-_=+[{]}\\|;:'\",<.>/?" {
		defaultSplitters = append(defaultSplitters, r)
	}
	ignoredWords = make(map[string]bool)
	for _, w := range strings.Split("the street road avenue crescent", " ") {
		ignoredWords[w] = true
	}
}
