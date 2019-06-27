package server

import (
	"fmt"
	"strings"
	"testing"
)

func verifyParser(t *testing.T, parser Parser, src string, expected ...string) {
	tokens := parser.Tokenize(src)
	msg := fmt.Sprintf("Expected [%v](len %v), but got [%v](len %v)\n", strings.Join(expected, " "), len(expected), strings.Join(tokens, " "), len(tokens))
	if len(tokens) != len(expected) {
		t.Error(msg)
		return
	}
	for i := 0; i < len(tokens); i++ {
		if tokens[i] != expected[i] {
			t.Error(msg)
			return
		}
	}
}

func TestDefaultParser(t *testing.T) {
	p := NewDefaultParser()

	p.ExcludeEntireStringFromResult = true
	verifyParser(t, p, "i-74e", "i", "74e")
	verifyParser(t, p, "brown fox", "brown", "fox")

	p.ExcludeEntireStringFromResult = false
	verifyParser(t, p, "i-74e", "i-74e", "i", "74e")
	verifyParser(t, p, "brown fox", "brown fox", "brown", "fox")
	verifyParser(t, p, "00brown fox", "00brown fox", "brown", "fox") // 0 prefix is erased
	verifyParser(t, p, "00brown", "00brown", "brown")
	verifyParser(t, p, " 00brown", "00brown", "brown")
	verifyParser(t, p, "  00brown", "00brown", "brown")
	verifyParser(t, p, "10 \t\n\r 30", "10 \t\n\r 30", "10", "30")
	verifyParser(t, p, "100brown", "100brown")
	verifyParser(t, p, "ab_99", "ab_99", "ab", "99")
	verifyParser(t, p, "ab+099", "ab+099", "ab", "99")
	verifyParser(t, p, "ab099", "ab099")                                                                // single zero doesn't split
	verifyParser(t, p, "ab∞99", "ab∞99")                                                                // random unicode characters are not splitters
	verifyParser(t, p, "123.321", "123.321", "123", "321")                                              // splitting on dot
	verifyParser(t, p, "05050000000113200002400000", "05050000000113200002400000", "505", "1132", "24") // water meter ID. We split 4 or more consecutive zeroes
	verifyParser(t, p, "abc/abc", "abc/abc", "abc")                                                     // result set may not contain duplicates
	verifyParser(t, p, "AbC", "abc")
	p.MinimumTokenLength = 1
	verifyParser(t, p, "the street road avenue crescent", "the street road avenue crescent") // minimum token length is 1 characters
	verifyParser(t, p, "3 Omega Crescent", "3 omega crescent", "3", "omega")
	p.MinimumTokenLength = 2
	verifyParser(t, p, "a bb", "a bb", "bb") // minimum token length is 2 characters
	verifyParser(t, p, "aa b", "aa b", "aa")
	verifyParser(t, p, "") // empty string produces no tokens
}

func strListEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func fieldListEq(a []fieldFullName, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if string(a[i]) != b[i] {
			return false
		}
	}
	return true
}

func pairListEq(a []*fieldValuePair, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if string(a[i].field)+"="+a[i].value != b[i] {
			return false
		}
	}
	return true
}

func formatPairs(a []*fieldValuePair) string {
	s := ""
	for _, pair := range a {
		s += string(pair.field) + "=" + pair.value + ","
	}
	if s != "" {
		s = s[:len(s)-1]
	}
	return s
}

func verifyPreparse(t *testing.T, query string, expectErr error, expectTokens string, expectFields []string, expectPairs []string) {
	var err error
	var tokens string
	var ins specialInstructions

	err, tokens, ins = queryPreparse(query)
	if err != expectErr || tokens != expectTokens || !fieldListEq(ins.fields, expectFields) || !pairListEq(ins.pairs, expectPairs) {
		t.Errorf("Query parse failed (query = %v) (err = %v) (tokens = '%v') (fields = %v) (pairs = %v)\n", query, err, tokens, ins.fields, formatPairs(ins.pairs))
	}
}

func TestPreparse(t *testing.T) {
	// escaping, parsing
	verifyPreparse(t, "abc<fields:|>|,||>xyz", nil, "abcxyz", []string{">,|"}, nil)
	verifyPreparse(t, "abc<fields:123>xyz", nil, "abcxyz", []string{"123"}, nil)
	verifyPreparse(t, "<fields:abc>xyz", nil, "xyz", []string{"abc"}, nil)
	verifyPreparse(t, "xyz<fields:abc>", nil, "xyz", []string{"abc"}, nil)

	// fields
	verifyPreparse(t, "abc <fields:dbx.tab1.fielda,dbx.tab2.fieldb>", nil, "abc ", []string{"dbx.tab1.fielda", "dbx.tab2.fieldb"}, nil)

	// pairs
	verifyPreparse(t, "abc <pairs:dbx.tab1.fielda=5>", nil, "abc ", nil, []string{"dbx.tab1.fielda=5"})
	verifyPreparse(t, "abc <pairs:dbx.tab1.fielda=5,dbx.tab1.fielda=10>", nil, "abc ", nil, []string{"dbx.tab1.fielda=5", "dbx.tab1.fielda=10"})

	// only pairs, no tokens
	verifyPreparse(t, "<pairs:dbx.tab1.fielda=5>", nil, "", nil, []string{"dbx.tab1.fielda=5"})

	// invalid query
	verifyPreparse(t, "<fields:a", errUnterminatedInstruction, "", nil, nil)
	verifyPreparse(t, "<fields:a ", errUnterminatedInstruction, "", nil, nil)
}

func TestBits(t *testing.T) {
	ts := tokenStateArray{}
	if ts.get(0) != tokenStateNotFound {
		t.Error("token state not default-initialized")
	}
	values := []tokenState{
		tokenStateNotFound,
		tokenStateExactMatch,
		tokenStatePrefixMatch,
		tokenStateKeywordMatch,
	}
	for i := 0; i < maxTokensInQuery; i++ {
		ts.set(i, values[i%len(values)])
	}
	for i := 0; i < maxTokensInQuery; i++ {
		if ts.get(i) != values[i%len(values)] {
			t.Errorf("tokenStateArray[%v] incorrect", i)
		}
	}

	// just a random sample
	bits := []bool{
		true,
		false,
		true,
		true,
		false,
	}

	// setBit/getBit
	storage := [2]uint32{}
	for i := 0; i < 64; i++ {
		setBit(storage[:], i, bits[i%len(bits)])
	}
	for i := 0; i < 64; i++ {
		if getBit(storage[:], i) != bits[i%len(bits)] {
			t.Errorf("getBit(%v) failed", i)
		}
	}

}
