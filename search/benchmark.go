package search

import (
	"database/sql"
	"fmt"
	"time"
)

/* Runs a set of queries, and determines the average query time

In order to populate the database with results that work for this benchmark, use the
following SQL:

-- copy table creation from db.go's migrations
delete from search_index;
vacuum search_index;
insert into search_index (token, srcfield, srcrow) values ('x', (random() * 100)::int, (random() * 1000000)::int);
insert into search_index (token, srcfield, srcrow) (select substring(md5(token || random()::text) from 1 for 10), (random() * 100)::int, (random() * 1000000)::int from search_index);
-- Keep repeating the last line until you have enough records

*/
func BenchmarkReads(indexDB *sql.DB) {
	tx, err := indexDB.Begin()
	if err != nil {
		panic(err)
	}
	defer tx.Commit()

	start := time.Now()
	var totalResults int64
	numQuery := 5000
	for i := 0; i < numQuery; i++ {
		low := fmt.Sprintf("01%04v", i)
		high := fmt.Sprintf("01%04v", i+5)
		rows, err := tx.Query("select token,srcfield,srcrow from search_index where token >= $1 and token < $2", low, high)
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var token string
			var srcfield, srcrow int
			if err := rows.Scan(&token, &srcfield, &srcrow); err != nil {
				panic(err)
			}
			//fmt.Printf("%v %v %v %v\n", i, token, srcfield, srcrow)
			totalResults++
		}
		rows.Close()
	}

	elapsed := time.Now().Sub(start)
	fmt.Printf("Average results per query: %v\n", float64(totalResults)/float64(numQuery))
	fmt.Printf("Average time per query: %.3v ms\n", float64(elapsed.Nanoseconds())/float64(numQuery)/1000000.0)
}
