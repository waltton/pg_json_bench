package main

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

func sizelimit(db *sql.DB) (err error) {
	err = singleInsertNoMetrics(db, "tbl_json")
	// err = singleInsertNoMetrics(db, "tbl_text")
	if err != nil {
		return errors.Wrap(err, "fail to load data once")
	}

	for i := 14; i <= 20; i += 1 {
		q := "TRUNCATE tbl_size_limit_json"
		// q := "TRUNCATE tbl_size_limit_text"
		_, err = db.Exec(q)
		if err != nil {
			return errors.Wrap(err, "fail to truncate table")
		}

		q1000 := "(SELECT data FROM tbl_json LIMIT %d)"
		// q1000 := "(SELECT data FROM tbl_text LIMIT %d)"
		q = "INSERT INTO tbl_size_limit_json SELECT json_agg(data) FROM (%s)_;"
		// q = "INSERT INTO tbl_size_limit_text SELECT jsonb_agg(data) FROM (%s)_;"

		qs := []string{}
		for j := 0; j < i; j++ {
			qs = append(qs, fmt.Sprintf(q1000, 100_000))
		}

		q = fmt.Sprintf(q, strings.Join(qs, " UNION ALL "))

		begin := time.Now()
		_, err = db.Exec(q)
		if err != nil {
			return errors.Wrap(err, "fail to run insert")
		}
		d := time.Since(begin)

		var bsize string
		q = "select pg_column_size(data) FROM (%s)_"
		q = fmt.Sprintf(q, strings.Join(qs, " UNION ALL "))
		err = db.QueryRow(q).Scan(&bsize)
		if err != nil {
			return errors.Wrap(err, "fail to get table size")
		}

		var size string
		q = "SELECT pg_size_pretty(pg_total_relation_size('tbl_size_limit_text'));"
		err = db.QueryRow(q).Scan(&size)
		if err != nil {
			return errors.Wrap(err, "fail to get table size")
		}

		fmt.Println("i:", i)
		fmt.Println("d:", d)
		fmt.Println("size:", size)
		fmt.Println("bsize:", bsize)
		fmt.Println("")
	}

	return nil
}
