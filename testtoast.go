package main

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

func testtoast(db *sql.DB) (err error) {
	_, _, dataParsed, err := loadJsonDataFromDisk()
	if err != nil {
		return err
	}

	err = truncate(db, "tbl_test_toast_seed")
	if err != nil {
		return
	}
	err = truncate(db, "tbl_test_toast")
	if err != nil {
		return
	}

	qa := "INSERT INTO tbl_test_toast_seed(data) VALUES ($1)"

	for i := range dataParsed {
		fmt.Println("i:", i)

		s := make([]any, 10)
		for j := 1; j < 10; j += 1 {
			s[j] = dataParsed[i+j]
		}

		dataParsed[i]["additional"] = s

		var tmp []byte
		tmp, err = json.Marshal(dataParsed[i])
		if err != nil {
			err = errors.Wrap(err, "fail to marshal data")
			return
		}

		_, err = db.Exec(qa, tmp)
		if err != nil {
			err = errors.Wrap(err, "fail to run insert a")
			return
		}

		if i >= len(dataParsed)-20 {
			break
		}
	}

	qb := `
		INSERT INTO tbl_test_toast(title, year, score, data)
		SELECT data->>'title'
		     , CAST(data->>'year' AS INT)
			 , CAST(data->>'score' AS FLOAT)
			 , data
		FROM tbl_test_toast_seed
	`
	_, err = db.Exec(qb)
	if err != nil {
		err = errors.Wrap(err, "fail to run insert b")
		return
	}

	return
}

func truncate(db *sql.DB, table string) (err error) {
	_, err = db.Exec(fmt.Sprintf("TRUNCATE %s", table))
	if err != nil {
		return errors.Wrapf(err, "fail to truncate table '%s'", table)
	}
	return
}
