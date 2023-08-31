package main

import (
	"database/sql"
	"embed"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
)

//go:embed data
var data embed.FS

// https://github.com/algolia/datasets/blob/master/movies/records.json
const recordsPath = "data/records.json"

const pushGateway = "http://localhost:9191/"

var r = rand.New(rand.NewSource(time.Now().Unix()))

func main() {
	if len(os.Args) < 2 {
		log.Print("missing command: insert sizelimit testtoast query")
		return
	}

	db, err := sql.Open("postgres", os.Getenv("DBCONN"))
	if err != nil {
		log.Print(errors.Wrap(err, "fail to connect to db"))
		return
	}

	switch os.Args[1] {
	case "insert":
		if len(os.Args) < 3 {
			log.Print("missing arg, with tables that will be used")
			return
		}

		err := insert(db, strings.Split(os.Args[2], ",")...)
		if err != nil {
			log.Print(err)
		}
	case "query":
		if len(os.Args) < 3 {
			log.Print("missing arg, command query requires the query name as a parameter")
			return
		}
		if len(os.Args) < 4 {
			log.Print("missing arg, with tables that will be used")
			return
		}

		err := query(db, os.Args[2], strings.Split(os.Args[3], ",")...)
		if err != nil {
			log.Print(err)
		}
	case "sizelimit":
		err := sizelimit(db)
		if err != nil {
			log.Print(err)
		}
	case "toast":
		err := testtoast(db)
		if err != nil {
			log.Print(err)
		}

	default:
		fmt.Printf("%s is an invalid command\n", os.Args[1])
		return
	}
}
