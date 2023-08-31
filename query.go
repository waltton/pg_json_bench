package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"golang.org/x/exp/slices"
)

var queryTmpls = map[string]map[string]string{
	"text": {
		"select_all":            "SELECT * FROM %s",
		"score_over_7":          "SELECT * FROM %s WHERE CAST(CAST(data AS JSON)->>'score' AS FLOAT) > 7.0",
		"count_score_over_7":    "SELECT '%s'", // skip
		"count_year_2000_at_gt": "SELECT '%s'", // skip
		"count_year_2000_eq":    "SELECT '%s'", // skip
	},
	"json": {
		"select_all":            "SELECT * FROM %s",
		"score_over_7":          "SELECT * FROM %s WHERE CAST(data->>'score' AS FLOAT) > 7.0",
		"count_score_over_7":    "SELECT COUNT(*) FROM %s WHERE CAST(data->>'score' AS FLOAT) > 7.0",
		"count_year_2000_at_gt": "SELECT '%s'", // skip
		"count_year_2000_eq":    "SELECT COUNT(*) FROM %s WHERE data->>'year' = '2000'",
	},
	"jsonb": {
		"select_all":            "SELECT * FROM %s",
		"score_over_7":          "SELECT * FROM %s WHERE CAST(data->>'score' AS FLOAT) > 7.0",
		"count_score_over_7":    "SELECT COUNT(*) FROM %s WHERE CAST(data->>'score' AS FLOAT) > 7.0",
		"count_year_2000_at_gt": "SELECT COUNT(*) FROM %s WHERE data @> '{\"year\": 2000}'",
		"count_year_2000_eq":    "SELECT COUNT(*) FROM %s WHERE data->>'year' = '2000'",
	},
	"btree_idx_score": {
		"select_all":            "SELECT * FROM %s",
		"score_over_7":          "SELECT * FROM %s WHERE CAST(data->>'score' AS FLOAT) > 7.0",
		"count_score_over_7":    "SELECT COUNT(*) FROM %s WHERE CAST(data->>'score' AS FLOAT) > 7.0",
		"count_year_2000_at_gt": "SELECT COUNT(*) FROM %s WHERE data @> '{\"year\": 2000}'",
		"count_year_2000_eq":    "SELECT COUNT(*) FROM %s WHERE data->>'year' = '2000'",
	},
	"gin_idx": {
		"select_all":            "SELECT * FROM %s",
		"score_over_7":          "SELECT * FROM %s WHERE CAST(data->>'score' AS FLOAT) > 7.0",
		"count_score_over_7":    "SELECT COUNT(*) FROM %s WHERE CAST(data->>'score' AS FLOAT) > 7.0",
		"count_year_2000_at_gt": "SELECT COUNT(*) FROM %s WHERE data @> '{\"year\": 2000}'",
		"count_year_2000_eq":    "SELECT COUNT(*) FROM %s WHERE data->>'year' = '2000'",
	},
	"gin_idx_path": {
		"select_all":            "SELECT * FROM %s",
		"score_over_7":          "SELECT * FROM %s WHERE CAST(data->>'score' AS FLOAT) > 7.0",
		"count_score_over_7":    "SELECT COUNT(*) FROM %s WHERE CAST(data->>'score' AS FLOAT) > 7.0",
		"count_year_2000_at_gt": "SELECT COUNT(*) FROM %s WHERE data @> '{\"year\": 2000}'",
		"count_year_2000_eq":    "SELECT COUNT(*) FROM %s WHERE data->>'year' = '2000'",
	},
}

var fieldTypes []string

func init() {
	for fieldType := range queryTmpls {
		fieldTypes = append(fieldTypes, fieldType)
	}
}

func query(db *sql.DB, qName string, tbls ...string) (err error) {
	pIterations := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "iterations",
		Help: "Number of iterations per type",
	})

	pFinishedAt := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "finished_at",
		Help: "Time that the run finished at",
	})

	iterations := 200
	pIterations.Set(float64(iterations))

	p := push.New(pushGateway, "query")
	p.Collector(pIterations)

	err = singleInsertNoMetrics(db, tbls...)
	if err != nil {
		return errors.Wrap(err, "fail to load data once")
	}

	var fieldTypes []string
	for fieldType := range queryTmpls {
		if slices.Contains(tbls, fieldType) {
			fieldTypes = append(fieldTypes, fieldType)
		}
	}

	for _, fieldType := range fieldTypes {
		tblName := fmt.Sprintf("tbl_%s", fieldType)

		pTableSize := prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "table_size",
			Help:        "Table size after loading data",
			ConstLabels: prometheus.Labels{"table": tblName},
		})
		ts, err := tableSize(db, tblName)
		if err != nil {
			return errors.Wrap(err, "fail to get table size")
		}
		pTableSize.Set(float64(ts))
		p.Collector(pTableSize)
	}

	pQuery := make(map[string]prometheus.Summary)

	for _, fieldType := range fieldTypes {
		pQuery[fieldType] = prometheus.NewSummary(prometheus.SummaryOpts{
			Name:        "query",
			Help:        "Count of executed queries",
			Objectives:  map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			ConstLabels: prometheus.Labels{"table": fmt.Sprintf("tbl_%s", fieldType)},
		})
	}

	for i := 0; i < iterations; i++ {
		r.Shuffle(len(fieldTypes), func(i, j int) {
			fieldTypes[i], fieldTypes[j] = fieldTypes[j], fieldTypes[i]
		})

		for _, fieldType := range fieldTypes {
			qTmpl, ok := queryTmpls[fieldType][qName]
			if !ok {
				return fmt.Errorf("query name '%s' passed as an argument is not valid", qName)
			}

			tblName := fmt.Sprintf("tbl_%s", fieldType)
			q := fmt.Sprintf(qTmpl, tblName)

			begin := time.Now()
			rows, err := db.Query(q)
			if err != nil {
				return errors.Wrapf(err, "fail to query all from %s; query: %s", tblName, q)
			}

			for rows.Next() {
				var tmp any
				err = rows.Scan(&tmp)
				if err != nil {
					return errors.Wrapf(err, "fail to scan record during query all from %s", tblName)
				}
			}

			d := time.Since(begin)
			pQuery[fieldType].Observe(d.Seconds())

			now := time.Now()

			fmt.Printf("query: %s\n", tblName)
			fmt.Printf("duration: %v\n", now.Sub(begin))
			fmt.Println("")

		}
	}

	for _, v := range pQuery {
		p.Collector(v)
	}

	pFinishedAt.SetToCurrentTime()
	p.Collector(pFinishedAt)

	runTS := time.Now().Format("2006-01-02T15:04:05")
	p.Grouping("q_name", qName)
	p.Grouping("run_ts", runTS)

	if err := p.Push(); err != nil {
		return errors.Wrap(err, "fail to push metrics to gateway")
	}

	grafanaBaseURL := "http://localhost:3000"
	queryDashboard := "ae9a6861-3fc6-4681-bdc5-8db2e4f93880/postgresql-benchmark-query"

	var tableParams string
	for _, t := range tbls {
		tableParams += "&var-table=tbl_" + t
	}

	murl := fmt.Sprintf("%s/d/%s?orgId=1&var-job=query&var-q_name=%s&var-run_ts=%s%s", grafanaBaseURL, queryDashboard, qName, runTS, tableParams)
	fmt.Printf("metrics are available on: %s\n", murl)

	return
}
