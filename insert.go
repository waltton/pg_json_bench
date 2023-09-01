package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"golang.org/x/exp/slices"

	"github.com/pkg/errors"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

var batch = 250

func insert(db *sql.DB, tbls ...string) (err error) {
	_, dataRaw, _, err := loadJsonDataFromDisk()
	if err != nil {
		return err
	}

	pIterations := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "iterations",
		Help: "Number of iterations per type",
	})

	pBatchSize := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "batch_size",
		Help: "Size of the batch",
	})

	pFinishedAt := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "finished_at",
		Help: "Time that the run finished at",
	})

	iterations := 15
	pIterations.Set(float64(iterations))

	pBatchSize.Set(float64(batch))

	p := push.New(pushGateway, "insert")
	p.Collector(pIterations)
	p.Collector(pBatchSize)

	var fieldTypes []string
	for fieldType := range queryTmpls {
		if slices.Contains(tbls, fieldType) {
			fieldTypes = append(fieldTypes, fieldType)
		}
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
			tblName, err := prepTable(db, fieldType)
			if err != nil {
				return errors.Wrapf(err, "fail on iteration #%d", i)
			}

			pq := pQuery[fieldType]
			err = dbinsert(db, i, tblName, dataRaw, &pq)
			if err != nil {
				return errors.Wrapf(err, "fail on iteration #%d", i)
			}

			pTableSize := prometheus.NewGauge(prometheus.GaugeOpts{
				Name:        "table_size",
				Help:        "Table size after inserting data",
				ConstLabels: prometheus.Labels{"table": tblName, "i": strconv.Itoa(i)},
			})
			ts, err := tableSize(db, tblName)
			if err != nil {
				return errors.Wrapf(err, "fail to get table size on iteration #%d", i)
			}
			pTableSize.Set(float64(ts))
			p.Collector(pTableSize)
		}
	}

	for _, v := range pQuery {
		p.Collector(v)
	}

	pFinishedAt.SetToCurrentTime()
	p.Collector(pFinishedAt)

	runTS := time.Now().Format("2006-01-02T15:04:05")
	p.Grouping("run_ts", runTS)

	if err := p.Push(); err != nil {
		return errors.Wrap(err, "fail to push metrics to gateway")
	}

	grafanaBaseURL := "http://localhost:3000"
	queryDashboard := "b3a7d255-4083-4a5a-80be-da20613ccd60/postgresql-benchmark-load"

	var tableParams string
	for _, t := range tbls {
		tableParams += "&var-table=tbl_" + t
	}

	murl := fmt.Sprintf("%s/d/%s?orgId=1&var-job=insert&var-run_ts=%s%s", grafanaBaseURL, queryDashboard, runTS, tableParams)
	fmt.Printf("metrics are available on: %s\n", murl)

	return nil
}

func singleInsertNoMetrics(db *sql.DB, tbls ...string) (err error) {
	_, dataRaw, _, err := loadJsonDataFromDisk()
	if err != nil {
		return err
	}

	for _, fieldType := range fieldTypes {
		if len(tbls) > 0 && !slices.Contains(tbls, fieldType) {
			continue
		}

		tblName, err := prepTable(db, fieldType)
		if err != nil {
			return err
		}

		err = dbinsert(db, 0, tblName, dataRaw, nil)
		if err != nil {
			return err
		}
	}

	return
}

func loadJsonDataFromDisk() (bdata []byte, dataRaw []json.RawMessage, dataParsed []map[string]any, err error) {
	bdata, err = data.ReadFile(recordsPath)
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "fail to read records %s", recordsPath)
	}

	err = json.Unmarshal(bdata, &dataRaw)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "fail to decode dataset into slice of raw json")
	}

	err = json.Unmarshal(bdata, &dataParsed)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "fail to decode dataset into slice of maps")
	}

	return bdata, dataRaw, dataParsed, nil
}

func dbinsert(db *sql.DB, i int, tblName string, dataRaw []json.RawMessage, pQuery *prometheus.Summary) (err error) {
	var (
		qc               int
		qd, qdmax, qdmin time.Duration
	)

	tmplInsert := "INSERT INTO %s(data) VALUES %s"
	q := fmt.Sprintf(tmplInsert, tblName, values(batch))

	for i := 0; i < len(dataRaw); i += batch {
		j := i + batch
		if j > len(dataRaw) {
			break
		}

		b := make([]interface{}, len(dataRaw[i:j]))
		for k := range dataRaw[i:j] {
			b[k] = dataRaw[i:j][k]
		}

		begin := time.Now()
		_, err = db.Exec(q, b...)
		if err != nil {
			err = errors.Wrapf(err, "fail to insert records from %d:%d", i, j)
			return
		}

		d := time.Since(begin)
		if pQuery != nil {
			(*pQuery).Observe(d.Seconds())
		}

		qd += d

		qc += 1
		if qdmax == 0 && qdmin == 0 {
			qdmax, qdmin = d, d
		}
		if d < qdmin {
			qdmin = d
		}
		if d > qdmax {
			qdmax = d
		}
	}

	fmt.Printf("insert: %s\n", tblName)
	fmt.Printf("count: %v\n", qc)
	fmt.Printf("duration: %v\n", qd)
	fmt.Printf("min: %v, avg: %v, max: %v\n", qdmin, qd/time.Duration(qc), qdmax)
	fmt.Println("")

	return
}

func values(batch int) string {
	rows := []string{}
	for i := 1; i <= batch; i++ {
		rows = append(rows, fmt.Sprintf(("($%d)"), i))
	}

	return strings.Join(rows, ",")
}

func prepTable(db *sql.DB, fieldType string) (tblName string, err error) {
	tblName = fmt.Sprintf("tbl_%s", fieldType)

	tmplDropTable := "TRUNCATE %s"
	_, err = db.Exec(fmt.Sprintf(tmplDropTable, tblName))
	if err != nil {
		return
	}

	return
}

func tableSize(db *sql.DB, tableName string) (size int, err error) {
	err = db.QueryRow(fmt.Sprintf("select pg_total_relation_size('%s')", tableName)).Scan(&size)
	return
}
