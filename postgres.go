package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/lib/pq"
)

// ExtractPostgres is the main entry point for frankenbot.
// It takes in a Config and kicks in concurrent workers
// for the entire process, starting with MakeTable for stores
// and then again for each name in config.TargetTables
func ExtractPostgres(config Config, parentGroup *sync.WaitGroup) {
	defer parentGroup.Done()
	pSourceConnStr := fmt.Sprintf("dbname=%s user=%s password=%s host=%s port=%s connect_timeout=10",
		config.PSource["name"], config.PSource["username"], config.PSource["password"],
		config.PSource["host"], config.PSource["port"])
	pDestinationConnStr := fmt.Sprintf("dbname=%s user=%s password=%s host=%s port=%s",
		config.PDestination["name"], config.PDestination["username"], config.PDestination["password"],
		config.PDestination["host"], config.PDestination["port"])
	pSourceDB, err := sql.Open("postgres", pSourceConnStr)
	defer pSourceDB.Close()
	if err != nil {
		log.Fatal("postgres.go:26: ", err)
	}
	pDestinationDB, err := sql.Open("postgres", pDestinationConnStr)
	defer pDestinationDB.Close()
	if err != nil {
		log.Fatal("postgres.go:31: ", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go MakeTable("stores", config, pSourceDB, pDestinationDB, wg)
	for _, name := range config.TargetTables {
		wg.Add(1)
		go MakeTable(name, config, pSourceDB, pDestinationDB, wg)
	}
	wg.Wait()

	log.Println("*** Finished extracting Postgres ***")
}

// MakeTable creates SQL tables and uses the fan out pattern to kick off
// extraction for that table for each store
func MakeTable(name string, config Config, source, destination *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 1", name)
	rows, err := source.Query(query)
	if err != nil {
		log.Fatal("postgres.go:49: ", err)
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	cTypes, _ := rows.ColumnTypes()
	columnTypes := make([]string, len(cTypes))
	for i, cType := range cTypes {
		columnTypes[i] = cType.DatabaseTypeName()
	}
	table := &Table{Name: name, Columns: columns, DatabaseTypes: columnTypes, Extracted: false}

	query = fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s_id_seq", name)
	_, err = destination.Exec(query)
	if err != nil {
		log.Fatal("postgres.go:65: ", err)
	}

	query = table.FormatCreate()
	log.Printf("Making table %s", name)
	_, err = destination.Exec(query)
	if err != nil {
		log.Fatal("postgres.go:72: ", err)
	}

	query = fmt.Sprintf(`SELECT pg_get_indexdef(idx.oid) || ';' as script
											FROM pg_index ind
											JOIN pg_class idx ON idx.oid = ind.indexrelid
											JOIN pg_class tbl on tbl.oid = ind.indrelid
											LEFT JOIN pg_namespace ns on ns.oid = tbl.relnamespace
											WHERE tbl.relname = '%s'`, name)
	rows, err = source.Query(query)
	if err != nil {
		log.Println("postgres.go:83: ", err)
	}
	var index string
	indexBuilder := make([]string, 2)

	for rows.Next() {
		rows.Scan(&index)
		indexBuilder = strings.Split(index, "INDEX")
		index = indexBuilder[0] + "INDEX IF NOT EXISTS" + indexBuilder[1]

		_, err = destination.Exec(index)
		if err != nil {
			log.Println("postgres.go:95: ", err)
		}
	}

	for _, id := range config.MatchingIds {
		wg.Add(1)
		go ExtractTable(id, table, config.Filters, config.Since, source, destination, wg)
	}
}

// ExtractTable does the heavy lifting of actually
// extracting data from one databaase into another.
// It first determines the count and then uses the fan out
// pattern to concurrently extract batches.
func ExtractTable(id int, table *Table, filters map[string]map[string]string, since map[string]map[string]string, source, destination *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()
	var key string
	if table.Name == "stores" {
		key = "id"
	} else {
		key = "store_id"
	}

	var count int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = %v", table.Name, key, id)
	if sinceMap, ok := since[table.Name]; ok {
		for col, date := range sinceMap {
			countQuery += fmt.Sprintf(" AND %s >= DATE '%s'", col, date)
		}
	}
	err := source.QueryRow(countQuery).Scan(&count)
	if err != nil {
		log.Println("postgres.go:132: ", err)
	}

	log.Printf("Extracting %s for store %v, %v rows found", table.Name, id, count)

	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = %v", table.Name, key, id)
	if sinceMap, ok := since[table.Name]; ok {
		for col, date := range sinceMap {
			query += fmt.Sprintf(" AND %s >= DATE '%s'", col, date)
		}
	}

	rows, err := source.Query(query)
	if err != nil {
		log.Println("postgres.go:146: ", err)
	}
	defer rows.Close()

	txn, err := destination.Begin()
	if err != nil {
		log.Println("postgres.go:152: ", err)
	}
	stmt, err := txn.Prepare(pq.CopyIn(table.Name, table.Columns...))
	if err != nil {
		log.Println("postgres.go:156: ", err)
	}
	scanner := newSliceScan(table.Columns)

	for rows.Next() {
		err = scanner.Update(rows)
		if err != nil {
			log.Println("postgres.go:163: ", err)
		}

		t := table.FormatInsert(scanner.Get(), filters)
		_, err = stmt.Exec(t...)
		if err != nil {
			log.Println("postgres:169: ", err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Println("postgres:175: ", err)
	}

	stmt.Close()
	err = txn.Commit()
	if err != nil {
		log.Println("postgres:181: ", err)
	}
	log.Printf("*** Finished extracting %s for store %v ***", table.Name, id)
}
