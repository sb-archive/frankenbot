// Package frankenbot provides Springbot data extraction,
// transformation, and transferring from a source
// database to a destination database
package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/lib/pq"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// Config instructs how the frankenbot should
// extract and transform the source database into
// the destination database
type Config struct {
	PSource           map[string]string
	PDestination      map[string]string
	MSource           map[string]string
	MDestination      map[string]string
	MSourceHosts      []string `yaml:"mSourceHosts"`
	MDestinationHosts []string `yaml:"mDestinationHosts"`
	MatchingIds       []int    `yaml:"matchingIds"`
	TargetTables      []string `yaml:"targetTables"`
	TargetCollections []string `yaml:"targetCollections"`
	Filters           map[string]map[string]string
	Since             map[string]map[string]string
}

// Table is an SQL table
type Table struct {
	Name          string
	Columns       []string
	DatabaseTypes []string
	Extracted     bool
}

// FormatCreate creates a string suitable to
// be used to create an SQL table
func (t *Table) FormatCreate() string {
	create := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGINT PRIMARY KEY DEFAULT nextval('%s_id_seq'::regClass)`,
		t.Name, t.Name)

	for i := 1; i < len(t.Columns); i++ {
		create += fmt.Sprintf(", %s %s", t.Columns[i], t.DatabaseTypes[i])
	}

	create += ")"

	return create
}

// FormatInsert create a string suitable to
// be used to insert into its SQL table
func (t *Table) FormatInsert(values []string, tableFilters map[string]map[string]string) []interface{} {
	slice := make([]interface{}, len(values))

	for i, v := range values {
		var value string

		if replacement, ok := tableFilters[t.Name][t.Columns[i]]; ok {
			value = replacement
		} else {
			value = v
		}

		if value == "" {
			if strings.HasPrefix(t.DatabaseTypes[i], "_") {
				slice[i] = "{}"
			} else {
				slice[i] = nil
			}
		} else if t.DatabaseTypes[i] == "VARCHAR" || t.DatabaseTypes[i] == "TEXT" {
			slice[i] = fmt.Sprintf("'%s'", strings.Replace(value, "'", "''", -1))
		} else {
			slice[i] = value
		}
	}

	return slice
}

type sliceScan struct {
	cp       []interface{}
	row      []string
	colCount int
	colNames []string
}

func newSliceScan(colNames []string) *sliceScan {
	lenCN := len(colNames)
	s := &sliceScan{
		cp:       make([]interface{}, lenCN),
		row:      make([]string, lenCN),
		colCount: lenCN,
		colNames: colNames,
	}
	for i := 0; i < lenCN; i++ {
		s.cp[i] = new([]byte)
	}

	return s
}

func (s sliceScan) Update(rows *sql.Rows) error {
	if err := rows.Scan(s.cp...); err != nil {
		return err
	}

	for i := 0; i < s.colCount; i++ {
		if rb, ok := s.cp[i].(*[]byte); ok {
			s.row[i] = string(*rb)
			*rb = nil
		} else {
			return fmt.Errorf("Cannot convert index %d column %s to type *[]byte", i, s.colNames[i])
		}
	}

	return nil
}

func (s sliceScan) Get() []string {
	return s.row
}

// ExtractPostgres is the main entry point for frankenbot.
// It takes in a Config and kicks in concurrent workers
// for the entire process
func ExtractPostgres(config Config, done chan bool) {
	pSourceConnStr := fmt.Sprintf("dbname=%s user=%s password=%s host=%s port=%s",
		config.PSource["name"], config.PSource["username"], config.PSource["password"],
		config.PSource["host"], config.PSource["port"])
	pDestinationConnStr := fmt.Sprintf("dbname=%s user=%s password=%s host=%s port=%s",
		config.PDestination["name"], config.PDestination["username"], config.PDestination["password"],
		config.PDestination["host"], config.PDestination["port"])
	pSourceDB, err := sql.Open("postgres", pSourceConnStr)
	defer pSourceDB.Close()
	if err != nil {
		log.Fatal("146: ", err)
	}
	pDestinationDB, err := sql.Open("postgres", pDestinationConnStr)
	defer pDestinationDB.Close()
	if err != nil {
		log.Fatal("151: ", err)
	}

	comm := make(chan bool)
	var progress int

	go MakeTable("stores", config, pSourceDB, pDestinationDB, comm)
	for _, name := range config.TargetTables {
		go MakeTable(name, config, pSourceDB, pDestinationDB, comm)
	}

	for {
		select {
		case <-comm:
			progress++
			log.Printf("*** Postgres Progress: %v out of %v ***", progress, (len(config.TargetTables)+1)*len(config.MatchingIds))

			if progress == (len(config.TargetTables)+1)*len(config.MatchingIds)+1 {
				done <- true
				return
			}
		}
	}
}

// MakeTable creates SQL tables and passes the table
// back along the provided channel
func MakeTable(name string, config Config, source, destination *sql.DB, comm chan bool) {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 1", name)
	rows, err := source.Query(query)
	if err != nil {
		log.Println("186: ", err)
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
		log.Fatal("200: ", err)
	}

	query = table.FormatCreate()
	log.Printf("Making table %s", name)
	_, err = destination.Exec(query)
	if err != nil {
		log.Fatal("206: ", err)
	}

	query = fmt.Sprintf(`SELECT pg_get_indexdef(idx.oid) || ';' as script
											FROM pg_index ind
											JOIN pg_class idx ON idx.oid = ind.indexrelid
											JOIN pg_class tbl on tbl.oid = ind.indrelid
											LEFT JOIN pg_namespace ns on ns.oid = tbl.relnamespace
											WHERE tbl.relname = '%s'`, name)
	rows, err = source.Query(query)
	if err != nil {
		log.Println("247: ", err)
	}
	var index string
	indexBuilder := make([]string, 2)

	for rows.Next() {
		rows.Scan(&index)
		indexBuilder = strings.Split(index, "INDEX")
		index = indexBuilder[0] + "INDEX IF NOT EXISTS" + indexBuilder[1]

		_, err = destination.Exec(index)
		if err != nil {
			log.Println("259: ", err)
		}
	}

	for _, id := range config.MatchingIds {
		go ExtractTable(id, table, config.Filters, config.Since, source, destination, comm)
	}
}

// ExtractTable does the heavy lifting of actually
// extracting data from on databaase into another
func ExtractTable(id int, table *Table, filters map[string]map[string]string, since map[string]map[string]string, source, destination *sql.DB, comm chan bool) {
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
		log.Println("308: ", err)
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
		log.Println("226: ", err)
	}

	transaction, _ := destination.Begin()
	scanner := newSliceScan(table.Columns)
	stmt, err := transaction.Prepare(pq.CopyIn(table.Name, table.Columns...))
	if err != nil {
		log.Println("295: ", err)
	}

	for rows.Next() {
		err = scanner.Update(rows)
		if err != nil {
			log.Println("234: ", err)
		}

		t := table.FormatInsert(scanner.Get(), filters)
		_, err = stmt.Exec(t...)
		if err != nil {
			log.Println("240: ", err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Println("315: ", err)
	}

	stmt.Close()

	err = transaction.Commit()
	if err != nil {
		log.Println("322: ", err)
	}

	log.Printf("*** Finished extracting %s for store %v ***", table.Name, id)

	comm <- true
}

// ExtractMongo performs similarly to ExtractPostgres, just
// for MongoDB
func ExtractMongo(config Config, done chan bool) {
	sourceConnStr := fmt.Sprintf(`mongodb://%s:%s@`, config.MSource["username"], config.MSource["password"])
	sourceConnStr += config.MSourceHosts[0]
	for _, host := range config.MSourceHosts[1:] {
		sourceConnStr += fmt.Sprintf(",%s", host)
	}
	sourceConnStr += fmt.Sprintf("/%s", config.MSource["name"])

	destinationConnStr := fmt.Sprintf("mongodb://%s:%s@", config.MDestination["username"], config.MDestination["password"])
	destinationConnStr += config.MDestinationHosts[0]
	for _, host := range config.MDestinationHosts[1:] {
		destinationConnStr += fmt.Sprintf(",%s", host)
	}
	destinationConnStr += fmt.Sprintf("/%s", config.MDestination["name"])

	sourceSession, err := mgo.Dial(sourceConnStr)
	if err != nil {
		log.Fatal("303: ", err)
	}
	defer sourceSession.Close()

	destinationSession, err := mgo.Dial(destinationConnStr)
	if err != nil {
		log.Fatal("308: ", err)
	}
	defer destinationSession.Close()

	sourceDB := sourceSession.DB(config.MSource["name"])
	destinationDB := destinationSession.DB(config.MDestination["name"])
	comm := make(chan bool)
	var progress int

	for _, id := range config.MatchingIds {
		log.Printf("*** Extracting collections for store %v ***", id)

		for _, collection := range config.TargetCollections {
			log.Printf("Extracting %s for store %v", collection, id)

			go ExtractCollection(collection, id, config.Filters, config.Since, sourceDB, destinationDB, comm)
		}
	}

	for _, collection := range config.TargetCollections {
		log.Printf("Ensuring indexes for %s", collection)

		go EnsureIndex(collection, sourceDB, destinationDB)
	}

	for {
		select {
		case <-comm:
			progress++
			log.Printf("*** Mongo Progress: %v out of %v ***", progress, len(config.TargetCollections)*len(config.MatchingIds))

			if progress >= len(config.TargetCollections)*len(config.MatchingIds) {
				done <- true
				return
			}
		}
	}
}

// ExtractCollection extracts mongodb collections
func ExtractCollection(collection string, id int, collectionFilters map[string]map[string]string, since map[string]map[string]string, source, destination *mgo.Database, done chan bool) {
	sourceCollection := source.C(collection)
	destinationCollection := destination.C(collection)
	doc := &bson.D{}

	query := bson.M{"store_id": id}
	if sinceMap, ok := since[collection]; ok {
		for field, date := range sinceMap {
			query[field] = bson.M{"gte": date}
		}
	}

	iter := sourceCollection.Find(query).Iter()

	for iter.Next(doc) {
		if filters, ok := collectionFilters[collection]; ok {
			filtered := doc.Map()

			for filter, replacement := range filters {
				filtered[filter] = replacement
			}

			destinationCollection.Insert(filtered)
		} else {
			destinationCollection.Insert(doc)
		}
	}

	log.Printf("*** Finished extracting %s for store %v ***", collection, id)
	done <- true
}

// EnsureIndex creates indexes for mongo collections
func EnsureIndex(collection string, source, destination *mgo.Database) {
	sCollection := source.C(collection)
	dCollection := destination.C(collection)

	indexes, err := sCollection.Indexes()
	if err != nil {
		log.Println("377: ", err)
	}
	for _, index := range indexes {
		dCollection.EnsureIndex(index)
	}
}
