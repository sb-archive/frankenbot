// Package frankenbot provides Springbot data extraction,
// transformation, and transferring from a source
// database to a destination database
package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	_ "github.com/lib/pq"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/yaml.v2"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Frankenbot needs a config file to lurch to life")
	}

	configFile := os.Args[1]
	configData, err := ioutil.ReadFile(configFile)

	if err != nil {
		log.Fatal("Line 26: ", err)
	}

	var config Config
	yaml.Unmarshal(configData, &config)

	// ExtractPostgres(config)
	ExtractMongo(config)
}

// Config for how the Frankenbot should
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
		id INT4 PRIMARY KEY DEFAULT nextval('%s_id_seq'::regClass)`,
		t.Name, t.Name)

	for i := 1; i < len(t.Columns); i++ {
		create += fmt.Sprintf(", %s %s", t.Columns[i], t.DatabaseTypes[i])
	}

	create += ")"

	return create
}

// FormatInsert create a string suitable to
// be used to insert into its SQL table
func (t *Table) FormatInsert(values []string, tableFilters map[string]map[string]string) string {
	insert := fmt.Sprintf(`INSERT INTO %s(`, t.Name)

	insert += t.Columns[0]
	for _, column := range t.Columns[1:] {
		insert += fmt.Sprintf(",%s", column)
	}

	insert += fmt.Sprintf(") VALUES (%s", values[0])

	buildValues := func(insert string, filters map[string]string) string {
		for i := 1; i <= len(values[1:]); i++ {
			var value string

			if replacement, ok := filters[t.Columns[i]]; ok {
				value = replacement
			} else {
				value = values[i]
			}

			if value == "" {
				if strings.HasPrefix(t.DatabaseTypes[i], "_") {
					insert += ",'{}'"
				} else {
					insert += ",null"
				}
			} else if t.DatabaseTypes[i] == "DATE" {
				insert += fmt.Sprintf(",DATE '%s'", value)
			} else if strings.HasPrefix(t.DatabaseTypes[i], "_") && value != "" {
				insert += fmt.Sprintf(",'%s'", value)
			} else if t.DatabaseTypes[i] == "VARCHAR" || t.DatabaseTypes[i] == "TEXT" {
				replacer := strings.NewReplacer("'", "''")
				insert += fmt.Sprintf(",'%s'", replacer.Replace(value))
			} else if t.DatabaseTypes[i] == "TIMESTAMP" {
				insert += fmt.Sprintf(",'%s'::timestamp", value)
			} else {
				insert += fmt.Sprintf(",%s", value)
			}
		}

		return insert
	}

	if filters, ok := tableFilters[t.Name]; ok {
		insert += buildValues(insert, filters)
	} else {
		insert += buildValues(insert, filters)
	}

	insert += ")"

	return insert
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
func ExtractPostgres(config Config) {
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

	comm := make(chan *Table)
	var progress int

	for _, id := range config.MatchingIds {
		log.Printf("*** Extracting tables for store %v ***", id)

		go MakeTable("stores", id, pSourceDB, pDestinationDB, comm)
		for _, name := range config.TargetTables {
			go MakeTable(name, id, pSourceDB, pDestinationDB, comm)
		}
	}

	for {
		select {
		case table := <-comm:
			if table.Extracted {
				progress++
				log.Printf("*** Postgres Progress: %v out of %v ***", progress, len(config.TargetTables)+1)

				if progress == len(config.TargetTables)+1 {
					return
				}
			} else {
				go ExtractTable(config.MatchingIds, table, config.Filters, pSourceDB, pDestinationDB, comm)
			}
		}
	}
}

// MakeTable creates SQL tables and passes the table
// back along the provided channel
func MakeTable(name string, id int, source, destination *sql.DB, comm chan *Table) {
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
	log.Printf("*** Making table %s ***", name)
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

	comm <- table
}

// ExtractTable does the heavy lifting of actually
// extracting data from on databaase into another
func ExtractTable(ids []int, table *Table, filters map[string]map[string]string, source, destination *sql.DB, comm chan *Table) {
	log.Printf("*** Extracting table %s ***", table.Name)

	var key string
	if table.Name == "stores" {
		key = "id"
	} else {
		key = "store_id"
	}

	for _, id := range ids {
		query := fmt.Sprintf("SELECT * FROM %s WHERE %s = %v", table.Name, key, id)
		rows, err := source.Query(query)
		if err != nil {
			log.Println("226: ", err)
		}

		scanner := newSliceScan(table.Columns)
		transaction, _ := destination.Begin()

		for rows.Next() {
			err = scanner.Update(rows)
			if err != nil {
				log.Println("234: ", err)
			}

			query = table.FormatInsert(scanner.Get(), filters)
			_, err = transaction.Exec(query)
			if err != nil {
				log.Println("240: ", err)
			}
		}

		transaction.Commit()
	}

	table.Extracted = true
	comm <- table
	log.Printf("*** Finished extracting table %s ***", table.Name)
}

// ExtractMongo performs similarly to ExtractPostgres, just
// for MongoDB
func ExtractMongo(config Config) {
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
			log.Printf("*** Extracting %s for store %v ***", collection, id)

			go ExtractCollection(collection, id, config.Filters, sourceDB, destinationDB, comm)
		}
	}

	for {
		select {
		case <-comm:
			progress++
			log.Printf("*** Mongo Progress: %v out of %v ***", progress, len(config.TargetCollections))

			if progress >= len(config.TargetCollections) {
				return
			}
		}
	}
}

// ExtractCollection extracts mongodb collections
func ExtractCollection(collection string, id int, collectionFilters map[string]map[string]string, source, destination *mgo.Database, comm chan bool) {
	sourceCollection := source.C(collection)
	destinationCollection := destination.C(collection)
	iter := sourceCollection.Find(bson.M{"store_id": id}).Iter()
	doc := &bson.D{}

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

	indexes, err := sourceCollection.Indexes()
	if err != nil {
		log.Println("377: ", err)
	}
	for _, index := range indexes {
		destinationCollection.EnsureIndex(index)
	}

	log.Printf("*** Finished extracting %s for store %v ***", collection, id)
	comm <- true
}
