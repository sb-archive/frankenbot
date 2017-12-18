package main

import (
	"database/sql"
	"fmt"
	"strings"
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

// FormatInsert creates a slice of generic interfaces from a slice of strings, formatting
// values as necessary
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
