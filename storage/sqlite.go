package storage

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"fmt"
)

type SQLite struct {
	*sql.DB
}

func newSQLite(path string) *SQLite {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		panic(err)
	}
	tables := make(map[string]struct{}, 2)
	rows, err := db.Query("select tbl_name from sqlite_master")
	defer rows.Close()
	for rows.Next() {
		var table string
		rows.Scan(&table)
		tables[table] = struct{}{}
	}

	if _, exists := tables["documents"]; exists == false {
		db.Exec("create table documents (id blob, value blob)")
	}

	if _, exists := tables["mappings"]; exists == false {
		db.Exec("create table mappings (id string, value blob)")
	}

	return &SQLite{db}
}

func (db *SQLite) PutDocument(id, value []byte) {
	result, _ := db.Exec("update documents set value = ? where id = ?", id, value)
	if c, _ := result.RowsAffected(); c == 0 {
		fmt.Println(id, string(id))
		db.Exec("insert into documents (id, value) values (?, ?)", id, value)
	}
}

func (db *SQLite) PutMapping(id string, value []byte) {
	result, _ := db.Exec("update mappings set value = ? where id = ?", id, value)
	if c, _ := result.RowsAffected(); c == 0 {
		db.Exec("insert into mappings (id, value) values (?, ?)", id, value)
	}
}

func (db *SQLite) RemoveDocument(id []byte) {
	db.Exec("delete from documents where id = ?", id)
}

func (db *SQLite) RemoveMapping(id string) {
	db.Exec("delete from mappings where id = ?", id)
}

func (db *SQLite) IterateDocuments(handler func(id, value []byte)) {
	rows, _ := db.Query("select id, value from documents")
	defer rows.Close()
	for rows.Next() {
		var id []byte
		var value []byte
		rows.Scan(&id, &value)
		handler(id, value)
	}
}

func (db *SQLite) IterateMappings(handler func(id string, value []byte)) {
	rows, _	 := db.Query("select id, value from mappings")
	defer rows.Close()
	for rows.Next() {
		var id string
		var value []byte
		rows.Scan(&id, &value)
		handler(id, value)
	}
}

func (db *SQLite) Close() error {
	return db.DB.Close()
}
