package logic

import (
	"database/sql"
)

var (
	// NewDatabase returns a new db instance. By default it opens a local postgres connection.
	// Users can customize this function to adapt to other SQL databases.
	NewDatabase = func() (*sql.DB, error) {
		dataSourceName := "host=localhost port=5432 user=postgres password=postgres dbname=postgres sslmode=disable"
		return sql.Open("postgres", dataSourceName)
	}
	CleanupDatabase = func(db *sql.DB) error {
		return db.Close()
	}
)
