package examples

import (
	"database/sql"
	"os"
	"path/filepath"
	"time"

	"github.com/cd365/hey/v6"
	_ "github.com/mattn/go-sqlite3"
)

// newSqlite3 Connect to sqlite
func newSqlite3() (*hey.Way, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	fileDb := filepath.Join(homeDir, ".example.sqlite3")

	db, err := sql.Open("sqlite3", fileDb)
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetConnMaxIdleTime(time.Minute * 3)
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)

	manual := hey.Sqlite()
	manual.Replacer = hey.NewReplacer() // Optional, You can customize it by using `table_or_column_name` instead of table_or_column_name
	newWay := hey.NewWay(
		hey.WithManual(manual),
		hey.WithDatabase(db),
		hey.WithDeleteRequireWhere(true),
		hey.WithUpdateRequireWhere(true),
		hey.WithTxMaxDuration(time.Second*5),
		// hey.WithTxOptions(&sql.TxOptions{Isolation: sql.LevelReadCommitted}),
	)
	return newWay, nil
}
