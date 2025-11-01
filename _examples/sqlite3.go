package examples

import (
	"database/sql"
	"os"
	"path/filepath"
	"time"

	"github.com/cd365/hey/v6"
	"github.com/cd365/logger/v9"
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

	manual := hey.Mysql()
	manual.Replacer = hey.NewReplacer() // Optional, You can customize it by using `table_or_column_name` instead of table_or_column_name
	newWay := hey.NewWay(
		hey.WithDatabase(db),
		hey.WithManual(manual),
		hey.WithDeleteRequireWhere(true),
		hey.WithUpdateRequireWhere(true),
		hey.WithTxMaxDuration(time.Second*5),
		hey.WithSqlWarnDuration(time.Millisecond*200),
		// hey.WithTxOptions(&sql.TxOptions{Isolation: sql.LevelReadCommitted}),
		hey.WithLogger(logger.NewLogger(os.Stdout)), // Optional, Record SQL call log
	)
	return newWay, nil
}
