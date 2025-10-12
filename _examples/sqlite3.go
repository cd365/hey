package examples

import (
	"database/sql"
	"os"
	"path/filepath"
	"time"

	"github.com/cd365/hey/v5"
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

	newWay := hey.NewWay(db)

	cfg := newWay.GetCfg()

	cfg.Manual = hey.Mysql()
	cfg.Manual.Replacer = hey.NewReplacer() // Optional, You can customize it by using `table_or_column_name` instead of table_or_column_name

	cfg.DeleteMustUseWhere = true
	cfg.UpdateMustUseWhere = true
	cfg.TransactionMaxDuration = time.Second * 5
	cfg.WarnDuration = time.Millisecond * 200
	// cfg.TransactionOptions = &sql.TxOptions{Isolation: sql.LevelReadCommitted}

	newWay.SetLogger(logger.NewLogger(os.Stdout)) // Optional, Record SQL call log

	return newWay, nil
}
