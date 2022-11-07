package hey

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const (
	Placeholder = "?"
)

var (
	InvalidTransaction       = fmt.Errorf("sql: invalid transaction")
	TryBeginTransactionTwice = fmt.Errorf("sql: attempt to start transaction twice")
)

type Way struct {
	db      *sql.DB
	tx      *sql.Tx
	prepare func(prepare string) (result string)
}

func NewWay(db *sql.DB) *Way {
	return &Way{
		db: db,
	}
}

func (s *Way) Close() error {
	return s.db.Close()
}

func (s *Way) Stats() sql.DBStats {
	return s.db.Stats()
}

func (s *Way) PingContext(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *Way) Ping() error {
	return s.PingContext(context.Background())
}

func (s *Way) BeginTx(ctx context.Context, opts *sql.TxOptions) error {
	if s.tx != nil {
		return TryBeginTransactionTwice
	}
	tx, err := s.db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	s.tx = tx
	return nil
}

func (s *Way) Begin() error {
	return s.BeginTx(context.Background(), nil)
}

func (s *Way) Commit() (err error) {
	if s.tx == nil {
		err = InvalidTransaction
		return
	}
	err = s.tx.Commit()
	s.tx = nil
	return
}

func (s *Way) Rollback() (err error) {
	if s.tx == nil {
		err = InvalidTransaction
		return
	}
	err = s.tx.Rollback()
	s.tx = nil
	return
}

func (s *Way) Idle() bool {
	return s.tx == nil
}

func (s *Way) QueryContext(ctx context.Context, result func(rows *sql.Rows) error, prepare string, args ...interface{}) (err error) {
	if result == nil || prepare == "" {
		return
	}
	if s.prepare != nil {
		prepare = s.prepare(prepare)
	}
	var stmt *sql.Stmt
	if s.tx != nil {
		stmt, err = s.tx.PrepareContext(ctx, prepare)
	} else {
		stmt, err = s.db.PrepareContext(ctx, prepare)
	}
	if err != nil {
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	rows, err = stmt.QueryContext(ctx, args...)
	if err != nil {
		return
	}
	defer rows.Close()
	err = result(rows)
	return
}

func (s *Way) ExecContext(ctx context.Context, prepare string, args ...interface{}) (rowsAffected int64, err error) {
	if prepare == "" {
		return
	}
	if s.prepare != nil {
		prepare = s.prepare(prepare)
	}
	var stmt *sql.Stmt
	if s.tx != nil {
		stmt, err = s.tx.PrepareContext(ctx, prepare)
	} else {
		stmt, err = s.db.PrepareContext(ctx, prepare)
	}
	if err != nil {
		return
	}
	defer stmt.Close()
	var result sql.Result
	result, err = stmt.ExecContext(ctx, args...)
	if err != nil {
		return
	}
	rowsAffected, err = result.RowsAffected()
	return
}

func (s *Way) Query(result func(rows *sql.Rows) (err error), prepare string, args ...interface{}) error {
	return s.QueryContext(context.Background(), result, prepare, args...)
}

func (s *Way) Exec(prepare string, args ...interface{}) (int64, error) {
	return s.ExecContext(context.Background(), prepare, args...)
}

func (s *Way) Prepare(prepare func(prepare string) (result string)) {
	if prepare != nil {
		s.prepare = prepare
	}
}

func SqlInsert(table string, field []string, value ...[]interface{}) (prepare string, args []interface{}) {
	length := len(field)
	if length == 0 {
		return
	}
	amount := len(value)
	if amount == 0 {
		return
	}
	for _, v := range value {
		if len(v) != length {
			return
		}
	}
	args = make([]interface{}, amount*length)
	buf := &bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("INSERT INTO %s ( ", table))
	for i := 0; i < length; i++ {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(field[i])
	}
	buf.WriteString(" )")
	buf.WriteString(" VALUES ")
	i := 0
	for key, val := range value {
		if key != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("( ")
		for k, v := range val {
			if k != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(Placeholder)
			args[i] = v
			i++
		}
		buf.WriteString(" )")
	}
	prepare = buf.String()
	return
}

func SqlDelete(table string, where Filter) (prepare string, args []interface{}) {
	prepare = fmt.Sprintf("DELETE FROM %s", table)
	if where != nil {
		key, val := where.Result()
		if key != "" {
			prepare = fmt.Sprintf("%s WHERE %s", prepare, key)
			args = val
		}
	}
	return
}

func SqlUpdate(table string, field []string, value []interface{}, where Filter) (prepare string, args []interface{}) {
	length := len(field)
	if length == 0 || len(value) != length {
		return
	}
	buf := &bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("UPDATE %s SET", table))
	for i := 0; i < length; i++ {
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf(" %s = %s", field[i], Placeholder))
	}
	args = value
	if where != nil {
		key, val := where.Result()
		if key != "" {
			buf.WriteString(fmt.Sprintf(" WHERE %s", key))
			args = append(args, val...)
		}
	}
	prepare = buf.String()
	return
}

func PreparePostgresql(prepare string) string {
	index := 0
	for strings.Contains(prepare, Placeholder) {
		index++
		prepare = strings.Replace(prepare, Placeholder, fmt.Sprintf("$%d", index), 1)
	}
	return prepare
}
