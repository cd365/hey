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
	script  func(err error, prepare string, args []interface{})
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

func (s *Way) Transaction(transaction func() (err error)) (err error) {
	if transaction == nil {
		return
	}
	if s.Idle() {
		err = s.Begin()
		if err != nil {
			return
		}
		defer func() {
			if err != nil {
				_ = s.Rollback()
			} else {
				_ = s.Commit()
			}
		}()
	}
	err = transaction()
	return
}

func (s *Way) QueryContext(ctx context.Context, result func(rows *sql.Rows) error, prepare string, args ...interface{}) (err error) {
	if s.prepare != nil {
		prepare = s.prepare(prepare)
	}
	if result == nil || prepare == "" {
		return
	}
	if s.script != nil {
		defer func() {
			s.script(err, prepare, args)
		}()
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
	if s.prepare != nil {
		prepare = s.prepare(prepare)
	}
	if prepare == "" {
		return
	}
	if s.script != nil {
		defer func() {
			s.script(err, prepare, args)
		}()
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

func (s *Way) Script(fn func(err error, prepare string, args []interface{})) {
	if fn != nil {
		s.script = fn
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

func Field(field ...string) []string {
	return field
}

func Value(value ...interface{}) []interface{} {
	return value
}

type Insert struct {
	db    *Way
	table string
	field []string
	value []interface{}
}

func NewInsert(db *Way) *Insert {
	return &Insert{db: db}
}

func (s *Insert) Table(table string) *Insert {
	s.table = table
	return s
}

func (s *Insert) Field(field string, value interface{}) *Insert {
	s.field = append(s.field, field)
	s.value = append(s.value, value)
	return s
}

func (s *Insert) Insert() (int64, error) {
	prepare, args := SqlInsert(s.table, s.field, s.value)
	return s.db.Exec(prepare, args...)
}

func (s *Insert) InsertAll(field []string, value ...[]interface{}) (int64, error) {
	prepare, args := SqlInsert(s.table, field, value...)
	return s.db.Exec(prepare, args...)
}

type Delete struct {
	db    *Way
	table string
}

func NewDelete(db *Way) *Delete {
	return &Delete{db: db}
}

func (s *Delete) Table(table string) *Delete {
	s.table = table
	return s
}

func (s *Delete) Delete(where Filter) (int64, error) {
	prepare, args := SqlDelete(s.table, where)
	return s.db.Exec(prepare, args...)
}

type Update struct {
	db      *Way
	table   string
	express []string
	value   []interface{}
}

func NewUpdate(db *Way) *Update {
	return &Update{db: db}
}

func (s *Update) Table(table string) *Update {
	s.table = table
	return s
}

func (s *Update) Assign(field []string, value []interface{}) *Update {
	length1, length2 := len(field), len(value)
	if length1 != length2 || length1 == 0 {
		return s
	}
	for key, val := range field {
		field[key] = fmt.Sprintf("%s = %s", val, Placeholder)
	}
	s.express = append(s.express, field...)
	s.value = append(s.value, value...)
	return s
}

func (s *Update) Express(express string, value ...interface{}) *Update {
	s.express = append(s.express, express)
	s.value = append(s.value, value...)
	return s
}

func (s *Update) Field(field string, value interface{}) *Update {
	field = fmt.Sprintf("%s = %s", field, Placeholder)
	s.express = append(s.express, field)
	s.value = append(s.value, value)
	return s
}

func (s *Update) Update(where Filter) (int64, error) {
	length := len(s.express)
	if length == 0 {
		return 0, nil
	}
	buf := &bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("UPDATE %s SET", s.table))
	for i := 0; i < length; i++ {
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf(" %s", s.express[i]))
	}
	args := s.value
	if where != nil {
		whereStr, whereArgs := where.Result()
		if whereStr != "" {
			buf.WriteString(fmt.Sprintf(" WHERE %s", whereStr))
			args = append(args, whereArgs...)
		}
	}
	prepare := buf.String()
	return s.db.Exec(prepare, args...)
}
