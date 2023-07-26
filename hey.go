package hey

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	DefaultTag = "db"
)

func FixPgsql(str string) string {
	index := 0
	for strings.Contains(str, Placeholder) {
		index++
		str = strings.Replace(str, Placeholder, fmt.Sprintf("$%d", index), 1)
	}
	return str
}

func Choose(way *Way, items ...*Way) *Way {
	for i := len(items) - 1; i >= 0; i-- {
		if items[i] != nil {
			return items[i]
		}
	}
	return way
}

type LogSql struct {
	TxId    string
	Prepare string
	Args    []interface{}
	StartAt time.Time
	EndAt   time.Time
	Error   error
}

type Way struct {
	db  *sql.DB             // the instance of the database connect pool
	tx  *sql.Tx             // the transaction instance
	tid string              // the transaction unique id
	fix func(string) string // fix prepare sql script before call prepare method
	tag string              // bind struct tag and table column
	log func(ls *LogSql)    // logger method
}

func NewWay(db *sql.DB) *Way {
	return &Way{
		db:  db,
		tag: DefaultTag,
	}
}

func (s *Way) Fix(fix func(string) string) *Way {
	s.fix = fix
	return s
}

func (s *Way) Tag(tag string) *Way {
	s.tag = tag
	return s
}

func (s *Way) Log(log func(ls *LogSql)) *Way {
	s.log = log
	return s
}

func (s *Way) DB() *sql.DB {
	return s.db
}

func (s *Way) Clone() *Way {
	return NewWay(s.db).Fix(s.fix).Tag(s.tag).Log(s.log)
}

func (s *Way) begin(ctx context.Context, opts *sql.TxOptions) (err error) {
	s.tx, err = s.db.BeginTx(ctx, opts)
	if err != nil {
		return
	}
	s.tid = fmt.Sprintf("tid.%d.%d.%p", time.Now().UnixNano(), os.Getpid(), s.tx)
	return
}

func (s *Way) commit() (err error) {
	err = s.tx.Commit()
	s.tx, s.tid = nil, ""
	return
}

func (s *Way) rollback() (err error) {
	err = s.tx.Rollback()
	s.tx, s.tid = nil, ""
	return
}

func (s *Way) TxNil() bool {
	return s.tx == nil
}

func (s *Way) Transaction(ctx context.Context, opts *sql.TxOptions, fn func(tx *Way) (err error)) (err error) {
	if s.tx != nil {
		return fn(s)
	}
	way := s.Clone()
	if err = way.begin(ctx, opts); err != nil {
		return
	}
	ok := false
	defer func() {
		if err == nil && ok {
			err = way.commit()
		} else {
			err = way.rollback()
		}
	}()
	if err = fn(way); err != nil {
		return
	}
	ok = true
	return
}

func (s *Way) Trans(fn func(tx *Way) (err error)) error {
	return s.Transaction(context.Background(), nil, fn)
}

func (s *Way) Query(ctx context.Context, query func(rows *sql.Rows) (err error), prepare string, args ...interface{}) (err error) {
	if query == nil || prepare == "" {
		return
	}
	if s.fix != nil {
		prepare = s.fix(prepare)
	}
	startAt := time.Now()
	endAt := time.Time{}
	if s.log != nil {
		defer func() {
			if endAt.IsZero() {
				endAt = time.Now()
			}
			ls := &LogSql{
				TxId:    s.tid,
				Prepare: prepare,
				Args:    args,
				StartAt: startAt,
				EndAt:   endAt,
				Error:   err,
			}
			s.log(ls)
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
	rows, err := stmt.QueryContext(ctx, args...)
	endAt = time.Now()
	if err != nil {
		return
	}
	defer rows.Close()
	err = query(rows)
	return
}

func (s *Way) Exec(ctx context.Context, prepare string, args ...interface{}) (rowsAffected int64, err error) {
	if prepare == "" {
		return
	}
	if s.fix != nil {
		prepare = s.fix(prepare)
	}
	startAt := time.Now()
	endAt := time.Time{}
	if s.log != nil {
		defer func() {
			if endAt.IsZero() {
				endAt = time.Now()
			}
			ls := &LogSql{
				TxId:    s.tid,
				Prepare: prepare,
				Args:    args,
				StartAt: startAt,
				EndAt:   endAt,
				Error:   err,
			}
			s.log(ls)
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
	sqlResult, err := stmt.ExecContext(ctx, args...)
	endAt = time.Now()
	if err != nil {
		return
	}
	rowsAffected, err = sqlResult.RowsAffected()
	return
}

func (s *Way) ExecAll(ctx context.Context, script string, args ...interface{}) (int64, error) {
	result, err := s.db.ExecContext(ctx, script, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *Way) ScanAll(ctx context.Context, result interface{}, prepare string, args ...interface{}) error {
	return s.Query(ctx, func(rows *sql.Rows) error {
		return ScanSliceStruct(rows, result, s.tag)
	}, prepare, args...)
}

func (s *Way) Add(table string) *Add {
	return NewAdd(s).Table(table)
}

func (s *Way) Del(table string) *Del {
	return NewDel(s).Table(table)
}

func (s *Way) Mod(table string) *Mod {
	return NewMod(s).Table(table)
}

func (s *Way) Get(table ...string) *Get {
	get := NewGet(s)
	for i := len(table) - 1; i >= 0; i-- {
		if table[i] != "" {
			get.Table(table[i])
			break
		}
	}
	return get
}
