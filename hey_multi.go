// Combining custom logic and multiple SQL statements.

package hey

import (
	"context"
	"database/sql"
	"reflect"
)

// Run Call all stored business logic.
type Run interface {
	// Run Call all stored business logic.
	Run(ctx context.Context) error
}

// Multi This stacks multiple SQL statements sequentially and executes them one by one at the end. You can add custom logic anywhere.
// For each SQL statement to be executed, you only need to focus on the following three points:
// 1. The SQL statement to be executed and its corresponding parameter list.
// 2. Receive or process SQL execution results.
// 3. Should custom logic be executed after the SQL statement executes successfully?
type Multi interface {
	Run

	ToEmpty

	V

	W

	// Len Number of SQL statements to be executed.
	Len() int

	// IsEmpty Are there no pending SQL statements?
	IsEmpty() bool

	// Add custom logic.
	Add(values ...func(ctx context.Context) error) Multi

	// AddQuery Add a query statement;
	AddQuery(maker Maker, query func(rows *sql.Rows) error) Multi

	// AddQueryRow Execute SQL statement using QueryRow; `dest` is the container for processing or storing the returned results.
	AddQueryRow(maker Maker, dest ...any) Multi

	// AddQueryScan Add a query statement; `result` is the container for processing or storing the returned results.
	AddQueryScan(maker Maker, result any) Multi

	// AddExists Add a query exists statement.
	AddExists(maker Maker, exists *bool) Multi

	// RowsAffected Get the number of affected rows.
	RowsAffected(rows *int64) func(value sql.Result) error

	// LastInsertId Get the id of the last inserted data.
	LastInsertId(id *int64) func(value sql.Result) error

	// AddExec Add a non-query statement; `result` is the container for processing or storing the returned results.
	// If the value of `result` is empty, the number of affected rows will be discarded.
	AddExec(maker Maker, result ...any) Multi
}

// NewMulti Create a Multi object.
func NewMulti(way *Way) Multi {
	return &multi{
		way: way,
	}
}

func (s *Way) Multi() Multi {
	return s.cfg.NewMulti(s)
}

// multi Implement the Multi interface.
type multi struct {
	way *Way

	values [][]func(ctx context.Context) error
}

func (s *multi) ToEmpty() {
	s.values = make([][]func(ctx context.Context) error, 0, 1<<1)
}

func (s *multi) Len() int {
	return len(s.values)
}

func (s *multi) IsEmpty() bool {
	return s.Len() == 0
}

func (s *multi) V() *Way {
	return s.way
}

func (s *multi) W(way *Way) {
	if way != nil {
		s.way = way
	}
}

func (s *multi) Add(values ...func(ctx context.Context) error) Multi {
	length := len(values)
	if length == 0 {
		return s
	}
	value := make([]func(ctx context.Context) error, 0, length)
	for i := 0; i < length; i++ {
		if values[i] == nil {
			continue
		}
		value = append(value, values[i])
	}
	if len(value) > 0 {
		s.values = append(s.values, value)
	}
	return s
}

// getScript If the statement to be executed is empty, it will be discarded;
// Using the Add method is not subject to this limitation.
func (s *multi) getScript(maker Maker) *SQL {
	if maker == nil {
		return nil
	}
	script := maker.ToSQL()
	if script == nil || script.IsEmpty() {
		return nil
	}
	return script
}

func (s *multi) AddQuery(maker Maker, query func(rows *sql.Rows) error) Multi {
	script := s.getScript(maker)
	if script == nil {
		return s
	}
	return s.Add(func(ctx context.Context) error {
		return s.way.Query(ctx, script, query)
	})
}

func (s *multi) AddQueryRow(maker Maker, dest ...any) Multi {
	script := s.getScript(maker)
	if script == nil {
		return s
	}
	return s.Add(func(ctx context.Context) error {
		query := s.way.RowScan(dest...)
		length := len(dest)
		if length == 1 {
			fetch, ok := dest[0].(func(row *sql.Row) error)
			if ok && fetch != nil {
				query = fetch
			}
		}
		return s.way.QueryRow(ctx, script, query)
	})
}

func (s *multi) AddQueryScan(maker Maker, result any) Multi {
	script := s.getScript(maker)
	if script == nil {
		return s
	}
	return s.Add(func(ctx context.Context) error {
		fx, ok := result.(func(rows *sql.Rows) error)
		if ok && fx != nil {
			return s.way.Query(ctx, script, fx)
		}
		return s.way.Scan(ctx, script, result)
	})
}

func (s *multi) AddExists(maker Maker, exists *bool) Multi {
	script := s.getScript(maker)
	if script == nil {
		return s
	}
	return s.Add(func(ctx context.Context) error {
		tmp, err := s.way.Exists(ctx, script)
		if err != nil {
			return err
		}
		*exists = tmp
		return nil
	})
}

func (s *multi) storeRowsAffected(result any, rows int64) {
	if result == nil {
		return
	}

	// Assignment based on type assertion takes precedence.
	i64, ok := result.(*int64)
	if ok {
		if i64 != nil {
			*i64 = rows
		}
		return
	}

	// By default, reflection is used for assignment.
	value := reflect.ValueOf(result)
	kind := value.Kind()
	if kind != reflect.Pointer {
		return
	}
	for kind == reflect.Pointer {
		if value.IsNil() {
			return
		}
		value = value.Elem()
		kind = value.Kind()
	}
	if value.Kind() == reflect.Int64 {
		value.SetInt(rows)
	}
}

func (s *multi) RowsAffected(rows *int64) func(value sql.Result) error {
	return func(value sql.Result) error {
		tmp, err := value.RowsAffected()
		if err != nil {
			return err
		}
		if rows != nil {
			*rows = tmp
		}
		return nil
	}
}

func (s *multi) LastInsertId(id *int64) func(value sql.Result) error {
	return func(value sql.Result) error {
		tmp, err := value.LastInsertId()
		if err != nil {
			return err
		}
		if id != nil {
			*id = tmp
		}
		return nil
	}
}

func (s *multi) AddExec(maker Maker, result ...any) Multi {
	script := s.getScript(maker)
	if script == nil {
		return s
	}
	return s.Add(func(ctx context.Context) error {
		custom := (any)(nil)
		length := len(result)
		for i := length - 1; i >= 0; i-- {
			if result[i] != nil {
				custom = result[i]
				break
			}
		}
		handle := func(tmp sql.Result) error {
			rows, err := tmp.RowsAffected()
			if err != nil {
				return err
			}
			s.storeRowsAffected(custom, rows)
			return nil
		}
		if custom != nil {
			fx, ok := custom.(func(tmp sql.Result) error)
			if ok && fx != nil {
				handle = fx
			}
		}
		tmp, err := s.way.Exec(ctx, script)
		if err != nil {
			return err
		}
		return handle(tmp)
	})
}

func (s *multi) Run(ctx context.Context) (err error) {
	for _, custom := range s.values {
		for _, value := range custom {
			if value == nil {
				continue
			}
			err = value(ctx)
			if err != nil {
				return
			}
		}
	}
	return
}
