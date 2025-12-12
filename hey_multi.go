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

// MyMulti This stacks multiple SQL statements sequentially and executes them one by one at the end. You can add custom logic anywhere.
// For each SQL statement to be executed, you only need to focus on the following three points:
// 1. The SQL statement to be executed and its corresponding parameter list.
// 2. Receive or process SQL execution results.
// 3. Should custom logic be executed after the SQL statement executes successfully?
type MyMulti interface {
	Run

	ToEmpty

	V

	W

	// Len Number of SQL statements to be executed.
	Len() int

	// IsEmpty Are there no pending SQL statements?
	IsEmpty() bool

	// Add custom logic.
	Add(values ...func(ctx context.Context) error) MyMulti

	// AddQuery Add a query statement; `result` is the container for processing or storing the returned results.
	AddQuery(maker Maker, result any) MyMulti

	// AddQueryRow Add a non-query statement; `dest` is the container for processing or storing the returned results.
	AddQueryRow(maker Maker, dest ...any) MyMulti

	// RowsAffected Get the number of affected rows.
	RowsAffected(rows *int64) func(value sql.Result) error

	// LastInsertId Get the id of the last inserted data.
	LastInsertId(id *int64) func(value sql.Result) error

	// AddExec Add a non-query statement; `result` is the container for processing or storing the returned results.
	AddExec(maker Maker, result any) MyMulti

	// AddExists Add a query exists statement.
	AddExists(maker Maker, exists *bool) MyMulti
}

func (s *Way) MyMulti() MyMulti {
	return &myMulti{
		way: s,
	}
}

type myMulti struct {
	way *Way

	values [][]func(ctx context.Context) error
}

func (s *myMulti) ToEmpty() {
	s.values = make([][]func(ctx context.Context) error, 0, 1<<1)
}

func (s *myMulti) Len() int {
	return len(s.values)
}

func (s *myMulti) IsEmpty() bool {
	return s.Len() == 0
}

func (s *myMulti) V() *Way {
	return s.way
}

func (s *myMulti) W(way *Way) {
	if way != nil {
		s.way = way
	}
}

func (s *myMulti) Add(values ...func(ctx context.Context) error) MyMulti {
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

func (s *myMulti) AddQuery(maker Maker, result any) MyMulti {
	return s.Add(func(ctx context.Context) error {
		if fc, ok := result.(func(rows *sql.Rows) error); ok && fc != nil {
			return s.way.Query(ctx, maker, fc)
		}
		return s.way.Scan(ctx, maker, result)
	})
}

func (s *myMulti) AddQueryRow(maker Maker, dest ...any) MyMulti {
	return s.Add(func(ctx context.Context) error {
		query := s.way.RowScan(dest...)
		if length := len(dest); length == 1 {
			scan, ok := dest[0].(func(row *sql.Row) error)
			if ok && scan != nil {
				query = scan
			}
		}
		return s.way.QueryRow(ctx, maker, query)
	})
}

func (s *myMulti) storeRowsAffected(result any, rows int64) {
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

func (s *myMulti) RowsAffected(rows *int64) func(value sql.Result) error {
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

func (s *myMulti) LastInsertId(id *int64) func(value sql.Result) error {
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

func (s *myMulti) AddExec(maker Maker, result any) MyMulti {
	return s.Add(func(ctx context.Context) error {
		handle := func(tmp sql.Result) error {
			rows, err := tmp.RowsAffected()
			if err != nil {
				return err
			}
			s.storeRowsAffected(result, rows)
			return nil
		}
		if result != nil {
			fc, ok := result.(func(tmp sql.Result) error)
			if ok && fc != nil {
				handle = fc
			}
		}
		tmp, err := s.way.Exec(ctx, maker)
		if err != nil {
			return err
		}
		return handle(tmp)
	})
}

func (s *myMulti) AddExists(maker Maker, exists *bool) MyMulti {
	return s.Add(func(ctx context.Context) error {
		tmp, err := s.way.Exists(ctx, maker)
		if err != nil {
			return err
		}
		*exists = tmp
		return nil
	})
}

func (s *myMulti) Run(ctx context.Context) (err error) {
	for _, custom := range s.values {
		for _, value := range custom {
			if value == nil {
				continue
			}
			if err = value(ctx); err != nil {
				return
			}
		}
	}
	return
}
