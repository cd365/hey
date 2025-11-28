// Combining custom logic and multiple SQL statements.

package hey

import (
	"context"
	"database/sql"
	"reflect"
)

// MyMulti This stacks multiple SQL statements sequentially and executes them one by one at the end. You can add custom logic anywhere.
// For each SQL statement to be executed, you only need to focus on the following three points:
// 1. The SQL statement to be executed and its corresponding parameter list.
// 2. Receive or process SQL execution results.
// 3. Should custom logic be executed after the SQL statement executes successfully?
type MyMulti interface {
	ToEmpty

	// Len Number of SQL statements to be executed.
	Len() int

	// IsEmpty Are there no pending SQL statements?
	IsEmpty() bool

	// V Use *Way given a non-nil value.
	V(values ...*Way) MyMulti

	// W Get the currently used *Way object.
	W() *Way

	// Custom Add custom logic.
	Custom(handle func(ctx context.Context) error) MyMulti

	// Query Add a query statement; `result` is the container for storing the query results.
	Query(maker Maker, result any, success ...func(ctx context.Context) error) MyMulti

	// RowScan Scan a row of SQL results containing one or more columns.
	RowScan(dest ...any) func(row *sql.Row) error

	// QueryRow Add a non-query statement; `result` is the container for processing or storing the returned results.
	QueryRow(maker Maker, result any, success ...func(ctx context.Context) error) MyMulti

	// RowsAffected Get the number of affected rows.
	RowsAffected(rows *int64) func(value sql.Result) error

	// LastInsertId Get the id of the last inserted data.
	LastInsertId(id *int64) func(value sql.Result) error

	// Exec Add a non-query statement; `result` is the container for processing or storing the returned results.
	Exec(maker Maker, result any, success ...func(ctx context.Context) error) MyMulti

	// Exists Add a query exists statement.
	Exists(maker Maker, exists *bool, success ...func(ctx context.Context) error) MyMulti

	// Run Execute multiple SQL statements.
	Run(ctx context.Context) error
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

func (s *myMulti) V(values ...*Way) MyMulti {
	s.way = s.way.V(values...)
	return s
}

func (s *myMulti) W() *Way {
	return s.way
}

func (s *myMulti) custom(handle func(ctx context.Context) error, success ...func(ctx context.Context) error) *myMulti {
	if handle == nil {
		return s
	}
	length := len(success)
	custom := make([]func(ctx context.Context) error, 1, length+1)
	custom[0] = handle
	custom = append(custom, success...)
	s.values = append(s.values, custom)
	return s
}

func (s *myMulti) Custom(handle func(ctx context.Context) error) MyMulti {
	return s.custom(handle)
}

func (s *myMulti) Query(maker Maker, result any, success ...func(ctx context.Context) error) MyMulti {
	return s.custom(func(ctx context.Context) error {
		return s.way.Scan(ctx, maker, result)
	}, success...)
}

func (s *myMulti) RowScan(dest ...any) func(row *sql.Row) error {
	return s.way.RowScan(dest...)
}

func (s *myMulti) QueryRow(maker Maker, result any, success ...func(ctx context.Context) error) MyMulti {
	return s.custom(func(ctx context.Context) error {
		fc, ok := result.(func(row *sql.Row) error)
		if ok && fc != nil {
			return s.way.QueryRow(ctx, maker, fc)
		}
		return s.way.QueryRow(ctx, maker, func(row *sql.Row) error {
			return row.Scan(result)
		})
	}, success...)
}

func storeRowsAffected(result any, rows int64) {
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
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Int64 {
		return
	}
	value.SetInt(rows)
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

func (s *myMulti) Exec(maker Maker, result any, success ...func(ctx context.Context) error) MyMulti {
	return s.custom(func(ctx context.Context) error {
		handle := func(value sql.Result) error {
			rows, err := value.RowsAffected()
			if err != nil {
				return err
			}
			storeRowsAffected(result, rows)
			return nil
		}
		if result != nil {
			fc, ok := result.(func(value sql.Result) error)
			if ok && fc != nil {
				handle = fc
			}
		}
		value, err := s.way.Exec(ctx, maker)
		if err != nil {
			return err
		}
		return handle(value)
	}, success...)
}

func (s *myMulti) Exists(maker Maker, exists *bool, success ...func(ctx context.Context) error) MyMulti {
	return s.custom(func(ctx context.Context) error {
		result, err := s.way.Exists(ctx, maker)
		if err != nil {
			return err
		}
		*exists = result
		return nil
	}, success...)
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
