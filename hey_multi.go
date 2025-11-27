// Combining custom logic and multiple SQL statements.

package hey

import (
	"context"
	"database/sql"
	"reflect"
)

// MyMulti This stacks multiple SQL statements sequentially and executes them one by one at the end. You can add custom logic anywhere.
// For each SQL statement to be executed, you only need to focus on the following two points:
// 1. The SQL statement to be executed and its corresponding parameter list.
// 2. Receive or process SQL execution results.
type MyMulti interface {
	// Len Number of SQL statements to be executed.
	Len() int

	// IsEmpty Are there no pending SQL statements?
	IsEmpty() bool

	// ToEmpty Clear all pending SQL statements.
	ToEmpty() MyMulti

	// Custom Add custom logic.
	Custom(handle func(ctx context.Context) error) MyMulti

	// Query Add a query statement; `result` is the container for storing the query results.
	Query(script *SQL, result any) MyMulti

	// RowScan Scan a row of SQL results containing one or more columns.
	RowScan(dest ...any) func(row *sql.Row) error

	// QueryRow Add a non-query statement; `result` is the container for processing or storing the returned results.
	QueryRow(script *SQL, result any) MyMulti

	// RowsAffected Get the number of affected rows.
	RowsAffected(rows *int64) func(value sql.Result) error

	// LastInsertId Get the id of the last inserted data.
	LastInsertId(id *int64) func(value sql.Result) error

	// Exec Add a non-query statement; `result` is the container for processing or storing the returned results.
	Exec(script *SQL, result any) MyMulti

	// Exists Add a query exists statement.
	Exists(script *SQL, exists *bool) MyMulti

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

	multiple []func(ctx context.Context) error
}

func (s *myMulti) Len() int {
	return len(s.multiple)
}

func (s *myMulti) IsEmpty() bool {
	return s.Len() == 0
}

func (s *myMulti) ToEmpty() MyMulti {
	s.multiple = make([]func(ctx context.Context) error, 0, 2)
	return s
}

func (s *myMulti) custom(handle func(ctx context.Context) error) *myMulti {
	if handle == nil {
		return s
	}
	s.multiple = append(s.multiple, handle)
	return s
}

func (s *myMulti) Custom(handle func(ctx context.Context) error) MyMulti {
	return s.custom(handle)
}

func (s *myMulti) Query(script *SQL, result any) MyMulti {
	return s.custom(func(ctx context.Context) error {
		return s.way.Scan(ctx, script, result)
	})
}

func (s *myMulti) RowScan(dest ...any) func(row *sql.Row) error {
	return s.way.RowScan(dest...)
}

func (s *myMulti) QueryRow(script *SQL, result any) MyMulti {
	return s.custom(func(ctx context.Context) error {
		fc, ok := result.(func(row *sql.Row) error)
		if ok && fc != nil {
			return s.way.QueryRow(ctx, script, fc)
		}
		return s.way.QueryRow(ctx, script, func(row *sql.Row) error {
			return row.Scan(result)
		})
	})
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

func (s *myMulti) Exec(script *SQL, result any) MyMulti {
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
		value, err := s.way.Exec(ctx, script)
		if err != nil {
			return err
		}
		return handle(value)
	})
}

func (s *myMulti) Exists(script *SQL, exists *bool) MyMulti {
	return s.custom(func(ctx context.Context) error {
		result, err := s.way.Exists(ctx, script)
		if err != nil {
			return err
		}
		*exists = result
		return nil
	})
}

func (s *myMulti) Run(ctx context.Context) (err error) {
	for _, value := range s.multiple {
		err = value(ctx)
		if err != nil {
			return
		}
	}
	return
}
