package pgsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/cd365/hey/v6/status"

	"examples/common"

	"github.com/cd365/hey/v6"
	"github.com/cd365/hey/v6/cst"
	_ "github.com/lib/pq"
)

const (
	initSql = `
DROP TABLE IF EXISTS department;
CREATE TABLE IF NOT EXISTS department (
  id bigserial NOT NULL,
  name character varying(32) DEFAULT NULL,
  serial_num integer NOT NULL DEFAULT 0,
  created_at bigint NOT NULL DEFAULT 0,
  updated_at bigint NOT NULL DEFAULT 0,
  deleted_at bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
);
CREATE UNIQUE INDEX IF NOT EXISTS department_name ON department USING btree (name);
CREATE INDEX IF NOT EXISTS department_serial_num ON department USING btree (serial_num);
CREATE INDEX IF NOT EXISTS department_deleted_at ON department USING btree (deleted_at);
COMMENT ON TABLE department IS 'department table';
COMMENT ON COLUMN department.id IS 'id';

DROP TABLE IF EXISTS employee;
CREATE TABLE IF NOT EXISTS employee (
  id bigserial NOT NULL,
  age int NOT NULL DEFAULT 0,
  name character varying(32) NOT NULL DEFAULT ''::character varying,
  email character varying(128) DEFAULT NULL,
  gender character varying(1) NOT NULL DEFAULT ''::character varying,
  height decimal(6,2) NOT NULL DEFAULT 0,
  weight decimal(6,2) NOT NULL DEFAULT 0,
  salary decimal(20,2) NOT NULL DEFAULT 0,
  department_id bigint NOT NULL DEFAULT 0,
  serial_num integer NOT NULL DEFAULT 0,
  created_at bigint NOT NULL DEFAULT 0,
  updated_at bigint NOT NULL DEFAULT 0,
  deleted_at bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
);
CREATE UNIQUE INDEX IF NOT EXISTS employee_email ON employee USING btree (email);
CREATE INDEX IF NOT EXISTS employee_serial_num ON employee USING btree (serial_num);
CREATE INDEX IF NOT EXISTS employee_deleted_at ON employee USING btree (deleted_at);
COMMENT ON TABLE employee IS 'employee table';
COMMENT ON COLUMN employee.id IS 'id';
`
)

var way *hey.Way

func initial() error {
	db, err := sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		return err
	}
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(20)
	db.SetConnMaxIdleTime(time.Minute * 3)
	db.SetConnMaxLifetime(time.Minute * 3)
	_, err = db.Exec(initSql)
	if err != nil {
		return err
	}
	options := make([]hey.Option, 0, 8)
	manual := hey.Postgresql()
	manual.Replacer = hey.NewReplacer()
	options = append(options, hey.WithManual(manual))
	options = append(options, hey.WithDatabase(db))
	options = append(options, hey.WithTrack(&common.MyTrack{}))
	way = hey.NewWay(options...)
	return nil
}

func Main() {
	log.Default().SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	if err := initial(); err != nil {
		panic(err)
	}

	defer func() {
		Delete()
	}()

	SelectEmpty()
	Insert()
	Update()
	Select()

	Transaction()

	Filter()

	MyMulti()

	MySchema()

	Complex()

	TableColumn()

	WindowFunc()

	WayMulti()

	Cache()
}

/* The following structured code can all be generated through code generation. */

const (
	DEPARTMENT = "public.department" // or "department"
	EMPLOYEE   = "public.employee"   // or "employee"
)

type Department struct {
	Id        int64   `json:"id" db:"id"`
	Name      *string `json:"name" db:"name"`
	SerialNum int     `json:"serial_num" db:"serial_num"`
	CreatedAt int64   `json:"created_at" db:"created_at"`
	UpdatedAt int64   `json:"updated_at" db:"updated_at"`
	DeletedAt int64   `json:"deleted_at" db:"deleted_at"`
}

type Employee struct {
	Id           int64   `json:"id" db:"id"`
	Age          int     `json:"age" db:"age"`
	Name         string  `json:"name" db:"name"`
	Email        *string `json:"email" db:"email"`
	Gender       string  `json:"gender" db:"gender"`
	Height       float64 `json:"height" db:"height"`
	Weight       float64 `json:"weight" db:"weight"`
	Salary       float64 `json:"salary" db:"salary"`
	DepartmentId int64   `json:"department_id" db:"department_id"`
	SerialNum    int     `json:"serial_num" db:"serial_num"`
	CreatedAt    int64   `json:"created_at" db:"created_at"`
	UpdatedAt    int64   `json:"updated_at" db:"updated_at"`
	DeletedAt    int64   `json:"deleted_at" db:"deleted_at"`
}

type SchemaDepartment struct {
	Id        string
	Name      string
	SerialNum string
	CreatedAt string
	UpdatedAt string
	DeletedAt string
}

func (s *SchemaDepartment) ColumnString() string {
	return `"id", "name", "serial_num", "created_at", "updated_at", "deleted_at"`
}

func (s *SchemaDepartment) init() *SchemaDepartment {
	s.Id = "id"
	s.Name = "name"
	s.SerialNum = "serial_num"
	s.CreatedAt = "created_at"
	s.UpdatedAt = "updated_at"
	s.DeletedAt = "deleted_at"
	return s
}

type SchemaEmployee struct {
	Id           string
	Age          string
	Name         string
	Email        string
	Gender       string
	Height       string
	Weight       string
	Salary       string
	DepartmentId string
	SerialNum    string
	CreatedAt    string
	UpdatedAt    string
	DeletedAt    string
}

func (s *SchemaEmployee) ColumnString() string {
	return `"id", "age", "name", "email", "gender", "height", "weight", "salary", "department_id", "serial_num", "created_at", "updated_at", "deleted_at"`
}

func (s *SchemaEmployee) init() *SchemaEmployee {
	s.Id = "id"
	s.Age = "age"
	s.Name = "name"
	s.Email = "email"
	s.Gender = "gender"
	s.Height = "height"
	s.Weight = "weight"
	s.Salary = "salary"
	s.DepartmentId = "department_id"
	s.SerialNum = "serial_num"
	s.CreatedAt = "created_at"
	s.UpdatedAt = "updated_at"
	s.DeletedAt = "deleted_at"
	return s
}

type DELETEDepartment struct {
	Id *int64 `json:"id" db:"id" validate:"required,min=1"` // id
}

type UPDATEDepartment struct {
	DELETEDepartment
	Name      *string `json:"name" db:"name" validate:"omitempty,min=0,max=32"`
	SerialNum *int    `json:"serial_num" db:"serial_num" validate:"omitempty"`
	UpdatedAt *int64  `json:"-" db:"updated_at" validate:"omitempty"`
	DeletedAt *int64  `json:"-" db:"deleted_at" validate:"omitempty"`
}

type DELETEEmployee struct {
	Id *int64 `json:"id" db:"id" validate:"required,min=1"` // id
}

type UPDATEEmployee struct {
	DELETEEmployee
	Age          *int     `json:"age" db:"age" validate:"omitempty"`
	Name         *string  `json:"name" db:"name" validate:"omitempty,min=0,max=32"`
	Email        *string  `json:"email" db:"email" validate:"omitempty,min=0,max=128"`
	Gender       *string  `json:"gender" db:"gender" validate:"omitempty,min=0,max=1"`
	Height       *float64 `json:"height" db:"height" validate:"omitempty"`
	Weight       *float64 `json:"weight" db:"weight" validate:"omitempty"`
	Salary       *float64 `json:"salary" db:"salary" validate:"omitempty"`
	DepartmentId *int64   `json:"department_id" db:"department_id" validate:"omitempty"`
	SerialNum    *int     `json:"serial_num" db:"serial_num" validate:"omitempty"`
	UpdatedAt    *int64   `json:"-" db:"updated_at" validate:"omitempty"`
	DeletedAt    *int64   `json:"-" db:"deleted_at" validate:"omitempty"`
}

/*
The following code demonstrates how to construct SQL statements using `way` and how to interact with the database using `way`.
*/

var (
	department = (&SchemaDepartment{}).init()
	employee   = (&SchemaEmployee{}).init()
)

func SelectEmpty() {
	tmp := way.Table(EMPLOYEE)
	tmp.Select(employee.ColumnString()).Desc(employee.Id).Limit(1)
	ctx := context.Background()
	exists := &Employee{}
	err := tmp.Scan(ctx, exists)
	if err != nil {
		if errors.Is(err, hey.ErrNoRows) {
			log.Printf("Data does not exist: %s", err.Error())
		} else {
			log.Fatal(err.Error())
		}
	}
	log.Printf("%#v", exists)
}

func Delete() {
	// Example 1: Simple condition deletion.
	{
		ctx := context.Background()
		script := way.Table(DEPARTMENT).WhereFunc(func(f hey.Filter) {
			f.Equal(department.Id, 1)
		})
		way.Debug(script.ToDelete())
		_, err := script.Delete(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Example 2: Deleting based on multiple values from the same column.
	{
		script := way.Table(DEPARTMENT).WhereFunc(func(f hey.Filter) {
			f.In(department.Id, 1, 2, 3)
		}).ToDelete()
		way.Debug(script)
	}

	// Example 3: Combination conditions of multiple columns.
	{
		script := way.Table(DEPARTMENT).WhereFunc(func(f hey.Filter) {
			f.GreaterThanEqual(department.Id, 1).Group(func(g hey.Filter) {
				g.IsNull(department.DeletedAt)
				g.OrGroup(func(g hey.Filter) {
					g.Equal(department.DeletedAt, 0)
				})
			})
		}).ToDelete()
		way.Debug(script)
	}

	// Example 4: Deletion of combined conditions with multiple columns and multiple logic.
	{
		script := way.Table(DEPARTMENT).WhereFunc(func(f hey.Filter) {
			f.InGroup([]string{department.Id, department.SerialNum}, [][]any{
				{1, 1},
				{2, 1},
				{3, 1},
			})
			f.OrGroup(func(g hey.Filter) {
				g.In(department.Id, way.Table(DEPARTMENT).Select(department.Id).WhereFunc(func(f hey.Filter) {
					f.GreaterThan(department.DeletedAt, 0)
				}))
			})
		}).ToDelete()
		way.Debug(script)
	}

	// Delete example data
	{
		ctx := context.Background()
		_, err := way.Table(DEPARTMENT).WhereFunc(func(f hey.Filter) {
			f.GreaterThan(department.Id, 0)
		}).Delete(ctx)
		if err != nil {
			log.Fatal(err)
		}
		_, err = way.Table(EMPLOYEE).WhereFunc(func(f hey.Filter) {
			f.GreaterThan(employee.Id, 0)
		}).Delete(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func Insert() {
	// Example 1: Simple insertion.
	{
		script := way.Table(DEPARTMENT).InsertFunc(func(i hey.SQLInsert) {
			i.ColumnValue(department.Name, "Sales Department")
			i.ColumnValue(department.SerialNum, 1)
		}).ToInsert()
		way.Debug(script)
	}

	// Example 2: Use default values and set SQL statement comments.
	{
		timestamp := way.Now().Unix()
		table := way.Table(DEPARTMENT).InsertFunc(func(i hey.SQLInsert) {
			i.ColumnValue(department.Name, "Sales Department")
			i.ColumnValue(department.SerialNum, 1)
			i.Default(department.CreatedAt, timestamp)
			i.Default(department.UpdatedAt, timestamp)
		})
		way.Debug(table.ToInsert())
		table.Comment("Example comment")
		way.Debug(table.ToInsert())

		// Not setting any columns will result in an incorrectly formatted SQL statement.
		table = way.Table(DEPARTMENT).InsertFunc(func(i hey.SQLInsert) {
			i.Default(department.CreatedAt, timestamp)
			i.Default(department.UpdatedAt, timestamp)
		})
		way.Debug(table.ToInsert())
	}

	// Example 3: Delete the specified column before inserting data.
	{
		timestamp := way.Now().Unix()
		script := way.Table(DEPARTMENT).InsertFunc(func(i hey.SQLInsert) {
			i.ColumnValue(department.Name, "Sales Department")
			i.ColumnValue(department.SerialNum, 1)
			i.ColumnValue(department.DeletedAt, timestamp)
			i.Default(department.CreatedAt, timestamp)
			i.Default(department.UpdatedAt, timestamp)
			// This deletes columns that have already been added.
			i.Remove(department.DeletedAt)
		}).ToInsert()
		way.Debug(script)
	}

	// Example 4: Use map insertion.
	{
		timestamp := way.Now().Unix()
		script := way.Table(DEPARTMENT).InsertFunc(func(i hey.SQLInsert) {
			i.Create(map[string]any{
				department.Name:      "Sales Department",
				department.SerialNum: 1,
				department.CreatedAt: timestamp,
				department.UpdatedAt: timestamp,
			})
		}).ToInsert()
		way.Debug(script)
	}

	// Example 5: Batch insertion using map slices.
	{
		timestamp := way.Now().Unix()
		script := way.Table(DEPARTMENT).InsertFunc(func(i hey.SQLInsert) {
			i.Create([]map[string]any{
				{
					department.Name:      "Sales Department1",
					department.SerialNum: 1,
					department.CreatedAt: timestamp,
					department.UpdatedAt: timestamp,
				},
				{
					department.Name:      "Sales Department2",
					department.SerialNum: 1,
					department.CreatedAt: timestamp,
					department.UpdatedAt: timestamp,
				},
			})
		}).ToInsert()
		way.Debug(script)
	}

	// Example 6: Inserting a structure.
	{
		timestamp := way.Now().Unix()
		script := way.Table(DEPARTMENT).InsertFunc(func(i hey.SQLInsert) {
			i.Forbid(department.Id, department.DeletedAt)
			name := "Sales Department"
			i.Create(&Department{
				Name:      &name,
				SerialNum: 1,
				CreatedAt: timestamp,
				UpdatedAt: timestamp,
			})
		}).ToInsert()
		way.Debug(script)
	}

	// Example 7: Batch insertion using structure slices.
	{
		timestamp := way.Now().Unix()
		script := way.Table(DEPARTMENT).InsertFunc(func(i hey.SQLInsert) {
			i.Forbid(department.Id, department.DeletedAt)
			name1 := "Sales Department1"
			name2 := "Sales Department2"
			name3 := "Sales Department3"
			i.Create([]*Department{
				{
					Name:      &name1,
					SerialNum: 1,
					CreatedAt: timestamp,
					UpdatedAt: timestamp,
				},
				{
					Name:      &name2,
					SerialNum: 1,
					CreatedAt: timestamp,
					UpdatedAt: timestamp,
				},
				{
					Name:      &name3,
					SerialNum: 1,
					CreatedAt: timestamp,
					UpdatedAt: timestamp,
				},
			})
		}).ToInsert()
		way.Debug(script)
	}

	// Example 8: Large amounts of data inserted in batches.
	{
		ctx := context.Background()
		timestamp := way.Now().Unix()
		length := 100
		create := make([]*Department, 0, length)
		for i := range length {
			name := fmt.Sprintf("Sales Department %d %d", i+1, time.Now().Nanosecond())
			create = append(create, &Department{
				Name:      &name,
				SerialNum: 1,
				CreatedAt: timestamp,
				UpdatedAt: timestamp,
			})
		}
		rows, err := way.Table(DEPARTMENT).LargerCreate(ctx, 30, create, func(i hey.SQLInsert) {
			i.Forbid(department.Id, department.DeletedAt)
		}, nil)
		if err != nil {
			log.Println(err.Error())
		} else {
			log.Println(rows)
		}
	}

	// Example 9: Insert a record and retrieve the id value of the inserted record(pgsql).
	{
		timestamp := way.Now().Unix()
		ctx := context.Background()
		id, err := way.Table(DEPARTMENT).InsertFunc(func(i hey.SQLInsert) {
			i.Forbid(department.Id, department.DeletedAt)
			name := fmt.Sprintf("Sales Department %d", time.Now().Nanosecond())
			i.Create(&Department{
				Name:      &name,
				SerialNum: 1,
				CreatedAt: timestamp,
				UpdatedAt: timestamp,
			})
			i.Returning(func(r hey.SQLReturning) {
				r.Returning(department.Id)
				r.Execute(func(ctx context.Context, stmt *hey.Stmt, args ...any) (id int64, err error) {
					err = stmt.QueryRow(ctx, func(row *sql.Row) error {
						return row.Scan(&id)
					}, args...)
					return
				})
			})
		}).Insert(ctx)
		if err != nil {
			log.Println(err.Error())
		} else {
			log.Println("id:", id)
		}
	}

	// Example 10: Insert a record and retrieve the id value of the inserted record(mysql).
	if false {
		timestamp := way.Now().Unix()
		ctx := context.Background()
		id, err := way.Table(DEPARTMENT).InsertFunc(func(i hey.SQLInsert) {
			i.Forbid(department.Id, department.DeletedAt)
			name := fmt.Sprintf("Sales Department %d", time.Now().Nanosecond())
			i.Create(&Department{
				Name:      &name,
				SerialNum: 1,
				CreatedAt: timestamp,
				UpdatedAt: timestamp,
			})
			i.Returning(func(r hey.SQLReturning) {
				r.Execute(func(ctx context.Context, stmt *hey.Stmt, args ...any) (id int64, err error) {
					result, err := stmt.Exec(ctx, args...)
					if err != nil {
						return 0, err
					}
					return result.LastInsertId()
				})
			})
		}).Insert(ctx)
		if err != nil {
			log.Println(err.Error())
		} else {
			log.Println("id:", id)
		}
	}

	// Example 11: Insert a single data record and retrieve one or more columns of the inserted data(pgsql).
	{
		timestamp := way.Now().Unix()
		ctx := context.Background()
		scanName := ""
		id, err := way.Table(DEPARTMENT).InsertFunc(func(i hey.SQLInsert) {
			i.Forbid(department.Id, department.DeletedAt)
			name := fmt.Sprintf("Sales Department %d", time.Now().Nanosecond())
			i.Create(&Department{
				Name:      &name,
				SerialNum: 1,
				CreatedAt: timestamp,
				UpdatedAt: timestamp,
			})
			i.Returning(func(r hey.SQLReturning) {
				r.Returning(department.Id, department.Name)
				r.Execute(func(ctx context.Context, stmt *hey.Stmt, args ...any) (id int64, err error) {
					err = stmt.QueryRow(ctx, func(row *sql.Row) error {
						return row.Scan(&id, &scanName)
					}, args...)
					return
				})
			})
		}).Insert(ctx)
		if err != nil {
			log.Println(err.Error())
		} else {
			log.Println("id:", id, "scan-name:", scanName)
		}
	}

	// Example 12: Use the query result set as the data source for insertion.
	{
		ctx := context.Background()
		rows, err := way.Table(DEPARTMENT).InsertFunc(func(i hey.SQLInsert) {
			i.Column(department.Name, department.SerialNum)
			i.Subquery(way.Table(DEPARTMENT).WhereFunc(func(f hey.Filter) {
				f.LessThan(department.Id, 0)
			}).Select(department.Name, department.SerialNum).Desc(department.Id).Limit(1000))
		}).Insert(ctx)
		if err != nil {
			log.Println(err.Error())
		} else {
			log.Println("rows:", rows)
		}
	}

	{
		ctx := context.Background()
		timestamp := time.Now().Unix()
		_, err := way.Table(EMPLOYEE).InsertFunc(func(i hey.SQLInsert) {
			i.ColumnValue(employee.Name, "Jack")
			i.ColumnValue(employee.Age, 18)
			i.ColumnValue(employee.CreatedAt, timestamp)
			i.ColumnValue(employee.UpdatedAt, timestamp)
		}).Insert(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
	}
}

func Update() {
	// Example 1: Simple update.
	{
		script := way.Table(DEPARTMENT).UpdateFunc(func(f hey.Filter, u hey.SQLUpdateSet) {
			f.Equal(department.Id, 1)
			u.Set(department.SerialNum, 999)
		}).Comment("Example").ToUpdate()
		way.Debug(script)
	}

	// Example 2: Update conditions using subquery.
	{
		ctx := context.Background()
		rows, err := way.Table(DEPARTMENT).UpdateFunc(func(f hey.Filter, u hey.SQLUpdateSet) {
			subquery := way.Table(DEPARTMENT).Limit(1).Select(department.Id).WhereFunc(func(f hey.Filter) {
				f.Equal(department.SerialNum, 11)
			}).Desc(department.Id)
			f.CompareEqual(department.Id, subquery)
			f.OrGroup(func(g hey.Filter) {
				g.In(department.Id, subquery)
			})
			u.Set(department.SerialNum, 999)
		}).Update(ctx)
		if err != nil {
			log.Println(err.Error())
		} else {
			log.Println("rows:", rows)
		}
	}

	// Example 3: Set the default update column.
	{
		script := way.Table(DEPARTMENT).UpdateFunc(func(f hey.Filter, u hey.SQLUpdateSet) {
			f.Equal(department.Id, 1)
			u.Set(department.SerialNum, 999)
			u.Default(department.UpdatedAt, way.Now().Unix())
		}).ToUpdate()
		way.Debug(script)
	}

	// Example 4: Update using map[string]any.
	{
		script := way.Table(DEPARTMENT).UpdateFunc(func(f hey.Filter, u hey.SQLUpdateSet) {
			f.Equal(department.Id, 1)
			u.Update(map[string]any{
				department.SerialNum: 999,
				department.Name:      "Sales Department",
			})
			u.Default(department.UpdatedAt, way.Now().Unix())
		}).ToUpdate()
		way.Debug(script)
	}

	// Example 5: Update using struct.
	{
		id := int64(1)
		name := "Sales Department"
		serialNum := 123
		update := &UPDATEDepartment{
			Name:      &name,
			SerialNum: &serialNum,
		}
		update.Id = &id
		script := way.Table(DEPARTMENT).UpdateFunc(func(f hey.Filter, u hey.SQLUpdateSet) {
			f.Equal(department.Id, 1)
			u.Forbid(department.Id)
			u.Update(update)
			u.Remove(department.UpdatedAt)
			u.Default(department.UpdatedAt, way.Now().Unix())
		}).ToUpdate()
		way.Debug(script)
	}

	// Example 6: Set column values to increment/decrement.
	{
		script := way.Table(DEPARTMENT).UpdateFunc(func(f hey.Filter, u hey.SQLUpdateSet) {
			f.Equal(department.Id, 1)
			u.Set(department.Name, "Sales Department")
			u.Incr(department.SerialNum, 1)
			u.Default(department.UpdatedAt, way.Now().Unix())
		}).ToUpdate()
		way.Debug(script)
	}

	// Example 7: Assign values directly to columns, or set raw values in the SQL script.
	{
		script := way.Table(DEPARTMENT).UpdateFunc(func(f hey.Filter, u hey.SQLUpdateSet) {
			f.Equal(department.Id, 1)
			u.Assign(department.DeletedAt, department.UpdatedAt)
			u.Assign(department.SerialNum, "123")
			// u.Assign(department.Name, cst.Empty)
			u.Assign(department.Name, cst.NULL)
			u.Remove(department.Name)
			u.Assign(department.Name, "'Sales Department'")
			u.Remove(department.CreatedAt)
		}).ToUpdate()
		way.Debug(script)
	}
}

func Select() {
	// SELECT VERSION()
	{
		ctx := context.Background()
		version := ""
		err := way.Table(nil).
			Select("VERSION()").
			Scan(ctx, &version)
		if err != nil {
			panic(err)
		}
		log.Println(version)

		// OR

		version = ""
		err = way.Table(nil).
			Select(hey.FuncSQL("VERSION")).
			Scan(ctx, &version)
		if err != nil {
			panic(err)
		}
		log.Println(version)
	}

	tmp := way.Table(EMPLOYEE)

	script := tmp.ToSelect()
	way.Debug(script)

	// ORDER BY xxx LIMIT n
	{
		tmp.ToEmpty()
		tmp.Asc(employee.Id).Limit(1)
		script = tmp.ToSelect()
		way.Debug(script)

		ctx := context.Background()
		value := &Employee{}
		if err := tmp.Scan(ctx, value); err != nil {
			log.Fatal(err.Error())
		}
		log.Printf("%#v", value)

		// values := make([]Employee, 0)
		values := make([]*Employee, 0) // make([]**Employee, 0) // Allow multilevel pointers
		if err := tmp.Scan(ctx, &values); err != nil {
			log.Fatal(err.Error())
		}
		for _, v := range values {
			log.Printf("%#v", v)
		}
	}

	// OFFSET
	{
		tmp.ToEmpty()
		tmp.Asc(employee.Id).Limit(1).Offset(10)
		script = tmp.ToSelect()
		way.Debug(script)
	}

	// PAGE
	{
		tmp.ToEmpty()
		tmp.Asc(employee.Id).Page(2, 10)
		script = tmp.ToSelect()
		way.Debug(script)
	}

	// comment
	{
		tmp.ToEmpty()
		tmp.Comment("test comment").Asc(employee.Id).Page(2, 10)
		script = tmp.ToSelect()
		way.Debug(script)
	}

	// SELECT columns
	{
		tmp.ToEmpty()
		tmp.Select(employee.Id, employee.Salary).Asc(employee.Id).Limit(1)
		script = tmp.ToSelect()
		way.Debug(script)
	}

	// GROUP BY
	{
		tmp.ToEmpty()
		tmp.Select(employee.Id).Group(employee.Id).GroupFunc(func(g hey.SQLGroupBy) {
			g.Having(func(having hey.Filter) {
				having.GreaterThanEqual(employee.Id, 0)
			})
		}).Asc(employee.Id).Limit(1)
		script = tmp.ToSelect()
		way.Debug(script)
	}

	// DISTINCT
	{
		tmp.ToEmpty()
		tmp.Distinct().Select(employee.Id, employee.SerialNum).Asc(employee.Id).Limit(1)
		script = tmp.ToSelect()
		way.Debug(script)
	}

	// WITH
	{
		a := "a"
		c := DEPARTMENT
		tmpWith := way.Table(a).Comment("test1").WithFunc(func(w hey.SQLWith) {
			w.Set(
				a,
				way.Table(c).Select(employee.Id, employee.SerialNum).WhereFunc(func(f hey.Filter) {
					f.Equal(employee.Id, 1)
				}).Desc(employee.Id).Limit(10).ToSelect(),
			)
		}).Asc(employee.Id).Limit(1)
		script = tmpWith.ToSelect()
		way.Debug(script)
	}

	// JOIN
	{
		a, b := "a", "b"
		where := way.F()
		get := way.Table(EMPLOYEE).Alias(a)
		get.LeftJoin(func(j hey.SQLJoin) (left hey.SQLAlias, right hey.SQLAlias, assoc hey.SQLJoinAssoc) {
			right = j.NewTable(DEPARTMENT, b)
			// j.TableColumn(j.GetMaster(), cst.Asterisk)
			j.Select(
				j.TableColumn(j.GetMaster(), cst.Asterisk),
				hey.Alias(hey.Coalesce(j.TableColumn(right, department.Name), hey.SQLString("")), "department_name"), // string
				j.TableColumn(right, department.SerialNum, "department_serial_num"),                                  // pointer int
			)
			assoc = j.OnEqual(employee.DepartmentId, department.Id)
			aid := j.TableColumn(j.GetMaster(), employee.Id)
			where.GreaterThan(aid, 0)
			get.Desc(aid)
			return
		})
		get.Where(where)
		get.Limit(10).Offset(1)
		// count, err := get.Count(context.Background())
		script = get.ToSelect()
		way.Debug(script)
	}

	// SELECT EXISTS, COUNT, Scan by map[string]any
	{
		tmp.ToEmpty()
		ctx := context.Background()
		exists, err := tmp.Table(EMPLOYEE).Exists(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
		log.Println(exists)
		count, err := tmp.Count(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
		log.Println(count)
		result, err := tmp.Limit(1).
			MapScan(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
		if len(result) > 0 {
			first := result[0]
			for k, v := range first {
				if v == nil {
					fmt.Printf("%s = %#v\n", k, v)
				} else {
					value := reflect.ValueOf(v).Elem().Interface()
					if val, ok := value.([]byte); ok {
						value = string(val)
					}
					fmt.Printf("%s = %#v\n", k, value)
				}
			}
		}
	}

	// More ways to call ...
}

func Transaction() {
	var err error

	rows := int64(0)

	idEqual := func(idValue any) hey.Filter { return way.F().Equal(employee.Id, idValue) }
	modify := map[string]any{
		"salary": 1500,
	}

	delete3 := way.Table("example3").Where(idEqual(3))
	delete4 := way.Table("example4").Where(idEqual(4))

	ctx := context.Background()
	err = way.Transaction(ctx, func(tx *hey.Way) error {
		tx.TransactionMessage("transaction-message-1")
		remove := tx.Table(EMPLOYEE).Where(idEqual(1))
		// _, _ = remove.Delete(ctx)
		script := remove.ToDelete()
		way.Debug(script)

		update := tx.Table(DEPARTMENT).Where(idEqual(1)).UpdateFunc(func(f hey.Filter, u hey.SQLUpdateSet) {
			u.Update(modify)
		})
		// _, _ = update.Update(ctx)
		script = update.ToDelete()
		way.Debug(script)

		if false {
			rows, err = tx.Execute(ctx, delete3.ToDelete())
			if err != nil {
				return err
			}
			if rows <= 0 {
				return hey.ErrNoRowsAffected
			}
			delete4.W(tx)
			rows, err = delete4.Delete(ctx)
			if err != nil {
				return err
			}
			if rows <= 0 {
				return hey.ErrNoRowsAffected
			}
		}

		return nil
	})
	if err != nil {
		fmt.Println(err.Error())
	}

	// Custom handling of transaction.
	err = func() (err error) {
		tx := (*hey.Way)(nil)
		tx, err = way.Begin(ctx)
		if err != nil {
			return err
		}

		tx.TransactionMessage("transaction-message-2")

		success := false

		defer func() {
			if !success {
				// for example:
				_ = tx.Rollback()
				if err == nil {
					// panic occurred in the database transaction.
					err = fmt.Errorf("%v", recover())
				}
			}
		}()

		/*
			This is where your business logic is handled.
		*/

		if err = tx.Commit(); err != nil {
			return err
		}

		success = true
		return err
	}()
	if err != nil {
		fmt.Println(err.Error())
	}
}

type (
	MyFilterInInt int
	MyFilterInAll interface {
		string | int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64 | bool | float32 | float64 | MyFilterInInt
	}
)

func filterInAll[T MyFilterInAll](v []T) []*T {
	result := make([]*T, len(v))
	for i := range result {
		result[i] = &v[i]
	}
	return result
}

func Filter() {
	f := way.F()
	{
		a1 := []string{"1", "2", "3"}
		a2 := []int{1, 2, 3}
		a3 := []int8{1, 2, 3}
		a4 := []int16{1, 2, 3}
		a5 := []int32{1, 2, 3}
		a6 := []int64{1, 2, 3}
		a7 := []uint{1, 2, 3}
		a8 := []uint8{1, 2, 3}
		a9 := []uint16{1, 2, 3}
		a10 := []uint32{1, 2, 3}
		a11 := []uint64{1, 2, 3}
		a12 := []bool{false, true}
		a13 := []float32{1, 2, 3}
		a14 := []float64{1, 2, 3}
		a15 := []MyFilterInInt{1, 2, 3}
		base := []any{
			a1, a2, a3,
			a4, a5, a6,
			a7, a8, a9,
			a10, a11, a12,
			a13, a14, a15,
		}
		inList := []any{
			[]any{1},
			[]any{1, 2, 3},
		}
		inList = append(inList, base...)
		inList = append(inList, filterInAll(a1))
		inList = append(inList, filterInAll(a2))
		inList = append(inList, filterInAll(a3))
		inList = append(inList, filterInAll(a4))
		inList = append(inList, filterInAll(a5))
		inList = append(inList, filterInAll(a6))
		inList = append(inList, filterInAll(a7))
		inList = append(inList, filterInAll(a8))
		inList = append(inList, filterInAll(a9))
		inList = append(inList, filterInAll(a10))
		inList = append(inList, filterInAll(a11))
		inList = append(inList, filterInAll(a12))
		inList = append(inList, filterInAll(a13))
		inList = append(inList, filterInAll(a14))
		inList = append(inList, filterInAll(a15))
		for _, v := range inList {
			f.ToEmpty()
			f.In(employee.Id, v)
			way.Debug(f)
		}
	}
	{
		g := f.Clone()
		way.Debug(f)
		way.Debug(g)
		g.Not()
		way.Debug(f)
		way.Debug(g)
		g.Between(employee.Age, 18, 20)
		way.Debug(g)
	}
	{
		f.ToEmpty()
		f.Equal(employee.Id, 1).Or(way.F().LessThanEqual(employee.Age, 18).ToSQL())
		way.Debug(f)
	}
	{
		f.ToEmpty()
		startAge := 18
		endAge := 20
		f.ToEmpty().Between(employee.Age, startAge, nil)
		way.Debug(f)
		f.ToEmpty().Between(employee.Age, nil, endAge)
		way.Debug(f)
		f.ToEmpty().Between(employee.Age, startAge, endAge)
		way.Debug(f)
		f.ToEmpty().Between(employee.Age, &startAge, nil)
		way.Debug(f)
		f.ToEmpty().Between(employee.Age, nil, &endAge)
		way.Debug(f)
		f.ToEmpty().Between(employee.Age, &startAge, &endAge)
		way.Debug(f)

		f.ToEmpty().Equal(
			employee.Id,
			way.Table(EMPLOYEE).Select(employee.Id).Desc(employee.Id).Limit(1),
		)
		way.Debug(f)

		f.ToEmpty().Equal(
			employee.Id,
			way.Table(EMPLOYEE).Select(employee.Id).Desc(employee.Id).Limit(1).ToSQL(),
		)
		way.Debug(f)

		f.ToEmpty().In(
			employee.Id,
			way.Table(EMPLOYEE).Select(employee.Id).Desc(employee.Id).Limit(10),
		)
		way.Debug(f)
	}
	{
		f.ToEmpty().Exists(way.Table(EMPLOYEE).Select(employee.Id).Desc(employee.Id).Limit(1))
		way.Debug(f)

		f.ToEmpty().NotExists(way.Table(EMPLOYEE).Select(employee.Id).Desc(employee.Id).Limit(1))
		way.Debug(f)

		f.ToEmpty().Like(employee.Name, "%Jick%")
		way.Debug(f)

		f.ToEmpty().NotLike(employee.Name, "%Jick%")
		way.Debug(f)

		f.ToEmpty().IsNotNull(employee.Email)
		way.Debug(f)

		f.ToEmpty().NotEqual(employee.Id, 1)
		way.Debug(f)

		f.ToEmpty().NotBetween(employee.Id, 1, 10)
		way.Debug(f)

		f.ToEmpty().NotIn(employee.Id, 1, 2, 3)
		way.Debug(f)

		f.ToEmpty().NotIn(employee.Id, []int64{1, 2, 3})
		way.Debug(f)

		f.ToEmpty().NotInGroup([]string{employee.Gender, employee.Age}, [][]any{
			{
				status.MALE,
				18,
			},
			{
				status.FEMALE,
				20,
			},
		})
		way.Debug(f)

		f.ToEmpty().InGroup([]string{employee.Gender, employee.Age}, way.Table(EMPLOYEE).Select(employee.Gender, employee.Age).Desc(employee.Id).Limit(10))
		way.Debug(f)

		f.ToEmpty().InGroup([]string{employee.Gender, employee.Age}, way.Table(EMPLOYEE).Select(employee.Gender, employee.Age).Desc(employee.Id).Limit(10).ToSQL())
		way.Debug(f)

		f.ToEmpty().Keyword("%test%", employee.Name, employee.Email)
		way.Debug(f)
	}

	{
		f.ToEmpty().CompareNotEqual(employee.CreatedAt, employee.UpdatedAt)
		f.CompareGreaterThan(employee.CreatedAt, employee.UpdatedAt)
		f.CompareGreaterThanEqual(employee.CreatedAt, employee.UpdatedAt)
		f.CompareLessThan(employee.CreatedAt, employee.UpdatedAt)
		f.CompareLessThanEqual(employee.CreatedAt, employee.UpdatedAt)
		way.Debug(f)
	}

	{
		queryId := way.Table(EMPLOYEE).Select(employee.Id).Desc(employee.Id).Limit(10)
		queryAge := way.Table(EMPLOYEE).Select(employee.Age).Group(employee.Age).HavingFunc(func(h hey.Filter) {
			h.Between(employee.Age, 18, 25)
		}).Desc(employee.Id).Limit(10)
		f.ToEmpty().AllQuantifier(func(q hey.Quantifier) {
			q.SetQuantifier(q.GetQuantifier())
			q.Equal(employee.Age, queryAge)
			q.NotEqual(employee.Id, queryId)
			q.LessThan(employee.Age, queryAge)
			q.LessThanEqual(employee.Age, queryAge)
			q.GreaterThan(employee.Age, queryAge)
			q.GreaterThanEqual(employee.Age, queryAge)
		})
		way.Debug(f)

		f.ToEmpty().AnyQuantifier(func(q hey.Quantifier) {
			q.Equal(employee.Age, queryAge)
			q.NotEqual(employee.Id, queryId)
		})
		way.Debug(f)
	}

	{
		createdAt := "1701234567,1801234567"
		salary := "1000,5000"
		name := "aaa,ccc"
		f.ToEmpty()
		f.ExtractFilter(func(e hey.ExtractFilter) {
			e.BetweenInt(employee.CreatedAt, &createdAt)
			e.BetweenInt64(employee.UpdatedAt, nil)
			f.OrGroup(func(g hey.Filter) {
				g.ExtractFilter(func(e hey.ExtractFilter) {
					e.BetweenInt64(employee.UpdatedAt, &createdAt)
				})
			})
			e.BetweenFloat64(employee.Salary, &salary)
			e.BetweenString(employee.Name, &name)
			e.InIntDirect(employee.Id, &createdAt)
			e.InInt64Direct(employee.Id, &createdAt)
			e.InStringDirect(employee.Name, &name)
			e.InIntVerify(employee.Id, &createdAt, func(index int, value int) bool {
				return true
			})
			e.InInt64Verify(employee.Id, &createdAt, func(index int, value int64) bool {
				return true
			})
			e.InStringVerify(employee.Name, &name, func(index int, value string) bool {
				return false
			})
		})
		way.Debug(f)

		f.ToEmpty()
		like := "Jack"
		f.ExtractFilter(func(g hey.ExtractFilter) {
			g.LikeSearch(&like, employee.Email, employee.Name)
		})
		way.Debug(f)
		f.ToEmpty()
		f.ExtractFilter(func(g hey.ExtractFilter) {
			g.LikeSearch(nil, employee.Email, employee.Name)
		})
		way.Debug(f)
		like = ""
		f.ToEmpty()
		f.ExtractFilter(func(g hey.ExtractFilter) {
			g.LikeSearch(&like, employee.Email, employee.Name)
		})
		way.Debug(f)
	}

	{
		f.ToEmpty()
		now := time.Now()
		f.TimeFilter(func(g hey.TimeFilter) {
			g.Timestamp(now.Unix())
			g.TimeLocation(now.Location())
			g.LastMinutes(employee.CreatedAt, 7)
			g.LastMinutes(employee.CreatedAt, 0)
			g.LastHours(employee.CreatedAt, 7)
			g.LastHours(employee.CreatedAt, -1)
			g.Today(employee.CreatedAt)
			g.Yesterday(employee.CreatedAt)
			g.LastDays(employee.CreatedAt, 7)
			g.LastDays(employee.CreatedAt, -7)
			g.ThisMonth(employee.CreatedAt)
			g.LastMonth(employee.CreatedAt)
			g.LastMonths(employee.CreatedAt, 3)
			g.LastMonths(employee.CreatedAt, -2)
			g.ThisQuarter(employee.CreatedAt)
			g.LastQuarter(employee.CreatedAt)
			g.LastQuarters(employee.CreatedAt, 2)
			g.LastQuarters(employee.CreatedAt, -1)
			g.LastQuarters(employee.CreatedAt, 20)
			g.ThisYear(employee.CreatedAt)
			g.LastYear(employee.CreatedAt)
			g.LastYears(employee.CreatedAt, 3)
			g.LastYears(employee.CreatedAt, 0)
		})
		way.Debug(f)
	}
}

func MyMulti() {
	ctx := context.Background()
	m := way.MyMulti()
	m.W(way)
	script := m.V().
		Table(EMPLOYEE).
		WhereFunc(func(f hey.Filter) {
			f.Equal(employee.Id, 1)
		}).
		Select(employee.Id, employee.Name, employee.Email, employee.CreatedAt).
		Limit(1)
	first := &Employee{}
	firstDepartment := &Department{}

	m.Add()    // for test
	m.Add(nil) // for test

	m.AddQuery(script, first)

	first1 := &Employee{}
	m.AddQuery(script.ToSelect(), func(rows *sql.Rows) error {
		return hey.RowsScan(rows, &first1, hey.DefaultTag)
	})
	m.Add(func(ctx context.Context) error {
		departmentId := first.DepartmentId
		if departmentId <= 0 {
			departmentId = 1
		}
		err := m.V().
			Table(DEPARTMENT).
			WhereFunc(func(f hey.Filter) {
				f.Equal(department.Id, departmentId)
			}).
			Limit(1).
			Scan(ctx, firstDepartment)
		if err != nil {
			if errors.Is(err, hey.ErrNoRows) {
				return nil
			}
			return err
		}
		return nil
	})
	m.Add(func(ctx context.Context) error {
		if firstDepartment.Id > 0 {
			_, err := m.V().
				Table(EMPLOYEE).
				UpdateFunc(func(f hey.Filter, u hey.SQLUpdateSet) {
					f.Equal(employee.Id, first.Id)
					u.Incr(employee.SerialNum, 1)
				}).
				Update(ctx)
			if err != nil {
				return err
			}
			return nil
		}
		return nil
	})

	exists := false
	m.AddExists(m.V().Table(DEPARTMENT).ToExists(), &exists)

	execute := m.V().Table(DEPARTMENT).UpdateFunc(func(f hey.Filter, u hey.SQLUpdateSet) {
		f.GreaterThan(department.Id, 0)
		u.Decr(department.SerialNum, 1)
	}).ToUpdate()
	m.AddExec(execute, nil)
	rows := int64(0)
	m.AddExec(execute, &rows)
	type myInt64 int64
	i64 := myInt64(0)
	m.AddExec(execute, &i64)
	m.AddExec(execute, m.RowsAffected(&rows))

	departmentRow := &Department{}
	queryRow := m.V().Table(DEPARTMENT).Select(department.Id, department.Name).Limit(1)
	m.AddQueryRow(queryRow, &departmentRow.Id, &departmentRow.Name)
	departmentRow1 := &Department{}
	m.AddQueryRow(queryRow, way.RowScan(&departmentRow1.Id, &departmentRow1.Name))

	if !m.IsEmpty() {
		err := m.Run(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
		log.Println("multi success")
	}
	m.ToEmpty()
}

func MySchema() {
	ctx := context.Background()
	columnString := strings.ReplaceAll(employee.ColumnString(), " ", "")
	// columnString = strings.ReplaceAll(columnString, "\"", "")
	schema := way.MySchema(EMPLOYEE, strings.Split(columnString, ","))
	{
		deleteAt := "deleted_at"
		updateAt := "updated_at"

		// Special note: If a table does not have a `deleted_at` field, special handling is required here.
		schema.SelectFilter(func(f hey.Filter) { f.Equal(deleteAt, 0) })
		schema.UpdateFilter(func(f hey.Filter) { f.Equal(deleteAt, 0) })

		// Reset hidden method
		schema.ResetHidden(func(ctx context.Context, where hey.Filter) (affectedRows int64, err error) {
			return schema.Update(ctx, where, func(u hey.SQLUpdateSet) {
				u.Set(deleteAt, way.Now().Unix())
			})
		})

		// Reset delete method
		schema.ResetDelete(func(ctx context.Context, where hey.Filter) (affectedRows int64, err error) {
			return way.Table(schema.Table()).Where(where.GreaterThan(deleteAt, 0)).Delete(ctx)
		})

		// Reset update method
		schema.ResetUpdate(func(ctx context.Context, where hey.Filter, update func(u hey.SQLUpdateSet)) (affectedRows int64, err error) {
			return way.Table(schema.Table()).Where(where.Equal(deleteAt, 0)).UpdateFunc(func(f hey.Filter, u hey.SQLUpdateSet) {
				update(u)
				if !u.IsEmpty() {
					u.Set(updateAt, way.Now().Unix())
				}
			}).Update(ctx)
		})
	}

	{
		// View table-name and columns.
		log.Println(schema.Table())
		log.Println(schema.Columns())
	}

	// query
	{
		count, err := schema.SelectCount(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
		log.Println(count)

		lists := make([]*Employee, 0)
		err = schema.SelectAll(ctx, &lists)
		if err != nil {
			log.Fatal(err.Error())
		}
		for _, tmp := range lists {
			log.Printf("%#v\n", tmp)
		}

		first := &Employee{}
		err = schema.SelectOne(ctx, &first)
		if err != nil {
			log.Fatal(err.Error())
		}
		log.Printf("%#v", first)

		exists, err := schema.SelectExists(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
		log.Println(exists)

		first = &Employee{}
		err = schema.SelectOneById(ctx, 1, first)
		if err != nil {
			log.Fatal(err.Error())
		}
		log.Printf("%#v\n", first)

		lists = make([]*Employee, 0)
		err = schema.SelectAll(ctx, &lists, func(o *hey.Table) {
			o.WhereFunc(func(f hey.Filter) {
				f.In(employee.Id, 1, 2)
			})
			o.Limit(10)
		})
		if err != nil {
			log.Fatal(err.Error())
		}
		for _, tmp := range lists {
			log.Printf("%#v\n", tmp)
		}
	}

	// execute
	{
		_, err := schema.UpdateById(ctx, 1, func(u hey.SQLUpdateSet) {
			u.Decr(employee.Age, -1)
		})
		if err != nil {
			log.Fatal(err.Error())
		}

		// reset insert one func
		schema.ResetInsertOne(func(ctx context.Context, insert any) (id int64, err error) {
			tmp := hey.ContextWay(ctx, way)
			table := tmp.Table(schema.Table())
			// context value
			ctx = context.WithValue(ctx, hey.MyInsertOne, true)
			ctx = context.WithValue(ctx, hey.MyTableName, schema.Table())
			ctx = context.WithValue(ctx, hey.MyTable, table)
			ctx = context.WithValue(ctx, hey.MyInsertData, insert)
			// prefix
			if before := schema.BeforeInsertValue(); before != nil {
				if ctx, err = before(ctx); err != nil {
					return
				}
			}
			// insert
			table.InsertFunc(func(i hey.SQLInsert) {
				i.Forbid(employee.Id)
				i.Create(insert)
				i.Returning(func(r hey.SQLReturning) {
					r.Returning(employee.Id)
					r.Execute(func(ctx context.Context, stmt *hey.Stmt, args ...any) (id int64, err error) {
						err = stmt.QueryRow(ctx, func(row *sql.Row) error {
							return row.Scan(&id)
						}, args...)
						return
					})
				})
			})
			if id, err = table.Insert(ctx); err != nil {
				return
			}
			// suffix
			ctx = context.WithValue(ctx, hey.MyInsertId, id)
			if after := schema.AfterInsertValue(); after != nil {
				if err = after(ctx); err != nil {
					return
				}
			}
			return
		})

		id, err := schema.InsertOne(ctx, map[string]any{
			employee.Email:     "example@gmail.com",
			employee.Name:      "example",
			employee.Age:       18,
			employee.CreatedAt: time.Now().Unix(),
		})
		if err != nil {
			log.Fatal(err.Error())
		}
		log.Println(id)

		rows := int64(0)
		timestamp := time.Now().Unix()
		rows, err = schema.InsertAll(ctx, []map[string]any{
			{
				employee.Email:     "example1@gmail.com",
				employee.Name:      "example1",
				employee.Age:       18,
				employee.CreatedAt: timestamp,
				employee.UpdatedAt: timestamp,
			},
			{
				employee.Email:     "example2@gmail.com",
				employee.Name:      "example2",
				employee.Age:       19,
				employee.CreatedAt: timestamp,
				employee.UpdatedAt: timestamp,
			},
		})
		if err != nil {
			log.Fatal(err.Error())
		}
		log.Println(rows)

		rows, err = schema.HiddenById(ctx, 2)
		if err != nil {
			log.Fatal(err.Error())
		}
		log.Println(rows)

		rows, err = schema.DeleteById(ctx, 2)
		if err != nil {
			log.Fatal(err.Error())
		}
		log.Println(rows)
	}
}

func Complex() {
	table := way.Table(EMPLOYEE)

	name := "test-complex"
	email := fmt.Sprintf("%s@gmail.com", name)
	timestamp := way.Now().Unix()

	// Preset insert data.
	table.InsertFunc(func(i hey.SQLInsert) {
		i.Forbid(employee.Id, employee.DeletedAt)
		i.Create(&Employee{
			Age:          18,
			Name:         name,
			Email:        &email,
			Gender:       "F",
			Height:       0,
			Weight:       0,
			Salary:       0,
			DepartmentId: 0,
			SerialNum:    0,
			CreatedAt:    timestamp,
			UpdatedAt:    timestamp,
		})
	})

	// Preset update data, data filtering conditions, and general deletion.
	table.UpdateFunc(func(f hey.Filter, u hey.SQLUpdateSet) {
		f.Equal(employee.Email, email)
		u.Set(employee.Name, name)
		u.Default(employee.UpdatedAt, timestamp)
	})

	// UPDATE or INSERT
	c := hey.NewComplex(table)
	ctx := context.Background()
	updateRows, insertRows, err := c.Upsert(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	log.Printf("Update: %d Insert: %d\n", updateRows, insertRows)
	updateRows, insertRows, err = c.Upsert(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	log.Printf("Update: %d Insert: %d\n", updateRows, insertRows)

	// DELETE and INSERT
	deleteRows, insertRows, err := c.DeleteCreate(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	log.Printf("Delete: %d Insert: %d\n", deleteRows, insertRows)
}

func TableColumn() {
	type QueryTableColumn struct {
		Employee
		DepartmentName      *string `db:"department_name"`
		DepartmentCreatedAt *int64  `db:"department_created_at"`
	}

	a := cst.A
	b := cst.B
	ac := way.T(a)
	bc := way.T(b)

	query := way.Table(EMPLOYEE).Alias(a).LeftJoin(func(j hey.SQLJoin) (left hey.SQLAlias, right hey.SQLAlias, assoc hey.SQLJoinAssoc) {
		ta := j.GetMaster()
		// left = ta // The left table is the default master table.
		right = j.NewTable(DEPARTMENT, b)
		assoc = j.OnEqual(employee.DepartmentId, department.Id)
		j.Select(j.TableColumn(ta, employee.Id, employee.Id))
		j.Select(j.TableColumns(ta, employee.Name, employee.Email, employee.DepartmentId))
		j.Select(
			bc.Column(department.Name, "department_name"),
		)
		dca := bc.Column(department.CreatedAt)
		j.Select(
			hey.Alias(hey.Coalesce(dca, 0), "department_created_at"),
		)
		return
	})
	query.WhereFunc(func(f hey.Filter) {
		f.GreaterThan(ac.Column(employee.Id), 0)
	})
	query.Desc(ac.Column(employee.Id))
	query.Desc(ac.Column(employee.SerialNum))
	query.Limit(1)
	ctx := context.Background()
	result := &QueryTableColumn{}
	err := query.Scan(ctx, result)
	if err != nil {
		log.Fatal(err.Error())
	}
	log.Printf("%#v\n", result)
}

func WindowFunc() {
	a := cst.A
	b := cst.B
	ac := way.T(a)
	bc := way.T(b)
	{
		query := way.Table(EMPLOYEE).Alias(a).LeftJoin(func(j hey.SQLJoin) (left hey.SQLAlias, right hey.SQLAlias, assoc hey.SQLJoinAssoc) {
			ta := j.GetMaster()
			right = j.NewTable(DEPARTMENT, b)
			assoc = j.OnEqual(employee.DepartmentId, department.Id)
			j.Select(j.TableColumn(ta, employee.Id, employee.Id))
			j.Select(j.TableColumns(ta, employee.Name, employee.Email, employee.DepartmentId))
			j.Select(
				way.WindowFunc("max_salary").Max(ac.Column(employee.DepartmentId)).OverFunc(func(o hey.SQLWindowFuncOver) {
					o.Partition(ac.Column(employee.DepartmentId))
					o.Desc(ac.Column(employee.Id))
				}),
				way.WindowFunc("avg_salary").Avg(ac.Column(employee.DepartmentId)).OverFunc(func(o hey.SQLWindowFuncOver) {
					o.Partition(ac.Column(employee.DepartmentId))
					o.Desc(ac.Column(employee.Id))
				}),
				way.WindowFunc("min_salary").Min(ac.Column(employee.DepartmentId)).OverFunc(func(o hey.SQLWindowFuncOver) {
					o.Partition(ac.Column(employee.DepartmentId))
					o.Desc(ac.Column(employee.Id))
				}),
			)
			j.Select(
				bc.Column(department.Name, "department_name"),
			)
			dca := bc.Column(department.CreatedAt)
			j.Select(
				hey.Alias(hey.Coalesce(dca, 0), "department_created_at"),
			)
			return
		})
		query.WhereFunc(func(f hey.Filter) {
			f.GreaterThan(ac.Column(employee.Id), 0)
		})
		query.Desc(ac.Column(employee.Id))
		query.Desc(ac.Column(employee.SerialNum))
		query.Limit(1)
		ctx := context.Background()
		result, err := query.MapScan(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
		for _, tmp := range result {
			for k, v := range tmp {
				if v == nil {
					fmt.Printf("%s = %#v\n", k, v)
				} else {
					value := reflect.ValueOf(v).Elem().Interface()
					if val, ok := value.([]byte); ok {
						value = string(val)
					}
					fmt.Printf("%s = %#v\n", k, value)
				}
			}
		}
	}

	// With window alias
	{
		query := way.Table(EMPLOYEE).Alias(a).LeftJoin(func(j hey.SQLJoin) (left hey.SQLAlias, right hey.SQLAlias, assoc hey.SQLJoinAssoc) {
			ta := j.GetMaster()
			right = j.NewTable(DEPARTMENT, b)
			assoc = j.OnEqual(employee.DepartmentId, department.Id)
			j.Select(j.TableColumn(ta, employee.Id, employee.Id))
			j.Select(j.TableColumns(ta, employee.Name, employee.Email, employee.DepartmentId))
			j.Select(
				way.WindowFunc("max_salary").Max(ac.Column(employee.DepartmentId)).Over("w1"),
				way.WindowFunc("avg_salary").Avg(ac.Column(employee.DepartmentId)).Over("w1"),
				way.WindowFunc("min_salary").Min(ac.Column(employee.DepartmentId)).Over("w1"),
			)
			j.Select(
				bc.Column(department.Name, "department_name"),
			)
			dca := bc.Column(department.CreatedAt)
			j.Select(
				hey.Alias(hey.Coalesce(dca, 0), "department_created_at"),
			)
			return
		})
		query.Window("w1", func(o hey.SQLWindowFuncOver) {
			o.Partition(ac.Column(employee.DepartmentId))
			o.Desc(ac.Column(employee.Id))
		})
		query.WhereFunc(func(f hey.Filter) {
			f.GreaterThan(ac.Column(employee.Id), 0)
		})
		query.Desc(ac.Column(employee.Id))
		query.Desc(ac.Column(employee.SerialNum))
		query.Limit(1)
		ctx := context.Background()
		result, err := query.MapScan(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
		for _, tmp := range result {
			for k, v := range tmp {
				if v == nil {
					fmt.Printf("%s = %#v\n", k, v)
				} else {
					value := reflect.ValueOf(v).Elem().Interface()
					if val, ok := value.([]byte); ok {
						value = string(val)
					}
					fmt.Printf("%s = %#v\n", k, value)
				}
			}
		}
	}
}

func WayMulti() {
	ctx := context.Background()
	timestamp := way.Now().Unix()

	// Query
	{
		employees := make([]*Employee, 0)
		{
			for _, id := range []int64{1, 3, 5, 7, 9} {
				tmp := &Employee{}
				tmp.Id = id
				employees = append(employees, tmp)
			}
			// OR
			// employee1 := &Employee{
			// 	Id: 1,
			// }
			// employee3 := &Employee{
			// 	Id: 3,
			// }
			// employee5 := &Employee{
			// 	Id: 5,
			// }
			// employee7 := &Employee{
			// 	Id: 7,
			// }
			// employee9 := &Employee{
			// 	Id: 9,
			// }
			// employees = append(employees, employee1, employee3, employee5, employee7, employee9)
		}
		lists := make([]any, 0)
		assoc := make(map[int64]*Employee)
		for _, v := range employees {
			tmp := &Employee{}
			if _, ok := assoc[v.Id]; ok {
				continue
			}
			lists = append(lists, tmp)
			assoc[v.Id] = tmp
		}
		script := way.Table(EMPLOYEE).WhereFunc(func(f hey.Filter) {
			f.Equal(employee.Id, 0)
		}).
			Select(employee.Id, employee.Name, employee.Email, employee.CreatedAt).
			ToSelect()
		if script == nil || script.IsEmpty() {
			return
		}
		makers := make([]hey.Maker, 0, len(employees))
		for _, e := range employees {
			makers = append(makers, hey.NewSQL(script.Prepare, e.Id))
		}
		err := way.MultiScan(ctx, makers, lists)
		if err != nil {
			if !errors.Is(err, hey.ErrNoRows) {
				log.Fatal(err.Error())
			}
		}
	}

	// Execute
	{
		update := map[int64]string{
			1: "multi-1",
			3: "multi-3",
			5: "multi-5",
		}
		args := make([][]any, 0, len(update))
		for k, v := range update {
			args = append(args, []any{
				v, timestamp, k,
			})
		}
		script := way.Table(EMPLOYEE).UpdateFunc(func(f hey.Filter, u hey.SQLUpdateSet) {
			f.Equal(employee.Id, 0)
			u.Set(employee.Name, "")
			u.Default(employee.UpdatedAt, 0)
		}).ToUpdate()
		makers := make([]hey.Maker, 0, len(args))
		for _, arg := range args {
			makers = append(makers, hey.NewSQL(script.Prepare, arg...))
		}

		// Execute in batch directly.
		{
			rows, err := way.MultiExecute(ctx, makers)
			if err != nil {
				log.Fatal(err.Error())
			}
			log.Println(rows)
		}

		// Ensure atomic operations through transactions.
		{
			var fail *hey.SQL
			for i, v := range makers {
				tmp, ok := v.(*hey.SQL)
				if ok && tmp != nil {
					if len(tmp.Args) > 0 {
						val, str := tmp.Args[0].(string)
						if str {
							tmp.Args[0] = fmt.Sprintf("%s-transaction", val)
							if i == 1 {
								fail = tmp
							}
						}
					}
				}
			}
			var err error
			rows := int64(0)
			err = way.Transaction(ctx, func(tx *hey.Way) error {
				rows, err = way.MultiExecute(ctx, makers)
				return err
			})
			if err != nil {
				log.Fatal(err.Error())
			}
			log.Println(rows)
			if fail != nil {
				b := &strings.Builder{}
				for range 300 {
					b.WriteString("x")
				}
				fail.Args[0] = fmt.Sprintf("%s-%s", fail.Args[0].(string), b.String())
				err = way.Transaction(ctx, func(tx *hey.Way) error {
					rows, err = way.MultiExecute(ctx, makers)
					return err
				})
				if err != nil {
					fmt.Println(err.Error())
				} else {
					log.Println(rows)
				}
			}
		}
	}
}

// myCache Simulate the implementation of the hey.Cacher interface.
type myCache struct {
	data map[string][]byte
}

func newMyCache() *myCache {
	return &myCache{
		data: make(map[string][]byte),
	}
}

func (s *myCache) Key(key string) string {
	return key
}

func (s *myCache) Get(ctx context.Context, key string) ([]byte, error) {
	tmp, ok := s.data[key]
	if !ok {
		return nil, hey.ErrNoDataInCache
	}
	return tmp, nil
}

func (s *myCache) Set(ctx context.Context, key string, value []byte, duration time.Duration) error {
	s.data[key] = value
	return nil
}

func (s *myCache) Del(ctx context.Context, key string) error {
	delete(s.data, key)
	return nil
}

func (s *myCache) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (s *myCache) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

var (
	cacheObject = hey.NewCache(newMyCache())
	cacheMutex  = hey.NewStringMutex(32)
)

func cacheQuery() (data *Employee, err error) {
	ctx := context.Background()

	query := way.Table(EMPLOYEE).WhereFunc(func(f hey.Filter) {
		f.Equal(employee.Id, 1)
	}).
		Select(employee.Id, employee.Name, employee.Email, employee.CreatedAt).
		Desc(employee.SerialNum).
		Limit(1).
		Offset(0)

	cacheMaker := hey.NewCacheMaker(cacheObject, query)
	cacheKey := ""
	cacheKey, err = cacheMaker.GetCacheKey()
	if err != nil {
		return
	}
	if cacheKey == "" {
		err = errors.New("cache key is empty")
		return
	}
	data = &Employee{}

	err = cacheMaker.GetUnmarshal(ctx, data)
	if err != nil {
		if !errors.Is(err, hey.ErrNoDataInCache) {
			return
		}
		err = nil
	}
	if data.Id > 0 {
		return
	}

	mu := cacheMutex.Get(cacheKey)
	mu.Lock()
	defer mu.Unlock()

	err = cacheMaker.GetUnmarshal(ctx, data)
	if err != nil {
		if !errors.Is(err, hey.ErrNoDataInCache) {
			return
		}
		err = nil
	}
	if data.Id > 0 {
		return
	}
	err = query.Scan(ctx, data)
	if err != nil {
		return
	}
	rd := hey.NewMinMaxDuration(time.Millisecond, 10, 30)
	err = cacheMaker.MarshalSet(ctx, data, rd.Get())
	return
}

func Cache() {
	total := 101
	for i := range total {
		data, err := cacheQuery()
		if err != nil {
			log.Printf("Get cache query error: %03d %s\n", i, err.Error())
			break
		}
		log.Printf("Get cache query data: %03d %#v\n", i, data)
	}
}
