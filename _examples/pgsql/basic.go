package pgsql

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
