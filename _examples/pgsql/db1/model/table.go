package model

// Department department table
type Department struct {
	Id        int64   `db:"id"` // id
	Name      *string `db:"name"`
	SerialNum int     `db:"serial_num"`
	CreatedAt int64   `db:"created_at"`
	UpdatedAt int64   `db:"updated_at"`
	DeletedAt int64   `db:"deleted_at"`
}

// Employee employee table
type Employee struct {
	Id           int64   `db:"id"` // id
	Age          int     `db:"age"`
	Name         string  `db:"name"`
	Email        *string `db:"email"`
	Gender       string  `db:"gender"`
	Height       float64 `db:"height"`
	Weight       float64 `db:"weight"`
	Salary       float64 `db:"salary"`
	DepartmentId int64   `db:"department_id"`
	SerialNum    int     `db:"serial_num"`
	CreatedAt    int64   `db:"created_at"`
	UpdatedAt    int64   `db:"updated_at"`
	DeletedAt    int64   `db:"deleted_at"`
}
