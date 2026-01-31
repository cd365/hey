package model

// Department department table
type Department struct {
	Id        int64   `db:"id" yaml:"id" json:"id" camel:"id" pascal:"Id" underline:"id"` // id
	Name      *string `db:"name" yaml:"name" json:"name" camel:"name" pascal:"Name" underline:"name"`
	SerialNum int     `db:"serial_num" yaml:"serial_num" json:"serialNum" camel:"serialNum" pascal:"SerialNum" underline:"serial_num"`
	CreatedAt int64   `db:"created_at" yaml:"created_at" json:"createdAt" camel:"createdAt" pascal:"CreatedAt" underline:"created_at"`
	UpdatedAt int64   `db:"updated_at" yaml:"updated_at" json:"updatedAt" camel:"updatedAt" pascal:"UpdatedAt" underline:"updated_at"`
	DeletedAt int64   `db:"deleted_at" yaml:"deleted_at" json:"deletedAt" camel:"deletedAt" pascal:"DeletedAt" underline:"deleted_at"`
}

// Employee employee table
type Employee struct {
	Id           int64   `db:"id" yaml:"id" json:"id" camel:"id" pascal:"Id" underline:"id"` // id
	Age          int     `db:"age" yaml:"age" json:"age" camel:"age" pascal:"Age" underline:"age"`
	Name         string  `db:"name" yaml:"name" json:"name" camel:"name" pascal:"Name" underline:"name"`
	Email        *string `db:"email" yaml:"email" json:"email" camel:"email" pascal:"Email" underline:"email"`
	Gender       string  `db:"gender" yaml:"gender" json:"gender" camel:"gender" pascal:"Gender" underline:"gender"`
	Height       float64 `db:"height" yaml:"height" json:"height" camel:"height" pascal:"Height" underline:"height"`
	Weight       float64 `db:"weight" yaml:"weight" json:"weight" camel:"weight" pascal:"Weight" underline:"weight"`
	Salary       float64 `db:"salary" yaml:"salary" json:"salary" camel:"salary" pascal:"Salary" underline:"salary"`
	DepartmentId int64   `db:"department_id" yaml:"department_id" json:"departmentId" camel:"departmentId" pascal:"DepartmentId" underline:"department_id"`
	SerialNum    int     `db:"serial_num" yaml:"serial_num" json:"serialNum" camel:"serialNum" pascal:"SerialNum" underline:"serial_num"`
	CreatedAt    int64   `db:"created_at" yaml:"created_at" json:"createdAt" camel:"createdAt" pascal:"CreatedAt" underline:"created_at"`
	UpdatedAt    int64   `db:"updated_at" yaml:"updated_at" json:"updatedAt" camel:"updatedAt" pascal:"UpdatedAt" underline:"updated_at"`
	DeletedAt    int64   `db:"deleted_at" yaml:"deleted_at" json:"deletedAt" camel:"deletedAt" pascal:"DeletedAt" underline:"deleted_at"`
}
