package schema

// Department1769850001 department table
type Department1769850001 struct {
	Id        string // id
	Name      string
	SerialNum string
	CreatedAt string
	UpdatedAt string
	DeletedAt string
}

// Table Get table name.
func (s Department1769850001) Table() string {
	return "department" // department table
}

// Select Get table all columns.
func (s Department1769850001) Select() []string {
	return []string{"id", "name", "serial_num", "created_at", "updated_at", "deleted_at"}
}

// Department department table
var Department = Department1769850001{
	Id:        "id", // id
	Name:      "name",
	SerialNum: "serial_num",
	CreatedAt: "created_at",
	UpdatedAt: "updated_at",
	DeletedAt: "deleted_at",
}

// Employee1769850001 employee table
type Employee1769850001 struct {
	Id           string // id
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

// Table Get table name.
func (s Employee1769850001) Table() string {
	return "employee" // employee table
}

// Select Get table all columns.
func (s Employee1769850001) Select() []string {
	return []string{"id", "age", "name", "email", "gender", "height", "weight", "salary", "department_id", "serial_num", "created_at", "updated_at", "deleted_at"}
}

// Employee employee table
var Employee = Employee1769850001{
	Id:           "id", // id
	Age:          "age",
	Name:         "name",
	Email:        "email",
	Gender:       "gender",
	Height:       "height",
	Weight:       "weight",
	Salary:       "salary",
	DepartmentId: "department_id",
	SerialNum:    "serial_num",
	CreatedAt:    "created_at",
	UpdatedAt:    "updated_at",
	DeletedAt:    "deleted_at",
}
