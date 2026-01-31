package replace

// MapTable All table name mappings.
var MapTable = map[string]string{
	"department": `"department"`, // department table
	"employee":   `"employee"`,   // employee table
}

// MapColumn All column name mappings.
var MapColumn = map[string]string{
	"id":            `"id"`,
	"name":          `"name"`,
	"serial_num":    `"serial_num"`,
	"created_at":    `"created_at"`,
	"updated_at":    `"updated_at"`,
	"deleted_at":    `"deleted_at"`,
	"age":           `"age"`,
	"email":         `"email"`,
	"gender":        `"gender"`,
	"height":        `"height"`,
	"weight":        `"weight"`,
	"salary":        `"salary"`,
	"department_id": `"department_id"`,
}
