package examples

import (
	"testing"

	"github.com/cd365/hey/v5"
	"github.com/stretchr/testify/assert"
)

const (
	table1 = "table1"
	table2 = "table2"
	table3 = "table3"

	field1 = "field1"
	field2 = "field2"
	field3 = "field3"
	field4 = "field4"
	field5 = "field5"
	field6 = "field6"
	field7 = "field7"

	id        = "id"
	status    = "status"
	createdAt = "created_at"
	updatedAt = "updated_at"
	deletedAt = "deleted_at"
)

var way *hey.Way

func init() {
	tmp, err := newSqlite3()
	if err != nil {
		panic(err)
	}
	way = tmp
}

func Assert(t *testing.T) *assert.Assertions {
	return assert.New(t)
}

func F(filters ...hey.Filter) hey.Filter {
	return way.F(filters...)
}
