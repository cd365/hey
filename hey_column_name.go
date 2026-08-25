// Handle column name.

package hey

import (
	"strings"

	"github.com/cd365/hey/v7/cst"
)

// ColumnName Column name assembly.
type ColumnName interface {
	// T Get TableName.
	T() string

	// C TableName.ColumnName[ AS AliasName].
	C(column string, alias ...string) string

	// Ccc Batch set table name prefixes for column names.
	Ccc(columns ...string) []string

	// Avg Call an aggregate function AVG() on a column and give it an alias.
	Avg(column string, alias ...string) string

	// Max Call an aggregate function MAX() on a column and give it an alias.
	Max(column string, alias ...string) string

	// Min Call an aggregate function MIN() on a column and give it an alias.
	Min(column string, alias ...string) string

	// Sum Call an aggregate function SUM() on a column and give it an alias.
	Sum(column string, alias ...string) string

	// Count Call an aggregate function COUNT() on a column and give it an alias.
	Count(column string, alias ...string) string
}

type columnName struct {
	tab TableColumn
	way *Way
}

func (s *columnName) T() string {
	return s.tab.Table()
}

func (s *columnName) C(column string, alias ...string) string {
	return s.tab.Column(column, alias...)
}

func (s *columnName) Ccc(columns ...string) []string {
	return s.tab.ColumnAll(columns...)
}

func (s *columnName) aliasName(column string, aliases ...string) string {
	alias := LastNotEmptyString(aliases)
	if alias != cst.Empty {
		return alias
	}
	if column == cst.Asterisk {
		return alias
	}
	index := strings.LastIndex(column, cst.Point)
	if index == -1 {
		return column
	}
	return column[index+1:]
}

func (s *columnName) aliasFunc(funcName string, column string, alias ...string) string {
	columnName := column
	if column != cst.Asterisk {
		columnName = s.C(columnName)
	}
	return s.way.Alias(FuncSQL(funcName, columnName), s.aliasName(column, alias...)).ToSQL().Prepare
}

func (s *columnName) Avg(column string, alias ...string) string {
	return s.aliasFunc(cst.AVG, column, alias...)
}

func (s *columnName) Max(column string, alias ...string) string {
	return s.aliasFunc(cst.MAX, column, alias...)
}

func (s *columnName) Min(column string, alias ...string) string {
	return s.aliasFunc(cst.MIN, column, alias...)
}

func (s *columnName) Sum(column string, alias ...string) string {
	return s.aliasFunc(cst.SUM, column, alias...)
}

func (s *columnName) Count(column string, alias ...string) string {
	aliasName := LastNotEmptyString(alias)
	if aliasName == cst.Empty {
		aliasName = "counts"
	}
	return s.aliasFunc(cst.COUNT, column, aliasName)
}

func NewColumnName(way *Way, tableName string) ColumnName {
	return &columnName{
		tab: way.T(tableName),
		way: way,
	}
}
