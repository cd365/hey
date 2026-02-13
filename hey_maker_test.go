package hey

import (
	"testing"

	"github.com/cd365/hey/v7/cst"
)

func TestNewOffsetRowsFetchNextRowsOnly(t *testing.T) {
	_ = NewOffsetRowsFetchNextRowsOnly(way)
}

func TestNewSQLCase(t *testing.T) {
	cases := NewSQLCase(way)
	cases.Case("column")
	cases.WhenThen("1", "'A'")
	cases.WhenThen("2", "'B'")
	cases.WhenThen("3", VarcharValue("C"))
	cases.Else(VarcharValue("D"))
	assert(cases, "CASE column WHEN 1 THEN 'A' WHEN 2 THEN 'B' WHEN 3 THEN 'C' ELSE 'D' END")

	cases = NewSQLCase(way)
	cases.WhenThen("score >= 90", 1)
	cases.WhenThen("score >= 80", 2)
	cases.WhenThen("score >= 60", 3)
	cases.Else(cst.NULL)
	assert(cases, "CASE WHEN score >= 90 THEN 1 WHEN score >= 80 THEN 2 WHEN score >= 60 THEN 3 ELSE NULL END")
}
