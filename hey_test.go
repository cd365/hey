package hey

import (
	"context"
	"testing"
)

const (
	equalMessage = "they should be equal"
)

func testWay() *Way {
	way := NewWay(nil)
	way.GetCfg().Manual = Sqlite()
	return way
}

func TestWayTransaction(t *testing.T) {

	way := NewWay(nil)

	if way.db != nil {
		_ = way.Transaction(context.Background(), func(tx *Way) error {
			// Use tx to execute SQL statements.
			table := "example"
			tx.Get(table)
			tx.Add(table)
			tx.Mod(table)
			tx.Del(table)
			return nil
		})

		// Manually control transactions.
		// way.Begin(context.Background())
	}

}
