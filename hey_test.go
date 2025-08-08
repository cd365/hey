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
		err := way.Transaction(context.Background(), func(tx *Way) error {
			// Use tx to execute SQL statements.
			table := "example"
			if false {
				tx.Get(table) // ...
			}
			{
				rows, err := tx.Add(table).Add()
				if err != nil {
					return err
				}
				if rows <= 0 {
					return ErrNoRowsAffected
				}
			}
			{
				rows, err := tx.Mod(table).Mod()
				if err != nil {
					return err
				}
				if rows <= 0 {
					return ErrNoRowsAffected
				}
			}
			{
				rows, err := tx.Del(table).Del()
				if err != nil {
					return err
				}
				if rows <= 0 {
					return ErrNoRowsAffected
				}
			}
			return nil
		})
		if err != nil {
			t.Log(err.Error())
		}

		// Manually control transactions.
		// way.Begin(context.Background())
	}
}
