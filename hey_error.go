// Common error

package hey

type Err string

func (s Err) Error() string {
	return string(s)
}

const (
	// ErrEmptyScript The script value executed is an empty string.
	ErrEmptyScript = Err("hey: the script value executed is an empty string")

	// ErrNoRows Error no rows.
	ErrNoRows = Err("hey: no rows")

	// ErrNoRowsAffected Error no rows affected.
	ErrNoRowsAffected = Err("hey: no rows affected")

	// ErrTransactionIsNil Error transaction isn't started.
	ErrTransactionIsNil = Err("hey: transaction is nil")

	// ErrDatabaseIsNil Error database is nil.
	ErrDatabaseIsNil = Err("hey: database is nil")

	// ErrMethodNotImplemented Error method not implemented.
	ErrMethodNotImplemented = Err("hey: method not implemented")
)
