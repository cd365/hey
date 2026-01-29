// Common error

package hey

type Err string

func (s Err) Error() string {
	return string(s)
}

const (
	// pin Error pointer is nil.
	pin = Err("hey: pointer is nil")

	// ErrEmptySqlStatement Error empty sql statement.
	ErrEmptySqlStatement = Err("hey: empty sql statement")

	// ErrNoRowsAffected Error no rows affected.
	ErrNoRowsAffected = Err("hey: no rows affected")

	// ErrNoWhereCondition Error no where condition.
	ErrNoWhereCondition = Err("hey: no where condition")

	// ErrTransactionIsNil Error transaction is nil.
	ErrTransactionIsNil = Err("hey: transaction is nil")

	// ErrDatabaseIsNil Error database is nil.
	ErrDatabaseIsNil = Err("hey: database is nil")

	// ErrMethodNotImplemented Error method not implemented.
	ErrMethodNotImplemented = Err("hey: method not implemented")

	// ErrInvalidMaker Error invalid maker.
	ErrInvalidMaker = Err("hey: invalid maker")

	// ErrUnexpectedParameterValue Error unexpected parameter value.
	ErrUnexpectedParameterValue = Err("hey: unexpected parameter value")
)
