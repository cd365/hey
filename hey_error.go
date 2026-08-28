// Common error

package hey

const (
	nilPointer = "hey: nil pointer"
)

type Err string

func (s Err) Error() string {
	return string(s)
}
