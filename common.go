package hey

import (
	"strings"
	"sync"
)

var (
	stringBuilder = &sync.Pool{
		New: func() interface{} {
			return &strings.Builder{}
		},
	}
)

func getStringBuilder() *strings.Builder {
	return stringBuilder.Get().(*strings.Builder)
}

func putStringBuilder(b *strings.Builder) {
	b.Reset()
	stringBuilder.Put(b)
}

// ParcelPrepare Parcel the SQL statement. `subquery` => ( `subquery` )
func ParcelPrepare(prepare string) string {
	if prepare == EmptyString {
		return prepare
	}
	return ConcatString(SqlLeftSmallBracket, SqlSpace, prepare, SqlSpace, SqlRightSmallBracket)
}

// ParcelCmder Parcel the SQL statement. `subquery` => ( `subquery` )
func ParcelCmder(cmder Cmder) Cmder {
	if IsEmptyCmder(cmder) {
		return cmder
	}
	prepare, args := cmder.Cmd()
	prepare = strings.TrimSpace(prepare)
	if prepare[0] != SqlLeftSmallBracket[0] {
		prepare = ParcelPrepare(prepare)
	}
	return NewCmder(prepare, args)
}

// Replacer Replace identifiers in sql statements.
type Replacer interface {
	Add(originalName string, useName string) Replacer

	Del(originalName string) Replacer

	DelAll() Replacer

	Get(originalName string) string

	GetAll(originalNames []string) []string

	// Replace map[original-name]used-name
	Replace() map[string]string
}

type replace struct {
	replace map[string]string
}

func (s *replace) Add(oldName string, newName string) Replacer {
	s.replace[oldName] = newName
	return s
}

func (s *replace) Del(name string) Replacer {
	delete(s.replace, name)
	return s
}

func (s *replace) DelAll() Replacer {
	s.replace = make(map[string]string, 1<<9)
	return s
}

func (s *replace) Get(name string) string {
	if value, ok := s.replace[name]; ok {
		return value
	} else {
		return name
	}
}

func (s *replace) GetAll(names []string) []string {
	length := len(names)
	if length == 0 {
		return names
	}
	replaced := make([]string, length)
	for i := 0; i < length; i++ {
		if value, ok := s.replace[names[i]]; ok {
			replaced[i] = value
		} else {
			replaced[i] = names[i]
		}
	}
	return replaced
}

func (s *replace) Replace() map[string]string {
	return s.replace
}

func NewReplacer() Replacer {
	return &replace{
		replace: make(map[string]string, 1<<9),
	}
}

// Cmder Used to build a SQL expression and its corresponding parameter list.
type Cmder interface {
	// Cmd Get a list of script statements and their corresponding parameters.
	Cmd() (prepare string, args []interface{})
}

type cmdSql struct {
	prepare string
	args    []interface{}
}

func (s *cmdSql) Cmd() (prepare string, args []interface{}) {
	if s.prepare != EmptyString {
		prepare, args = s.prepare, s.args
	}
	return
}

// NewCmder Construct a Cmder using SQL statements and parameter lists.
func NewCmder(prepare string, args []interface{}) Cmder {
	return &cmdSql{
		prepare: prepare,
		args:    args,
	}
}

// IsEmpty Check if an object value is empty.
type IsEmpty interface {
	IsEmpty() bool
}

// IsEmptyCmder Check whether the result (SQL statement) of Cmder is empty.
func IsEmptyCmder(cmder Cmder) bool {
	if cmder == nil {
		return true
	}
	prepare, _ := cmder.Cmd()
	return prepare == EmptyString
}

// ConcatCmder Concat multiple Cmder.
func ConcatCmder(concat string, custom func(index int, cmder Cmder) Cmder, items ...Cmder) Cmder {
	length := len(items)
	lists := make([]Cmder, 0, length)
	index := 0
	for _, script := range items {
		if custom != nil {
			script = custom(index, script)
		}
		if IsEmptyCmder(script) {
			continue
		}
		lists = append(lists, script)
		index++
	}
	if index == 0 {
		return nil
	}
	if index < 2 {
		return lists[0]
	}

	b := getStringBuilder()
	defer putStringBuilder(b)
	args := make([]interface{}, 0, 32)
	concat = strings.TrimSpace(concat)
	for i := 0; i < index; i++ {
		prepare, param := lists[i].Cmd()
		if i > 0 {
			b.WriteString(SqlSpace)
			if concat != EmptyString {
				b.WriteString(concat)
				b.WriteString(SqlSpace)
			}
		}
		b.WriteString(ParcelPrepare(prepare))
		args = append(args, param...)
	}
	prepare := b.String()
	prepare = ParcelPrepare(prepare)
	return NewCmder(prepare, args)
}

// UnionCmder ( QUERY_A ) UNION ( QUERY_B ) UNION ( QUERY_C )...
func UnionCmder(items ...Cmder) Cmder {
	return ConcatCmder(SqlUnion, nil, items...)
}

// UnionAllCmder ( QUERY_A ) UNION ALL ( QUERY_B ) UNION ALL ( QUERY_C )...
func UnionAllCmder(items ...Cmder) Cmder {
	return ConcatCmder(SqlUnionAll, nil, items...)
}

// ExceptCmder ( QUERY_A ) EXCEPT ( QUERY_B )...
func ExceptCmder(items ...Cmder) Cmder {
	return ConcatCmder(SqlExpect, nil, items...)
}

// IntersectCmder ( QUERY_A ) INTERSECT ( QUERY_B )...
func IntersectCmder(items ...Cmder) Cmder {
	return ConcatCmder(SqlIntersect, nil, items...)
}
