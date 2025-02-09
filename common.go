package hey

import (
	"strings"
	"sync"
)

var (
	stringBuilder = &sync.Pool{
		New: func() interface{} { return &strings.Builder{} },
	}
)

func getStringBuilder() *strings.Builder {
	return stringBuilder.Get().(*strings.Builder)
}

func putStringBuilder(b *strings.Builder) {
	b.Reset()
	stringBuilder.Put(b)
}

type Replace struct {
	replace map[string]string
}

func (s *Replace) Add(oldName string, newName string) *Replace {
	s.replace[oldName] = newName
	return s
}

func (s *Replace) Del(name string) *Replace {
	delete(s.replace, name)
	return s
}

func (s *Replace) DelAll() *Replace {
	s.replace = make(map[string]string, 1<<9)
	return s
}

func (s *Replace) Get(name string) string {
	if value, ok := s.replace[name]; ok {
		return value
	} else {
		return name
	}
}

func (s *Replace) GetAll(names []string) []string {
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

func (s *Replace) Replace() map[string]string {
	return s.replace
}

func NewReplace() *Replace {
	return &Replace{
		replace: make(map[string]string, 1<<9),
	}
}

// Cmd Used to build a SQL expression and its corresponding parameter list.
type Cmd interface {
	// Cmd Get a list of script statements and their corresponding parameters.
	Cmd() (prepare string, args []interface{})
}

type sqlCmd struct {
	prepare string
	args    []interface{}
}

func (s *sqlCmd) Cmd() (prepare string, args []interface{}) {
	if s.prepare != EmptyString {
		prepare, args = s.prepare, s.args
	}
	return
}

func NewCmd(prepare string, args []interface{}) Cmd {
	return &sqlCmd{
		prepare: prepare,
		args:    args,
	}
}

// IsEmpty Check if an object value is empty.
type IsEmpty interface {
	IsEmpty() bool
}

func IsEmptyCmd(cmd Cmd) bool {
	if cmd == nil {
		return true
	}
	prepare, _ := cmd.Cmd()
	return prepare == EmptyString
}

// ConcatCmd Concat multiple queries.
func ConcatCmd(concat string, custom func(index int, cmd Cmd) Cmd, items ...Cmd) Cmd {
	length := len(items)
	lists := make([]Cmd, 0, length)
	index := 0
	for _, script := range items {
		if custom != nil {
			script = custom(index, script)
		}
		if IsEmptyCmd(script) {
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
		b.WriteString("( ")
		b.WriteString(prepare)
		b.WriteString(" )")
		args = append(args, param...)
	}
	return NewCmd(b.String(), args)
}

// UnionCmd ( QUERY_A ) UNION ( QUERY_B ) UNION ( QUERY_C )...
func UnionCmd(items ...Cmd) Cmd {
	return ConcatCmd(SqlUnion, nil, items...)
}

// UnionAllCmd ( QUERY_A ) UNION ALL ( QUERY_B ) UNION ALL ( QUERY_C )...
func UnionAllCmd(items ...Cmd) Cmd {
	return ConcatCmd(SqlUnionAll, nil, items...)
}

// ExceptCmd ( QUERY_A ) EXCEPT ( QUERY_B )...
func ExceptCmd(items ...Cmd) Cmd {
	return ConcatCmd(SqlExpect, nil, items...)
}

// IntersectCmd ( QUERY_A ) INTERSECT ( QUERY_B )...
func IntersectCmd(items ...Cmd) Cmd {
	return ConcatCmd(SqlIntersect, nil, items...)
}
