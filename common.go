package hey

import (
	"context"
	"database/sql"
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

// ParcelFilter Parcel the SQL filter statement. `SQL_FILTER_STATEMENT` => ( `SQL_FILTER_STATEMENT` )
func ParcelFilter(tmp Filter) Filter {
	if tmp == nil {
		return tmp
	}
	if num := tmp.Num(); num != 1 {
		return tmp
	}
	prepare, args := tmp.Cmd()
	if !strings.HasPrefix(prepare, SqlLeftSmallBracket) {
		prepare = ParcelPrepare(prepare)
	}
	return tmp.New().And(prepare, args...)
}

// ParcelCancelPrepare Cancel parcel the SQL statement. ( `subquery` ) => `subquery` OR ( ( `subquery` ) ) => ( `subquery` )
func ParcelCancelPrepare(prepare string) string {
	prepare = strings.TrimSpace(prepare)
	prepare = strings.TrimPrefix(prepare, ConcatString(SqlLeftSmallBracket, SqlSpace))
	return strings.TrimSuffix(prepare, ConcatString(SqlSpace, SqlRightSmallBracket))
}

// ParcelCancelCmder Cancel parcel the SQL statement. ( `subquery` ) => `subquery` OR ( ( `subquery` ) ) => ( `subquery` )
func ParcelCancelCmder(cmder Cmder) Cmder {
	if IsEmptyCmder(cmder) {
		return cmder
	}
	prepare, args := cmder.Cmd()
	return NewCmder(ParcelCancelPrepare(prepare), args)
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

// UnionCmder CmderA, CmderB, CmderC ... => ( ( QUERY_A ) UNION ( QUERY_B ) UNION ( QUERY_C ) ... )
func UnionCmder(items ...Cmder) Cmder {
	return ConcatCmder(SqlUnion, nil, items...)
}

// UnionAllCmder CmderA, CmderB, CmderC ... => ( ( QUERY_A ) UNION ALL ( QUERY_B ) UNION ALL ( QUERY_C ) ... )
func UnionAllCmder(items ...Cmder) Cmder {
	return ConcatCmder(SqlUnionAll, nil, items...)
}

// ExceptCmder CmderA, CmderB ... => ( ( QUERY_A ) EXCEPT ( QUERY_B ) ... )
func ExceptCmder(items ...Cmder) Cmder {
	return ConcatCmder(SqlExpect, nil, items...)
}

// IntersectCmder CmderA, CmderB ... => ( ( QUERY_A ) INTERSECT ( QUERY_B ) ... )
func IntersectCmder(items ...Cmder) Cmder {
	return ConcatCmder(SqlIntersect, nil, items...)
}

// RowsScanStructAll Rows scan to any struct, based on struct scan data.
func RowsScanStructAll[V interface{}](ctx context.Context, way *Way, scan func(rows *sql.Rows, v *V) error, prepare string, args ...interface{}) ([]*V, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var err error
	length := 1 << 5
	result := make([]*V, 0, length)
	err = way.QueryContext(ctx, func(rows *sql.Rows) error {
		index := 0
		values := make([]V, length)
		for rows.Next() {
			if err = scan(rows, &values[index]); err != nil {
				return err
			} else {
				result = append(result, &values[index])
			}
			index++
			if index == length {
				index, values = 0, make([]V, length)
			}
		}
		return nil
	}, prepare, args...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// RowsScanStructOne Rows scan to any struct, based on struct scan data.
func RowsScanStructOne[V interface{}](ctx context.Context, way *Way, scan func(rows *sql.Rows, v *V) error, prepare string, args ...interface{}) (*V, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var (
		err error
		has bool
		tmp V
	)
	err = way.QueryContext(ctx, func(rows *sql.Rows) error {
		for rows.Next() {
			if err = scan(rows, &tmp); err != nil {
				return err
			}
			if !has {
				has = !has
			}
		}
		return nil
	}, prepare, args...)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, RecordDoesNotExists
	}
	return &tmp, nil
}

// RowsScanStructAllCmder Rows scan to any struct, based on struct scan data.
func RowsScanStructAllCmder[V interface{}](ctx context.Context, way *Way, scan func(rows *sql.Rows, v *V) error, cmder Cmder) ([]*V, error) {
	if IsEmptyCmder(cmder) {
		return nil, nil
	}
	prepare, args := cmder.Cmd()
	return RowsScanStructAll(ctx, way, scan, prepare, args...)
}

// RowsScanStructOneCmder Rows scan to any struct, based on struct scan data.
func RowsScanStructOneCmder[V interface{}](ctx context.Context, way *Way, scan func(rows *sql.Rows, v *V) error, cmder Cmder) (*V, error) {
	if IsEmptyCmder(cmder) {
		return nil, nil
	}
	prepare, args := cmder.Cmd()
	return RowsScanStructOne(ctx, way, scan, prepare, args...)
}

func MergeAssoc[K comparable, V interface{}](values ...map[K]V) map[K]V {
	length := len(values)
	result := make(map[K]V)
	for i := 0; i < length; i++ {
		if i == 0 {
			result = values[i]
			continue
		}
		for k, v := range values[i] {
			result[k] = v
		}
	}
	return result
}

func MergeArray[V interface{}](values ...[]V) []V {
	length := len(values)
	result := make([]V, 0)
	for i := 0; i < length; i++ {
		if i == 0 {
			result = values[i]
			continue
		}
		result = append(result, values[i]...)
	}
	return result
}

func AssocToArray[K comparable, V interface{}, W interface{}](values map[K]V, fc func(k K, v V) W) []W {
	length := len(values)
	result := make([]W, length)
	for index, value := range values {
		result = append(result, fc(index, value))
	}
	return result
}

func ArrayToAssoc[V interface{}, K comparable, W interface{}](values []V, fc func(v V) (K, W)) map[K]W {
	length := len(values)
	result := make(map[K]W, length)
	for i := 0; i < length; i++ {
		k, v := fc(values[i])
		result[k] = v
	}
	return result
}

func ArrayToArray[V interface{}, W interface{}](values []V, fc func(v V) W) []W {
	length := len(values)
	result := make([]W, length)
	for i := 0; i < length; i++ {
		result[i] = fc(values[i])
	}
	return result
}

func ArrayRemoveIndex[V interface{}](values []V, indexes []int) []V {
	count := len(indexes)
	if count == 0 {
		return values
	}
	length := len(values)
	mp := make(map[int]*struct{}, count)
	for i := 0; i < count; i++ {
		mp[indexes[i]] = &struct{}{}
	}
	ok := false
	result := make([]V, 0, length)
	for i := 0; i < length; i++ {
		if _, ok = mp[i]; !ok {
			result = append(result, values[i])
		}
	}
	return result
}
