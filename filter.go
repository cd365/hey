// Implementing SQL filter

package hey

import (
	"reflect"
	"strings"
	"sync"
)

// toInterfaceSlice Convert any type of slice to []any.
func toInterfaceSlice[T any](slice []T) []any {
	result := make([]any, len(slice))
	for i, v := range slice {
		result[i] = v
	}
	return result
}

// argsCompatible Compatibility parameter.
// Calling Method A: argsCompatible(args...) (T type must be any).
// Calling Method B: argsCompatible(args) (T type can be any, int, int64, string ...).
func argsCompatible(args ...any) []any {
	if len(args) != 1 {
		return args
	}
	switch v := args[0].(type) {
	case []any:
		return argsCompatible(v...)
	case []string:
		return toInterfaceSlice(v)
	case []int:
		return toInterfaceSlice(v)
	case []int8:
		return toInterfaceSlice(v)
	case []int16:
		return toInterfaceSlice(v)
	case []int32:
		return toInterfaceSlice(v)
	case []int64:
		return toInterfaceSlice(v)
	case []uint:
		return toInterfaceSlice(v)
	case []uint8: // []byte
		return args
	case []uint16:
		return toInterfaceSlice(v)
	case []uint32:
		return toInterfaceSlice(v)
	case []uint64:
		return toInterfaceSlice(v)
	case []bool:
		return toInterfaceSlice(v)
	case []float32:
		return toInterfaceSlice(v)
	case []float64:
		return toInterfaceSlice(v)
	case []*string:
		return toInterfaceSlice(v)
	case []*int:
		return toInterfaceSlice(v)
	case []*int8:
		return toInterfaceSlice(v)
	case []*int16:
		return toInterfaceSlice(v)
	case []*int32:
		return toInterfaceSlice(v)
	case []*int64:
		return toInterfaceSlice(v)
	case []*uint:
		return toInterfaceSlice(v)
	case []*uint8:
		return toInterfaceSlice(v)
	case []*uint16:
		return toInterfaceSlice(v)
	case []*uint32:
		return toInterfaceSlice(v)
	case []*uint64:
		return toInterfaceSlice(v)
	case []*bool:
		return toInterfaceSlice(v)
	case []*float32:
		return toInterfaceSlice(v)
	case []*float64:
		return toInterfaceSlice(v)
	default:
		rt := reflect.TypeOf(args[0])
		if rt.Kind() != reflect.Slice {
			return args
		}
		// expand slice members when there is only one slice type value.
		rv := reflect.ValueOf(args[0])
		count := rv.Len()
		result := make([]any, 0, count)
		for i := range count {
			result = append(result, rv.Index(i).Interface())
		}
		return result
	}
}

func filterSqlExpr(column string, compare string) string {
	if column == EmptyString {
		return EmptyString
	}
	return ConcatString(column, SqlSpace, compare, SqlSpace, SqlPlaceholder)
}

func filterEqual(column string) string {
	return filterSqlExpr(column, SqlEqual)
}

func filterNotEqual(column string) string {
	return filterSqlExpr(column, SqlNotEqual)
}

func filterMoreThan(column string) string {
	return filterSqlExpr(column, SqlMoreThan)
}

func filterMoreThanEqual(column string) string {
	return filterSqlExpr(column, SqlMoreThanEqual)
}

func filterLessThan(column string) string {
	return filterSqlExpr(column, SqlLessThan)
}

func filterLessThanEqual(column string) string {
	return filterSqlExpr(column, SqlLessThanEqual)
}

func filterIn(column string, values []any, not bool) (prepare string, args []any) {
	if column == EmptyString || values == nil {
		return
	}
	values = argsCompatible(values...)
	length := len(values)
	if length == 0 {
		return
	}
	values = DiscardDuplicate(nil, values...)
	length = len(values)
	if length == 1 {
		if not {
			prepare = filterNotEqual(column)
		} else {
			prepare = filterEqual(column)
		}
		args = []any{values[0]}
		return
	}
	args = values
	length2 := length * 2
	result := make([]string, 0, length2)
	for i := 0; i < length; i++ {
		if i == 0 {
			result = append(result, SqlPlaceholder)
			continue
		}
		result = append(result, SqlConcat, SqlPlaceholder)
	}
	tmp := make([]string, 0, len(result)+7)
	tmp = append(tmp, column)
	if not {
		tmp = append(tmp, SqlSpace, SqlNot)
	}
	tmp = append(tmp, SqlSpace, SqlIn, SqlSpace, SqlLeftSmallBracket, SqlSpace)
	tmp = append(tmp, result...)
	tmp = append(tmp, SqlSpace, SqlRightSmallBracket)
	prepare = ConcatString(tmp...)
	return
}

func filterInSql(column string, prepare string, args []any, not bool) (string, []any) {
	if column == EmptyString || prepare == EmptyString {
		return EmptyString, nil
	}
	result := column
	if not {
		result = ConcatString(result, SqlSpace, SqlNot)
	}
	result = ConcatString(result, SqlSpace, SqlIn, SqlSpace, SqlLeftSmallBracket, SqlSpace, prepare, SqlSpace, SqlRightSmallBracket)
	return result, args
}

func filterInGroupColumns(columns ...string) string {
	return ConcatString(SqlLeftSmallBracket, SqlSpace, strings.Join(columns, SqlConcat), SqlSpace, SqlRightSmallBracket)
}

func filterInGroup(columns []string, values [][]any, not bool) (prepare string, args []any) {
	count := len(columns)
	if count == 0 {
		return
	}
	length := len(values)
	if length == 0 {
		return
	}
	for i := range length {
		if len(values[i]) != count {
			args = nil
			return
		}
		args = append(args, values[i][:]...)
	}
	oneGroup := make([]string, count)
	for i := range count {
		oneGroup[i] = SqlPlaceholder
	}
	oneGroupString := ConcatString(SqlLeftSmallBracket, SqlSpace, strings.Join(oneGroup, SqlConcat), SqlSpace, SqlRightSmallBracket)
	valueGroup := make([]string, length)
	for i := range length {
		valueGroup[i] = oneGroupString
	}
	tmp := make([]string, 0, 8)
	tmp = append(tmp, filterInGroupColumns(columns...))
	if not {
		tmp = append(tmp, SqlSpace, SqlNot)
	}
	tmp = append(tmp, SqlSpace, SqlIn, SqlSpace, SqlLeftSmallBracket, SqlSpace)
	tmp = append(tmp, strings.Join(valueGroup, SqlConcat))
	tmp = append(tmp, SqlSpace, SqlRightSmallBracket)
	prepare = ConcatString(tmp...)
	return
}

func filterInGroupSql(columns []string, prepare string, args []any, not bool) (string, []any) {
	count := len(columns)
	if count == 0 || prepare == EmptyString {
		return EmptyString, nil
	}
	tmp := make([]string, 0, 8)
	tmp = append(tmp, filterInGroupColumns(columns...))
	if not {
		tmp = append(tmp, SqlSpace, SqlNot)
	}
	tmp = append(tmp, SqlSpace, SqlIn, SqlSpace, SqlLeftSmallBracket, SqlSpace)
	tmp = append(tmp, prepare)
	tmp = append(tmp, SqlSpace, SqlRightSmallBracket)
	return ConcatString(tmp...), args
}

func filterExists(prepare string, args []any, not bool) (string, []any) {
	if prepare == EmptyString {
		return EmptyString, nil
	}
	exists := SqlExists
	if not {
		exists = ConcatString(SqlNot, SqlSpace, exists)
	}
	return ConcatString(exists, SqlSpace, SqlLeftSmallBracket, SqlSpace, prepare, SqlSpace, SqlRightSmallBracket), args
}

func filterBetween(column string, not bool) (prepare string) {
	if column == EmptyString {
		return
	}
	prepare = column
	if not {
		prepare = ConcatString(prepare, SqlSpace, SqlNot)
	}
	prepare = ConcatString(prepare, SqlSpace, SqlBetween, SqlSpace, SqlPlaceholder, SqlSpace, SqlAnd, SqlSpace, SqlPlaceholder)
	return
}

func filterLike(column string, not bool) (prepare string) {
	if column == EmptyString {
		return
	}
	prepare = column
	if not {
		prepare = ConcatString(prepare, SqlSpace, SqlNot)
	}
	prepare = ConcatString(prepare, SqlSpace, SqlLike, SqlSpace, SqlPlaceholder)
	return
}

func filterIsNull(column string, not bool) (prepare string) {
	if column == EmptyString {
		return
	}
	prepare = ConcatString(column, SqlSpace, SqlIs)
	if not {
		prepare = ConcatString(prepare, SqlSpace, SqlNot)
	}
	prepare = ConcatString(prepare, SqlSpace, SqlNull)
	return
}

// filterUseValue Get value's current value as an any, otherwise return nil.
func filterUseValue(value any) any {
	if value == nil {
		return nil
	}
	v := reflect.ValueOf(value)
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	return v.Interface()
}

// Filter Implement SQL statement conditional filtering (general conditional filtering).
type Filter interface {
	Maker

	// Clean Clear the existing conditional filtering of the current object.
	Clean() Filter

	// Num Number of conditions used.
	Num() int

	// Empty Is the current object an empty object?
	Empty() bool

	// Not Negate the result of the current conditional filter object. Multiple negations are allowed.
	Not() Filter

	// And Use logical operator `AND` to combine custom conditions.
	And(prepare string, args ...any) Filter

	// Or Use logical operator `OR` to combine custom conditions.
	Or(prepare string, args ...any) Filter

	// Group Add a new condition group, which is connected by the `AND` logical operator by default.
	Group(group func(f Filter)) Filter

	// OrGroup Add a new condition group, which is connected by the `OR` logical operator by default.
	OrGroup(group func(f Filter)) Filter

	// Use Implement import a set of conditional filter objects into the current object.
	Use(filters ...Filter) Filter

	// New Create a new conditional filter object based on a set of conditional filter objects.
	New(filters ...Filter) Filter

	// Equal Implement conditional filtering: column = value .
	Equal(column string, value any, usingNull ...bool) Filter

	// LessThan Implement conditional filtering: column < value .
	LessThan(column string, value any) Filter

	// LessThanEqual Implement conditional filtering: column <= value .
	LessThanEqual(column string, value any) Filter

	// MoreThan Implement conditional filtering: column > value.
	MoreThan(column string, value any) Filter

	// MoreThanEqual Implement conditional filtering: column >= value .
	MoreThanEqual(column string, value any) Filter

	// Between Implement conditional filtering: column BETWEEN value1 AND value2 .
	Between(column string, start any, end any) Filter

	// In Implement conditional filtering: column IN ( value1, value2, value3... ) .
	In(column string, values ...any) Filter

	// InQuery Implement conditional filtering: column IN (subquery).
	InQuery(column string, subquery Maker) Filter

	// Exists Implement conditional filtering: EXISTS (subquery) .
	Exists(subquery Maker) Filter

	// Like Implement conditional filtering: column LIKE value.
	Like(column string, value any) Filter

	// IsNull Implement conditional filtering: column IS NULL .
	IsNull(column string) Filter

	// InGroup Implement conditional filtering: ( column1, column2, column3... ) IN ( ( value1, value2, value3... ), ( value21, value22, value23... )... ) .
	InGroup(columns []string, values ...[]any) Filter

	// InGroupQuery Implement conditional filtering: ( column1, column2, column3... ) IN ( subquery ) .
	InGroupQuery(columns []string, subquery Maker) Filter

	// NotEqual Implement conditional filtering: column <> value .
	NotEqual(column string, value any, notUsingNull ...bool) Filter

	// NotBetween Implement conditional filtering: column NOT BETWEEN value1 AND value2 .
	NotBetween(column string, start any, end any) Filter

	// NotIn Implement conditional filtering: column NOT IN ( value1, value2, value3... ) .
	NotIn(column string, values ...any) Filter

	// NotInGroup Implement conditional filtering: ( column1, column2, column3... ) NOT IN ( ( value1, value2, value3... ), ( value21, value22, value23... )... ) .
	NotInGroup(columns []string, values ...[]any) Filter

	// NotLike Implement conditional filtering: column NOT LIKE value .
	NotLike(column string, value any) Filter

	// IsNotNull Implement conditional filtering: column IS NOT NULL .
	IsNotNull(column string) Filter

	// AllQuantifier Implement conditional filtering: column {=||<>||>||>=||<||<=} ALL ( subquery ) .
	AllQuantifier(fc func(q Quantifier)) Filter

	// AnyQuantifier Implement conditional filtering: column {=||<>||>||>=||<||<=} ANY ( subquery ) .
	AnyQuantifier(fc func(q Quantifier)) Filter

	// GetWay For get *Way.
	GetWay() *Way

	// SetWay For set *Way.
	SetWay(way *Way) Filter

	// Compare Implement conditional filtering: column1 {=||<>||>||>=||<||<=} column2 .
	Compare(column1 string, compare string, column2 string, args ...any) Filter

	// CompareEqual Implement conditional filtering: column1 = column2 .
	CompareEqual(column1 string, column2 string, args ...any) Filter

	// CompareNotEqual Implement conditional filtering: column1 <> column2 .
	CompareNotEqual(column1 string, column2 string, args ...any) Filter

	// CompareMoreThan Implement conditional filtering: column1 > column2 .
	CompareMoreThan(column1 string, column2 string, args ...any) Filter

	// CompareMoreThanEqual Implement conditional filtering: column1 >= column2 .
	CompareMoreThanEqual(column1 string, column2 string, args ...any) Filter

	// CompareLessThan Implement conditional filtering: column1 < column2.
	CompareLessThan(column1 string, column2 string, args ...any) Filter

	// CompareLessThanEqual Implement conditional filtering: column1 <= column2 .
	CompareLessThanEqual(column1 string, column2 string, args ...any) Filter

	// You might be thinking why there is no method with the prefix `Or` defined to implement methods like OrEqual, OrLike, OrIn ...
	// 1. Considering that, most of the OR is not used frequently in the business development process.
	// 2. If the business really needs to use it, you can use the OrGroup method: OrGroup(func(g Filter) { g.Equal("column", 1) }) .
}

// filter Implementing interface Filter.
type filter struct {
	prepare *strings.Builder
	way     *Way
	args    []any
	num     int
	not     bool
}

// filterNew New a Filter.
func filterNew() *filter {
	return &filter{
		prepare: &strings.Builder{},
	}
}

// F New a Filter.
func F() Filter {
	return filterNew()
}

// poolFilter filter pool.
var poolFilter = &sync.Pool{
	New: func() any {
		return filterNew()
	},
}

func getFilter() Filter {
	return poolFilter.Get().(*filter)
}

func putFilter(f Filter) {
	f.Clean()
	poolFilter.Put(f)
}

func (s *filter) ToSQL() *SQL {
	if s.num == 0 {
		return NewSQL(EmptyString)
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	if s.not {
		b.WriteString(SqlNot)
		b.WriteString(SqlSpace)
	}
	if s.num > 1 {
		b.WriteString(SqlLeftSmallBracket)
		b.WriteString(SqlSpace)
		b.WriteString(s.prepare.String())
		b.WriteString(SqlSpace)
		b.WriteString(SqlRightSmallBracket)
	} else {
		b.WriteString(s.prepare.String())
	}
	return NewSQL(b.String(), s.args[:]...)
}

func (s *filter) clean() {
	s.not = false
	s.num = 0
	s.prepare.Reset()
	s.args = nil
	s.way = nil
}

func (s *filter) Clean() Filter {
	s.clean()
	return s
}

func (s *filter) Num() int {
	return s.num
}

func (s *filter) Empty() bool {
	return s.num == 0
}

func (s *filter) Not() Filter {
	s.not = !s.not
	return s
}

func (s *filter) add(logic string, prepare string, args ...any) *filter {
	if prepare == EmptyString {
		return s
	}
	if s.num == 0 {
		s.prepare.WriteString(prepare)
		s.args = args[:]
		s.num++
		return s
	}
	s.prepare.WriteString(ConcatString(SqlSpace, logic, SqlSpace, prepare))
	s.args = append(s.args, args[:]...)
	s.num++
	return s
}

func (s *filter) addGroup(logic string, group func(g Filter)) *filter {
	if group == nil {
		return s
	}
	tmp := getFilter().SetWay(s.way)
	defer putFilter(tmp)
	group(tmp)
	if tmp.Empty() {
		return s
	}
	script := tmp.ToSQL()
	s.add(logic, script.Prepare, script.Args...)
	return s
}

func (s *filter) And(prepare string, args ...any) Filter {
	return s.add(SqlAnd, prepare, args...)
}

func (s *filter) Or(prepare string, args ...any) Filter {
	return s.add(SqlOr, prepare, args...)
}

func (s *filter) Group(group func(f Filter)) Filter {
	return s.addGroup(SqlAnd, group)
}

func (s *filter) OrGroup(group func(f Filter)) Filter {
	return s.addGroup(SqlOr, group)
}

func (s *filter) Use(filters ...Filter) Filter {
	groups := getFilter().SetWay(s.way)
	defer putFilter(groups)
	for _, tmp := range filters {
		if tmp == nil || tmp.Empty() {
			continue
		}
		script := tmp.ToSQL()
		groups.And(script.Prepare, script.Args...)
	}
	script := groups.ToSQL()
	return s.And(script.Prepare, script.Args...)
}

func (s *filter) New(filters ...Filter) Filter {
	return filterNew().SetWay(s.way).Use(filters...)
}

func (s *filter) replace(key string) string {
	if s.way == nil {
		return key
	}
	return s.way.Replace(key)
}

func (s *filter) replaces(keys []string) []string {
	if s.way == nil {
		return keys
	}
	return s.way.Replaces(keys)
}

func (s *filter) Equal(column string, value any, usingNull ...bool) Filter {
	if value == nil {
		if length := len(usingNull); length > 0 && usingNull[length-1] {
			return s.IsNull(s.replace(column))
		}
		return s
	}
	if value = filterUseValue(value); value != nil {
		s.add(SqlAnd, filterEqual(s.replace(column)), value)
	}
	return s
}

func (s *filter) LessThan(column string, value any) Filter {
	if value = filterUseValue(value); value != nil {
		s.add(SqlAnd, filterLessThan(s.replace(column)), value)
	}
	return s
}

func (s *filter) LessThanEqual(column string, value any) Filter {
	if value = filterUseValue(value); value != nil {
		s.add(SqlAnd, filterLessThanEqual(s.replace(column)), value)
	}
	return s
}

func (s *filter) MoreThan(column string, value any) Filter {
	if value = filterUseValue(value); value != nil {
		s.add(SqlAnd, filterMoreThan(s.replace(column)), value)
	}
	return s
}

func (s *filter) MoreThanEqual(column string, value any) Filter {
	if value = filterUseValue(value); value != nil {
		s.add(SqlAnd, filterMoreThanEqual(s.replace(column)), value)
	}
	return s
}

func (s *filter) Between(column string, start any, end any) Filter {
	if start, end = filterUseValue(start), filterUseValue(end); start != nil && end != nil {
		s.add(SqlAnd, filterBetween(s.replace(column), false), start, end)
	}
	return s
}

func (s *filter) In(column string, values ...any) Filter {
	prepare, args := filterIn(s.replace(column), values, false)
	return s.add(SqlAnd, prepare, args...)
}

func (s *filter) InQuery(column string, subquery Maker) Filter {
	if subquery == nil {
		return s
	}
	script := subquery.ToSQL()
	if script.Empty() {
		return s
	}
	prepare, args := filterInSql(s.replace(column), script.Prepare, script.Args, false)
	return s.add(SqlAnd, prepare, args...)
}

func (s *filter) Exists(subquery Maker) Filter {
	if subquery == nil {
		return s
	}
	script := subquery.ToSQL()
	if script == nil || script.Empty() {
		return s
	}
	prepare, args := filterExists(script.Prepare, script.Args, false)
	return s.add(SqlAnd, prepare, args...)
}

func (s *filter) Like(column string, value any) Filter {
	value = filterUseValue(value)
	if value == nil {
		return s
	}
	like := EmptyString
	if tmp, ok := value.(string); !ok {
		if item := reflect.ValueOf(value); item.Kind() == reflect.String {
			like = item.String()
		}
	} else {
		like = tmp
	}
	if like != EmptyString {
		s.add(SqlAnd, filterLike(s.replace(column), false), like)
	}
	return s
}

func (s *filter) IsNull(column string) Filter {
	return s.add(SqlAnd, filterIsNull(s.replace(column), false))
}

func (s *filter) InGroup(columns []string, values ...[]any) Filter {
	prepare, args := filterInGroup(s.replaces(columns), values, false)
	return s.add(SqlAnd, prepare, args...)
}

func (s *filter) InGroupQuery(columns []string, subquery Maker) Filter {
	if subquery == nil {
		return s
	}
	script := subquery.ToSQL()
	if script.Empty() {
		return s
	}
	prepare, args := filterInGroupSql(s.replaces(columns), script.Prepare, script.Args, false)
	return s.add(SqlAnd, prepare, args...)
}

func (s *filter) NotEqual(column string, value any, notUsingNull ...bool) Filter {
	if value == nil {
		if length := len(notUsingNull); length > 0 && notUsingNull[length-1] {
			return s.IsNotNull(s.replace(column))
		}
		return s
	}
	if value = filterUseValue(value); value != nil {
		s.add(SqlAnd, filterNotEqual(s.replace(column)), value)
	}
	return s
}

func (s *filter) NotBetween(column string, start any, end any) Filter {
	if start, end = filterUseValue(start), filterUseValue(end); start != nil && end != nil {
		s.add(SqlAnd, filterBetween(s.replace(column), true), start, end)
	}
	return s
}

func (s *filter) NotIn(column string, values ...any) Filter {
	prepare, args := filterIn(s.replace(column), values, true)
	return s.add(SqlAnd, prepare, args...)
}

func (s *filter) NotInGroup(columns []string, values ...[]any) Filter {
	prepare, args := filterInGroup(s.replaces(columns), values, true)
	return s.add(SqlAnd, prepare, args...)
}

func (s *filter) NotLike(column string, value any) Filter {
	value = filterUseValue(value)
	if value == nil {
		return s
	}
	like := EmptyString
	if tmp, ok := value.(string); !ok {
		if item := reflect.ValueOf(value); item.Kind() == reflect.String {
			like = item.String()
		}
	} else {
		like = tmp
	}
	if like != EmptyString {
		s.add(SqlAnd, filterLike(s.replace(column), true), like)
	}
	return s
}

func (s *filter) IsNotNull(column string) Filter {
	return s.add(SqlAnd, filterIsNull(s.replace(column), true))
}

func (s *filter) AllQuantifier(fc func(q Quantifier)) Filter {
	if fc == nil {
		return s
	}
	tmp := &quantifier{
		filter:     s.New(),
		quantifier: SqlAll,
	}
	fc(tmp)
	return s.Use(tmp.filter)
}

func (s *filter) AnyQuantifier(fc func(q Quantifier)) Filter {
	if fc == nil {
		return s
	}
	tmp := &quantifier{
		filter:     s.New(),
		quantifier: SqlAny,
	}
	fc(tmp)
	return s.Use(tmp.filter)
}

func (s *filter) GetWay() *Way {
	return s.way
}

func (s *filter) SetWay(way *Way) Filter {
	s.way = way
	return s
}

func (s *filter) Compare(column1 string, compare string, column2 string, args ...any) Filter {
	if column1 == EmptyString || compare == EmptyString || column2 == EmptyString {
		return s
	}
	column1, column2 = s.replace(column1), s.replace(column2)
	return s.And(ConcatString(column1, SqlSpace, compare, SqlSpace, column2), args...)
}

func (s *filter) CompareEqual(column1 string, column2 string, args ...any) Filter {
	return s.Compare(column1, SqlEqual, column2, args...)
}

func (s *filter) CompareNotEqual(column1 string, column2 string, args ...any) Filter {
	return s.Compare(column1, SqlNotEqual, column2, args...)
}

func (s *filter) CompareMoreThan(column1 string, column2 string, args ...any) Filter {
	return s.Compare(column1, SqlMoreThan, column2, args...)
}

func (s *filter) CompareMoreThanEqual(column1 string, column2 string, args ...any) Filter {
	return s.Compare(column1, SqlMoreThanEqual, column2, args...)
}

func (s *filter) CompareLessThan(column1 string, column2 string, args ...any) Filter {
	return s.Compare(column1, SqlLessThan, column2, args...)
}

func (s *filter) CompareLessThanEqual(column1 string, column2 string, args ...any) Filter {
	return s.Compare(column1, SqlLessThanEqual, column2, args...)
}

// Quantifier Implement the filter condition: column {=||<>||>||>=||<||<=} [QUANTIFIER ]( subquery ) .
// QUANTIFIER is usually one of ALL, ANY, SOME ... or EmptyString.
type Quantifier interface {
	GetQuantifier() string

	SetQuantifier(quantifierString string) Quantifier

	Equal(column string, subquery Maker) Quantifier

	NotEqual(column string, subquery Maker) Quantifier

	LessThan(column string, subquery Maker) Quantifier

	LessThanEqual(column string, subquery Maker) Quantifier

	MoreThan(column string, subquery Maker) Quantifier

	MoreThanEqual(column string, subquery Maker) Quantifier
}

type quantifier struct {
	filter     Filter
	quantifier string // The value is usually one of ALL, ANY, SOME.
}

// GetQuantifier Get quantifier value.
func (s *quantifier) GetQuantifier() string {
	return s.quantifier
}

// SetQuantifier Set quantifier value.
func (s *quantifier) SetQuantifier(quantifierString string) Quantifier {
	s.quantifier = quantifierString
	return s
}

// build Add SQL filter statement.
func (s *quantifier) build(column string, logic string, subquery Maker) Quantifier {
	if column == EmptyString || logic == EmptyString || subquery == nil {
		return s
	}
	script := subquery.ToSQL()
	if script.Empty() {
		return s
	}
	if way := s.filter.GetWay(); way != nil {
		column = way.Replace(column)
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(column)
	b.WriteString(SqlSpace)
	b.WriteString(logic)
	b.WriteString(SqlSpace)
	if s.quantifier != EmptyString {
		b.WriteString(s.quantifier)
		b.WriteString(SqlSpace)
	}
	b.WriteString(SqlLeftSmallBracket)
	b.WriteString(SqlSpace)
	b.WriteString(script.Prepare)
	b.WriteString(SqlSpace)
	b.WriteString(SqlRightSmallBracket)
	s.filter.And(b.String(), script.Args...)
	return s
}

// Equal Implement the filter condition: column = QUANTIFIER ( subquery ) .
func (s *quantifier) Equal(column string, subquery Maker) Quantifier {
	return s.build(column, SqlEqual, subquery)
}

// NotEqual Implement the filter condition: column <> QUANTIFIER ( subquery ) .
func (s *quantifier) NotEqual(column string, subquery Maker) Quantifier {
	return s.build(column, SqlNotEqual, subquery)
}

// LessThan Implement the filter condition: column < QUANTIFIER ( subquery ) .
func (s *quantifier) LessThan(column string, subquery Maker) Quantifier {
	return s.build(column, SqlLessThan, subquery)
}

// LessThanEqual Implement the filter condition: column <= QUANTIFIER ( subquery ) .
func (s *quantifier) LessThanEqual(column string, subquery Maker) Quantifier {
	return s.build(column, SqlLessThanEqual, subquery)
}

// MoreThan Implement the filter condition: column > QUANTIFIER ( subquery ) .
func (s *quantifier) MoreThan(column string, subquery Maker) Quantifier {
	return s.build(column, SqlMoreThan, subquery)
}

// MoreThanEqual Implement the filter condition: column >= QUANTIFIER ( subquery ) .
func (s *quantifier) MoreThanEqual(column string, subquery Maker) Quantifier {
	return s.build(column, SqlMoreThanEqual, subquery)
}

// InValues Build column IN ( values[0].attributeN, values[1].attributeN, values[2].attributeN ... )
func InValues[T any](values []T, fc func(tmp T) any) []any {
	if fc == nil {
		return nil
	}
	length := len(values)
	if length == 0 {
		return nil
	}
	result := make([]any, length)
	for index, tmp := range values {
		result[index] = fc(tmp)
	}
	return result
}

// InGroupValues Build ( column1, column2, column3 ... ) IN ( ( values[0].attribute1, values[0].attribute2, values[0].attribute3 ... ), ( values[1].attribute1, values[1].attribute2, values[1].attribute3 ... ) ... )
func InGroupValues[T any](values []T, fc func(tmp T) []any) [][]any {
	if fc == nil {
		return nil
	}
	length := len(values)
	if length == 0 {
		return nil
	}
	result := make([][]any, length)
	for index, tmp := range values {
		result[index] = fc(tmp)
	}
	return result
}
