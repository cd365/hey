package hey

import (
	"reflect"
	"strings"
	"sync"
)

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

// AnyAny Convert any type of slice to []any.
func AnyAny[T any](slice []T) []any {
	result := make([]any, len(slice))
	for i, v := range slice {
		result[i] = v
	}
	return result
}

// inArgs Compatibility parameter.
// Calling Method A: inArgs(args...) (args type must be []any).
// Calling Method B: inArgs(args) (args type can be []any, []int, []int64, []string ...).
func inArgs(args ...any) []any {
	if len(args) != 1 {
		return args
	}
	switch v := args[0].(type) {
	case []any:
		return inArgs(v...)
	case []string:
		return AnyAny(v)
	case []int:
		return AnyAny(v)
	case []int8:
		return AnyAny(v)
	case []int16:
		return AnyAny(v)
	case []int32:
		return AnyAny(v)
	case []int64:
		return AnyAny(v)
	case []uint:
		return AnyAny(v)
	case []uint8: // []byte
		return args
	case []uint16:
		return AnyAny(v)
	case []uint32:
		return AnyAny(v)
	case []uint64:
		return AnyAny(v)
	case []bool:
		return AnyAny(v)
	case []float32:
		return AnyAny(v)
	case []float64:
		return AnyAny(v)
	case []*string:
		return AnyAny(v)
	case []*int:
		return AnyAny(v)
	case []*int8:
		return AnyAny(v)
	case []*int16:
		return AnyAny(v)
	case []*int32:
		return AnyAny(v)
	case []*int64:
		return AnyAny(v)
	case []*uint:
		return AnyAny(v)
	case []*uint8:
		return AnyAny(v)
	case []*uint16:
		return AnyAny(v)
	case []*uint32:
		return AnyAny(v)
	case []*uint64:
		return AnyAny(v)
	case []*bool:
		return AnyAny(v)
	case []*float32:
		return AnyAny(v)
	case []*float64:
		return AnyAny(v)
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

// filterUsingValue Get value's current value as an any, otherwise return nil.
func filterUsingValue(value any) any {
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

	// ToEmpty Clear the existing conditional filtering of the current object.
	ToEmpty() Filter

	// Num Number of conditions used.
	Num() int

	// IsEmpty Is the current object an empty object?
	IsEmpty() bool

	// Not Negate the result of the current conditional filter object. Multiple negations are allowed.
	Not() Filter

	// And Use logical operator `AND` to combine custom conditions.
	And(script *SQL) Filter

	// Or Use logical operator `OR` to combine custom conditions.
	Or(script *SQL) Filter

	// Group Add a new condition group, which is connected by the `AND` logical operator by default.
	Group(group func(g Filter)) Filter

	// OrGroup Add a new condition group, which is connected by the `OR` logical operator by default.
	OrGroup(group func(g Filter)) Filter

	// Use Implement import a set of conditional filter objects into the current object.
	Use(filters ...Filter) Filter

	// New Create a new conditional filter object based on a set of conditional filter objects.
	New(filters ...Filter) Filter

	// Equal Implement conditional filtering: column = value .
	Equal(column any, value any, null ...bool) Filter

	// LessThan Implement conditional filtering: column < value .
	LessThan(column any, value any) Filter

	// LessThanEqual Implement conditional filtering: column <= value .
	LessThanEqual(column any, value any) Filter

	// MoreThan Implement conditional filtering: column > value.
	MoreThan(column any, value any) Filter

	// MoreThanEqual Implement conditional filtering: column >= value .
	MoreThanEqual(column any, value any) Filter

	// Between Implement conditional filtering: column BETWEEN value1 AND value2 .
	Between(column any, start any, end any) Filter

	// In Implement conditional filtering: column IN ( value1, value2, value3... ) || column IN ( subquery ) .
	In(column any, values ...any) Filter

	// InGroup Implement conditional filtering: ( column1, column2, column3... ) IN ( ( value1, value2, value3... ), ( value21, value22, value23... )... ) || ( column1, column2, column3... ) IN ( subquery ) .
	InGroup(columns any, values any) Filter

	// Exists Implement conditional filtering: EXISTS (subquery) .
	Exists(subquery Maker) Filter

	// Like Implement conditional filtering: column LIKE value.
	Like(column any, value any) Filter

	// IsNull Implement conditional filtering: column IS NULL .
	IsNull(column any) Filter

	// NotEqual Implement conditional filtering: column <> value .
	NotEqual(column any, value any, null ...bool) Filter

	// NotBetween Implement conditional filtering: column NOT BETWEEN value1 AND value2 .
	NotBetween(column any, start any, end any) Filter

	// NotIn Implement conditional filtering: column NOT IN ( value1, value2, value3... ) .
	NotIn(column any, values ...any) Filter

	// NotInGroup Implement conditional filtering: ( column1, column2, column3... ) NOT IN ( ( value1, value2, value3... ), ( value21, value22, value23... )... ) || ( column1, column2, column3... ) NOT IN ( subquery ) .
	NotInGroup(columns any, values any) Filter

	// NotExists Implement conditional filtering: NOT EXISTS (subquery) .
	NotExists(subquery Maker) Filter

	// NotLike Implement conditional filtering: column NOT LIKE value .
	NotLike(column any, value any) Filter

	// IsNotNull Implement conditional filtering: column IS NOT NULL .
	IsNotNull(column any) Filter

	// AllQuantifier Implement conditional filtering: column {=||<>||>||>=||<||<=} ALL ( subquery ) .
	AllQuantifier(fc func(q Quantifier)) Filter

	// AnyQuantifier Implement conditional filtering: column {=||<>||>||>=||<||<=} ANY ( subquery ) .
	AnyQuantifier(fc func(q Quantifier)) Filter

	// GetReplacer For get *Way.
	GetReplacer() Replacer

	// SetReplacer For set *Way.
	SetReplacer(replacer Replacer) Filter

	// CompareEqual Implement conditional filtering: script1 = script2 .
	CompareEqual(column1 any, column2 any) Filter

	// CompareNotEqual Implement conditional filtering: script1 <> script2 .
	CompareNotEqual(column1 any, column2 any) Filter

	// CompareMoreThan Implement conditional filtering: script1 > script2 .
	CompareMoreThan(column1 any, column2 any) Filter

	// CompareMoreThanEqual Implement conditional filtering: script1 >= script2 .
	CompareMoreThanEqual(column1 any, column2 any) Filter

	// CompareLessThan Implement conditional filtering: script1 < script2.
	CompareLessThan(column1 any, column2 any) Filter

	// CompareLessThanEqual Implement conditional filtering: script1 <= script2 .
	CompareLessThanEqual(column1 any, column2 any) Filter

	// You might be thinking why there is no method with the prefix `Or` defined to implement methods like OrEqual, OrLike, OrIn ...
	// 1. Considering that, most of the OR is not used frequently in the business development process.
	// 2. If the business really needs to use it, you can use the OrGroup method: OrGroup(func(g Filter) { g.Equal("column", 1) }) .
}

// filter Implementing interface Filter.
type filter struct {
	prepare  *strings.Builder
	replacer Replacer
	args     []any
	num      int
	not      bool
}

// newFilter New a Filter.
func newFilter() *filter {
	return &filter{prepare: &strings.Builder{}}
}

// F New a Filter.
func F() Filter {
	return newFilter()
}

// poolFilter filter pool.
var poolFilter = &sync.Pool{
	New: func() any { return newFilter() },
}

func poolGetFilter() Filter {
	return poolFilter.Get().(*filter)
}

func poolPutFilter(f Filter) {
	poolFilter.Put(f.ToEmpty())
}

func (s *filter) ToSQL() *SQL {
	if s.IsEmpty() {
		return NewSQL(StrEmpty)
	}

	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)

	if s.not {
		b.WriteString(StrNot)
		b.WriteString(StrSpace)
	}

	if s.num > 1 {
		b.WriteString(ParcelPrepare(s.prepare.String()))
	} else {
		b.WriteString(s.prepare.String())
	}

	args := make([]any, len(s.args))
	copy(args, s.args)

	return NewSQL(b.String(), args...)
}

func (s *filter) toEmpty() *filter {
	s.not = false
	s.num = 0
	s.prepare.Reset()
	s.args = nil
	return s
}

func (s *filter) ToEmpty() Filter {
	return s.toEmpty()
}

func (s *filter) Num() int {
	return s.num
}

func (s *filter) IsEmpty() bool {
	return s.num == 0
}

func (s *filter) Not() Filter {
	s.not = !s.not
	return s
}

func (s *filter) add(logic string, script *SQL) *filter {
	if script == nil || script.IsEmpty() {
		return s
	}

	length := len(script.Args)
	args := make([]any, length)
	if length > 0 {
		copy(args, script.Args)
	}

	if s.num == 0 {
		s.prepare.WriteString(script.Prepare)
		s.args = args
		s.num++
		return s
	}

	s.prepare.WriteString(Strings(StrSpace, logic, StrSpace, script.Prepare))
	if length > 0 {
		s.args = append(s.args, args...)
	}
	s.num++

	return s
}

func (s *filter) addGroup(logic string, group func(g Filter)) *filter {
	if group == nil {
		return s
	}

	tmp := poolGetFilter().SetReplacer(s.replacer)
	defer poolPutFilter(tmp)

	group(tmp)

	if tmp.IsEmpty() {
		return s
	}

	s.add(logic, tmp.ToSQL())

	return s
}

func (s *filter) firstNext(first *SQL, next ...any) *SQL {
	if first == nil || first.IsEmpty() {
		return NewEmptySQL()
	}
	return JoinSQLSpace(firstNext(first, next...)...)
}

func (s *filter) And(script *SQL) Filter {
	return s.add(StrAnd, script)
}

func (s *filter) Or(script *SQL) Filter {
	return s.add(StrOr, script)
}

func (s *filter) Group(group func(g Filter)) Filter {
	return s.addGroup(StrAnd, group)
}

func (s *filter) OrGroup(group func(g Filter)) Filter {
	return s.addGroup(StrOr, group)
}

func (s *filter) Use(filters ...Filter) Filter {
	group := poolGetFilter().SetReplacer(s.replacer)
	defer poolPutFilter(group)
	for _, tmp := range filters {
		if tmp == nil || tmp.IsEmpty() {
			continue
		}
		group.And(tmp.ToSQL())
	}
	return s.And(group.ToSQL())
}

func (s *filter) New(filters ...Filter) Filter {
	return newFilter().SetReplacer(s.GetReplacer()).Use(filters...)
}

func (s *filter) get(key string) string {
	if s.replacer == nil {
		return key
	}
	return s.replacer.Get(key)
}

func (s *filter) compare(logic string, column any, compare string, value any) Filter {
	if column == nil || value == nil {
		return s
	}

	if value = filterUsingValue(value); value == nil {
		return s
	}

	if script, ok := column.(string); ok {
		column = s.get(script)
	}

	first := any2sql(column)
	if first.IsEmpty() {
		return s
	}

	next := make([]any, 0, 2)
	next = append(next, compare)
	args := ([]any)(nil)
	switch v := value.(type) {
	case *SQL:
		next = append(next, ParcelSQL(v))
	case Maker:
		if v == nil {
			return s
		}
		tmp := v.ToSQL()
		if tmp == nil || tmp.IsEmpty() {
			return s
		}
		next = append(next, ParcelSQL(tmp))
	default:
		next = append(next, StrPlaceholder)
		args = []any{value}
	}

	result := s.firstNext(first, next...)
	if args != nil {
		result.Args = append(result.Args, args...)
	}

	return s.add(logic, result)
}

func (s *filter) Equal(column any, value any, null ...bool) Filter {
	if column == nil {
		return s
	}
	if value == nil {
		if length := len(null); length > 0 && null[length-1] {
			return s.IsNull(column)
		}
		return s
	}
	return s.compare(StrAnd, column, StrEqual, value)
}

func (s *filter) LessThan(column any, value any) Filter {
	return s.compare(StrAnd, column, StrLessThan, value)
}

func (s *filter) LessThanEqual(column any, value any) Filter {
	return s.compare(StrAnd, column, StrLessThanEqual, value)
}

func (s *filter) MoreThan(column any, value any) Filter {
	return s.compare(StrAnd, column, StrMoreThan, value)
}

func (s *filter) MoreThanEqual(column any, value any) Filter {
	return s.compare(StrAnd, column, StrMoreThanEqual, value)
}

func (s *filter) between(logic string, column any, start any, end any, not bool) Filter {
	start, end = filterUsingValue(start), filterUsingValue(end)
	if column == nil || start == nil || end == nil {
		return s
	}

	if script, ok := column.(string); ok {
		column = s.get(script)
	}

	first := any2sql(column)
	if first.IsEmpty() {
		return s
	}

	next := make([]any, 0, 5)
	if not {
		next = append(next, StrNot)
	}
	next = append(next, StrBetween)
	args := make([]any, 0, 2)
	if value, ok := start.(*SQL); ok {
		next = append(next, ParcelSQL(value.Copy()))
	} else {
		next = append(next, StrPlaceholder)
		args = append(args, start)
	}
	next = append(next, StrAnd)
	if value, ok := end.(*SQL); ok {
		next = append(next, ParcelSQL(value.Copy()))
	} else {
		next = append(next, StrPlaceholder)
		args = append(args, end)
	}

	result := s.firstNext(first, next...)
	if len(args) > 0 {
		result.Args = append(result.Args, args...)
	}

	return s.add(logic, result)
}

func (s *filter) Between(column any, start any, end any) Filter {
	return s.between(StrAnd, column, start, end, false)
}

func (s *filter) in(logic string, column any, values []any, not bool) Filter {
	if column == nil || values == nil {
		return s
	}

	if script, ok := column.(string); ok {
		column = s.get(script)
	}

	script := any2sql(column)
	if script.IsEmpty() {
		return s
	}

	length := len(values)
	if length == 0 {
		return s
	}

	if length == 1 {
		if value, ok := values[0].(Maker); ok { // subquery value
			if value != nil {
				if subquery := value.ToSQL(); subquery != nil && !subquery.IsEmpty() {
					latest := subquery.Copy()
					latest.Prepare = ParcelPrepare(latest.Prepare)
					lists := make([]any, 0, 3)
					if not {
						lists = append(lists, StrNot)
					}
					lists = append(lists, StrIn, latest)
					return s.add(logic, s.firstNext(script, lists...))
				}
			}
			return s
		}
	}

	values = inArgs(values...)
	length = len(values)
	if length == 0 {
		return s
	}

	values = DiscardDuplicate(nil, values...)
	length = len(values)
	if length == 1 {
		if not {
			return s.NotEqual(script, values[0])
		}
		return s.Equal(script, values[0])
	}

	places := make([]string, length)
	for i := range length {
		places[i] = StrPlaceholder
	}
	next := make([]any, 0, 3)
	if not {
		next = append(next, StrNot)
	}
	next = append(next, StrIn, NewSQL(ParcelPrepare(strings.Join(places, StrCommaSpace))))

	result := s.firstNext(script, next...)
	result.Args = append(result.Args, values...)

	return s.add(logic, result)
}

func (s *filter) In(column any, values ...any) Filter {
	return s.in(StrAnd, column, values, false)
}

func (s *filter) inGroup(logic string, columns any, values any, not bool) Filter {
	if columns == nil || values == nil {
		return s
	}

	if lists, ok := columns.([]string); ok {
		if length := len(lists); length == 0 {
			return s
		}
		for key, val := range lists {
			lists[key] = s.get(val)
		}
		columns = NewSQL(ParcelPrepare(strings.Join(lists, StrCommaSpace)))
	}

	fields := any2sql(columns)
	if fields.IsEmpty() {
		return s
	}

	switch value := values.(type) {
	case [][]any:
		length := len(value)
		if length == 0 {
			return s
		}
		count := len(value[0])
		group := make([]string, count)
		for i := range count {
			group[i] = StrPlaceholder
		}
		args := make([]any, 0, length*count)
		lines := make([]string, length)
		place := ParcelPrepare(strings.Join(group, StrCommaSpace))
		for i := range length {
			args = append(args, value[i]...)
			lines[i] = place
		}
		values = NewSQL(ParcelPrepare(strings.Join(lines, StrCommaSpace)), args...)
	case *SQL:
		if value == nil || value.IsEmpty() {
			return s
		}
		values = ParcelSQL(value)
	case Maker:
		if tmp := value.ToSQL(); tmp == nil || tmp.IsEmpty() {
			return s
		} else {
			values = ParcelSQL(tmp)
		}
	default:
		return s
	}

	next := make([]any, 0, 3)
	if not {
		next = append(next, StrNot)
	}
	next = append(next, StrIn, values)

	return s.add(logic, s.firstNext(fields, next...))
}

func (s *filter) InGroup(columns any, values any) Filter {
	return s.inGroup(StrAnd, columns, values, false)
}

func (s *filter) exists(subquery Maker, not bool) Filter {
	if subquery == nil {
		return s
	}

	script := subquery.ToSQL()
	if script == nil || script.IsEmpty() {
		return s
	}

	next := make([]any, 0, 3)
	if not {
		next = append(next, StrNot)
	}
	next = append(next, StrExists, ParcelSQL(script))

	return s.add(StrAnd, JoinSQLSpace(next...))
}

func (s *filter) Exists(subquery Maker) Filter {
	return s.exists(subquery, false)
}

func (s *filter) like(logic string, column any, value any, not bool) Filter {
	if value = filterUsingValue(value); value == nil {
		return s
	}

	if script, ok := column.(string); ok {
		column = s.get(script)
	}
	script := any2sql(column)
	if script.IsEmpty() {
		return s
	}

	lists := make([]any, 0, 3)
	if not {
		lists = append(lists, StrNot)
	}
	lists = append(lists, StrLike)
	args := make([]any, 0, 1)
	if like, ok := value.(string); ok {
		lists = append(lists, StrPlaceholder)
		args = append(args, like)
		result := s.firstNext(script, lists...)
		result.Args = append(result.Args, args...)
		return s.add(logic, result)
	}
	if like := any2sql(value); !like.IsEmpty() {
		lists = append(lists, like)
		return s.add(logic, s.firstNext(script, lists...))
	}
	return s
}

func (s *filter) Like(column any, value any) Filter {
	if value = filterUsingValue(value); value == nil {
		return s
	}
	return s.like(StrAnd, column, value, false)
}

func (s *filter) isNull(column any, not bool) Filter {
	if script, ok := column.(string); ok {
		column = s.get(script)
	}

	script := any2sql(column)
	if script.IsEmpty() {
		return s
	}

	lists := make([]any, 0, 3)
	lists = append(lists, StrIs)
	if not {
		lists = append(lists, StrNot)
	}
	lists = append(lists, StrNull)
	return s.add(StrAnd, s.firstNext(script, lists...))
}

func (s *filter) IsNull(column any) Filter {
	return s.isNull(column, false)
}

func (s *filter) NotEqual(column any, value any, null ...bool) Filter {
	if column == nil {
		return s
	}
	if value == nil {
		if length := len(null); length > 0 && null[length-1] {
			return s.IsNotNull(column)
		}
		return s
	}
	return s.compare(StrAnd, column, StrNotEqual, value)
}

func (s *filter) NotBetween(column any, start any, end any) Filter {
	return s.between(StrAnd, column, start, end, true)
}

func (s *filter) NotIn(column any, values ...any) Filter {
	return s.in(StrAnd, column, values, true)
}

func (s *filter) NotInGroup(columns any, values any) Filter {
	return s.inGroup(StrAnd, columns, values, true)
}

func (s *filter) NotExists(subquery Maker) Filter {
	return s.exists(subquery, true)
}

func (s *filter) NotLike(column any, value any) Filter {
	if value = filterUsingValue(value); value == nil {
		return s
	}
	return s.like(StrAnd, column, value, true)
}

func (s *filter) IsNotNull(column any) Filter {
	return s.isNull(column, true)
}

func (s *filter) AllQuantifier(fc func(q Quantifier)) Filter {
	tmp := &quantifier{
		filter:     s.New(),
		quantifier: StrAll,
	}
	fc(tmp)
	return s.Use(tmp.filter)
}

func (s *filter) AnyQuantifier(fc func(q Quantifier)) Filter {
	tmp := &quantifier{
		filter:     s.New(),
		quantifier: StrAny,
	}
	fc(tmp)
	return s.Use(tmp.filter)
}

func (s *filter) GetReplacer() Replacer {
	return s.replacer
}

func (s *filter) SetReplacer(replacer Replacer) Filter {
	s.replacer = replacer
	return s
}

func (s *filter) compares(column1 any, compare string, column2 any) Filter {
	if column1 == nil || compare == StrEmpty || column2 == nil {
		return s
	}

	if column, ok := column1.(string); ok {
		if column = s.get(column); column == StrEmpty {
			return s
		}
		column1 = column
	}

	if column, ok := column2.(string); ok {
		if column = s.get(column); column == StrEmpty {
			return s
		}
		column2 = column
	}

	prefix, suffix := any2sql(column1), any2sql(column2)
	if prefix == nil || prefix.IsEmpty() || suffix == nil || suffix.IsEmpty() {
		return s
	}

	return s.And(JoinSQLSpace(prefix, compare, suffix))
}

func (s *filter) CompareEqual(column1 any, column2 any) Filter {
	return s.compares(column1, StrEqual, column2)
}

func (s *filter) CompareNotEqual(column1 any, column2 any) Filter {
	return s.compares(column1, StrNotEqual, column2)
}

func (s *filter) CompareMoreThan(column1 any, column2 any) Filter {
	return s.compares(column1, StrMoreThan, column2)
}

func (s *filter) CompareMoreThanEqual(column1 any, column2 any) Filter {
	return s.compares(column1, StrMoreThanEqual, column2)
}

func (s *filter) CompareLessThan(column1 any, column2 any) Filter {
	return s.compares(column1, StrLessThan, column2)
}

func (s *filter) CompareLessThanEqual(column1 any, column2 any) Filter {
	return s.compares(column1, StrLessThanEqual, column2)
}

// Quantifier Implement the filter condition: column {=||<>||>||>=||<||<=} [QUANTIFIER ]( subquery ) .
// QUANTIFIER is usually one of ALL, ANY, SOME ... or EmptyString.
type Quantifier interface {
	GetQuantifier() string

	SetQuantifier(quantifierString string) Quantifier

	Equal(column any, subquery Maker) Quantifier

	NotEqual(column any, subquery Maker) Quantifier

	LessThan(column any, subquery Maker) Quantifier

	LessThanEqual(column any, subquery Maker) Quantifier

	MoreThan(column any, subquery Maker) Quantifier

	MoreThanEqual(column any, subquery Maker) Quantifier
}

type quantifier struct {
	filter Filter

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
func (s *quantifier) build(column any, logic string, subquery Maker) Quantifier {
	if column == nil || logic == StrEmpty || subquery == nil {
		return s
	}
	if script, ok := column.(string); ok {
		if tmp := s.filter.GetReplacer(); tmp != nil {
			column = tmp.Get(script)
		}
	}
	prefix, suffix := any2sql(column), subquery.ToSQL()
	if prefix == nil || prefix.IsEmpty() || suffix == nil || suffix.IsEmpty() {
		return s
	}
	s.filter.And(JoinSQLSpace(prefix, logic, s.quantifier, ParcelSQL(suffix)))
	return s
}

// Equal Implement the filter condition: column = QUANTIFIER ( subquery ) .
func (s *quantifier) Equal(column any, subquery Maker) Quantifier {
	return s.build(column, StrEqual, subquery)
}

// NotEqual Implement the filter condition: column <> QUANTIFIER ( subquery ) .
func (s *quantifier) NotEqual(column any, subquery Maker) Quantifier {
	return s.build(column, StrNotEqual, subquery)
}

// LessThan Implement the filter condition: column < QUANTIFIER ( subquery ) .
func (s *quantifier) LessThan(column any, subquery Maker) Quantifier {
	return s.build(column, StrLessThan, subquery)
}

// LessThanEqual Implement the filter condition: column <= QUANTIFIER ( subquery ) .
func (s *quantifier) LessThanEqual(column any, subquery Maker) Quantifier {
	return s.build(column, StrLessThanEqual, subquery)
}

// MoreThan Implement the filter condition: column > QUANTIFIER ( subquery ) .
func (s *quantifier) MoreThan(column any, subquery Maker) Quantifier {
	return s.build(column, StrMoreThan, subquery)
}

// MoreThanEqual Implement the filter condition: column >= QUANTIFIER ( subquery ) .
func (s *quantifier) MoreThanEqual(column any, subquery Maker) Quantifier {
	return s.build(column, StrMoreThanEqual, subquery)
}

// LikeSearch Implement the filter condition: ( column1 LIKE 'value' OR column2 LIKE 'value' OR column3 LIKE 'value' ... ) .
func LikeSearch(filter Filter, value any, columns ...string) {
	if filter == nil {
		return
	}
	columns = DiscardDuplicate(func(tmp string) bool { return tmp == StrEmpty }, columns...)
	count := len(columns)
	if count == 0 {
		return
	}
	filter.Group(func(g Filter) {
		for _, column := range columns {
			g.OrGroup(func(tmp Filter) { tmp.Like(column, value) })
		}
	})
}
