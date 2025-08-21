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
	Group(group func(f Filter)) Filter

	// OrGroup Add a new condition group, which is connected by the `OR` logical operator by default.
	OrGroup(group func(f Filter)) Filter

	// Use Implement import a set of conditional filter objects into the current object.
	Use(filters ...Filter) Filter

	// New Create a new conditional filter object based on a set of conditional filter objects.
	New(filters ...Filter) Filter

	// Equal Implement conditional filtering: column = value .
	Equal(script any, value any, null ...bool) Filter

	// LessThan Implement conditional filtering: column < value .
	LessThan(script any, value any) Filter

	// LessThanEqual Implement conditional filtering: column <= value .
	LessThanEqual(script any, value any) Filter

	// MoreThan Implement conditional filtering: column > value.
	MoreThan(script any, value any) Filter

	// MoreThanEqual Implement conditional filtering: column >= value .
	MoreThanEqual(script any, value any) Filter

	// Between Implement conditional filtering: column BETWEEN value1 AND value2 .
	Between(script any, start any, end any) Filter

	// In Implement conditional filtering: column IN ( value1, value2, value3... ) .
	In(script any, values ...any) Filter

	// InQuery Implement conditional filtering: column IN (subquery).
	InQuery(script any, subquery Maker) Filter

	// Exists Implement conditional filtering: EXISTS (subquery) .
	Exists(subquery Maker) Filter

	// Like Implement conditional filtering: column LIKE value.
	Like(script any, value any) Filter

	// IsNull Implement conditional filtering: column IS NULL .
	IsNull(script any) Filter

	// InGroup Implement conditional filtering: ( column1, column2, column3... ) IN ( ( value1, value2, value3... ), ( value21, value22, value23... )... ) .
	InGroup(script any, values ...[]any) Filter

	// InGroupQuery Implement conditional filtering: ( column1, column2, column3... ) IN ( subquery ) .
	InGroupQuery(script any, subquery Maker) Filter

	// NotEqual Implement conditional filtering: column <> value .
	NotEqual(script any, value any, null ...bool) Filter

	// NotBetween Implement conditional filtering: column NOT BETWEEN value1 AND value2 .
	NotBetween(script any, start any, end any) Filter

	// NotIn Implement conditional filtering: column NOT IN ( value1, value2, value3... ) .
	NotIn(script any, values ...any) Filter

	// NotInGroup Implement conditional filtering: ( column1, column2, column3... ) NOT IN ( ( value1, value2, value3... ), ( value21, value22, value23... )... ) .
	NotInGroup(script any, values ...[]any) Filter

	// NotExists Implement conditional filtering: NOT EXISTS (subquery) .
	NotExists(subquery Maker) Filter

	// NotLike Implement conditional filtering: column NOT LIKE value .
	NotLike(script any, value any) Filter

	// IsNotNull Implement conditional filtering: column IS NOT NULL .
	IsNotNull(script any) Filter

	// AllQuantifier Implement conditional filtering: column {=||<>||>||>=||<||<=} ALL ( subquery ) .
	AllQuantifier(fc func(q Quantifier)) Filter

	// AnyQuantifier Implement conditional filtering: column {=||<>||>||>=||<||<=} ANY ( subquery ) .
	AnyQuantifier(fc func(q Quantifier)) Filter

	// GetReplacer For get *Way.
	GetReplacer() Replacer

	// SetReplacer For set *Way.
	SetReplacer(replacer Replacer) Filter

	// Compare Implement conditional filtering: script1 {=||<>||>||>=||<||<=} script2 .
	Compare(script1 any, compare string, script2 any) Filter

	// CompareEqual Implement conditional filtering: script1 = script2 .
	CompareEqual(script1 any, script2 any) Filter

	// CompareNotEqual Implement conditional filtering: script1 <> script2 .
	CompareNotEqual(script1 any, script2 any) Filter

	// CompareMoreThan Implement conditional filtering: script1 > script2 .
	CompareMoreThan(script1 any, script2 any) Filter

	// CompareMoreThanEqual Implement conditional filtering: script1 >= script2 .
	CompareMoreThanEqual(script1 any, script2 any) Filter

	// CompareLessThan Implement conditional filtering: script1 < script2.
	CompareLessThan(script1 any, script2 any) Filter

	// CompareLessThanEqual Implement conditional filtering: script1 <= script2 .
	CompareLessThanEqual(script1 any, script2 any) Filter

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
		b.WriteString(StrLeftSmallBracket)
		b.WriteString(StrSpace)
		b.WriteString(s.prepare.String())
		b.WriteString(StrSpace)
		b.WriteString(StrRightSmallBracket)
	} else {
		b.WriteString(s.prepare.String())
	}
	return NewSQL(b.String(), s.args[:]...)
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
	if s.num == 0 {
		s.prepare.WriteString(script.Prepare)
		s.args = make([]any, len(script.Args))
		copy(s.args, script.Args)
		s.num++
		return s
	}
	s.prepare.WriteString(Strings(StrSpace, logic, StrSpace, script.Prepare))
	s.args = append(s.args, script.Args...)
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

func (s *filter) Group(group func(f Filter)) Filter {
	return s.addGroup(StrAnd, group)
}

func (s *filter) OrGroup(group func(f Filter)) Filter {
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

func (s *filter) compare(logic string, script any, compare string, value any) Filter {
	if script == nil || value == nil {
		return s
	}
	if value = filterUsingValue(value); value == nil {
		return s
	}
	if column, ok := script.(string); ok {
		script = s.get(column)
	}
	first := any2sql(script)
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

func (s *filter) Equal(script any, value any, null ...bool) Filter {
	if script == nil {
		return s
	}
	if value == nil {
		if length := len(null); length > 0 && null[length-1] {
			return s.IsNull(script)
		}
		return s
	}
	return s.compare(StrAnd, script, StrEqual, value)
}

func (s *filter) LessThan(script any, value any) Filter {
	return s.compare(StrAnd, script, StrLessThan, value)
}

func (s *filter) LessThanEqual(script any, value any) Filter {
	return s.compare(StrAnd, script, StrLessThanEqual, value)
}

func (s *filter) MoreThan(script any, value any) Filter {
	return s.compare(StrAnd, script, StrMoreThan, value)
}

func (s *filter) MoreThanEqual(script any, value any) Filter {
	return s.compare(StrAnd, script, StrMoreThanEqual, value)
}

func (s *filter) between(logic string, script any, start any, end any, not bool) Filter {
	start, end = filterUsingValue(start), filterUsingValue(end)
	if script == nil || start == nil || end == nil {
		return s
	}
	if column, ok := script.(string); ok {
		script = s.get(column)
	}
	first := any2sql(script)
	if first.IsEmpty() {
		return s
	}
	next := make([]any, 0, 5)
	if not {
		next = append(next, StrNot)
	}
	next = append(next, StrBetween, StrPlaceholder, StrAnd, StrPlaceholder)
	result := s.firstNext(first, next...)
	result.Args = append(result.Args, start, end)
	return s.add(logic, result)
}

func (s *filter) Between(script any, start any, end any) Filter {
	return s.between(StrAnd, script, start, end, false)
}

func (s *filter) in(logic string, script any, values []any, not bool) Filter {
	if script == nil || values == nil {
		return s
	}
	values = inArgs(values...)
	length := len(values)
	if length == 0 {
		return s
	}
	values = DiscardDuplicate(nil, values...)
	length = len(values)
	if length == 1 {
		if not {
			return s.NotEqual(script, values[0])
		} else {
			return s.Equal(script, values[0])
		}
	}
	if column, ok := script.(string); ok {
		script = s.get(column)
	}
	first := any2sql(script)
	if first.IsEmpty() {
		return s
	}
	places := make([]string, length)
	for i := range length {
		places[i] = StrPlaceholder
	}
	next := make([]any, 0, 5)
	if not {
		next = append(next, StrNot)
	}
	next = append(next, StrIn, StrLeftSmallBracket, strings.Join(places, StrCommaSpace), StrRightSmallBracket)
	result := s.firstNext(first, next...)
	result.Args = append(result.Args, values...)
	return s.add(logic, result)
}

func (s *filter) In(script any, values ...any) Filter {
	return s.in(StrAnd, script, values, false)
}

func (s *filter) InQuery(script any, subquery Maker) Filter {
	if script == nil || subquery == nil {
		return s
	}
	if column, ok := script.(string); ok {
		script = s.get(column)
	}
	first := any2sql(script)
	if first.IsEmpty() {
		return s
	}
	value := subquery.ToSQL()
	if value == nil || value.IsEmpty() {
		return s
	}
	return s.add(StrAnd, s.firstNext(first, StrIn, ParcelSQL(value)))
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

func (s *filter) Like(script any, value any) Filter {
	if value = filterUsingValue(value); value == nil {
		return s
	}
	if column, ok := script.(string); ok {
		script = s.get(column)
	}
	first := any2sql(script)
	if first.IsEmpty() {
		return s
	}
	if like, ok := value.(string); ok {
		result := s.firstNext(first, StrLike, StrPlaceholder)
		result.Args = append(result.Args, like)
		return s.add(StrAnd, result)
	}
	if like := any2sql(value); !like.IsEmpty() {
		return s.add(StrAnd, s.firstNext(first, StrLike, like))
	}
	return s
}

func (s *filter) IsNull(script any) Filter {
	if column, ok := script.(string); ok {
		script = s.get(column)
	}
	first := any2sql(script)
	if first.IsEmpty() {
		return s
	}
	return s.add(StrAnd, s.firstNext(first, StrIs, StrNull))
}

func (s *filter) inGroup(logic string, script any, value any, not bool) Filter {
	if script == nil || value == nil {
		return s
	}
	if columns, ok := script.([]string); ok {
		if length := len(columns); length == 0 {
			return s
		}
		for index, column := range columns {
			columns[index] = s.get(column)
		}
		script = ParcelSQL(NewSQL(strings.Join(columns, StrCommaSpace)))
	}
	first := any2sql(script)
	if first.IsEmpty() {
		return s
	}
	switch values := value.(type) {
	case [][]any:
		length := len(values)
		if length == 0 {
			return s
		}
		count := len(values[0])
		group := make([]string, count)
		for i := range count {
			group[i] = StrPlaceholder
		}
		line := Strings(StrLeftSmallBracket, StrSpace, strings.Join(group, StrCommaSpace), StrSpace, StrRightSmallBracket)
		args := make([]any, 0, length*count)
		lines := make([]string, length)
		for i := range length {
			args = append(args, values[i]...)
			lines[i] = line
		}
		value = NewSQL(ParcelPrepare(strings.Join(lines, StrCommaSpace)), args...)
	case *SQL:
		if values == nil || values.IsEmpty() {
			return s
		}
		value = ParcelSQL(values)
	case Maker:
		if tmp := values.ToSQL(); tmp == nil || tmp.IsEmpty() {
			return s
		} else {
			value = ParcelSQL(tmp)
		}
	default:
		return s
	}
	next := make([]any, 0, 3)
	if not {
		next = append(next, StrNot)
	}
	next = append(next, StrIn, value)
	return s.add(logic, s.firstNext(first, next...))
}

func (s *filter) InGroup(script any, values ...[]any) Filter {
	return s.inGroup(StrAnd, script, values, false)
}

func (s *filter) InGroupQuery(script any, subquery Maker) Filter {
	return s.inGroup(StrAnd, script, subquery, false)
}

func (s *filter) NotEqual(script any, value any, null ...bool) Filter {
	if script == nil {
		return s
	}
	if value == nil {
		if length := len(null); length > 0 && null[length-1] {
			return s.IsNotNull(script)
		}
		return s
	}
	return s.compare(StrAnd, script, StrNotEqual, value)
}

func (s *filter) NotBetween(script any, start any, end any) Filter {
	return s.between(StrAnd, script, start, end, true)
}

func (s *filter) NotIn(script any, values ...any) Filter {
	return s.in(StrAnd, script, values, true)
}

func (s *filter) NotInGroup(script any, values ...[]any) Filter {
	return s.inGroup(StrAnd, script, values, true)
}

func (s *filter) NotExists(subquery Maker) Filter {
	return s.exists(subquery, true)
}

func (s *filter) NotLike(script any, value any) Filter {
	if value = filterUsingValue(value); value == nil {
		return s
	}
	if column, ok := script.(string); ok {
		script = s.get(column)
	}
	first := any2sql(script)
	if first.IsEmpty() {
		return s
	}
	if like, ok := value.(string); ok {
		result := s.firstNext(first, StrNot, StrLike, StrPlaceholder)
		result.Args = append(result.Args, like)
		return s.add(StrAnd, result)
	}
	if like := any2sql(value); !like.IsEmpty() {
		return s.add(StrAnd, s.firstNext(first, StrNot, StrLike, like))
	}
	return s
}

func (s *filter) IsNotNull(script any) Filter {
	if column, ok := script.(string); ok {
		script = s.get(column)
	}
	first := any2sql(script)
	if first.IsEmpty() {
		return s
	}
	return s.add(StrAnd, s.firstNext(first, StrIs, StrNot, StrNull))
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

func (s *filter) Compare(script1 any, compare string, script2 any) Filter {
	if script1 == nil || compare == StrEmpty || script2 == nil {
		return s
	}
	if column, ok := script1.(string); ok {
		if column = s.get(column); column == StrEmpty {
			return s
		}
		script1 = column
	}
	if column, ok := script2.(string); ok {
		if column = s.get(column); column == StrEmpty {
			return s
		}
		script2 = column
	}
	prefix, suffix := any2sql(script1), any2sql(script2)
	if prefix == nil || prefix.IsEmpty() || suffix == nil || suffix.IsEmpty() {
		return s
	}
	return s.And(JoinSQLSpace(prefix, compare, suffix))
}

func (s *filter) CompareEqual(script1 any, script2 any) Filter {
	return s.Compare(script1, StrEqual, script2)
}

func (s *filter) CompareNotEqual(script1 any, script2 any) Filter {
	return s.Compare(script1, StrNotEqual, script2)
}

func (s *filter) CompareMoreThan(script1 any, script2 any) Filter {
	return s.Compare(script1, StrMoreThan, script2)
}

func (s *filter) CompareMoreThanEqual(script1 any, script2 any) Filter {
	return s.Compare(script1, StrMoreThanEqual, script2)
}

func (s *filter) CompareLessThan(script1 any, script2 any) Filter {
	return s.Compare(script1, StrLessThan, script2)
}

func (s *filter) CompareLessThanEqual(script1 any, script2 any) Filter {
	return s.Compare(script1, StrLessThanEqual, script2)
}

// Quantifier Implement the filter condition: column {=||<>||>||>=||<||<=} [QUANTIFIER ]( subquery ) .
// QUANTIFIER is usually one of ALL, ANY, SOME ... or EmptyString.
type Quantifier interface {
	GetQuantifier() string

	SetQuantifier(quantifierString string) Quantifier

	Equal(script any, subquery Maker) Quantifier

	NotEqual(script any, subquery Maker) Quantifier

	LessThan(script any, subquery Maker) Quantifier

	LessThanEqual(script any, subquery Maker) Quantifier

	MoreThan(script any, subquery Maker) Quantifier

	MoreThanEqual(script any, subquery Maker) Quantifier
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
func (s *quantifier) build(script any, logic string, subquery Maker) Quantifier {
	if script == nil || logic == StrEmpty || subquery == nil {
		return s
	}
	if column, ok := script.(string); ok {
		if tmp := s.filter.GetReplacer(); tmp != nil {
			script = tmp.Get(column)
		}
	}
	prefix, suffix := any2sql(script), subquery.ToSQL()
	if prefix == nil || prefix.IsEmpty() || suffix == nil || suffix.IsEmpty() {
		return s
	}
	s.filter.And(JoinSQLSpace(prefix, logic, s.quantifier, ParcelSQL(suffix)))
	return s
}

// Equal Implement the filter condition: column = QUANTIFIER ( subquery ) .
func (s *quantifier) Equal(script any, subquery Maker) Quantifier {
	return s.build(script, StrEqual, subquery)
}

// NotEqual Implement the filter condition: column <> QUANTIFIER ( subquery ) .
func (s *quantifier) NotEqual(script any, subquery Maker) Quantifier {
	return s.build(script, StrNotEqual, subquery)
}

// LessThan Implement the filter condition: column < QUANTIFIER ( subquery ) .
func (s *quantifier) LessThan(script any, subquery Maker) Quantifier {
	return s.build(script, StrLessThan, subquery)
}

// LessThanEqual Implement the filter condition: column <= QUANTIFIER ( subquery ) .
func (s *quantifier) LessThanEqual(script any, subquery Maker) Quantifier {
	return s.build(script, StrLessThanEqual, subquery)
}

// MoreThan Implement the filter condition: column > QUANTIFIER ( subquery ) .
func (s *quantifier) MoreThan(script any, subquery Maker) Quantifier {
	return s.build(script, StrMoreThan, subquery)
}

// MoreThanEqual Implement the filter condition: column >= QUANTIFIER ( subquery ) .
func (s *quantifier) MoreThanEqual(script any, subquery Maker) Quantifier {
	return s.build(script, StrMoreThanEqual, subquery)
}
