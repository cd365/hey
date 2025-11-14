// SQL condition filtering

package hey

import (
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cd365/hey/v6/cst"
)

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

	// Clone Copy the current object.
	Clone() Filter

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
	Equal(column any, value any) Filter

	// LessThan Implement conditional filtering: column < value .
	LessThan(column any, value any) Filter

	// LessThanEqual Implement conditional filtering: column <= value .
	LessThanEqual(column any, value any) Filter

	// GreaterThan Implement conditional filtering: column > value.
	GreaterThan(column any, value any) Filter

	// GreaterThanEqual Implement conditional filtering: column >= value .
	GreaterThanEqual(column any, value any) Filter

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
	NotEqual(column any, value any) Filter

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

	// Keyword Implement the filter condition: ( column1 LIKE 'value' OR column2 LIKE 'value' OR column3 LIKE 'value' ... ) .
	Keyword(keyword string, columns ...string) Filter

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

	// CompareGreaterThan Implement conditional filtering: script1 > script2 .
	CompareGreaterThan(column1 any, column2 any) Filter

	// CompareGreaterThanEqual Implement conditional filtering: script1 >= script2 .
	CompareGreaterThanEqual(column1 any, column2 any) Filter

	// CompareLessThan Implement conditional filtering: script1 < script2.
	CompareLessThan(column1 any, column2 any) Filter

	// CompareLessThanEqual Implement conditional filtering: script1 <= script2 .
	CompareLessThanEqual(column1 any, column2 any) Filter

	// ExtractFilter Call Filter using ExtractFilter.
	ExtractFilter(fc func(f ExtractFilter)) Filter

	// TimeFilter Call Filter using TimeFilter.
	TimeFilter(fc func(f TimeFilter)) Filter
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
	return &filter{
		prepare: &strings.Builder{},
	}
}

// F New a Filter.
func F() Filter {
	return newFilter()
}

// poolFilter filter pool.
var poolFilter = &sync.Pool{
	New: func() any {
		return newFilter()
	},
}

func poolGetFilter() Filter {
	return poolFilter.Get().(*filter)
}

func poolPutFilter(f Filter) {
	poolFilter.Put(f.ToEmpty())
}

func (s *filter) ToSQL() *SQL {
	if s.IsEmpty() {
		return NewSQL(cst.Empty)
	}

	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)

	if s.not {
		b.WriteString(cst.NOT)
		b.WriteString(cst.Space)
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

func (s *filter) Clone() Filter {
	clone := &filter{
		prepare:  &strings.Builder{},
		replacer: s.replacer,
		args:     make([]any, len(s.args)),
		num:      s.num,
		not:      s.not,
	}
	clone.prepare.WriteString(s.prepare.String())
	copy(clone.args, s.args)
	return clone
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

	s.prepare.WriteString(JoinString(cst.Space, logic, cst.Space, script.Prepare))
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

func firstNext(first any, next ...any) []any {
	result := make([]any, 0, len(next)+1)
	result = append(result, first)
	result = append(result, next...)
	return result
}

func (s *filter) firstNext(first *SQL, next ...any) *SQL {
	if first == nil || first.IsEmpty() {
		return NewEmptySQL()
	}
	return JoinSQLSpace(firstNext(first, next...)...)
}

func (s *filter) And(script *SQL) Filter {
	return s.add(cst.AND, script)
}

func (s *filter) Or(script *SQL) Filter {
	return s.add(cst.OR, script)
}

func (s *filter) Group(group func(g Filter)) Filter {
	return s.addGroup(cst.AND, group)
}

func (s *filter) OrGroup(group func(g Filter)) Filter {
	return s.addGroup(cst.OR, group)
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

	first := AnyToSQL(column)
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
		next = append(next, cst.Placeholder)
		args = []any{value}
	}

	result := s.firstNext(first, next...)
	if args != nil {
		result.Args = append(result.Args, args...)
	}

	return s.add(logic, result)
}

func (s *filter) Equal(column any, value any) Filter {
	return s.compare(cst.AND, column, cst.Equal, value)
}

func (s *filter) LessThan(column any, value any) Filter {
	return s.compare(cst.AND, column, cst.LessThan, value)
}

func (s *filter) LessThanEqual(column any, value any) Filter {
	return s.compare(cst.AND, column, cst.LessThanEqual, value)
}

func (s *filter) GreaterThan(column any, value any) Filter {
	return s.compare(cst.AND, column, cst.GreaterThan, value)
}

func (s *filter) GreaterThanEqual(column any, value any) Filter {
	return s.compare(cst.AND, column, cst.GreaterThanEqual, value)
}

func (s *filter) between(logic string, column any, start any, end any, not bool) Filter {
	if column == nil {
		return s
	}
	start, end = filterUsingValue(start), filterUsingValue(end)
	if start == nil {
		if end == nil {
			return s
		} else {
			return s.LessThanEqual(column, end)
		}
	} else {
		if end == nil {
			return s.GreaterThanEqual(column, start)
		}
	}

	if script, ok := column.(string); ok {
		column = s.get(script)
	}

	first := AnyToSQL(column)
	if first.IsEmpty() {
		return s
	}

	next := make([]any, 0, 5)
	if not {
		next = append(next, cst.NOT)
	}
	next = append(next, cst.BETWEEN)
	args := make([]any, 0, 2)
	if value, ok := start.(*SQL); ok {
		next = append(next, ParcelSQL(value.Clone()))
	} else {
		next = append(next, cst.Placeholder)
		args = append(args, start)
	}
	next = append(next, cst.AND)
	if value, ok := end.(*SQL); ok {
		next = append(next, ParcelSQL(value.Clone()))
	} else {
		next = append(next, cst.Placeholder)
		args = append(args, end)
	}

	result := s.firstNext(first, next...)
	if len(args) > 0 {
		result.Args = append(result.Args, args...)
	}

	return s.add(logic, result)
}

func (s *filter) Between(column any, start any, end any) Filter {
	return s.between(cst.AND, column, start, end, false)
}

func (s *filter) in(logic string, column any, values []any, not bool) Filter {
	if column == nil || values == nil {
		return s
	}

	if script, ok := column.(string); ok {
		column = s.get(script)
	}

	script := AnyToSQL(column)
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
					latest := subquery.Clone()
					latest.Prepare = ParcelPrepare(latest.Prepare)
					lists := make([]any, 0, 3)
					if not {
						lists = append(lists, cst.NOT)
					}
					lists = append(lists, cst.IN, latest)
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
		places[i] = cst.Placeholder
	}
	next := make([]any, 0, 3)
	if not {
		next = append(next, cst.NOT)
	}
	next = append(next, cst.IN, NewSQL(ParcelPrepare(strings.Join(places, cst.CommaSpace))))

	result := s.firstNext(script, next...)
	result.Args = append(result.Args, values...)

	return s.add(logic, result)
}

func (s *filter) In(column any, values ...any) Filter {
	return s.in(cst.AND, column, values, false)
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
		columns = NewSQL(ParcelPrepare(strings.Join(lists, cst.CommaSpace)))
	}

	fields := AnyToSQL(columns)
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
			group[i] = cst.Placeholder
		}
		args := make([]any, 0, length*count)
		lines := make([]string, length)
		place := ParcelPrepare(strings.Join(group, cst.CommaSpace))
		for i := range length {
			args = append(args, value[i]...)
			lines[i] = place
		}
		values = NewSQL(ParcelPrepare(strings.Join(lines, cst.CommaSpace)), args...)
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
		next = append(next, cst.NOT)
	}
	next = append(next, cst.IN, values)

	return s.add(logic, s.firstNext(fields, next...))
}

func (s *filter) InGroup(columns any, values any) Filter {
	return s.inGroup(cst.AND, columns, values, false)
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
		next = append(next, cst.NOT)
	}
	next = append(next, cst.EXISTS, ParcelSQL(script))

	return s.add(cst.AND, JoinSQLSpace(next...))
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
	script := AnyToSQL(column)
	if script.IsEmpty() {
		return s
	}

	lists := make([]any, 0, 3)
	if not {
		lists = append(lists, cst.NOT)
	}
	lists = append(lists, cst.LIKE)
	if like, ok := value.(string); ok && like != cst.Empty {
		lists = append(lists, cst.Placeholder)
		result := s.firstNext(script, lists...)
		result.Args = append(result.Args, like)
		return s.add(logic, result)
	}
	if like := AnyToSQL(value); !like.IsEmpty() {
		lists = append(lists, like)
		return s.add(logic, s.firstNext(script, lists...))
	}
	return s
}

func (s *filter) Like(column any, value any) Filter {
	if value = filterUsingValue(value); value == nil {
		return s
	}
	return s.like(cst.AND, column, value, false)
}

func (s *filter) isNull(column any, not bool) Filter {
	if script, ok := column.(string); ok {
		column = s.get(script)
	}

	script := AnyToSQL(column)
	if script.IsEmpty() {
		return s
	}

	lists := make([]any, 0, 3)
	lists = append(lists, cst.IS)
	if not {
		lists = append(lists, cst.NOT)
	}
	lists = append(lists, cst.NULL)
	return s.add(cst.AND, s.firstNext(script, lists...))
}

func (s *filter) IsNull(column any) Filter {
	return s.isNull(column, false)
}

func (s *filter) NotEqual(column any, value any) Filter {
	return s.compare(cst.AND, column, cst.NotEqual, value)
}

func (s *filter) NotBetween(column any, start any, end any) Filter {
	return s.between(cst.AND, column, start, end, true)
}

func (s *filter) NotIn(column any, values ...any) Filter {
	return s.in(cst.AND, column, values, true)
}

func (s *filter) NotInGroup(columns any, values any) Filter {
	return s.inGroup(cst.AND, columns, values, true)
}

func (s *filter) NotExists(subquery Maker) Filter {
	return s.exists(subquery, true)
}

func (s *filter) NotLike(column any, value any) Filter {
	if value = filterUsingValue(value); value == nil {
		return s
	}
	return s.like(cst.AND, column, value, true)
}

func (s *filter) IsNotNull(column any) Filter {
	return s.isNull(column, true)
}

// Keyword Implement the filter condition: ( column1 LIKE 'value' OR column2 LIKE 'value' OR column3 LIKE 'value' ... ) .
func (s *filter) Keyword(value string, columns ...string) Filter {
	if value == cst.Empty {
		return s
	}
	length := len(columns)
	if length == 0 {
		return s
	}
	fields := make([]string, 0, length)
	fieldsMap := make(map[string]*struct{})
	for _, column := range columns {
		if column == cst.Empty {
			continue
		}
		_, ok := fieldsMap[column]
		if ok {
			continue
		}
		fields = append(fields, column)
		fieldsMap[column] = nil
	}
	length = len(fields)
	if length == 0 {
		return s
	}
	s.Group(func(g Filter) {
		for _, column := range fields {
			g.OrGroup(func(tmp Filter) { tmp.Like(column, value) })
		}
	})
	return s
}

func (s *filter) AllQuantifier(fc func(q Quantifier)) Filter {
	tmp := &quantifier{
		filter:     s.New(),
		quantifier: cst.ALL,
	}
	fc(tmp)
	return s.Use(tmp.filter)
}

func (s *filter) AnyQuantifier(fc func(q Quantifier)) Filter {
	tmp := &quantifier{
		filter:     s.New(),
		quantifier: cst.ANY,
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
	if column1 == nil || compare == cst.Empty || column2 == nil {
		return s
	}

	if column, ok := column1.(string); ok {
		if column = s.get(column); column == cst.Empty {
			return s
		}
		column1 = column
	}

	if column, ok := column2.(string); ok {
		if column = s.get(column); column == cst.Empty {
			return s
		}
		column2 = column
	}

	prefix, suffix := AnyToSQL(column1), AnyToSQL(column2)
	if prefix == nil || prefix.IsEmpty() || suffix == nil || suffix.IsEmpty() {
		return s
	}

	return s.And(JoinSQLSpace(prefix, compare, suffix))
}

func (s *filter) CompareEqual(column1 any, column2 any) Filter {
	return s.compares(column1, cst.Equal, column2)
}

func (s *filter) CompareNotEqual(column1 any, column2 any) Filter {
	return s.compares(column1, cst.NotEqual, column2)
}

func (s *filter) CompareGreaterThan(column1 any, column2 any) Filter {
	return s.compares(column1, cst.GreaterThan, column2)
}

func (s *filter) CompareGreaterThanEqual(column1 any, column2 any) Filter {
	return s.compares(column1, cst.GreaterThanEqual, column2)
}

func (s *filter) CompareLessThan(column1 any, column2 any) Filter {
	return s.compares(column1, cst.LessThan, column2)
}

func (s *filter) CompareLessThanEqual(column1 any, column2 any) Filter {
	return s.compares(column1, cst.LessThanEqual, column2)
}

func (s *filter) ExtractFilter(fc func(f ExtractFilter)) Filter {
	tmp := poolGetExtractFilter(s)
	defer poolPutExtractFilter(tmp)
	fc(tmp)
	return s
}

func (s *filter) TimeFilter(fc func(f TimeFilter)) Filter {
	tmp := newTimeFilter(s, time.Now().Unix())
	fc(tmp)
	return s
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

	GreaterThan(column any, subquery Maker) Quantifier

	GreaterThanEqual(column any, subquery Maker) Quantifier
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
	if column == nil || logic == cst.Empty || subquery == nil {
		return s
	}
	if script, ok := column.(string); ok {
		if tmp := s.filter.GetReplacer(); tmp != nil {
			column = tmp.Get(script)
		}
	}
	prefix, suffix := AnyToSQL(column), subquery.ToSQL()
	if prefix == nil || prefix.IsEmpty() || suffix == nil || suffix.IsEmpty() {
		return s
	}
	s.filter.And(JoinSQLSpace(prefix, logic, s.quantifier, ParcelSQL(suffix)))
	return s
}

// Equal Implement the filter condition: column = QUANTIFIER ( subquery ) .
func (s *quantifier) Equal(column any, subquery Maker) Quantifier {
	return s.build(column, cst.Equal, subquery)
}

// NotEqual Implement the filter condition: column <> QUANTIFIER ( subquery ) .
func (s *quantifier) NotEqual(column any, subquery Maker) Quantifier {
	return s.build(column, cst.NotEqual, subquery)
}

// LessThan Implement the filter condition: column < QUANTIFIER ( subquery ) .
func (s *quantifier) LessThan(column any, subquery Maker) Quantifier {
	return s.build(column, cst.LessThan, subquery)
}

// LessThanEqual Implement the filter condition: column <= QUANTIFIER ( subquery ) .
func (s *quantifier) LessThanEqual(column any, subquery Maker) Quantifier {
	return s.build(column, cst.LessThanEqual, subquery)
}

// GreaterThan Implement the filter condition: column > QUANTIFIER ( subquery ) .
func (s *quantifier) GreaterThan(column any, subquery Maker) Quantifier {
	return s.build(column, cst.GreaterThan, subquery)
}

// GreaterThanEqual Implement the filter condition: column >= QUANTIFIER ( subquery ) .
func (s *quantifier) GreaterThanEqual(column any, subquery Maker) Quantifier {
	return s.build(column, cst.GreaterThanEqual, subquery)
}

// ExtractFilter Extract the available condition values for the Filter.
// Use a delimiter string to separate multiple element values, with ',' as the default.
type ExtractFilter interface {
	// Delimiter Custom a delimiter string is used to split the target string.
	Delimiter(delimiter string) ExtractFilter

	// BetweenInt Use a delimiter string to separate multiple element values, with ',' as the default.
	// column BETWEEN int-min AND int-max, column >= int-min, column <= int-max
	BetweenInt(column string, value *string, verifies ...func(minimum int, maximum int) bool) ExtractFilter

	// BetweenInt64 Use a delimiter string to separate multiple element values, with ',' as the default.
	// column BETWEEN int64-min AND int64-max, column >= int64-min, column <= int64-max
	BetweenInt64(column string, value *string, verifies ...func(minimum int64, maximum int64) bool) ExtractFilter

	// BetweenFloat64 Use a delimiter string to separate multiple element values, with ',' as the default.
	// column BETWEEN float64-min AND float64-max, column >= float64-min, column <= float64-max
	BetweenFloat64(column string, value *string, verifies ...func(minimum float64, maximum float64) bool) ExtractFilter

	// BetweenString Use a delimiter string to separate multiple element values, with ',' as the default.
	// column BETWEEN string-min AND string-max, column >= string-min, column <= string-max
	BetweenString(column string, value *string, verifies ...func(minimum string, maximum string) bool) ExtractFilter

	// InInt Use a delimiter string to separate multiple element values, with ',' as the default.
	// column IN ( int-value1, int-value2, int-value3 ... )
	InInt(column string, value *string, verify func(index int, value int) bool, keepOnly func(i []int) []int) ExtractFilter

	// InInt64 Use a delimiter string to separate multiple element values, with ',' as the default.
	// column IN ( int64-value1, int64-value2, int64-value3 ... )
	InInt64(column string, value *string, verify func(index int, value int64) bool, keepOnly func(i []int64) []int64) ExtractFilter

	// InString Use a delimiter string to separate multiple element values, with ',' as the default.
	// column IN ( string-value1, string-value2, string-value3 ... )
	InString(column string, value *string, verify func(index int, value string) bool, keepOnly func(i []string) []string) ExtractFilter

	// InIntDirect A simplified method for convenient use.
	InIntDirect(column string, value *string) ExtractFilter

	// InInt64Direct A simplified method for convenient use.
	InInt64Direct(column string, value *string) ExtractFilter

	// InStringDirect A simplified method for convenient use.
	InStringDirect(column string, value *string) ExtractFilter

	// InIntVerify A simplified method for convenient use.
	InIntVerify(column string, value *string, verify func(index int, value int) bool) ExtractFilter

	// InInt64Verify A simplified method for convenient use.
	InInt64Verify(column string, value *string, verify func(index int, value int64) bool) ExtractFilter

	// InStringVerify A simplified method for convenient use.
	InStringVerify(column string, value *string, verify func(index int, value string) bool) ExtractFilter

	// LikeSearch Fuzzy search for a single keyword across multiple column values, ( column1 LIKE '%value%' OR column2 LIKE '%value%' OR column3 LIKE '%value%' ... )
	LikeSearch(value *string, columns ...string) ExtractFilter
}

type extractFilter struct {
	filter    Filter
	delimiter string
}

var poolExtractFilter = &sync.Pool{
	New: func() any {
		return &extractFilter{}
	},
}

func poolGetExtractFilter(filter Filter) *extractFilter {
	result := poolExtractFilter.Get().(*extractFilter)
	result.filter = filter
	result.delimiter = cst.Comma
	return result
}

func poolPutExtractFilter(b *extractFilter) {
	b.filter = nil
	b.delimiter = cst.Empty
	poolExtractFilter.Put(b)
}

func (s *extractFilter) Delimiter(delimiter string) ExtractFilter {
	s.delimiter = delimiter
	return s
}

func (s *extractFilter) between(column string, value *string, parse func(values []string) ([]any, bool)) *extractFilter {
	if column == cst.Empty || value == nil || *value == cst.Empty || parse == nil {
		return s
	}
	values := strings.Split(*value, s.delimiter)
	if len(values) != 2 {
		return s
	}
	between, ok := parse([]string{strings.TrimSpace(values[0]), strings.TrimSpace(values[1])})
	if !ok || len(between) != 2 {
		return s
	}
	if between[0] != nil {
		if between[1] != nil {
			s.filter.Between(column, between[0], between[1])
		} else {
			s.filter.GreaterThanEqual(column, between[0])
		}
	} else {
		if between[1] != nil {
			s.filter.LessThanEqual(column, between[1])
		}
	}
	return s
}

func (s *extractFilter) BetweenInt(column string, value *string, verifies ...func(minimum int, maximum int) bool) ExtractFilter {
	var verify func(minimum int, maximum int) bool
	for i := len(verifies) - 1; i >= 0; i-- {
		if verifies[i] != nil {
			verify = verifies[i]
			break
		}
	}
	return s.between(column, value, func(values []string) ([]any, bool) {
		result := make([]any, 2)
		minimum, maximum := 0, 0
		for key, val := range values {
			if key > 1 {
				return nil, false
			}
			tmp, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				switch key {
				case 0:
					result[0] = nil
				case 1:
					result[1] = nil
				}
			} else {
				switch key {
				case 0:
					minimum = int(tmp)
					result[0] = minimum
				case 1:
					maximum = int(tmp)
					result[1] = maximum
				}
			}
		}
		if result[0] != nil && result[1] != nil && maximum < minimum {
			return nil, false
		}
		if verify != nil {
			ok := verify(minimum, maximum)
			if !ok {
				return nil, false
			}
		}
		return result, true
	})
}

func (s *extractFilter) BetweenInt64(column string, value *string, verifies ...func(minimum int64, maximum int64) bool) ExtractFilter {
	var verify func(minimum int64, maximum int64) bool
	for i := len(verifies) - 1; i >= 0; i-- {
		if verifies[i] != nil {
			verify = verifies[i]
			break
		}
	}
	return s.between(column, value, func(values []string) ([]any, bool) {
		result := make([]any, 2)
		minimum, maximum := int64(0), int64(0)
		for key, val := range values {
			if key > 1 {
				return nil, false
			}
			tmp, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				switch key {
				case 0:
					result[0] = nil
				case 1:
					result[1] = nil
				}
			} else {
				switch key {
				case 0:
					minimum = tmp
					result[0] = minimum
				case 1:
					maximum = tmp
					result[1] = maximum
				}
			}
		}
		if result[0] != nil && result[1] != nil && maximum < minimum {
			return nil, false
		}
		if verify != nil {
			ok := verify(minimum, maximum)
			if !ok {
				return nil, false
			}
		}
		return result, true
	})
}

func (s *extractFilter) BetweenFloat64(column string, value *string, verifies ...func(minimum float64, maximum float64) bool) ExtractFilter {
	var verify func(minimum float64, maximum float64) bool
	for i := len(verifies) - 1; i >= 0; i-- {
		if verifies[i] != nil {
			verify = verifies[i]
			break
		}
	}
	return s.between(column, value, func(values []string) ([]any, bool) {
		result := make([]any, 2)
		minimum, maximum := float64(0), float64(0)
		for key, val := range values {
			if key > 1 {
				return nil, false
			}
			tmp, err := strconv.ParseFloat(val, 64)
			if err != nil {
				switch key {
				case 0:
					result[0] = nil
				case 1:
					result[1] = nil
				}
			} else {
				switch key {
				case 0:
					minimum = tmp
					result[0] = minimum
				case 1:
					maximum = tmp
					result[1] = maximum
				}
			}
		}
		if result[0] != nil && result[1] != nil && maximum < minimum {
			return nil, false
		}
		if verify != nil {
			ok := verify(minimum, maximum)
			if !ok {
				return nil, false
			}
		}
		return result, true
	})
}

func (s *extractFilter) BetweenString(column string, value *string, verifies ...func(minimum string, maximum string) bool) ExtractFilter {
	var verify func(minimum string, maximum string) bool
	for i := len(verifies) - 1; i >= 0; i-- {
		if verifies[i] != nil {
			verify = verifies[i]
			break
		}
	}
	return s.between(column, value, func(values []string) ([]any, bool) {
		if len(values) != 2 {
			return nil, false
		}
		if verify != nil {
			ok := verify(values[0], values[1])
			if !ok {
				return nil, false
			}
		}
		return []any{values[0], values[1]}, true
	})
}

// KeepOnlyFirst Only use the first element value.
func KeepOnlyFirst[T any](i []T) []T {
	length := len(i)
	if length == 0 {
		return i
	}
	return []T{i[0]}
}

// KeepOnlyLast Only use the value of the last element.
func KeepOnlyLast[T any](i []T) []T {
	length := len(i)
	if length == 0 {
		return i
	}
	return []T{i[length-1]}
}

func (s *extractFilter) in(column string, value *string, parse func(i int, v string) (any, bool), keepOnly any) *extractFilter {
	if column == cst.Empty || value == nil || *value == cst.Empty || parse == nil {
		return s
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == cst.Empty {
		return s
	}
	splits := strings.Split(trimmed, s.delimiter)
	length := len(splits)
	if length == 0 {
		return s
	}
	result := DiscardDuplicate(nil, splits...)
	length = len(result)
	if keepOnly == nil {
		values := make([]any, length)
		for k, v := range result {
			val, ok := parse(k, v)
			if !ok {
				return s
			}
			values[k] = val
		}
		s.filter.In(column, values...)
		return s
	}
	switch fc := keepOnly.(type) {
	case func(i []int) []int:
		values := make([]int, length)
		for k, v := range result {
			val, ok := parse(k, v)
			if !ok {
				return s
			}
			tmp, ok := val.(int)
			if !ok {
				return s
			}
			values[k] = tmp
		}
		if fc == nil {
			s.filter.In(column, AnyAny(values)...)
		} else {
			s.filter.In(column, AnyAny(fc(values))...)
		}
	case func(i []int64) []int64:
		values := make([]int64, length)
		for k, v := range result {
			val, ok := parse(k, v)
			if !ok {
				return s
			}
			tmp, ok := val.(int64)
			if !ok {
				return s
			}
			values[k] = tmp
		}
		if fc == nil {
			s.filter.In(column, AnyAny(values)...)
		} else {
			s.filter.In(column, AnyAny(fc(values))...)
		}
	case func(i []string) []string:
		values := make([]string, length)
		for k, v := range result {
			val, ok := parse(k, v)
			if !ok {
				return s
			}
			tmp, ok := val.(string)
			if !ok {
				return s
			}
			values[k] = tmp
		}
		if fc == nil {
			s.filter.In(column, AnyAny(values)...)
		} else {
			s.filter.In(column, AnyAny(fc(values))...)
		}
	default:

	}
	return s
}

func (s *extractFilter) InInt(column string, value *string, verify func(index int, value int) bool, keepOnly func(i []int) []int) ExtractFilter {
	return s.in(column, value, func(i int, v string) (any, bool) {
		val, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, false
		}
		result := int(val)
		if verify != nil {
			ok := verify(i, result)
			if !ok {
				return nil, false
			}
		}
		return result, true
	}, keepOnly)
}

func (s *extractFilter) InInt64(column string, value *string, verify func(index int, value int64) bool, keepOnly func(i []int64) []int64) ExtractFilter {
	return s.in(column, value, func(i int, v string) (any, bool) {
		val, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, false
		}
		if verify != nil {
			ok := verify(i, val)
			if !ok {
				return nil, false
			}
		}
		return val, true
	}, keepOnly)
}

func (s *extractFilter) InString(column string, value *string, verify func(index int, value string) bool, keepOnly func(i []string) []string) ExtractFilter {
	return s.in(column, value, func(i int, v string) (any, bool) {
		if verify != nil {
			ok := verify(i, v)
			if !ok {
				return nil, false
			}
		}
		return v, true
	}, keepOnly)
}

func (s *extractFilter) InIntDirect(column string, value *string) ExtractFilter {
	return s.InInt(column, value, nil, nil)
}

func (s *extractFilter) InInt64Direct(column string, value *string) ExtractFilter {
	return s.InInt64(column, value, nil, nil)
}

func (s *extractFilter) InStringDirect(column string, value *string) ExtractFilter {
	return s.InString(column, value, nil, nil)
}

func (s *extractFilter) InIntVerify(column string, value *string, verify func(index int, value int) bool) ExtractFilter {
	return s.InInt(column, value, verify, nil)
}

func (s *extractFilter) InInt64Verify(column string, value *string, verify func(index int, value int64) bool) ExtractFilter {
	return s.InInt64(column, value, verify, nil)
}

func (s *extractFilter) InStringVerify(column string, value *string, verify func(index int, value string) bool) ExtractFilter {
	return s.InString(column, value, verify, nil)
}

func (s *extractFilter) LikeSearch(value *string, columns ...string) ExtractFilter {
	if value == nil {
		return nil
	}
	search := strings.TrimSpace(*value)
	if search == cst.Empty {
		return s
	}
	length := len(columns)
	if length == 0 {
		return s
	}
	search = JoinString(cst.PercentSign, search, cst.PercentSign)
	s.filter.Keyword(search, columns...)
	return s
}

// TimeFilter Commonly used timestamp range filtering conditions.
type TimeFilter interface {
	Timestamp(timestamp int64) TimeFilter

	TimeLocation(location *time.Location) TimeFilter

	LastMinutes(column string, minutes int) TimeFilter

	LastHours(column string, hours int) TimeFilter

	Today(column string) TimeFilter

	Yesterday(column string) TimeFilter

	LastDays(column string, days int) TimeFilter

	ThisMonth(column string) TimeFilter

	LastMonth(column string) TimeFilter

	LastMonths(column string, months int) TimeFilter

	ThisQuarter(column string) TimeFilter

	LastQuarter(column string) TimeFilter

	LastQuarters(column string, quarters int) TimeFilter

	ThisYear(column string) TimeFilter

	LastYear(column string) TimeFilter

	LastYears(column string, years int) TimeFilter
}

type timeFilter struct {
	filter    Filter
	location  *time.Location
	timestamp int64
}

func newTimeFilter(filter Filter, timestamp int64) TimeFilter {
	return &timeFilter{
		filter:    filter,
		timestamp: timestamp,
		location:  time.Local,
	}
}

func (s *timeFilter) monthStartAt(timestamp int64) int64 {
	t := time.Unix(timestamp, 0).In(s.location)
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).Unix()
}

func (s *timeFilter) quarterStartAt(timestamp int64) int64 {
	t := time.Unix(timestamp, 0).In(s.location)
	year := t.Year()
	month := t.Month()
	var startMonth time.Month
	switch {
	case month <= 3:
		startMonth = 1
	case month <= 6:
		startMonth = 4
	case month <= 9:
		startMonth = 7
	default:
		startMonth = 10
	}
	return time.Date(year, startMonth, 1, 0, 0, 0, 0, t.Location()).Unix()
}

func (s *timeFilter) yearStartAt(timestamp int64) int64 {
	t := time.Unix(timestamp, 0).In(s.location)
	return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location()).Unix()
}

func (s *timeFilter) Timestamp(timestamp int64) TimeFilter {
	s.timestamp = timestamp
	return s
}

func (s *timeFilter) TimeLocation(location *time.Location) TimeFilter {
	s.location = location
	return s
}

func (s *timeFilter) LastMinutes(column string, minutes int) TimeFilter {
	if minutes <= 0 {
		return s
	}
	s.filter.Between(column, s.timestamp-int64(minutes)*60, s.timestamp)
	return s
}

func (s *timeFilter) LastHours(column string, hours int) TimeFilter {
	if hours <= 0 {
		return s
	}
	s.filter.Between(column, s.timestamp-int64(hours)*3600, s.timestamp)
	return s
}

func (s *timeFilter) Today(column string) TimeFilter {
	at := time.Unix(s.timestamp, 0).In(s.location)
	todayStartAt := time.Date(at.Year(), at.Month(), at.Day(), 0, 0, 0, 0, at.Location()).Unix()
	s.filter.Between(column, todayStartAt, s.timestamp)
	return s
}

func (s *timeFilter) Yesterday(column string) TimeFilter {
	at := time.Unix(s.timestamp, 0).In(s.location)
	todayStartAt := time.Date(at.Year(), at.Month(), at.Day(), 0, 0, 0, 0, at.Location()).Unix()
	s.filter.Between(column, todayStartAt-86400, todayStartAt-1)
	return s
}

func (s *timeFilter) LastDays(column string, days int) TimeFilter {
	if days <= 0 {
		return s
	}
	s.filter.Between(column, s.timestamp-int64(days)*86400, s.timestamp)
	return s
}

func (s *timeFilter) ThisMonth(column string) TimeFilter {
	s.filter.Between(column, s.monthStartAt(s.timestamp), s.timestamp)
	return s
}

func (s *timeFilter) LastMonth(column string) TimeFilter {
	thisMonthStartAt := s.monthStartAt(s.timestamp)
	lastMonthStartAt := time.Unix(thisMonthStartAt, 0).In(s.location).AddDate(0, -1, 0).Unix()
	s.filter.Between(column, lastMonthStartAt, thisMonthStartAt-1)
	return s
}

func (s *timeFilter) LastMonths(column string, months int) TimeFilter {
	if months <= 0 {
		return s
	}
	thisMonthStartAt := s.monthStartAt(s.timestamp)
	lastMonthStartAt := time.Unix(thisMonthStartAt, 0).In(s.location).AddDate(0, -months+1, 0).Unix()
	s.filter.Between(column, lastMonthStartAt, s.timestamp)
	return s
}

func (s *timeFilter) ThisQuarter(column string) TimeFilter {
	thisQuarterStartAt := s.quarterStartAt(s.timestamp)
	s.filter.Between(column, thisQuarterStartAt, s.timestamp)
	return s
}

func (s *timeFilter) LastQuarter(column string) TimeFilter {
	thisQuarterStartAt := s.quarterStartAt(s.timestamp)
	lastQuarterStartAt := s.quarterStartAt(thisQuarterStartAt - 1)
	s.filter.Between(column, lastQuarterStartAt, thisQuarterStartAt-1)
	return s
}

func (s *timeFilter) LastQuarters(column string, quarters int) TimeFilter {
	if quarters <= 0 {
		return s
	}
	startAt := int64(0)
	for i := range quarters {
		if i == 0 {
			startAt = s.timestamp
		}
		startAt = s.quarterStartAt(startAt)
		startAt--
	}
	s.filter.Between(column, startAt+1, s.timestamp)
	return s
}

func (s *timeFilter) ThisYear(column string) TimeFilter {
	s.filter.Between(column, s.yearStartAt(s.timestamp), s.timestamp)
	return s
}

func (s *timeFilter) LastYear(column string) TimeFilter {
	endAt := s.yearStartAt(s.timestamp) - 1
	startAt := s.yearStartAt(endAt)
	s.filter.Between(column, startAt, endAt)
	return s
}

func (s *timeFilter) LastYears(column string, years int) TimeFilter {
	if years <= 0 {
		return s
	}
	startAt := time.Unix(s.timestamp, 0).AddDate(-years+1, 0, 0).In(s.location).Unix()
	s.filter.Between(column, startAt, s.timestamp)
	return s
}
