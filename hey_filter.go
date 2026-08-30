// SQL condition filtering

package hey

import (
	"database/sql/driver"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cd365/hey/v8/cst"
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
		return v
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
	case []uint8: // Do not consider byte slices
		return []any{args[0]}
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
		if args[0] == nil {
			return args
		}
		rt := reflect.TypeOf(args[0])
		if rt.Kind() != reflect.Slice {
			return args
		}
		// expand slice members when there is only one slice type value.
		rv := reflect.ValueOf(args[0])
		count := rv.Len()
		result := make([]any, 0, count)
		for i := 0; i < count; i++ {
			result = append(result, rv.Index(i).Interface())
		}
		return result
	}
}

// filterUsingValue unwraps pointers and interfaces while preserving SQL-aware values.
func filterUsingValue(value any) any {
	if value == nil {
		return nil
	}
	v := reflect.ValueOf(value)
	for {
		kind := v.Kind()
		if (kind == reflect.Interface || kind == reflect.Pointer) && v.IsNil() {
			return nil
		}
		current := v.Interface()
		// Preserve SQL builders and database values before dereferencing their pointer receivers.
		if _, ok := current.(Maker); ok {
			return current
		}
		if _, ok := current.(driver.Valuer); ok {
			return current
		}
		if kind != reflect.Interface && kind != reflect.Pointer {
			return current
		}
		v = v.Elem()
	}
}

// filterUsingBoundaryValue normalizes a range boundary and rejects SQL NULL values.
func filterUsingBoundaryValue(value any) any {
	value = filterUsingValue(value)
	if filterNilValue(value) {
		return nil
	}
	return value
}

// filterNilValue recognizes both deeply nested nil pointers and Valuers that produce SQL NULL.
func filterNilValue(value any) bool {
	value = filterUsingValue(value)
	if value == nil {
		return true
	}
	if _, ok := value.(Maker); ok {
		return false
	}
	if valuer, ok := value.(driver.Valuer); ok {
		current, err := valuer.Value()
		// Preserve conversion errors so database/sql can report them during execution.
		if err != nil {
			return false
		}
		return filterPhysicalNilValue(current)
	}
	return filterPhysicalNilValue(value)
}

// filterPhysicalNilValue unwraps pointer and interface layers without invoking user code.
func filterPhysicalNilValue(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	for {
		switch v.Kind() {
		case reflect.Interface, reflect.Pointer:
			if v.IsNil() {
				return true
			}
			v = v.Elem()
		case reflect.Chan, reflect.Func, reflect.Map, reflect.Slice:
			return v.IsNil()
		default:
			return false
		}
	}
}

// filterUsingString accepts built-in and named string values after unwrapping pointers and interfaces.
func filterUsingString(value any) (string, bool) {
	value = filterUsingValue(value)
	if value == nil {
		return cst.Empty, false
	}
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.String {
		return cst.Empty, false
	}
	return v.String(), true
}

// filterUsingColumn quotes string-like column names while preserving explicit SQL makers.
func filterUsingColumn(where Filter, column any) any {
	if column == nil {
		return nil
	}
	if _, ok := column.(Maker); ok {
		return column
	}
	if value, ok := filterUsingString(column); ok {
		return where.V().Replace(value)
	}
	return column
}

// Filter Implement SQL statement conditional filtering (general conditional filtering).
type Filter interface {
	Maker

	V

	W

	// ToEmpty Clear existing filter criteria.
	ToEmpty()

	// Clone Copy the current object.
	Clone() Filter

	// Num Number of conditions used.
	Num() int

	// IsEmpty Are the filter criteria empty?
	IsEmpty() bool

	// Not Negate the result of the current conditional filter object. Multiple negations are allowed.
	Not() Filter

	// And Use logical operator `AND` to combine custom conditions.
	// Please verify the validity of the parameter values and be wary of SQL injection attacks.
	And(maker Maker) Filter

	// Or Use logical operator `OR` to combine custom conditions.
	// Please verify the validity of the parameter values and be wary of SQL injection attacks.
	Or(maker Maker) Filter

	// Group Add a new condition group, which is connected by the `AND` logical operator by default.
	Group(group func(g Filter)) Filter

	// OrGroup Add a new condition group, which is connected by the `OR` logical operator by default.
	OrGroup(group func(g Filter)) Filter

	// New Create a new conditional filter object.
	New() Filter

	// Use Implement import a set of conditional filter objects into the current object.
	Use(values ...Maker) Filter

	// NewUse Create a new conditional filter object, and import a set of conditional filter objects into the new object.
	NewUse(values ...Maker) Filter

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
	// Please do not use []uint8 as the value of values.
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

	// GroupLike Implement the filter condition: ( column1 LIKE 'value' OR column2 LIKE 'value' OR column3 LIKE 'value' ... ) .
	GroupLike(value string, columns ...string) Filter

	// GroupLikeSearch Implement the filter condition: ( column1 LIKE '%value%' OR column2 LIKE '%value%' OR column3 LIKE '%value%' ... ) .
	GroupLikeSearch(value string, columns ...string) Filter

	// CompareAll Implement conditional filtering: column {=||<>||>||>=||<||<=} ALL ( subquery ) .
	CompareAll(compare func(ck CompareKey)) Filter

	// CompareAny Implement conditional filtering: column {=||<>||>||>=||<||<=} ANY ( subquery ) .
	CompareAny(compare func(ck CompareKey)) Filter

	// CompareEqual Implement conditional filtering: script1 = script2 .
	CompareEqual(column1 any, column2 any) Filter

	// CompareNotEqual Implement conditional filtering: script1 <> script2 .
	CompareNotEqual(column1 any, column2 any) Filter

	// CompareGreaterThan Implement conditional filtering: script1 > script2 .
	CompareGreaterThan(column1 any, column2 any) Filter

	// CompareGreaterThanEqual Implement conditional filtering: script1 >= script2 .
	CompareGreaterThanEqual(column1 any, column2 any) Filter

	// CompareLessThan Implement conditional filtering: script1 < script2 .
	CompareLessThan(column1 any, column2 any) Filter

	// CompareLessThanEqual Implement conditional filtering: script1 <= script2 .
	CompareLessThanEqual(column1 any, column2 any) Filter

	// You might be thinking why there is no method with the prefix `Or` defined to implement methods like OrEqual, OrLike, OrIn ...
	// 1. Considering that, most of the OR is not used frequently in the business development process.
	// 2. If the business really needs to use it, you can use the OrGroup method: OrGroup(func(g Filter) { g.Equal("column", 1) }) .

	// NewStringFilter Create a StringFilter.
	NewStringFilter(filters ...Filter) StringFilter

	// NewTimeFilter Create a TimeFilter.
	NewTimeFilter(filters ...Filter) TimeFilter
}

func newSQLFilter(way *Way) Filter {
	if way == nil {
		panic(nilPointer)
	}
	result := newFilter()
	result.W(way)
	return result
}

// filter Implementing interface Filter.
type filter struct {
	way *Way

	prepare *strings.Builder

	args []any

	num int

	not bool
}

// newFilter New a Filter.
func newFilter() *filter {
	return &filter{
		prepare: &strings.Builder{},
	}
}

func (s *filter) V() *Way {
	return s.way
}

func (s *filter) W(way *Way) {
	if way != nil {
		s.way = way
	}
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

	// NOT must always target a parenthesized expression, even when it contains one condition.
	if s.not || s.num > 1 {
		b.WriteString(cst.LeftParenthesis)
		b.WriteString(cst.Space)
		b.WriteString(s.prepare.String())
		b.WriteString(cst.Space)
		b.WriteString(cst.RightParenthesis)
	} else {
		b.WriteString(s.prepare.String())
	}

	args := append([]any(nil), s.args...)
	return NewSQL(b.String(), args...)
}

func (s *filter) toEmpty() {
	s.not = false
	s.num = 0
	s.prepare.Reset()
	s.args = nil
}

func (s *filter) ToEmpty() {
	s.toEmpty()
}

func (s *filter) Clone() Filter {
	clone := &filter{
		prepare: &strings.Builder{},
		way:     s.way,
		args:    make([]any, len(s.args)),
		num:     s.num,
		not:     s.not,
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

func (s *filter) add(logic string, maker Maker) *filter {
	if logic == cst.Empty || maker == nil {
		return s
	}
	script := AnyToSQL(maker)
	if script.IsEmpty() {
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

func (s *filter) addCustom(logic string, maker Maker) *filter {
	if maker == nil {
		return s
	}
	script := AnyToSQL(maker)
	if script.IsEmpty() {
		return s
	}
	// Treat each custom maker as one logical operand without mutating caller-owned SQL.
	script = script.Clone()
	script.Prepare = ParcelPrepare(strings.TrimSpace(script.Prepare))
	return s.add(logic, script)
}

func (s *filter) addGroup(logic string, group func(g Filter)) *filter {
	if group == nil {
		return s
	}

	tmp := s.New()

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

func (s *filter) And(maker Maker) Filter {
	return s.addCustom(cst.AND, maker)
}

func (s *filter) Or(maker Maker) Filter {
	return s.addCustom(cst.OR, maker)
}

func (s *filter) Group(group func(g Filter)) Filter {
	return s.addGroup(cst.AND, group)
}

func (s *filter) OrGroup(group func(g Filter)) Filter {
	return s.addGroup(cst.OR, group)
}

func (s *filter) New() Filter {
	return s.way.cfg.NewSQLFilter(s.way)
}

func (s *filter) Use(values ...Maker) Filter {
	group := s.New()
	for _, value := range values {
		// A Filter already owns its logical boundary; adding it directly avoids redundant wrapping.
		if _, ok := value.(Filter); ok {
			if current, ok := group.(*filter); ok {
				current.add(cst.AND, value)
				continue
			}
		}
		group.And(value)
	}
	// The combined group is already parenthesized when required by its condition count.
	return s.add(cst.AND, group.ToSQL())
}

func (s *filter) NewUse(values ...Maker) Filter {
	return s.New().Use(values...)
}

func (s *filter) get(key string) string {
	return s.way.Replace(key)
}

func (s *filter) compare(logic string, column any, compare string, value any) Filter {
	if column == nil || value == nil {
		return s
	}

	column = filterUsingColumn(s, column)
	first := AnyToSQL(column)
	if first.IsEmpty() {
		return s
	}

	// Normalize pointers without stripping Maker or driver.Valuer implementations.
	value = filterUsingValue(value)
	if filterNilValue(value) {
		return s
	}

	next := make([]any, 0, 2)
	next = append(next, compare)
	args := ([]any)(nil)
	switch v := value.(type) {
	case Maker:
		tmp := ParcelSQL(AnyToSQL(v))
		if tmp.IsEmpty() {
			return s
		}
		next = append(next, tmp)
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
	start, end = filterUsingBoundaryValue(start), filterUsingBoundaryValue(end)
	if start == nil {
		if end == nil {
			return s
		}
		if not {
			return s.GreaterThan(column, end)
		}
		return s.LessThanEqual(column, end)
	}

	if end == nil {
		if not {
			return s.LessThan(column, start)
		}
		return s.GreaterThanEqual(column, start)
	}

	column = filterUsingColumn(s, column)
	first := AnyToSQL(column)
	if first.IsEmpty() {
		return s
	}

	next := make([]any, 0, 5)
	if not {
		next = append(next, cst.NOT)
	}
	next = append(next, cst.BETWEEN)
	node := func(i any) bool {
		if i == nil {
			return false
		}
		if value, ok := i.(Maker); ok {
			tmp := ParcelSQL(AnyToSQL(value))
			if tmp.IsEmpty() {
				return false
			}
			next = append(next, tmp)
		} else {
			next = append(next, NewSQL(cst.Placeholder, i))
		}
		return true
	}
	if !node(start) {
		return s
	}
	next = append(next, cst.AND)
	if !node(end) {
		return s
	}

	result := s.firstNext(first, next...)
	return s.add(logic, result)
}

func (s *filter) Between(column any, start any, end any) Filter {
	return s.between(cst.AND, column, start, end, false)
}

func (s *filter) in(logic string, column any, values []any, not bool) Filter {
	if column == nil || values == nil {
		return s
	}

	column = filterUsingColumn(s, column)
	script := AnyToSQL(column)
	if script.IsEmpty() {
		return s
	}

	values = inArgs(values...)
	length := len(values)
	if length == 0 {
		return s
	}

	values = DiscardDuplicateAny(filterNilValue, values...)
	length = len(values)
	if length == 0 {
		return s
	}

	if length == 1 {
		if value, ok := values[0].(Maker); ok {
			latest := ParcelSQL(AnyToSQL(value))
			if latest.IsEmpty() {
				return s
			}
			lists := make([]any, 0, 3)
			if not {
				lists = append(lists, cst.NOT)
			}
			lists = append(lists, cst.IN, latest)
			return s.add(logic, s.firstNext(script, lists...))
		}
		if not {
			return s.NotEqual(script, values[0])
		}
		return s.Equal(script, values[0])
	}

	// A subquery must be the only IN value; it cannot be mixed with bound scalar values.
	for _, value := range values {
		if _, ok := value.(Maker); ok {
			return s
		}
	}

	places := make([]string, length)
	for i := 0; i < length; i++ {
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

	length := 0
	if lists, ok := columns.([]string); ok {
		length = len(lists)
		if length == 0 {
			return s
		}
		lists = append([]string(nil), lists...)
		for key, val := range lists {
			val = s.get(val)
			// Reject the complete tuple instead of emitting an invalid empty identifier.
			if strings.TrimSpace(val) == cst.Empty {
				return s
			}
			lists[key] = val
		}
		columns = NewSQL(ParcelPrepare(strings.Join(lists, cst.CommaSpace)))
	}

	fields := AnyToSQL(columns)
	if fields.IsEmpty() {
		return s
	}

	switch value := values.(type) {
	case [][]any:
		total := len(value)
		if total == 0 {
			return s
		}
		count := len(value[0])
		if count <= 1 {
			// At least two columns.
			return s
		}
		if length > 0 && length != count {
			return s
		}
		for i := 1; i < total; i++ {
			if count != len(value[i]) {
				return s
			}
		}
		if not {
			rows := make([][]any, 0, total)
			for _, row := range value {
				hasNil := false
				for _, item := range row {
					if filterNilValue(item) {
						hasNil = true
						break
					}
				}
				if !hasNil {
					rows = append(rows, row)
				}
			}
			value = rows
			total = len(value)
			if total == 0 {
				return s
			}
		}
		group := make([]string, count)
		for i := 0; i < count; i++ {
			group[i] = cst.Placeholder
		}
		args := make([]any, 0, total*count)
		lines := make([]string, total)
		place := ParcelPrepare(strings.Join(group, cst.CommaSpace))
		for i := 0; i < total; i++ {
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
		tmp := ParcelSQL(AnyToSQL(value))
		if tmp.IsEmpty() {
			return s
		}
		values = tmp
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

	script := AnyToSQL(subquery)
	if script.IsEmpty() {
		return s
	}

	capacity := 1 << 2
	if not {
		capacity++
	}
	next := make([]any, 0, capacity)
	if not {
		next = append(next, cst.NOT)
	}
	next = append(next, cst.EXISTS, cst.LeftParenthesis, script, cst.RightParenthesis)

	return s.add(cst.AND, JoinSQLSpace(next...))
}

func (s *filter) Exists(subquery Maker) Filter {
	return s.exists(subquery, false)
}

func (s *filter) like(logic string, column any, value any, not bool) Filter {
	if value = filterUsingBoundaryValue(value); value == nil {
		return s
	}

	column = filterUsingColumn(s, column)
	script := AnyToSQL(column)
	if script.IsEmpty() {
		return s
	}

	lists := make([]any, 0, 3)
	if not {
		lists = append(lists, cst.NOT)
	}
	lists = append(lists, cst.LIKE)
	// String-like values are bound as parameters, while makers remain explicit SQL expressions.
	if _, ok := value.(Maker); !ok {
		if like, ok := filterUsingString(value); ok {
			if like == cst.Empty {
				return s
			}
			lists = append(lists, cst.Placeholder)
			result := s.firstNext(script, lists...)
			result.Args = append(result.Args, like)
			return s.add(logic, result)
		}
		// Valuers are scalar parameters; keep the original object for database/sql conversion.
		if _, ok := value.(driver.Valuer); ok {
			lists = append(lists, cst.Placeholder)
			result := s.firstNext(script, lists...)
			result.Args = append(result.Args, value)
			return s.add(logic, result)
		}
	}
	if like := AnyToSQL(value); !like.IsEmpty() {
		lists = append(lists, like)
		return s.add(logic, s.firstNext(script, lists...))
	}
	return s
}

func (s *filter) Like(column any, value any) Filter {
	if value = filterUsingBoundaryValue(value); value == nil {
		return s
	}
	return s.like(cst.AND, column, value, false)
}

func (s *filter) isNull(column any, not bool) Filter {
	column = filterUsingColumn(s, column)
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
	if value = filterUsingBoundaryValue(value); value == nil {
		return s
	}
	return s.like(cst.AND, column, value, true)
}

func (s *filter) IsNotNull(column any) Filter {
	return s.isNull(column, true)
}

func (s *filter) GroupLike(value string, columns ...string) Filter {
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

func (s *filter) GroupLikeSearch(value string, columns ...string) Filter {
	if value == cst.Empty {
		return s
	}
	return s.GroupLike(JoinString(cst.PercentSign, value, cst.PercentSign), columns...)
}

func (s *filter) newCompareKey(group Filter) CompareKey {
	return s.way.cfg.NewCompareKey(group)
}

func (s *filter) CompareAll(compare func(ck CompareKey)) Filter {
	if compare != nil {
		group := s.New()
		compare(s.newCompareKey(group).SetKey(cst.ALL))
		return s.Use(group)
	}
	return s
}

func (s *filter) CompareAny(compare func(ck CompareKey)) Filter {
	if compare != nil {
		group := s.New()
		compare(s.newCompareKey(group).SetKey(cst.ANY))
		return s.Use(group)
	}
	return s
}

func (s *filter) compares(column1 any, compare string, column2 any) Filter {
	if column1 == nil || compare == cst.Empty || column2 == nil {
		return s
	}

	column1 = filterUsingColumn(s, column1)
	prefix := AnyToSQL(column1)
	if prefix == nil || prefix.IsEmpty() {
		return s
	}

	var suffix *SQL
	column2 = filterUsingColumn(s, column2)
	switch value := column2.(type) {
	case string:
		suffix = AnyToSQL(value)
	case *SQL:
		suffix = ParcelSQL(value)
		if suffix.IsEmpty() {
			return s
		}
	case Maker:
		suffix = ParcelSQL(AnyToSQL(value))
		if suffix.IsEmpty() {
			return s
		}
	default:
		suffix = AnyToSQL(value)
	}

	if suffix == nil || suffix.IsEmpty() {
		return s
	}

	// The comparison is already one complete operand and does not need custom-maker wrapping.
	return s.add(cst.AND, JoinSQLSpace(prefix, compare, suffix))
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

func (s *filter) NewStringFilter(filters ...Filter) StringFilter {
	inputs := s.New()
	length := len(filters)
	for i := range length {
		inputs.Use(filters[i])
	}
	if inputs.IsEmpty() {
		return s.way.NewStringFilter(s)
	}
	return s.way.NewStringFilter(inputs)
}

func (s *filter) NewTimeFilter(filters ...Filter) TimeFilter {
	inputs := s.New()
	length := len(filters)
	for i := range length {
		inputs.Use(filters[i])
	}
	if inputs.IsEmpty() {
		return s.way.NewTimeFilter(s)
	}
	return s.way.NewTimeFilter(inputs)
}

// CompareKey Implement the filter condition: column {=||<>||>||>=||<||<=} [KEY ]( subquery ) .
// KEY is usually one of ALL, ANY, SOME ... or EmptyString.
type CompareKey interface {
	// GetKey Get key value.
	GetKey() string

	// SetKey Set key value.
	SetKey(key string) CompareKey

	// Equal Implement the filter condition: column = KEY ( subquery ) .
	Equal(column any, subquery Maker) CompareKey

	// NotEqual Implement the filter condition: column <> KEY ( subquery ) .
	NotEqual(column any, subquery Maker) CompareKey

	// LessThan Implement the filter condition: column < KEY ( subquery ) .
	LessThan(column any, subquery Maker) CompareKey

	// LessThanEqual Implement the filter condition: column <= KEY ( subquery ) .
	LessThanEqual(column any, subquery Maker) CompareKey

	// GreaterThan Implement the filter condition: column > KEY ( subquery ) .
	GreaterThan(column any, subquery Maker) CompareKey

	// GreaterThanEqual Implement the filter condition: column >= KEY ( subquery ) .
	GreaterThanEqual(column any, subquery Maker) CompareKey
}

type compareKey struct {
	filter Filter

	key string // The value is usually one of ALL, ANY, SOME.
}

func newCompareKey(filter Filter) CompareKey {
	if filter == nil {
		panic(nilPointer)
	}
	return &compareKey{
		filter: filter,
	}
}

func (s *compareKey) GetKey() string {
	return s.key
}

func (s *compareKey) SetKey(key string) CompareKey {
	s.key = key
	return s
}

// build Add SQL filter statement.
func (s *compareKey) build(column any, logic string, subquery Maker) CompareKey {
	if column == nil || logic == cst.Empty || subquery == nil {
		return s
	}
	column = filterUsingColumn(s.filter, column)
	prefix, suffix := AnyToSQL(column), ParcelSQL(AnyToSQL(subquery))
	if prefix == nil || prefix.IsEmpty() || suffix == nil || suffix.IsEmpty() {
		return s
	}
	script := JoinSQLSpace(prefix, logic, s.key, suffix)
	// Add the complete comparison directly when the concrete filter implementation is available.
	if where, ok := s.filter.(*filter); ok {
		where.add(cst.AND, script)
	} else {
		s.filter.And(script)
	}
	return s
}

func (s *compareKey) Equal(column any, subquery Maker) CompareKey {
	return s.build(column, cst.Equal, subquery)
}

func (s *compareKey) NotEqual(column any, subquery Maker) CompareKey {
	return s.build(column, cst.NotEqual, subquery)
}

func (s *compareKey) LessThan(column any, subquery Maker) CompareKey {
	return s.build(column, cst.LessThan, subquery)
}

func (s *compareKey) LessThanEqual(column any, subquery Maker) CompareKey {
	return s.build(column, cst.LessThanEqual, subquery)
}

func (s *compareKey) GreaterThan(column any, subquery Maker) CompareKey {
	return s.build(column, cst.GreaterThan, subquery)
}

func (s *compareKey) GreaterThanEqual(column any, subquery Maker) CompareKey {
	return s.build(column, cst.GreaterThanEqual, subquery)
}

// StringFilter Extract the available condition values for the Filter.
// Use a delimiter string to separate multiple element values, with ',' as the default.
type StringFilter interface {
	// GetDelimiter Get the string delimiter character.
	GetDelimiter() string

	// SetDelimiter Custom a delimiter string is used to split the target string.
	SetDelimiter(delimiter string) StringFilter

	// Between Use a delimiter string to separate multiple element values, with ',' as the default.
	Between(column string, value *string, handle func(values []string) []any) StringFilter

	// IntBetween Use a delimiter string to separate multiple element values, with ',' as the default.
	// column BETWEEN int-min AND int-max, column >= int-min, column <= int-max
	IntBetween(column string, value *string) StringFilter

	// Int64Between Use a delimiter string to separate multiple element values, with ',' as the default.
	// column BETWEEN int64-min AND int64-max, column >= int64-min, column <= int64-max
	Int64Between(column string, value *string) StringFilter

	// Float64Between Use a delimiter string to separate multiple element values, with ',' as the default.
	// column BETWEEN float64-min AND float64-max, column >= float64-min, column <= float64-max
	Float64Between(column string, value *string) StringFilter

	// StringBetween Use a delimiter string to separate multiple element values, with ',' as the default.
	// column BETWEEN string-min AND string-max, column >= string-min, column <= string-max
	StringBetween(column string, value *string) StringFilter

	// In Use a delimiter string to separate multiple element values, with ',' as the default.
	In(column string, value *string, handle func(values []string) []any) StringFilter

	// IntIn Use a delimiter string to separate multiple element values, with ',' as the default.
	// column IN ( int-value1, int-value2, int-value3 ... )
	IntIn(column string, value *string) StringFilter

	// Int64In Use a delimiter string to separate multiple element values, with ',' as the default.
	// column IN ( int64-value1, int64-value2, int64-value3 ... )
	Int64In(column string, value *string) StringFilter

	// StringIn Use a delimiter string to separate multiple element values, with ',' as the default.
	// column IN ( string-value1, string-value2, string-value3 ... )
	StringIn(column string, value *string) StringFilter
}

type stringFilter struct {
	delimiter string
	filter    Filter
}

func newStringFilter(filter Filter) StringFilter {
	if filter == nil {
		panic(nilPointer)
	}
	return &stringFilter{
		delimiter: cst.Comma,
		filter:    filter,
	}
}

func (s *stringFilter) GetDelimiter() string {
	return s.delimiter
}

func (s *stringFilter) SetDelimiter(delimiter string) StringFilter {
	s.delimiter = delimiter
	return s
}

func string2any(kind reflect.Kind, value string) (any, error) {
	switch kind {
	case reflect.String:
		return value, nil
	case reflect.Int:
		val, err := strconv.ParseInt(value, 10, strconv.IntSize)
		if err != nil {
			return nil, err
		}
		return int(val), nil
	case reflect.Int64:
		val, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, err
		}
		return val, nil
	case reflect.Float64:
		val, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, err
		}
		return val, nil
	default:
		return nil, fmt.Errorf("unsupported kind: %s", kind.String())
	}
}

func (s *stringFilter) string2any(category reflect.Kind) func(values []string) []any {
	return func(values []string) []any {
		length := len(values)
		result := make([]any, 0, length)
		for i := 0; i < length; i++ {
			val, err := string2any(category, values[i])
			if err != nil {
				continue
			}
			result = append(result, val)
		}
		return result
	}
}

func (s *stringFilter) split(column string, value *string, handle func(values []string) []any) []any {
	if column == cst.Empty || value == nil || *value == cst.Empty || s.delimiter == cst.Empty || handle == nil {
		return nil
	}
	values := strings.Split(*value, s.delimiter)
	length := len(values)
	if length == 0 {
		return nil
	}
	return handle(values)
}

func (s *stringFilter) between(column string, value *string, handle func(values []string) []any) *stringFilter {
	if column == cst.Empty || value == nil || *value == cst.Empty || s.delimiter == cst.Empty || handle == nil {
		return s
	}

	raw := strings.Split(*value, s.delimiter)
	if len(raw) != 2 {
		return s
	}

	trim := strings.TrimSpace(*value)
	leftOpen := strings.HasPrefix(trim, s.delimiter)
	rightOpen := strings.HasSuffix(trim, s.delimiter)
	if leftOpen && rightOpen {
		return s
	}

	if leftOpen {
		values := handle(raw[1:])
		if len(values) == 1 {
			s.filter.LessThanEqual(column, values[0])
		}
		return s
	}
	if rightOpen {
		values := handle(raw[:1])
		if len(values) == 1 {
			s.filter.GreaterThanEqual(column, values[0])
		}
		return s
	}

	values := handle(raw)
	if len(values) == 2 {
		s.filter.Between(column, values[0], values[1])
	}
	return s
}

func (s *stringFilter) Between(column string, value *string, handle func(values []string) []any) StringFilter {
	return s.between(column, value, handle)
}

func (s *stringFilter) IntBetween(column string, value *string) StringFilter {
	return s.between(column, value, s.string2any(reflect.Int))
}

func (s *stringFilter) Int64Between(column string, value *string) StringFilter {
	return s.between(column, value, s.string2any(reflect.Int64))
}

func (s *stringFilter) Float64Between(column string, value *string) StringFilter {
	return s.between(column, value, s.string2any(reflect.Float64))
}

func (s *stringFilter) StringBetween(column string, value *string) StringFilter {
	return s.between(column, value, s.string2any(reflect.String))
}

func (s *stringFilter) in(column string, value *string, handle func(values []string) []any) *stringFilter {
	values := s.split(column, value, handle)
	length := len(values)
	if length == 0 {
		return s
	}
	result := make([]any, 0, length)
	for i := 0; i < length; i++ {
		if values[i] != nil {
			result = append(result, values[i])
		}
	}
	length = len(result)
	if length == 0 {
		return s
	}
	s.filter.In(column, result)
	return s
}

func (s *stringFilter) In(column string, value *string, handle func(values []string) []any) StringFilter {
	return s.in(column, value, handle)
}

func (s *stringFilter) IntIn(column string, value *string) StringFilter {
	return s.in(column, value, s.string2any(reflect.Int))
}

func (s *stringFilter) Int64In(column string, value *string) StringFilter {
	return s.in(column, value, s.string2any(reflect.Int64))
}

func (s *stringFilter) StringIn(column string, value *string) StringFilter {
	return s.in(column, value, s.string2any(reflect.String))
}

func (s *Way) NewStringFilter(filter Filter) StringFilter {
	return s.cfg.NewStringFilter(filter)
}

// TimeFilter Commonly used timestamp range filtering conditions.
type TimeFilter interface {
	// TimeIn Set time zone.
	TimeIn(loc *time.Location) TimeFilter

	// GetTime Get the time point.
	GetTime() time.Time

	// SetTime Set the time point.
	SetTime(timeAt time.Time) TimeFilter

	// LastMinutes Current minute and previous n-1 minute buckets.
	LastMinutes(column string, minutes int) TimeFilter

	// LastHours Current hour and previous n-1 hour buckets.
	LastHours(column string, hours int) TimeFilter

	// Today Time range that has passed today.
	Today(column string) TimeFilter

	// Yesterday From midnight yesterday to midnight today.
	Yesterday(column string) TimeFilter

	// LastDays Last n days.
	LastDays(column string, days int) TimeFilter

	// ThisMonth This month.
	ThisMonth(column string) TimeFilter

	// LastMonth Last month.
	LastMonth(column string) TimeFilter

	// LastMonths Last n months.
	LastMonths(column string, months int) TimeFilter

	// ThisQuarter This quarter.
	ThisQuarter(column string) TimeFilter

	// LastQuarter Last quarter.
	LastQuarter(column string) TimeFilter

	// LastQuarters Last n quarters.
	LastQuarters(column string, quarters int) TimeFilter

	// ThisYear This year.
	ThisYear(column string) TimeFilter

	// LastYear Last year.
	LastYear(column string) TimeFilter

	// LastYears Last n years.
	LastYears(column string, years int) TimeFilter
}

type timeFilter struct {
	filter Filter
	time   time.Time
}

const (
	maxTimeFilterYears    = 10000
	maxTimeFilterQuarters = maxTimeFilterYears * 4
	maxTimeFilterMonths   = maxTimeFilterYears * 12
	maxTimeFilterDays     = maxTimeFilterYears * 366
)

func newTimeFilter(filter Filter) TimeFilter {
	if filter == nil {
		panic(nilPointer)
	}
	return &timeFilter{
		filter: filter,
		time:   filter.V().Now(),
	}
}

func (s *timeFilter) timeAt(timestamp int64) time.Time {
	return time.Unix(timestamp, 0).In(s.time.Location())
}

func (s *timeFilter) minuteStartAt(timestamp int64) int64 {
	t := s.timeAt(timestamp)
	// Subtraction preserves the selected UTC offset when a local minute occurs twice.
	return timestamp - int64(t.Second())
}

func (s *timeFilter) hourStartAt(timestamp int64) int64 {
	t := s.timeAt(timestamp)
	// Reconstructing the local hour with time.Date may select the wrong side of a DST fallback.
	return timestamp - int64(t.Minute()*60+t.Second())
}

func (s *timeFilter) dayStartAt(timestamp int64) int64 {
	t := s.timeAt(timestamp)
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, s.time.Location()).Unix()
}

func (s *timeFilter) monthStartAt(timestamp int64) int64 {
	t := s.timeAt(timestamp)
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, s.time.Location()).Unix()
}

func (s *timeFilter) quarterStartAt(timestamp int64) int64 {
	t := s.timeAt(timestamp)
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
	return time.Date(year, startMonth, 1, 0, 0, 0, 0, s.time.Location()).Unix()
}

func (s *timeFilter) yearStartAt(timestamp int64) int64 {
	t := s.timeAt(timestamp)
	return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, s.time.Location()).Unix()
}

func subtractTimeUnits(timestamp int64, count int, seconds int64) (int64, bool) {
	if count <= 0 {
		return 0, false
	}
	units := int64(count) - 1
	if units > math.MaxInt64/seconds {
		return 0, false
	}
	delta := units * seconds
	if timestamp < math.MinInt64+delta {
		return 0, false
	}
	return timestamp - delta, true
}

func (s *timeFilter) TimeIn(loc *time.Location) TimeFilter {
	if loc != nil {
		s.time = s.time.In(loc)
	}
	return s
}

func (s *timeFilter) GetTime() time.Time {
	return s.time
}

func (s *timeFilter) SetTime(timeAt time.Time) TimeFilter {
	s.time = timeAt
	return s
}

func (s *timeFilter) LastMinutes(column string, minutes int) TimeFilter {
	timestamp := s.time.Unix()
	startAt, ok := subtractTimeUnits(s.minuteStartAt(timestamp), minutes, 60)
	if !ok {
		return s
	}
	s.filter.Between(column, startAt, timestamp)
	return s
}

func (s *timeFilter) LastHours(column string, hours int) TimeFilter {
	timestamp := s.time.Unix()
	startAt, ok := subtractTimeUnits(s.hourStartAt(timestamp), hours, 3600)
	if !ok {
		return s
	}
	s.filter.Between(column, startAt, timestamp)
	return s
}

func (s *timeFilter) Today(column string) TimeFilter {
	timestamp := s.time.Unix()
	s.filter.Between(column, s.dayStartAt(timestamp), timestamp)
	return s
}

func (s *timeFilter) Yesterday(column string) TimeFilter {
	timestamp := s.time.Unix()
	dayStartAt := s.dayStartAt(timestamp)
	s.filter.Between(column, s.dayStartAt(dayStartAt-1), dayStartAt-1)
	return s
}

func (s *timeFilter) LastDays(column string, days int) TimeFilter {
	if days <= 0 || days > maxTimeFilterDays {
		return s
	}
	timestamp := s.time.Unix()
	startAt := time.Unix(s.dayStartAt(timestamp), 0).In(s.time.Location()).AddDate(0, 0, -days+1).Unix()
	s.filter.Between(column, startAt, timestamp)
	return s
}

func (s *timeFilter) ThisMonth(column string) TimeFilter {
	timestamp := s.time.Unix()
	s.filter.Between(column, s.monthStartAt(timestamp), timestamp)
	return s
}

func (s *timeFilter) LastMonth(column string) TimeFilter {
	timestamp := s.time.Unix()
	thisMonthStartAt := s.monthStartAt(timestamp)
	lastMonthStartAt := time.Unix(thisMonthStartAt, 0).In(s.time.Location()).AddDate(0, -1, 0).Unix()
	s.filter.Between(column, lastMonthStartAt, thisMonthStartAt-1)
	return s
}

func (s *timeFilter) LastMonths(column string, months int) TimeFilter {
	if months <= 0 || months > maxTimeFilterMonths {
		return s
	}
	timestamp := s.time.Unix()
	thisMonthStartAt := s.monthStartAt(timestamp)
	lastMonthStartAt := time.Unix(thisMonthStartAt, 0).In(s.time.Location()).AddDate(0, -months+1, 0).Unix()
	s.filter.Between(column, lastMonthStartAt, timestamp)
	return s
}

func (s *timeFilter) ThisQuarter(column string) TimeFilter {
	timestamp := s.time.Unix()
	thisQuarterStartAt := s.quarterStartAt(timestamp)
	s.filter.Between(column, thisQuarterStartAt, timestamp)
	return s
}

func (s *timeFilter) LastQuarter(column string) TimeFilter {
	timestamp := s.time.Unix()
	thisQuarterStartAt := s.quarterStartAt(timestamp)
	lastQuarterStartAt := s.quarterStartAt(thisQuarterStartAt - 1)
	s.filter.Between(column, lastQuarterStartAt, thisQuarterStartAt-1)
	return s
}

func (s *timeFilter) LastQuarters(column string, quarters int) TimeFilter {
	if quarters <= 0 || quarters > maxTimeFilterQuarters {
		return s
	}
	timestamp := s.time.Unix()
	thisQuarterStartAt := s.timeAt(s.quarterStartAt(timestamp))
	startAt := thisQuarterStartAt.AddDate(0, -(quarters-1)*3, 0).Unix()
	s.filter.Between(column, startAt, timestamp)
	return s
}

func (s *timeFilter) ThisYear(column string) TimeFilter {
	timestamp := s.time.Unix()
	s.filter.Between(column, s.yearStartAt(timestamp), timestamp)
	return s
}

func (s *timeFilter) LastYear(column string) TimeFilter {
	timestamp := s.time.Unix()
	endAt := s.yearStartAt(timestamp) - 1
	startAt := s.yearStartAt(endAt)
	s.filter.Between(column, startAt, endAt)
	return s
}

func (s *timeFilter) LastYears(column string, years int) TimeFilter {
	if years <= 0 || years > maxTimeFilterYears {
		return s
	}
	timestamp := s.time.Unix()
	startAt := time.Unix(s.yearStartAt(timestamp), 0).In(s.time.Location()).AddDate(-years+1, 0, 0).Unix()
	s.filter.Between(column, startAt, timestamp)
	return s
}

func (s *Way) NewTimeFilter(filter Filter) TimeFilter {
	return s.cfg.NewTimeFilter(filter).SetTime(s.Now())
}
