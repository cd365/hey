// Implementing SQL filter

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

func filterExpression(column string, compare string) string {
	if column == StrEmpty {
		return StrEmpty
	}
	return Strings(column, StrSpace, compare, StrSpace, StrPlaceholder)
}

func filterEqual(column string) string {
	return filterExpression(column, StrEqual)
}

func filterNotEqual(column string) string {
	return filterExpression(column, StrNotEqual)
}

func filterMoreThan(column string) string {
	return filterExpression(column, StrMoreThan)
}

func filterMoreThanEqual(column string) string {
	return filterExpression(column, StrMoreThanEqual)
}

func filterLessThan(column string) string {
	return filterExpression(column, StrLessThan)
}

func filterLessThanEqual(column string) string {
	return filterExpression(column, StrLessThanEqual)
}

func filterIn(column string, values []any, not bool) *SQL {
	if column == StrEmpty || values == nil {
		return NewEmptySQL()
	}
	values = argsCompatible(values...)
	length := len(values)
	if length == 0 {
		return NewEmptySQL()
	}
	script := NewEmptySQL()
	values = DiscardDuplicate(nil, values...)
	length = len(values)
	if length == 1 {
		if not {
			script.Prepare = filterNotEqual(column)
		} else {
			script.Prepare = filterEqual(column)
		}
		script.Args = []any{values[0]}
		return script
	}
	script.Args = values
	length2 := length * 2
	result := make([]string, 0, length2)
	for i := 0; i < length; i++ {
		if i == 0 {
			result = append(result, StrPlaceholder)
			continue
		}
		result = append(result, StrCommaSpace, StrPlaceholder)
	}
	tmp := make([]string, 0, len(result)+7)
	tmp = append(tmp, column)
	if not {
		tmp = append(tmp, StrSpace, StrNot)
	}
	tmp = append(tmp, StrSpace, StrIn, StrSpace, StrLeftSmallBracket, StrSpace)
	tmp = append(tmp, result...)
	tmp = append(tmp, StrSpace, StrRightSmallBracket)
	script.Prepare = Strings(tmp...)
	return script
}

func filterInSQL(column string, value *SQL, not bool) *SQL {
	if value == nil || value.IsEmpty() {
		return NewEmptySQL()
	}
	result := column
	if not {
		result = Strings(result, StrSpace, StrNot)
	}
	result = Strings(result, StrSpace, StrIn, StrSpace, StrLeftSmallBracket, StrSpace, value.Prepare, StrSpace, StrRightSmallBracket)
	return NewSQL(result, value.Args...)
}

func filterInGroupColumns(columns ...string) string {
	return Strings(StrLeftSmallBracket, StrSpace, strings.Join(columns, StrCommaSpace), StrSpace, StrRightSmallBracket)
}

func filterInGroup(columns []string, values [][]any, not bool) *SQL {
	count := len(columns)
	if count == 0 {
		return NewEmptySQL()
	}
	length := len(values)
	if length == 0 {
		return NewEmptySQL()
	}
	script := NewEmptySQL()
	for i := range length {
		if len(values[i]) != count {
			return NewEmptySQL()
		}
		script.Args = append(script.Args, values[i][:]...)
	}
	oneGroup := make([]string, count)
	for i := range count {
		oneGroup[i] = StrPlaceholder
	}
	oneGroupString := Strings(StrLeftSmallBracket, StrSpace, strings.Join(oneGroup, StrCommaSpace), StrSpace, StrRightSmallBracket)
	valueGroup := make([]string, length)
	for i := range length {
		valueGroup[i] = oneGroupString
	}
	tmp := make([]string, 0, 8)
	tmp = append(tmp, filterInGroupColumns(columns...))
	if not {
		tmp = append(tmp, StrSpace, StrNot)
	}
	tmp = append(tmp, StrSpace, StrIn, StrSpace, StrLeftSmallBracket, StrSpace)
	tmp = append(tmp, strings.Join(valueGroup, StrCommaSpace))
	tmp = append(tmp, StrSpace, StrRightSmallBracket)
	script.Prepare = Strings(tmp...)
	return script
}

func filterInGroupSQL(columns []string, value *SQL, not bool) *SQL {
	count := len(columns)
	if count == 0 || value == nil || value.IsEmpty() {
		return NewEmptySQL()
	}
	tmp := make([]string, 0, 8)
	tmp = append(tmp, filterInGroupColumns(columns...))
	if not {
		tmp = append(tmp, StrSpace, StrNot)
	}
	tmp = append(tmp, StrSpace, StrIn, StrSpace, StrLeftSmallBracket, StrSpace)
	tmp = append(tmp, value.Prepare)
	tmp = append(tmp, StrSpace, StrRightSmallBracket)
	return NewSQL(Strings(tmp...), value.Args...)
}

func filterExists(value Maker, not bool) *SQL {
	if value == nil {
		return NewEmptySQL()
	}
	script := value.ToSQL()
	if script.IsEmpty() {
		return NewEmptySQL()
	}
	exists := StrExists
	if not {
		exists = Strings(StrNot, StrSpace, exists)
	}
	return NewSQL(Strings(exists, StrSpace, StrLeftSmallBracket, StrSpace, script.Prepare, StrSpace, StrRightSmallBracket), script.Args...)
}

func filterBetween(column string, not bool) (prepare string) {
	if column == StrEmpty {
		return
	}
	prepare = column
	if not {
		prepare = Strings(prepare, StrSpace, StrNot)
	}
	prepare = Strings(prepare, StrSpace, StrBetween, StrSpace, StrPlaceholder, StrSpace, StrAnd, StrSpace, StrPlaceholder)
	return
}

func filterLike(column string, not bool) (prepare string) {
	if column == StrEmpty {
		return
	}
	prepare = column
	if not {
		prepare = Strings(prepare, StrSpace, StrNot)
	}
	prepare = Strings(prepare, StrSpace, StrLike, StrSpace, StrPlaceholder)
	return
}

func filterIsNull(column string, not bool) (prepare string) {
	if column == StrEmpty {
		return
	}
	prepare = Strings(column, StrSpace, StrIs)
	if not {
		prepare = Strings(prepare, StrSpace, StrNot)
	}
	prepare = Strings(prepare, StrSpace, StrNull)
	return
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

// Replacer SQL Identifier Replacer.
// All identifier mapping relationships should be set before the program is initialized.
// They cannot be set again while the program is running to avoid concurrent reading and writing of the map.
type Replacer interface {
	Get(key string) string

	Set(key string, value string) Replacer

	Del(key string) Replacer

	Map() map[string]string

	GetAll(keys []string) []string
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

	// GetReplacer For get *Way.
	GetReplacer() Replacer

	// SetReplacer For set *Way.
	SetReplacer(replacer Replacer) Filter

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

	replacer Replacer

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
	s.replacer = nil
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

func (s *filter) add(logic string, prepare string, args ...any) *filter {
	if prepare == StrEmpty {
		return s
	}
	if s.num == 0 {
		s.prepare.WriteString(prepare)
		s.args = args[:]
		s.num++
		return s
	}
	s.prepare.WriteString(Strings(StrSpace, logic, StrSpace, prepare))
	s.args = append(s.args, args[:]...)
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
	script := tmp.ToSQL()
	s.add(logic, script.Prepare, script.Args...)
	return s
}

func (s *filter) addSQL(logic string, value *SQL) *filter {
	if value == nil || value.IsEmpty() {
		return s
	}
	return s.add(logic, value.Prepare, value.Args...)
}

func (s *filter) And(prepare string, args ...any) Filter {
	return s.add(StrAnd, prepare, args...)
}

func (s *filter) Or(prepare string, args ...any) Filter {
	return s.add(StrOr, prepare, args...)
}

func (s *filter) Group(group func(f Filter)) Filter {
	return s.addGroup(StrAnd, group)
}

func (s *filter) OrGroup(group func(f Filter)) Filter {
	return s.addGroup(StrOr, group)
}

func (s *filter) Use(filters ...Filter) Filter {
	groups := poolGetFilter().SetReplacer(s.replacer)
	defer poolPutFilter(groups)
	for _, tmp := range filters {
		if tmp == nil || tmp.IsEmpty() {
			continue
		}
		result := tmp.ToSQL()
		groups.And(result.Prepare, result.Args...)
	}
	script := groups.ToSQL()
	return s.And(script.Prepare, script.Args...)
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

func (s *filter) getAll(keys []string) []string {
	if s.replacer == nil {
		return keys
	}
	return s.replacer.GetAll(keys)
}

func (s *filter) columnCompareSubquery(logic string, column string, compare string, subquery any) bool {
	maker, ok := subquery.(Maker)
	if !ok || maker == nil {
		return false
	}
	script := ParcelSQL(maker.ToSQL())
	script.Prepare = Strings(s.get(column), StrSpace, compare, StrSpace, script.Prepare)
	s.addSQL(logic, script)
	return ok
}

func (s *filter) andColumnCompareSubquery(column string, compare string, subquery any) bool {
	return s.columnCompareSubquery(StrAnd, column, compare, subquery)
}

func (s *filter) Equal(column string, value any, usingNull ...bool) Filter {
	if value == nil {
		if length := len(usingNull); length > 0 && usingNull[length-1] {
			return s.IsNull(s.get(column))
		}
		return s
	}
	if s.andColumnCompareSubquery(column, StrEqual, value) {
		return s
	}
	if value = filterUsingValue(value); value != nil {
		s.add(StrAnd, filterEqual(s.get(column)), value)
	}
	return s
}

func (s *filter) LessThan(column string, value any) Filter {
	if s.andColumnCompareSubquery(column, StrLessThan, value) {
		return s
	}
	if value = filterUsingValue(value); value != nil {
		s.add(StrAnd, filterLessThan(s.get(column)), value)
	}
	return s
}

func (s *filter) LessThanEqual(column string, value any) Filter {
	if s.andColumnCompareSubquery(column, StrLessThanEqual, value) {
		return s
	}
	if value = filterUsingValue(value); value != nil {
		s.add(StrAnd, filterLessThanEqual(s.get(column)), value)
	}
	return s
}

func (s *filter) MoreThan(column string, value any) Filter {
	if s.andColumnCompareSubquery(column, StrMoreThan, value) {
		return s
	}
	if value = filterUsingValue(value); value != nil {
		s.add(StrAnd, filterMoreThan(s.get(column)), value)
	}
	return s
}

func (s *filter) MoreThanEqual(column string, value any) Filter {
	if s.andColumnCompareSubquery(column, StrMoreThanEqual, value) {
		return s
	}
	if value = filterUsingValue(value); value != nil {
		s.add(StrAnd, filterMoreThanEqual(s.get(column)), value)
	}
	return s
}

func (s *filter) Between(column string, start any, end any) Filter {
	if start, end = filterUsingValue(start), filterUsingValue(end); start != nil && end != nil {
		s.add(StrAnd, filterBetween(s.get(column), false), start, end)
	}
	return s
}

func (s *filter) In(column string, values ...any) Filter {
	return s.addSQL(StrAnd, filterIn(s.get(column), values, false))
}

func (s *filter) InQuery(column string, subquery Maker) Filter {
	if subquery == nil {
		return s
	}
	return s.addSQL(StrAnd, filterInSQL(s.get(column), subquery.ToSQL(), false))
}

func (s *filter) Exists(subquery Maker) Filter {
	if subquery == nil {
		return s
	}
	return s.addSQL(StrAnd, filterExists(subquery, false))
}

func (s *filter) Like(column string, value any) Filter {
	value = filterUsingValue(value)
	if value == nil {
		return s
	}
	like := StrEmpty
	if tmp, ok := value.(string); !ok {
		if item := reflect.ValueOf(value); item.Kind() == reflect.String {
			like = item.String()
		}
	} else {
		like = tmp
	}
	if like != StrEmpty {
		s.add(StrAnd, filterLike(s.get(column), false), like)
	}
	return s
}

func (s *filter) IsNull(column string) Filter {
	return s.add(StrAnd, filterIsNull(s.get(column), false))
}

func (s *filter) InGroup(columns []string, values ...[]any) Filter {
	return s.addSQL(StrAnd, filterInGroup(s.getAll(columns), values, false))
}

func (s *filter) InGroupQuery(columns []string, subquery Maker) Filter {
	if subquery == nil {
		return s
	}
	return s.addSQL(StrAnd, filterInGroupSQL(s.getAll(columns), subquery.ToSQL(), false))
}

func (s *filter) NotEqual(column string, value any, notUsingNull ...bool) Filter {
	if value == nil {
		if length := len(notUsingNull); length > 0 && notUsingNull[length-1] {
			return s.IsNotNull(s.get(column))
		}
		return s
	}
	if value = filterUsingValue(value); value != nil {
		s.add(StrAnd, filterNotEqual(s.get(column)), value)
	}
	return s
}

func (s *filter) NotBetween(column string, start any, end any) Filter {
	if start, end = filterUsingValue(start), filterUsingValue(end); start != nil && end != nil {
		s.add(StrAnd, filterBetween(s.get(column), true), start, end)
	}
	return s
}

func (s *filter) NotIn(column string, values ...any) Filter {
	return s.addSQL(StrAnd, filterIn(s.get(column), values, true))
}

func (s *filter) NotInGroup(columns []string, values ...[]any) Filter {
	return s.addSQL(StrAnd, filterInGroup(s.getAll(columns), values, true))
}

func (s *filter) NotLike(column string, value any) Filter {
	value = filterUsingValue(value)
	if value == nil {
		return s
	}
	like := StrEmpty
	if tmp, ok := value.(string); !ok {
		if item := reflect.ValueOf(value); item.Kind() == reflect.String {
			like = item.String()
		}
	} else {
		like = tmp
	}
	if like != StrEmpty {
		s.add(StrAnd, filterLike(s.get(column), true), like)
	}
	return s
}

func (s *filter) IsNotNull(column string) Filter {
	return s.add(StrAnd, filterIsNull(s.get(column), true))
}

func (s *filter) AllQuantifier(fc func(q Quantifier)) Filter {
	if fc == nil {
		return s
	}
	tmp := &quantifier{
		filter:     s.New(),
		quantifier: StrAll,
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

func (s *filter) Compare(column1 string, compare string, column2 string, args ...any) Filter {
	if column1 == StrEmpty || compare == StrEmpty || column2 == StrEmpty {
		return s
	}
	column1, column2 = s.get(column1), s.get(column2)
	return s.And(Strings(column1, StrSpace, compare, StrSpace, column2), args...)
}

func (s *filter) CompareEqual(column1 string, column2 string, args ...any) Filter {
	return s.Compare(column1, StrEqual, column2, args...)
}

func (s *filter) CompareNotEqual(column1 string, column2 string, args ...any) Filter {
	return s.Compare(column1, StrNotEqual, column2, args...)
}

func (s *filter) CompareMoreThan(column1 string, column2 string, args ...any) Filter {
	return s.Compare(column1, StrMoreThan, column2, args...)
}

func (s *filter) CompareMoreThanEqual(column1 string, column2 string, args ...any) Filter {
	return s.Compare(column1, StrMoreThanEqual, column2, args...)
}

func (s *filter) CompareLessThan(column1 string, column2 string, args ...any) Filter {
	return s.Compare(column1, StrLessThan, column2, args...)
}

func (s *filter) CompareLessThanEqual(column1 string, column2 string, args ...any) Filter {
	return s.Compare(column1, StrLessThanEqual, column2, args...)
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
	if column == StrEmpty || logic == StrEmpty || subquery == nil {
		return s
	}
	script := subquery.ToSQL()
	if script.IsEmpty() {
		return s
	}
	if replacer := s.filter.GetReplacer(); replacer != nil {
		column = replacer.Get(column)
	}
	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	b.WriteString(column)
	b.WriteString(StrSpace)
	b.WriteString(logic)
	b.WriteString(StrSpace)
	if s.quantifier != StrEmpty {
		b.WriteString(s.quantifier)
		b.WriteString(StrSpace)
	}
	b.WriteString(StrLeftSmallBracket)
	b.WriteString(StrSpace)
	b.WriteString(script.Prepare)
	b.WriteString(StrSpace)
	b.WriteString(StrRightSmallBracket)
	s.filter.And(b.String(), script.Args...)
	return s
}

// Equal Implement the filter condition: column = QUANTIFIER ( subquery ) .
func (s *quantifier) Equal(column string, subquery Maker) Quantifier {
	return s.build(column, StrEqual, subquery)
}

// NotEqual Implement the filter condition: column <> QUANTIFIER ( subquery ) .
func (s *quantifier) NotEqual(column string, subquery Maker) Quantifier {
	return s.build(column, StrNotEqual, subquery)
}

// LessThan Implement the filter condition: column < QUANTIFIER ( subquery ) .
func (s *quantifier) LessThan(column string, subquery Maker) Quantifier {
	return s.build(column, StrLessThan, subquery)
}

// LessThanEqual Implement the filter condition: column <= QUANTIFIER ( subquery ) .
func (s *quantifier) LessThanEqual(column string, subquery Maker) Quantifier {
	return s.build(column, StrLessThanEqual, subquery)
}

// MoreThan Implement the filter condition: column > QUANTIFIER ( subquery ) .
func (s *quantifier) MoreThan(column string, subquery Maker) Quantifier {
	return s.build(column, StrMoreThan, subquery)
}

// MoreThanEqual Implement the filter condition: column >= QUANTIFIER ( subquery ) .
func (s *quantifier) MoreThanEqual(column string, subquery Maker) Quantifier {
	return s.build(column, StrMoreThanEqual, subquery)
}
