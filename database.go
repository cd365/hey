package hey

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

/**
 * database helper.
 **/

// Identifier Add or remove quote characters in sql identifiers.
type Identifier interface {
	SymbolAddAll(keys []string) []string
	SymbolAddOne(key string) string
	SymbolDelAll(keys []string) []string
	SymbolDelOne(key string) string
	Symbol() string
}

type identifier struct {
	identify         string
	identifierSymbol *regexp.Regexp
	add              map[string]string
	addRWMutex       *sync.RWMutex
	del              map[string]string
	delRWMutex       *sync.RWMutex
}

func (s *identifier) getAdd(key string) (value string, ok bool) {
	s.addRWMutex.RLock()
	defer s.addRWMutex.RUnlock()
	value, ok = s.add[key]
	return
}

func (s *identifier) setAdd(key string, value string) {
	s.addRWMutex.Lock()
	defer s.addRWMutex.Unlock()
	s.add[key] = value
	return
}

func (s *identifier) getDel(key string) (value string, ok bool) {
	s.delRWMutex.RLock()
	defer s.delRWMutex.RUnlock()
	value, ok = s.del[key]
	return
}

func (s *identifier) setDel(key string, value string) {
	s.delRWMutex.Lock()
	defer s.delRWMutex.Unlock()
	s.del[key] = value
	return
}

func (s *identifier) SymbolAddAll(keys []string) []string {
	length := len(keys)
	identify := s.identify
	if length == 0 || identify == EmptyString {
		return keys
	}
	result := make([]string, 0, length)
	for i := 0; i < length; i++ {
		if keys[i] == EmptyString {
			result = append(result, keys[i])
			continue
		}
		value, ok := s.getAdd(keys[i])
		if !ok {
			if !s.identifierSymbol.MatchString(keys[i]) {
				result = append(result, keys[i])
				s.setAdd(keys[i], keys[i])
				continue
			}
			value = strings.ReplaceAll(keys[i], identify, EmptyString)
			value = strings.TrimSpace(value)
			values := strings.Split(value, SqlPoint)
			for k, v := range values {
				values[k] = fmt.Sprintf("%s%s%s", identify, v, identify)
			}
			value = strings.Join(values, SqlPoint)
			s.setAdd(keys[i], value)
		}
		result = append(result, value)
	}
	return result
}

func (s *identifier) SymbolAddOne(key string) string {
	return s.SymbolAddAll([]string{key})[0]
}

func (s *identifier) SymbolDelAll(keys []string) []string {
	length := len(keys)
	identify := s.identify
	if length == 0 || identify == EmptyString {
		return keys
	}
	result := make([]string, length)
	for i := 0; i < length; i++ {
		value, ok := s.getDel(keys[i])
		if !ok {
			value = strings.ReplaceAll(keys[i], identify, EmptyString)
			s.setDel(keys[i], value)
		}
		result[i] = value
	}
	return result
}

func (s *identifier) SymbolDelOne(key string) string {
	return s.SymbolDelAll([]string{key})[0]
}

func (s *identifier) Symbol() string {
	return s.identify
}

func NewIdentifier(identify string) Identifier {
	return &identifier{
		identify:         identify,
		identifierSymbol: regexp.MustCompile("^[A-Za-z][A-Za-z0-9_.]*$"),
		add:              make(map[string]string, 512),
		addRWMutex:       &sync.RWMutex{},
		del:              make(map[string]string, 512),
		delRWMutex:       &sync.RWMutex{},
	}
}

// IsEmpty Check if an object value is empty.
type IsEmpty interface {
	IsEmpty() bool
}

// Script Used to build a SQL expression and its corresponding parameter list.
type Script interface {
	// Script Get a list of script statements and their corresponding parameters.
	Script() (prepare string, args []interface{})
}

type sqlScript struct {
	prepare string
	args    []interface{}
}

func (s *sqlScript) Script() (prepare string, args []interface{}) {
	if s.prepare != EmptyString {
		prepare, args = s.prepare, s.args
	}
	return
}

func NewScript(prepare string, args []interface{}) Script {
	return &sqlScript{
		prepare: prepare,
		args:    args,
	}
}

func IsEmptyScript(script Script) bool {
	if script == nil {
		return true
	}
	prepare, _ := script.Script()
	return prepare == EmptyString
}

// TableScript Used to construct expressions that can use table aliases and their corresponding parameter lists.
type TableScript interface {
	IsEmpty

	Script

	// Alias Setting aliases for script statements.
	Alias(alias string) TableScript

	// GetAlias Getting aliases for script statements.
	GetAlias() string
}

type tableScript struct {
	script Script
	alias  string
}

func (s *tableScript) IsEmpty() bool {
	return IsEmptyScript(s.script)
}

func (s *tableScript) Script() (prepare string, args []interface{}) {
	if s.IsEmpty() {
		return
	}
	prepare, args = s.script.Script()
	if s.alias != EmptyString {
		prepare = ConcatString(prepare, SqlSpace, s.alias)
	}
	return
}

func (s *tableScript) Alias(alias string) TableScript {
	s.alias = alias // allow setting empty values
	return s
}

func (s *tableScript) GetAlias() string {
	return s.alias
}

func NewTableScript(prepare string, args []interface{}) TableScript {
	return &tableScript{
		script: NewScript(prepare, args),
	}
}

// WithScript CTE: Common Table Expression.
type WithScript interface {
	IsEmpty

	Script

	// With Set common table expression.
	With(alias string, script Script) WithScript

	// Alias Setting aliases for script statements.
	Alias(alias string) WithScript

	// GetAlias Getting aliases for script statements.
	GetAlias() string
}

type withScript struct {
	alias  string
	script Script
}

func NewWithScript() WithScript {
	return &withScript{}
}

func (s *withScript) IsEmpty() bool {
	return s.alias == EmptyString || IsEmptyScript(s.script)
}

func (s *withScript) Script() (prepare string, args []interface{}) {
	if s.IsEmpty() {
		return
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(s.alias)
	b.WriteString(" AS ( ")
	prepare, args = s.script.Script()
	b.WriteString(prepare)
	b.WriteString(" )")
	prepare = b.String()
	return
}

func (s *withScript) With(alias string, script Script) WithScript {
	if alias == EmptyString || IsEmptyScript(s.script) {
		return s
	}
	s.alias, s.script = alias, script
	return s
}

func (s *withScript) Alias(alias string) WithScript {
	if alias != EmptyString {
		s.alias = alias
	}
	return s
}

func (s *withScript) GetAlias() string {
	return s.alias
}

// SelectColumns Used to build the list of columns to be queried.
type SelectColumns struct {
	columns     []string
	columnsMap  map[string]int
	columnsArgs map[int][]interface{}
}

func NewSelectColumns() *SelectColumns {
	return &SelectColumns{
		columns:     make([]string, 0, 32),
		columnsMap:  make(map[string]int, 32),
		columnsArgs: make(map[int][]interface{}, 32),
	}
}

func (s *SelectColumns) Script() (prepare string, args []interface{}) {
	length := len(s.columns)
	if length == 0 {
		return SqlStar, nil
	}
	columns := make([]string, 0, length)
	for i := 0; i < length; i++ {
		tmpArgs, ok := s.columnsArgs[i]
		if !ok {
			continue
		}
		columns = append(columns, s.columns[i])
		if tmpArgs != nil {
			args = append(args, tmpArgs...)
		}
	}
	prepare = strings.Join(columns, ", ")
	return
}

func (s *SelectColumns) ColumnIndex(column string) int {
	index, ok := s.columnsMap[column]
	if !ok {
		return -1
	}
	return index
}

func (s *SelectColumns) Exists(column string) bool {
	return s.ColumnIndex(column) >= 0
}

func (s *SelectColumns) Add(column string, args ...interface{}) *SelectColumns {
	if column == EmptyString {
		return s
	}
	index, ok := s.columnsMap[column]
	if ok {
		s.columnsArgs[index] = args
		return s
	}
	index = len(s.columns)
	s.columns = append(s.columns, column)
	s.columnsMap[column] = index
	s.columnsArgs[index] = args
	return s
}

func (s *SelectColumns) AddAll(columns ...string) *SelectColumns {
	for _, column := range columns {
		s.Add(column)
	}
	return s
}

func (s *SelectColumns) Del(columns ...string) *SelectColumns {
	deleted := make(map[int]*struct{}, len(columns))
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		index, ok := s.columnsMap[column]
		if !ok {
			continue
		}
		deleted[index] = &struct{}{}
	}
	length := len(s.columns)
	resultColumns := make([]string, 0, length)
	for index, column := range s.columns {
		if _, ok := deleted[index]; ok {
			delete(s.columnsMap, column)
			delete(s.columnsArgs, index)
		} else {
			resultColumns = append(resultColumns, column)
		}
	}
	s.columns = resultColumns
	return s
}

func (s *SelectColumns) DelAll() *SelectColumns {
	s.columns = make([]string, 0, 32)
	s.columnsMap = make(map[string]int, 32)
	s.columnsArgs = make(map[int][]interface{}, 32)
	return s
}

func (s *SelectColumns) Len() int {
	return len(s.columns)
}

func (s *SelectColumns) GetColumnsArgs() ([]string, map[int][]interface{}) {
	return s.columns, s.columnsArgs
}

func (s *SelectColumns) SetColumnsArgs(columns []string, columnsArgs map[int][]interface{}) *SelectColumns {
	columnsMap := make(map[string]int, 32)
	for i, column := range columns {
		columnsMap[column] = i
	}
	s.columns, s.columnsMap, s.columnsArgs = columns, columnsMap, columnsArgs
	return s
}

// JoinRequire Constructing conditions for join queries.
type JoinRequire func(leftAlias string, rightAlias string) (prepare string, args []interface{})

// JoinTableScript Constructing table for join queries.
type JoinTableScript interface {
	TableScript

	AddSelectColumns(columns ...string) JoinTableScript

	DelSelectColumns(columns ...string) JoinTableScript

	DelAllSelectColumns() JoinTableScript

	ExistsSelectColumns() bool

	GetSelectColumns() []string

	GetSelectColumnsString() string
}

type joinTableScript struct {
	TableScript
	selectColumns *SelectColumns
}

func (s *joinTableScript) AddSelectColumns(columns ...string) JoinTableScript {
	for _, column := range columns {
		s.selectColumns.Add(column)
	}
	return s
}

func (s *joinTableScript) DelSelectColumns(columns ...string) JoinTableScript {
	for _, column := range columns {
		s.selectColumns.Del(column)
	}
	return s
}

func (s *joinTableScript) DelAllSelectColumns() JoinTableScript {
	s.selectColumns.DelAll()
	return s
}

func (s *joinTableScript) ExistsSelectColumns() bool {
	return s.selectColumns.Len() > 0
}

func (s *joinTableScript) GetSelectColumns() []string {
	if s.TableScript == nil {
		return nil
	}
	alias := s.GetAlias()
	if alias == EmptyString {
		return nil
	}
	prefix := fmt.Sprintf("%s.", alias)
	prepare, _ := s.selectColumns.Script()
	prepare = strings.ReplaceAll(prepare, " ", "")
	columns := strings.Split(prepare, ",")
	result := make([]string, 0, len(columns))
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		if !strings.HasPrefix(column, prefix) {
			column = fmt.Sprintf("%s%s", prefix, column)
		}
		result = append(result, column)
	}
	return result
}

func (s *joinTableScript) GetSelectColumnsString() string {
	return strings.Join(s.GetSelectColumns(), ", ")
}

func NewJoinTableScript(table string, alias string, args ...interface{}) JoinTableScript {
	return &joinTableScript{
		TableScript:   NewTableScript(table, args).Alias(alias),
		selectColumns: NewSelectColumns(),
	}
}

// JoinScript Constructing multi-table join queries.
type JoinScript interface {
	Script() (prepare string, args []interface{})

	GetMaster() JoinTableScript

	SetMaster(master JoinTableScript) JoinScript

	On(requires ...func(leftAlias string, rightAlias string) Script) JoinRequire

	Using(using []string, requires ...func(leftAlias string, rightAlias string) Script) JoinRequire

	OnEqual(leftColumn string, rightColumn string, requires ...func(leftAlias string, rightAlias string) Script) JoinRequire

	Join(joinTypeString string, leftTable JoinTableScript, rightTable JoinTableScript, joinRequire JoinRequire) JoinScript

	InnerJoin(leftTable JoinTableScript, rightTable JoinTableScript, joinRequire JoinRequire) JoinScript

	LeftJoin(leftTable JoinTableScript, rightTable JoinTableScript, joinRequire JoinRequire) JoinScript

	RightJoin(leftTable JoinTableScript, rightTable JoinTableScript, joinRequire JoinRequire) JoinScript

	// Where Don't forget to prefix the specific columns with the table name?
	Where(where func(where Filter)) JoinScript

	// SelectExtendColumns The queried column uses a conditional statement or calls a function (not the direct column name of the table); the table alias prefix is not automatically added.
	SelectExtendColumns(custom func(sc *SelectColumns)) JoinScript
}

type joinTable struct {
	joinType    string
	rightTable  JoinTableScript
	joinRequire Script
}
type joinScript struct {
	master JoinTableScript
	joins  []*joinTable
	filter Filter
	// selectExtendColumns The queried column uses a conditional statement or calls a function (not the direct column name of the table); the table alias prefix is not automatically added.
	selectExtendColumns *SelectColumns
}

func NewJoinScript() JoinScript {
	tmp := &joinScript{
		joins:               make([]*joinTable, 0, 8),
		filter:              F(),
		selectExtendColumns: NewSelectColumns(),
	}
	return tmp
}

func (s *joinScript) GetMaster() JoinTableScript {
	return s.master
}

func (s *joinScript) SetMaster(master JoinTableScript) JoinScript {
	if master != nil && !master.IsEmpty() {
		s.master = master
	}
	return s
}

func (s *joinScript) Script() (prepare string, args []interface{}) {
	columns := NewSelectColumns()
	if s.master.ExistsSelectColumns() {
		if column := s.master.GetSelectColumnsString(); column != EmptyString {
			columns.Add(column)
		}
	}

	for _, tmp := range s.joins {
		if tmp.rightTable.ExistsSelectColumns() {
			if column := tmp.rightTable.GetSelectColumnsString(); column != EmptyString {
				columns.Add(column)
			}
		}
	}
	if s.selectExtendColumns != nil && s.selectExtendColumns.Len() > 0 {
		extendColumnsPrepare, extendColumnsArgs := s.selectExtendColumns.Script()
		columns.Add(extendColumnsPrepare, extendColumnsArgs...)
	}

	selectColumnsPrepare, selectColumnsArgs := columns.Script()
	if selectColumnsArgs != nil {
		args = append(args, selectColumnsArgs...)
	}

	b := getStringBuilder()
	defer putStringBuilder(b)

	b.WriteString("SELECT ")
	b.WriteString(selectColumnsPrepare)
	b.WriteString(" FROM ")
	masterPrepare, masterArgs := s.master.Script()
	b.WriteString(masterPrepare)
	b.WriteString(SqlSpace)
	args = append(args, masterArgs...)
	for index, tmp := range s.joins {
		if tmp == nil {
			continue
		}
		if index > 0 {
			b.WriteString(SqlSpace)
		}
		b.WriteString(tmp.joinType)
		b.WriteString(SqlSpace)
		joinPrepare, joinArgs := tmp.rightTable.Script()
		b.WriteString(joinPrepare)
		b.WriteString(SqlSpace)
		args = append(args, joinArgs...)
		if tmp.joinRequire != nil {
			joinRequirePrepare, joinRequireArgs := tmp.joinRequire.Script()
			if joinRequirePrepare != EmptyString {
				b.WriteString(joinRequirePrepare)
				if joinRequireArgs != nil {
					args = append(args, joinRequireArgs...)
				}
			}
		}
	}
	prepare = b.String()
	return
}

// On For `... JOIN ON ...`
func (s *joinScript) On(requires ...func(leftAlias string, rightAlias string) Script) JoinRequire {
	return func(leftAlias string, rightAlias string) (prepare string, args []interface{}) {
		b := getStringBuilder()
		defer putStringBuilder(b)
		added := false
		var param []interface{}
		for _, require := range requires {
			script := require(leftAlias, rightAlias)
			if script == nil {
				continue
			}
			prepare, param = script.Script()
			if prepare == EmptyString {
				continue
			}
			if !added {
				added = true
				b.WriteString("ON ")
			} else {
				b.WriteString(" AND ")
			}
			b.WriteString(prepare)
			if param != nil {
				args = append(args, param...)
			}
		}
		prepare = b.String()
		return
	}
}

// Using For `... JOIN USING ...`
func (s *joinScript) Using(using []string, requires ...func(leftAlias string, rightAlias string) Script) JoinRequire {
	return func(leftAlias string, rightAlias string) (prepare string, args []interface{}) {
		columns := make([]string, 0, len(using))
		for _, column := range using {
			if column == EmptyString {
				continue
			}
			columns = append(columns, column)
		}
		if len(columns) == 0 {
			return
		}
		b := getStringBuilder()
		defer putStringBuilder(b)
		b.WriteString("USING ( ")
		b.WriteString(strings.Join(using, ", "))
		b.WriteString(" )")
		var param []interface{}
		for _, require := range requires {
			if require == nil {
				continue
			}
			script := require(leftAlias, rightAlias)
			if script == nil {
				continue
			}
			prepare, param = script.Script()
			if prepare != EmptyString {
				b.WriteString(" AND ")
				b.WriteString(prepare)
				if param != nil {
					args = append(args, param...)
				}
			}
		}
		prepare = b.String()
		return
	}
}

// OnEqual For `... JOIN ON ... = ... [...]`
func (s *joinScript) OnEqual(leftColumn string, rightColumn string, requires ...func(leftAlias string, rightAlias string) Script) JoinRequire {
	onRequire := make([]func(leftAlias string, rightAlias string) Script, 0, len(requires)+1)
	onEqual := func(leftAlias string, rightAlias string) Script {
		if leftColumn == EmptyString || rightColumn == EmptyString {
			return nil
		}
		return NewScript(fmt.Sprintf("%s.%s = %s.%s", leftAlias, leftColumn, rightAlias, rightColumn), nil)
	}
	onRequire = append(onRequire, onEqual)
	onRequire = append(onRequire, requires...)
	return s.On(onRequire...)
}

func (s *joinScript) Join(joinTypeString string, leftTable JoinTableScript, rightTable JoinTableScript, joinRequire JoinRequire) JoinScript {
	if joinTypeString == EmptyString {
		joinTypeString = SqlJoinInner
	}
	if leftTable == nil || leftTable.IsEmpty() {
		leftTable = s.master
	}
	if rightTable == nil || rightTable.IsEmpty() {
		return s
	}
	join := &joinTable{
		joinType:   joinTypeString,
		rightTable: rightTable,
	}
	if joinRequire != nil {
		joinRequirePrepare, joinRequireArgs := joinRequire(leftTable.GetAlias(), rightTable.GetAlias())
		if joinRequirePrepare != EmptyString {
			join.joinRequire = NewScript(joinRequirePrepare, joinRequireArgs)
		}
	}
	s.joins = append(s.joins, join)
	return s
}

func (s *joinScript) InnerJoin(leftTable JoinTableScript, rightTable JoinTableScript, joinRequire JoinRequire) JoinScript {
	return s.Join(SqlJoinInner, leftTable, rightTable, joinRequire)
}

func (s *joinScript) LeftJoin(leftTable JoinTableScript, rightTable JoinTableScript, joinRequire JoinRequire) JoinScript {
	return s.Join(SqlJoinLeft, leftTable, rightTable, joinRequire)
}

func (s *joinScript) RightJoin(leftTable JoinTableScript, rightTable JoinTableScript, joinRequire JoinRequire) JoinScript {
	return s.Join(SqlJoinRight, leftTable, rightTable, joinRequire)
}

func (s *joinScript) Where(where func(where Filter)) JoinScript {
	if where == nil {
		return s
	}
	where(s.filter)
	return s
}

func (s *joinScript) SelectExtendColumns(custom func(sc *SelectColumns)) JoinScript {
	if s.selectExtendColumns == nil {
		s.selectExtendColumns = NewSelectColumns()
	}
	if custom != nil {
		custom(s.selectExtendColumns)
	}
	return s
}

// GroupScript Constructing query groups.
type GroupScript interface {
	IsEmpty

	Script

	Group(columns ...string) GroupScript

	Having(having func(having Filter)) GroupScript
}

type groupScript struct {
	group    []string
	groupMap map[string]int
	having   Filter
}

func (s *groupScript) IsEmpty() bool {
	return len(s.group) == 0
}

func (s *groupScript) Script() (prepare string, args []interface{}) {
	if s.IsEmpty() {
		return
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString("GROUP BY ")
	b.WriteString(strings.Join(s.group, ", "))
	if !s.having.IsEmpty() {
		b.WriteString(" HAVING ")
		having, havingArgs := s.having.Script()
		b.WriteString(having)
		if havingArgs != nil {
			args = append(args, havingArgs...)
		}
	}
	prepare = b.String()
	return
}

func (s *groupScript) Group(columns ...string) GroupScript {
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		if _, ok := s.groupMap[column]; ok {
			continue
		}
		s.groupMap[column] = len(s.group)
		s.group = append(s.group, column)
	}
	return s
}

func (s *groupScript) Having(having func(having Filter)) GroupScript {
	if having != nil {
		having(s.having)
	}
	return s
}

func NewGroupScript() GroupScript {
	return &groupScript{
		group:    make([]string, 0, 8),
		groupMap: make(map[string]int, 8),
		having:   F(),
	}
}

// OrderScript Constructing query orders.
type OrderScript interface {
	IsEmpty

	Script

	Asc(columns ...string) OrderScript

	Desc(columns ...string) OrderScript
}

type orderScript struct {
	orderBy  []string
	orderMap map[string]int
}

func (s *orderScript) IsEmpty() bool {
	return len(s.orderBy) == 0
}

func (s *orderScript) Script() (prepare string, args []interface{}) {
	if s.IsEmpty() {
		return
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString("ORDER BY ")
	b.WriteString(strings.Join(s.orderBy, ", "))
	prepare = b.String()
	return
}

func (s *orderScript) Asc(columns ...string) OrderScript {
	index := len(s.orderBy)
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		if _, ok := s.orderMap[column]; ok {
			continue
		}
		s.orderMap[column] = index
		index++
		column = fmt.Sprintf("%s %s", column, SqlAsc)
		s.orderBy = append(s.orderBy, column)
	}
	return s
}

func (s *orderScript) Desc(columns ...string) OrderScript {
	index := len(s.orderBy)
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		if _, ok := s.orderMap[column]; ok {
			continue
		}
		s.orderMap[column] = index
		index++
		column = fmt.Sprintf("%s %s", column, SqlDesc)
		s.orderBy = append(s.orderBy, column)
	}
	return s
}

func NewOrderScript() OrderScript {
	return &orderScript{
		orderBy:  make([]string, 0, 8),
		orderMap: make(map[string]int, 8),
	}
}

// LimitScript Constructing query limits.
type LimitScript interface {
	IsEmpty

	Script

	Limit(limit int64) LimitScript

	Offset(offset int64) LimitScript

	Page(page int64, limit ...int64) LimitScript
}

type limitScript struct {
	limit  *int64
	offset *int64
}

func (s *limitScript) IsEmpty() bool {
	return s.limit == nil
}

func (s *limitScript) Script() (prepare string, args []interface{}) {
	if s.IsEmpty() {
		return
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(fmt.Sprintf("LIMIT %d", *s.limit))
	if s.offset != nil && *s.offset >= 0 {
		b.WriteString(fmt.Sprintf(" OFFSET %d", *s.offset))
	}
	prepare = b.String()
	return
}

func (s *limitScript) Limit(limit int64) LimitScript {
	if limit > 0 {
		s.limit = &limit
	}
	return s
}

func (s *limitScript) Offset(offset int64) LimitScript {
	if offset > 0 {
		s.offset = &offset
	}
	return s
}

func (s *limitScript) Page(page int64, limit ...int64) LimitScript {
	if page <= 0 {
		return s
	}
	for i := len(limit) - 1; i >= 0; i-- {
		if limit[i] > 0 {
			s.Limit(limit[i]).Offset((page - 1) * limit[i])
			break
		}
	}
	return s
}

func NewLimitScript() LimitScript {
	return &limitScript{}
}

// ConcatScript Concat multiple scripts.
func ConcatScript(custom func(index int, script Script) Script, scripts ...Script) Script {
	b := getStringBuilder()
	defer putStringBuilder(b)
	args := make([]interface{}, 0, 32)
	index := 0
	for _, script := range scripts {
		if script == nil || IsEmptyScript(script) {
			continue
		}
		if custom != nil {
			script = custom(index, script)
		}
		if script == nil || IsEmptyScript(script) {
			continue
		}
		prepare, param := script.Script()
		if index > 0 {
			b.WriteString(SqlSpace)
		}
		b.WriteString(prepare)
		args = append(args, param...)
		index++
	}
	return NewScript(b.String(), args)
}

// UnionScript (query1) UNION (query2) UNION (query3)...
func UnionScript(scripts ...Script) Script {
	return ConcatScript(func(index int, script Script) Script {
		if index == 0 {
			return script
		}
		prepare, args := script.Script()
		b := getStringBuilder()
		defer putStringBuilder(b)
		b.WriteString("UNION ( ")
		b.WriteString(prepare)
		b.WriteString(" )")
		return NewScript(b.String(), args)
	}, scripts...)
}

// UnionAllScript (query1) UNION ALL (query2) UNION ALL (query3)...
func UnionAllScript(scripts ...Script) Script {
	return ConcatScript(func(index int, script Script) Script {
		if index == 0 {
			return script
		}
		prepare, args := script.Script()
		b := getStringBuilder()
		defer putStringBuilder(b)
		b.WriteString("UNION ALL ( ")
		b.WriteString(prepare)
		b.WriteString(" )")
		return NewScript(b.String(), args)
	}, scripts...)
}

// ExceptScript (query1) EXCEPT (query2) EXCEPT (query3)...
func ExceptScript(scripts ...Script) Script {
	return ConcatScript(func(index int, script Script) Script {
		prepare, args := script.Script()
		if index == 0 {
			prepare = fmt.Sprintf("( %s )", prepare)
			return NewScript(prepare, args)
		}
		b := getStringBuilder()
		defer putStringBuilder(b)
		b.WriteString(SqlExpect)
		b.WriteString(" ( ")
		b.WriteString(prepare)
		b.WriteString(" )")
		return NewScript(b.String(), args)
	}, scripts...)
}

// IntersectScript (query1) INTERSECT (query2) INTERSECT (query3)...
func IntersectScript(scripts ...Script) Script {
	return ConcatScript(func(index int, script Script) Script {
		prepare, args := script.Script()
		if index == 0 {
			prepare = fmt.Sprintf("( %s )", prepare)
			return NewScript(prepare, args)
		}
		b := getStringBuilder()
		defer putStringBuilder(b)
		b.WriteString(SqlIntersect)
		b.WriteString(" ( ")
		b.WriteString(prepare)
		b.WriteString(" )")
		return NewScript(b.String(), args)
	}, scripts...)
}

// InsertFieldsScript Constructing insert fields.
type InsertFieldsScript interface {
	IsEmpty

	Script

	Add(fields ...string) InsertFieldsScript

	Del(fields ...string) InsertFieldsScript

	DelUseIndex(indexes ...int) InsertFieldsScript

	FieldIndex(field string) int

	FieldExists(field string) bool

	Range(custom func(index int, field string) (toBreak bool)) InsertFieldsScript

	Len() int

	SetFields(fields []string) InsertFieldsScript

	GetFields() []string
}

type insertFieldsScript struct {
	identifier Identifier
	fields     []string
	fieldsMap  map[string]int
}

func NewInsertFieldScript() InsertFieldsScript {
	return &insertFieldsScript{
		fields:    make([]string, 0, 32),
		fieldsMap: make(map[string]int, 32),
	}
}

func (s *insertFieldsScript) IsEmpty() bool {
	return len(s.fields) == 0
}

func (s *insertFieldsScript) Script() (prepare string, args []interface{}) {
	if s.IsEmpty() {
		return
	}
	return ConcatString("( ", strings.Join(s.fields, ", "), " )"), nil
}

func (s *insertFieldsScript) Identifier(identifier Identifier) InsertFieldsScript {
	s.identifier = identifier
	return s
}

func (s *insertFieldsScript) Add(fields ...string) InsertFieldsScript {
	num := len(s.fields)
	for _, column := range fields {
		if column == EmptyString {
			continue
		}
		exists := column
		if s.identifier != nil {
			exists = s.identifier.SymbolDelOne(exists)
		}
		if _, ok := s.fieldsMap[exists]; ok {
			continue
		}
		s.fields = append(s.fields, column)
		s.fieldsMap[column] = num
		num++
	}
	return s
}

func (s *insertFieldsScript) Del(fields ...string) InsertFieldsScript {
	deleted := make(map[int]*struct{}, len(fields))
	for _, column := range fields {
		if column == EmptyString {
			continue
		}
		exists := column
		if s.identifier != nil {
			exists = s.identifier.SymbolDelOne(exists)
		}
		index, ok := s.fieldsMap[exists]
		if !ok {
			continue
		}
		deleted[index] = &struct{}{}
	}
	length := len(s.fields)
	fields = make([]string, 0, length)
	columnsMap := make(map[string]int, length)
	num := 0
	for index, column := range s.fields {
		if _, ok := deleted[index]; !ok {
			fields = append(fields, column)
			columnsMap[column] = num
			num++
		}
	}
	s.fields, s.fieldsMap = fields, columnsMap
	return s
}

func (s *insertFieldsScript) DelUseIndex(indexes ...int) InsertFieldsScript {
	length := len(s.fields)
	minIndex, maxIndex := 0, length
	if maxIndex == minIndex {
		return s
	}
	deleted := make(map[int]*struct{}, len(indexes))
	for _, index := range indexes {
		if index >= minIndex && index < maxIndex {
			deleted[index] = &struct{}{}
		}
	}
	fields := make([]string, 0, length)
	for index := range s.fields {
		if _, ok := deleted[index]; ok {
			continue
		}
		fields = append(fields, s.fields[index])
	}
	s.fields = fields
	return s
}

func (s *insertFieldsScript) FieldIndex(field string) int {
	exists := field
	if s.identifier != nil {
		exists = s.identifier.SymbolDelOne(exists)
	}
	index, ok := s.fieldsMap[exists]
	if !ok {
		return -1
	}
	return index
}

func (s *insertFieldsScript) FieldExists(field string) bool {
	return s.FieldIndex(field) >= 0
}

func (s *insertFieldsScript) Range(custom func(index int, field string) (toBreak bool)) InsertFieldsScript {
	if custom == nil {
		return s
	}
	for index, field := range s.fields {
		if custom(index, field) {
			break
		}
	}
	return s
}

func (s *insertFieldsScript) SetFields(fields []string) InsertFieldsScript {
	return s.Add(fields...)
}

func (s *insertFieldsScript) GetFields() []string {
	return s.fields[:]
}

func (s *insertFieldsScript) Len() int {
	return len(s.fields)
}

// InsertValuesScript Constructing insert values.
type InsertValuesScript interface {
	IsEmpty

	Script

	SetScript(script Script) InsertValuesScript

	SetValues(values ...[]interface{}) InsertValuesScript

	Set(index int, value interface{}) InsertValuesScript

	Del(indexes ...int) InsertValuesScript

	LenValues() int

	GetValues() [][]interface{}
}

type insertValuesScript struct {
	script Script
	values [][]interface{}
}

func NewInsertValuesScript() InsertValuesScript {
	return &insertValuesScript{}
}

func (s *insertValuesScript) IsEmpty() bool {
	return IsEmptyScript(s.script) && (len(s.values) == 0 || len(s.values[0]) == 0)
}

func (s *insertValuesScript) Script() (prepare string, args []interface{}) {
	if s.IsEmpty() {
		return
	}
	if !IsEmptyScript(s.script) {
		prepare, args = s.script.Script()
		return
	}
	count := len(s.values)
	length := len(s.values[0])
	line := make([]string, length)
	args = make([]interface{}, 0, count*length)
	for i := 0; i < length; i++ {
		line[i] = SqlPlaceholder
	}
	rows := make([]string, count)
	for i := 0; i < count; i++ {
		rows[i] = ConcatString("( ", strings.Join(line, ", "), " )")
		args = append(args, s.values[i]...)
	}
	prepare = strings.Join(rows, ", ")
	return
}

func (s *insertValuesScript) SetScript(script Script) InsertValuesScript {
	s.script = script
	return s
}

func (s *insertValuesScript) SetValues(values ...[]interface{}) InsertValuesScript {
	s.values = values
	return s
}

func (s *insertValuesScript) Set(index int, value interface{}) InsertValuesScript {
	if index < 0 {
		return s
	}
	if s.values == nil {
		s.values = make([][]interface{}, 1)
	}
	for num, tmp := range s.values {
		length := len(tmp)
		if index > length {
			continue
		}
		if index == length {
			s.values[num] = append(s.values[num], value)
		} else {
			s.values[num][index] = value
		}
	}
	return s
}

func (s *insertValuesScript) Del(indexes ...int) InsertValuesScript {
	if s.values == nil {
		return s
	}
	length := len(indexes)
	if length == 0 {
		return s
	}
	deleted := make(map[int]*struct{}, length)
	for _, index := range indexes {
		if index < 0 {
			continue
		}
		deleted[index] = &struct{}{}
	}
	length = len(deleted)
	if length == 0 {
		return s
	}
	values := make([][]interface{}, len(s.values))
	for index, value := range s.values {
		values[index] = make([]interface{}, 0, len(value))
		for num, tmp := range value {
			if _, ok := deleted[num]; ok {
				continue
			}
			values[index] = append(values[index], tmp)
		}
	}
	s.values = values
	return s
}

func (s *insertValuesScript) LenValues() int {
	return len(s.values)
}

func (s *insertValuesScript) GetValues() [][]interface{} {
	return s.values
}

// UpdateSetScript Constructing update sets.
type UpdateSetScript interface {
	IsEmpty

	Script

	Identifier(identifier Identifier) UpdateSetScript

	Update(update string, args ...interface{}) UpdateSetScript

	Set(column string, value interface{}) UpdateSetScript

	Decr(column string, decr interface{}) UpdateSetScript

	Incr(column string, incr interface{}) UpdateSetScript

	SetMap(columnValue map[string]interface{}) UpdateSetScript

	SetSlice(columns []string, values []interface{}) UpdateSetScript

	Len() int

	GetUpdate() (updates []string, args [][]interface{})

	UpdateIndex(prepare string) int

	UpdateExists(prepare string) bool
}

type updateSetScript struct {
	identifier Identifier
	update     []string
	updateArgs [][]interface{}
	updateMap  map[string]int
}

func NewUpdateSetScript() UpdateSetScript {
	return &updateSetScript{
		update:     make([]string, 0, 8),
		updateArgs: make([][]interface{}, 0, 8),
		updateMap:  make(map[string]int, 8),
	}
}

func (s *updateSetScript) IsEmpty() bool {
	return len(s.update) == 0
}

func (s *updateSetScript) Script() (prepare string, args []interface{}) {
	if s.IsEmpty() {
		return
	}
	prepare = strings.Join(s.update, ", ")
	for _, tmp := range s.updateArgs {
		args = append(args, tmp...)
	}
	return
}

func (s *updateSetScript) beautifyUpdate(update string) string {
	update = strings.TrimSpace(update)
	for strings.Contains(update, "  ") {
		update = strings.ReplaceAll(update, "  ", " ")
	}
	return update
}

func (s *updateSetScript) Identifier(identifier Identifier) UpdateSetScript {
	s.identifier = identifier
	return s
}

func (s *updateSetScript) Update(update string, args ...interface{}) UpdateSetScript {
	if update == EmptyString {
		return s
	}
	update = s.beautifyUpdate(update)
	if update == EmptyString {
		return s
	}
	exists := update
	if s.identifier != nil {
		exists = s.identifier.SymbolDelOne(exists)
	}
	index, ok := s.updateMap[exists]
	if ok {
		s.update[index], s.updateArgs[index] = update, args
		return s
	}
	s.updateMap[update] = len(s.update)
	s.update = append(s.update, update)
	s.updateArgs = append(s.updateArgs, args)
	return s
}

func (s *updateSetScript) Set(column string, value interface{}) UpdateSetScript {
	return s.Update(fmt.Sprintf("%s = %s", column, SqlPlaceholder), value)
}

func (s *updateSetScript) Decr(column string, decrement interface{}) UpdateSetScript {
	return s.Update(fmt.Sprintf("%s = %s - %s", column, column, SqlPlaceholder), decrement)
}

func (s *updateSetScript) Incr(column string, increment interface{}) UpdateSetScript {
	return s.Update(fmt.Sprintf("%s = %s + %s", column, column, SqlPlaceholder), increment)
}

func (s *updateSetScript) SetMap(columnValue map[string]interface{}) UpdateSetScript {
	for column, value := range columnValue {
		s.Set(column, value)
	}
	return s
}

func (s *updateSetScript) SetSlice(columns []string, values []interface{}) UpdateSetScript {
	for index, column := range columns {
		s.Set(column, values[index])
	}
	return s
}

func (s *updateSetScript) Len() int {
	return len(s.update)
}

func (s *updateSetScript) GetUpdate() ([]string, [][]interface{}) {
	return s.update, s.updateArgs
}

func (s *updateSetScript) UpdateIndex(update string) int {
	update = s.beautifyUpdate(update)
	exists := update
	if s.identifier != nil {
		exists = s.identifier.SymbolDelOne(exists)
	}
	index, ok := s.updateMap[exists]
	if !ok {
		return -1
	}
	return index
}

func (s *updateSetScript) UpdateExists(update string) bool {
	return s.UpdateIndex(update) >= 0
}

type Helper interface {
	// DriverName Get the driver name.
	DriverName() []byte

	// DataSourceName Get the data source name.
	DataSourceName() []byte

	// SetIdentifier Custom Identifier.
	SetIdentifier(identifier Identifier) Helper

	Identifier

	// SetPrepare Custom method.
	SetPrepare(prepare func(prepare string) string) Helper

	// Prepare Before executing preprocessing, adjust the preprocessing SQL format.
	Prepare(prepare string) string

	// SetIfNull Custom method.
	SetIfNull(ifNull func(columnName string, columnDefaultValue string) string) Helper

	// IfNull Set a default value when the query field value is NULL.
	IfNull(columnName string, columnDefaultValue string) string

	// SetBinaryDataToHexString Custom method.
	SetBinaryDataToHexString(binaryDataToHexString func(binaryData []byte) string) Helper

	// BinaryDataToHexString Convert binary data to hexadecimal string.
	BinaryDataToHexString(binaryData []byte) string
}

// MysqlHelper helper for mysql.
type MysqlHelper struct {
	driverName     []byte
	dataSourceName []byte

	Identifier

	prepare               func(prepare string) string
	ifNull                func(columnName string, columnDefaultValue string) string
	binaryDataToHexString func(binaryData []byte) string
}

func (s *MysqlHelper) DriverName() []byte {
	return s.driverName
}

func (s *MysqlHelper) DataSourceName() []byte {
	return s.dataSourceName
}

func (s *MysqlHelper) SetIdentifier(identifier Identifier) Helper {
	if identifier != nil {
		s.Identifier = identifier
	}
	return s
}

func (s *MysqlHelper) SetPrepare(prepare func(prepare string) string) Helper {
	if prepare != nil {
		s.prepare = prepare
	}
	return s
}

func (s *MysqlHelper) Prepare(prepare string) string {
	if s.prepare != nil {
		return s.prepare(prepare)
	}
	return prepare
}

func (s *MysqlHelper) SetIfNull(ifNull func(columnName string, columnDefaultValue string) string) Helper {
	if ifNull != nil {
		s.ifNull = ifNull
	}
	return s
}

func (s *MysqlHelper) IfNull(columnName string, columnDefaultValue string) string {
	if s.ifNull != nil {
		return s.ifNull(columnName, columnDefaultValue)
	}
	return fmt.Sprintf("IFNULL(%s,%s)", columnName, columnDefaultValue)
}

func (s *MysqlHelper) SetBinaryDataToHexString(binaryDataToHexString func(binaryData []byte) string) Helper {
	if binaryDataToHexString != nil {
		s.binaryDataToHexString = binaryDataToHexString
	}
	return s
}

func (s *MysqlHelper) BinaryDataToHexString(binaryData []byte) string {
	if s.binaryDataToHexString != nil {
		return s.binaryDataToHexString(binaryData)
	}
	return fmt.Sprintf("UNHEX('%s')", hex.EncodeToString(binaryData))
}

func NewMysqlHelper() *MysqlHelper {
	return &MysqlHelper{
		Identifier: NewIdentifier("`"),
	}
}

// PostgresHelper helper for postgresql.
type PostgresHelper struct {
	driverName     []byte
	dataSourceName []byte

	Identifier

	prepare               func(prepare string) string
	ifNull                func(columnName string, columnDefaultValue string) string
	binaryDataToHexString func(binaryData []byte) string
}

func (s *PostgresHelper) DriverName() []byte {
	return s.driverName
}

func (s *PostgresHelper) DataSourceName() []byte {
	return s.dataSourceName
}

func (s *PostgresHelper) SetIdentifier(identifier Identifier) Helper {
	if identifier != nil {
		s.Identifier = identifier
	}
	return s
}

func (s *PostgresHelper) SetPrepare(prepare func(prepare string) string) Helper {
	if prepare != nil {
		s.prepare = prepare
	}
	return s
}

func (s *PostgresHelper) Prepare(prepare string) string {
	if s.prepare != nil {
		return s.prepare(prepare)
	}
	var index int64
	latest := getStringBuilder()
	defer putStringBuilder(latest)
	origin := []byte(prepare)
	length := len(origin)
	dollar := byte('$')       // $
	questionMark := byte('?') // ?
	for i := 0; i < length; i++ {
		if origin[i] == questionMark {
			index++
			latest.WriteByte(dollar)
			latest.WriteString(strconv.FormatInt(index, 10))
		} else {
			latest.WriteByte(origin[i])
		}
	}
	return latest.String()
}

func (s *PostgresHelper) SetIfNull(ifNull func(columnName string, columnDefaultValue string) string) Helper {
	if ifNull != nil {
		s.ifNull = ifNull
	}
	return s
}

func (s *PostgresHelper) IfNull(columnName string, columnDefaultValue string) string {
	if s.ifNull != nil {
		return s.ifNull(columnName, columnDefaultValue)
	}
	return fmt.Sprintf("COALESCE(%s,%s)", columnName, columnDefaultValue)
}

func (s *PostgresHelper) SetBinaryDataToHexString(binaryDataToHexString func(binaryData []byte) string) Helper {
	if binaryDataToHexString != nil {
		s.binaryDataToHexString = binaryDataToHexString
	}
	return s
}

func (s *PostgresHelper) BinaryDataToHexString(binaryData []byte) string {
	if s.binaryDataToHexString != nil {
		return s.binaryDataToHexString(binaryData)
	}
	return fmt.Sprintf(`E'\\x%s'`, hex.EncodeToString(binaryData))
}

func NewPostgresHelper() *PostgresHelper {
	return &PostgresHelper{
		Identifier: NewIdentifier(`"`),
	}
}

// Sqlite3Helper helper for sqlite3.
type Sqlite3Helper struct {
	driverName     []byte
	dataSourceName []byte

	Identifier

	prepare               func(prepare string) string
	ifNull                func(columnName string, columnDefaultValue string) string
	binaryDataToHexString func(binaryData []byte) string
}

func (s *Sqlite3Helper) DriverName() []byte {
	return s.driverName
}

func (s *Sqlite3Helper) DataSourceName() []byte {
	return s.dataSourceName
}

func (s *Sqlite3Helper) SetIdentifier(identifier Identifier) Helper {
	if identifier != nil {
		s.Identifier = identifier
	}
	return s
}

func (s *Sqlite3Helper) SetPrepare(prepare func(prepare string) string) Helper {
	if prepare != nil {
		s.prepare = prepare
	}
	return s
}

func (s *Sqlite3Helper) Prepare(prepare string) string {
	if s.prepare != nil {
		return s.prepare(prepare)
	}
	return prepare
}

func (s *Sqlite3Helper) SetIfNull(ifNull func(columnName string, columnDefaultValue string) string) Helper {
	if ifNull != nil {
		s.ifNull = ifNull
	}
	return s
}

func (s *Sqlite3Helper) IfNull(columnName string, columnDefaultValue string) string {
	if s.ifNull != nil {
		return s.ifNull(columnName, columnDefaultValue)
	}
	return fmt.Sprintf("COALESCE(%s,%s)", columnName, columnDefaultValue)
}

func (s *Sqlite3Helper) SetBinaryDataToHexString(binaryDataToHexString func(binaryData []byte) string) Helper {
	if binaryDataToHexString != nil {
		s.binaryDataToHexString = binaryDataToHexString
	}
	return s
}

func (s *Sqlite3Helper) BinaryDataToHexString(binaryData []byte) string {
	if s.binaryDataToHexString != nil {
		return s.binaryDataToHexString(binaryData)
	}
	return fmt.Sprintf(`X'%s'`, hex.EncodeToString(binaryData))
}

func NewSqlite3Helper() *Sqlite3Helper {
	return &Sqlite3Helper{
		Identifier: NewIdentifier("`"),
	}
}

/**
 * sql identifier.
 **/

type AdjustColumn struct {
	alias string
	way   *Way
}

// Alias Get the alias name value.
func (s *AdjustColumn) Alias() string {
	return s.alias
}

// SetAlias Set the alias name value.
func (s *AdjustColumn) SetAlias(alias string) *AdjustColumn {
	s.alias = alias
	return s
}

// Adjust Batch adjust columns.
func (s *AdjustColumn) Adjust(adjust func(column string) string, columns ...string) []string {
	if adjust != nil {
		for index, column := range columns {
			columns[index] = adjust(column)
		}
	}
	return columns
}

// ColumnAll Add table name prefix to column names in batches.
func (s *AdjustColumn) ColumnAll(columns ...string) []string {
	if s.alias == EmptyString {
		return columns
	}
	prefix := fmt.Sprintf("%s%s", s.alias, SqlPoint)
	for index, column := range columns {
		if !strings.HasPrefix(column, prefix) {
			columns[index] = fmt.Sprintf("%s%s", prefix, column)
		}
	}
	return columns
}

// Column Add table name prefix to single column name, allowing column alias to be set.
func (s *AdjustColumn) Column(column string, aliases ...string) string {
	return SqlAlias(s.ColumnAll(column)[0], LastNotEmptyString(aliases))
}

// Sum SUM(column[, alias])
func (s *AdjustColumn) Sum(column string, aliases ...string) string {
	return SqlAlias(fmt.Sprintf("SUM(%s)", s.Column(column)), LastNotEmptyString(aliases))
}

// Max MAX(column[, alias])
func (s *AdjustColumn) Max(column string, aliases ...string) string {
	return SqlAlias(fmt.Sprintf("MAX(%s)", s.Column(column)), LastNotEmptyString(aliases))
}

// Min MIN(column[, alias])
func (s *AdjustColumn) Min(column string, aliases ...string) string {
	return SqlAlias(fmt.Sprintf("MIN(%s)", s.Column(column)), LastNotEmptyString(aliases))
}

// Avg AVG(column[, alias])
func (s *AdjustColumn) Avg(column string, aliases ...string) string {
	return SqlAlias(fmt.Sprintf("AVG(%s)", s.Column(column)), LastNotEmptyString(aliases))
}

// Count Example
// Count(): `COUNT(*) AS counts`
// Count("total"): `COUNT(*) AS total`
// Count("1", "total"): `COUNT(1) AS total`
// Count("id", "counts"): `COUNT(id) AS counts`
func (s *AdjustColumn) Count(counts ...string) string {
	count := "COUNT(*)"
	length := len(counts)
	if length == 0 {
		// using default expression: `COUNT(*) AS counts`
		return SqlAlias(count, s.way.cfg.Helper.SymbolAddOne(DefaultAliasNameCount))
	}
	if length == 1 && counts[0] != EmptyString {
		// only set alias name
		return SqlAlias(count, counts[0])
	}
	// set COUNT function parameters and alias name
	countAlias := s.way.cfg.Helper.SymbolAddOne(DefaultAliasNameCount)
	field := false
	for i := 0; i < length; i++ {
		if counts[i] == EmptyString {
			continue
		}
		if field {
			countAlias = counts[i]
			break
		}
		count, field = fmt.Sprintf("COUNT(%s)", counts[i]), true
	}
	return SqlAlias(count, countAlias)
}

// IfNull If the value is NULL, set the default value.
func (s *AdjustColumn) IfNull(column string, defaultValue string, aliases ...string) string {
	return SqlAlias(s.way.cfg.Helper.IfNull(s.Column(column), defaultValue), LastNotEmptyString(aliases))
}

// IfNullSum IF_NULL(SUM(column),0)[ AS column_name]
func (s *AdjustColumn) IfNullSum(column string, aliases ...string) string {
	return SqlAlias(s.IfNull(s.Sum(column), "0"), LastNotEmptyString(aliases))
}

// IfNullMax IF_NULL(MAX(column),0)[ AS column_name]
func (s *AdjustColumn) IfNullMax(column string, aliases ...string) string {
	return SqlAlias(s.IfNull(s.Max(column), "0"), LastNotEmptyString(aliases))
}

// IfNullMin IF_NULL(MIN(column),0)[ AS column_name]
func (s *AdjustColumn) IfNullMin(column string, aliases ...string) string {
	return SqlAlias(s.IfNull(s.Min(column), "0"), LastNotEmptyString(aliases))
}

// IfNullAvg IF_NULL(AVG(column),0)[ AS column_name]
func (s *AdjustColumn) IfNullAvg(column string, aliases ...string) string {
	return SqlAlias(s.IfNull(s.Avg(column), "0"), LastNotEmptyString(aliases))
}

func NewAdjustColumn(way *Way, aliases ...string) *AdjustColumn {
	return &AdjustColumn{
		alias: LastNotEmptyString(aliases),
		way:   way,
	}
}

/**
 * sql window functions.
 **/

// WindowFunc sql window function.
type WindowFunc struct {
	// Helper Database helper.
	Helper Helper

	// withFunc The window function used.
	withFunc string

	// partition Setting up window partitions.
	partition []string

	// order Sorting data within a group.
	order []string

	// windowFrame Window frame clause. `ROWS` or `RANGE`.
	windowFrame string

	// alias Serial number column alias.
	alias string
}

// WithFunc Using custom function. for example: CUME_DIST(), PERCENT_RANK(), PERCENTILE_CONT(), PERCENTILE_DISC()...
func (s *WindowFunc) WithFunc(withFunc string) *WindowFunc {
	s.withFunc = withFunc
	return s
}

// RowNumber ROW_NUMBER() Assign a unique serial number to each row, in the order specified, starting with 1.
func (s *WindowFunc) RowNumber() *WindowFunc {
	return s.WithFunc("ROW_NUMBER()")
}

// Rank RANK() Assign a rank to each row, if there are duplicate values, the rank is skipped.
func (s *WindowFunc) Rank() *WindowFunc {
	return s.WithFunc("RANK()")
}

// DenseRank DENSE_RANK() Similar to RANK(), but does not skip rankings.
func (s *WindowFunc) DenseRank() *WindowFunc {
	return s.WithFunc("DENSE_RANK()")
}

// Ntile NTILE() Divide the rows in the window into n buckets and assign a bucket number to each row.
func (s *WindowFunc) Ntile(buckets int64) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("NTILE(%d)", buckets))
}

// Sum SUM() Returns the sum of all rows in the window.
func (s *WindowFunc) Sum(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("SUM(%s)", column))
}

// Max MAX() Returns the maximum value within the window.
func (s *WindowFunc) Max(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("MAX(%s)", column))
}

// Min MIN() Returns the minimum value within the window.
func (s *WindowFunc) Min(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("MIN(%s)", column))
}

// Avg AVG() Returns the average of all rows in the window.
func (s *WindowFunc) Avg(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("AVG(%s)", column))
}

// Count COUNT() Returns the number of rows in the window.
func (s *WindowFunc) Count(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("COUNT(%s)", column))
}

// Lag LAG() Returns the value of the row before the current row.
func (s *WindowFunc) Lag(column string, offset int64, defaultValue any) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("LAG(%s, %d, %s)", column, offset, ArgString(s.Helper, defaultValue)))
}

// Lead LEAD() Returns the value of a row after the current row.
func (s *WindowFunc) Lead(column string, offset int64, defaultValue any) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("LEAD(%s, %d, %s)", column, offset, ArgString(s.Helper, defaultValue)))
}

// NthValue NTH_VALUE() The Nth value can be returned according to the specified order. This is very useful when you need to get data at a specific position.
func (s *WindowFunc) NthValue(column string, LineNumber int64) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("NTH_VALUE(%s, %d)", column, LineNumber))
}

// FirstValue FIRST_VALUE() Returns the value of the first row in the window.
func (s *WindowFunc) FirstValue(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("FIRST_VALUE(%s)", column))
}

// LastValue LAST_VALUE() Returns the value of the last row in the window.
func (s *WindowFunc) LastValue(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("LAST_VALUE(%s)", column))
}

// Partition The OVER clause defines window partitions so that the window function is calculated independently in each partition.
func (s *WindowFunc) Partition(column ...string) *WindowFunc {
	s.partition = append(s.partition, column...)
	return s
}

// Asc Define the sorting within the partition so that the window function is calculated in order.
func (s *WindowFunc) Asc(column string) *WindowFunc {
	s.order = append(s.order, fmt.Sprintf("%s %s", column, SqlAsc))
	return s
}

// Desc Define the sorting within the partition so that the window function is calculated in descending order.
func (s *WindowFunc) Desc(column string) *WindowFunc {
	s.order = append(s.order, fmt.Sprintf("%s %s", column, SqlDesc))
	return s
}

// WindowFrame Set custom window frame clause.
func (s *WindowFunc) WindowFrame(windowFrame string) *WindowFunc {
	s.windowFrame = windowFrame
	return s
}

// Alias Set the alias of the field that uses the window function.
func (s *WindowFunc) Alias(alias string) *WindowFunc {
	s.alias = alias
	return s
}

// Result Query column expressions.
func (s *WindowFunc) Result() string {
	if s.withFunc == EmptyString || s.partition == nil || s.order == nil || s.alias == EmptyString {
		panic("hey: the SQL window function parameters are incomplete.")
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(s.withFunc)
	b.WriteString(" OVER ( PARTITION BY ")
	b.WriteString(strings.Join(s.partition, ", "))
	b.WriteString(" ORDER BY ")
	b.WriteString(strings.Join(s.order, ", "))
	if s.windowFrame != "" {
		b.WriteString(" ")
		b.WriteString(s.windowFrame)
	}
	b.WriteString(" ) AS ")
	b.WriteString(s.alias)
	return b.String()
}

func NewWindowFunc(way *Way, aliases ...string) *WindowFunc {
	return &WindowFunc{
		Helper: way.cfg.Helper,
		alias:  LastNotEmptyString(aliases),
	}
}

// ScriptQuery execute the built SQL statement and scan query result.
func ScriptQuery(ctx context.Context, way *Way, script Script, query func(rows *sql.Rows) (err error)) error {
	prepare, args := script.Script()
	return way.QueryContext(ctx, query, prepare, args...)
}

// ScriptGet execute the built SQL statement and scan query result.
func ScriptGet(ctx context.Context, way *Way, script Script, result interface{}) error {
	prepare, args := script.Script()
	return way.TakeAllContext(ctx, result, prepare, args...)
}

// ScriptScanAll execute the built SQL statement and scan all from the query results.
func ScriptScanAll(ctx context.Context, way *Way, script Script, custom func(rows *sql.Rows) error) error {
	return ScriptQuery(ctx, way, script, func(rows *sql.Rows) error {
		return way.ScanAll(rows, custom)
	})
}

// ScriptScanOne execute the built SQL statement and scan at most once from the query results.
func ScriptScanOne(ctx context.Context, way *Way, script Script, dest ...interface{}) error {
	return ScriptQuery(ctx, way, script, func(rows *sql.Rows) error {
		return way.ScanOne(rows, dest...)
	})
}

// ScriptViewMap execute the built SQL statement and scan all from the query results.
func ScriptViewMap(ctx context.Context, way *Way, script Script) (result []map[string]interface{}, err error) {
	err = ScriptQuery(ctx, way, script, func(rows *sql.Rows) (err error) {
		result, err = ScanViewMap(rows)
		return
	})
	return
}
