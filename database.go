package hey

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// CmdTable Used to construct expressions that can use table aliases and their corresponding parameter lists.
type CmdTable interface {
	IsEmpty

	Cmd

	// Alias Setting aliases for script statements.
	Alias(alias string) CmdTable

	// GetAlias Getting aliases for script statements.
	GetAlias() string
}

type cmdTable struct {
	cmd   Cmd
	alias string
}

func (s *cmdTable) IsEmpty() bool {
	return IsEmptyCmd(s.cmd)
}

func (s *cmdTable) Cmd() (prepare string, args []interface{}) {
	if s.IsEmpty() {
		return
	}
	prepare, args = s.cmd.Cmd()
	if s.alias != EmptyString {
		prepare = ConcatString(prepare, SqlSpace, s.alias)
	}
	return
}

func (s *cmdTable) Alias(alias string) CmdTable {
	s.alias = alias // allow setting empty values
	return s
}

func (s *cmdTable) GetAlias() string {
	return s.alias
}

func NewCmdTable(prepare string, args []interface{}) CmdTable {
	return &cmdTable{
		cmd: NewCmd(prepare, args),
	}
}

func NewCmdGet(alias string, get *Get) CmdTable {
	return NewCmdTable(get.Cmd()).Alias(alias)
}

// QueryWith CTE: Common Table Expression.
type QueryWith interface {
	IsEmpty

	Cmd

	// Add Set common table expression.
	Add(alias string, cmd Cmd) QueryWith

	// Del Remove common table expression.
	Del(alias string) QueryWith
}

type queryWith struct {
	with    []string
	withMap map[string]Cmd
}

func NewQueryWith() QueryWith {
	return &queryWith{
		with:    make([]string, 0, 1<<3),
		withMap: make(map[string]Cmd, 1<<3),
	}
}

func (s *queryWith) IsEmpty() bool {
	return len(s.with) == 0
}

func (s *queryWith) Cmd() (prepare string, args []interface{}) {
	if s.IsEmpty() {
		return
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString("WITH")
	b.WriteString(SqlSpace)
	var param []interface{}
	for index, alias := range s.with {
		if index > 0 {
			b.WriteString(", ")
		}
		script := s.withMap[alias]
		b.WriteString(alias)
		b.WriteString(" AS ( ")
		prepare, param = script.Cmd()
		b.WriteString(prepare)
		b.WriteString(" )")
		args = append(args, param...)
	}
	prepare = b.String()
	return
}

func (s *queryWith) Add(alias string, cmd Cmd) QueryWith {
	if alias == EmptyString || IsEmptyCmd(cmd) {
		return s
	}
	if _, ok := s.withMap[alias]; ok {
		s.withMap[alias] = cmd
		return s
	}
	s.with = append(s.with, alias)
	s.withMap[alias] = cmd
	return s
}

func (s *queryWith) Del(alias string) QueryWith {
	if alias == EmptyString {
		return s
	}
	with := make([]string, 0, len(s.with))
	for _, tmp := range s.with {
		if tmp != alias {
			with = append(with, tmp)
		}
	}
	delete(s.withMap, alias)
	s.with = with
	return s
}

// QueryField Used to build the list of fields to be queried.
type QueryField interface {
	IsEmpty

	Cmd

	Index(field string) int

	Exists(field string) bool

	Add(field string, args ...interface{}) QueryField

	AddFields(fields ...string) QueryField

	DelFields(fields ...string) QueryField

	DelAll() QueryField

	Len() int

	Get() ([]string, map[int][]interface{})

	Set(fields []string, fieldsArgs map[int][]interface{}) QueryField
}

type queryField struct {
	fields     []string
	fieldsMap  map[string]int
	fieldsArgs map[int][]interface{}

	way *Way
}

func (s *Way) QueryField() QueryField {
	return &queryField{
		fields:     make([]string, 0, 1<<5),
		fieldsMap:  make(map[string]int, 1<<5),
		fieldsArgs: make(map[int][]interface{}, 1<<5),
		way:        s,
	}
}

func (s *queryField) IsEmpty() bool {
	return len(s.fields) == 0
}

func (s *queryField) Cmd() (prepare string, args []interface{}) {
	length := len(s.fields)
	if length == 0 {
		return SqlStar, nil
	}
	columns := make([]string, 0, length)
	for i := 0; i < length; i++ {
		tmpArgs, ok := s.fieldsArgs[i]
		if !ok {
			continue
		}
		columns = append(columns, s.fields[i])
		if tmpArgs != nil {
			args = append(args, tmpArgs...)
		}
	}
	prepare = strings.Join(s.way.NameReplaces(columns), ", ")
	return
}

func (s *queryField) Index(field string) int {
	index, ok := s.fieldsMap[field]
	if !ok {
		return -1
	}
	return index
}

func (s *queryField) Exists(field string) bool {
	return s.Index(field) >= 0
}

func (s *queryField) Add(field string, args ...interface{}) QueryField {
	if field == EmptyString {
		return s
	}
	index, ok := s.fieldsMap[field]
	if ok {
		s.fieldsArgs[index] = args
		return s
	}
	index = len(s.fields)
	s.fields = append(s.fields, field)
	s.fieldsMap[field] = index
	s.fieldsArgs[index] = args
	return s
}

func (s *queryField) AddFields(fields ...string) QueryField {
	for _, field := range fields {
		s.Add(field)
	}
	return s
}

func (s *queryField) DelFields(fields ...string) QueryField {
	deleted := make(map[int]*struct{}, len(fields))
	for _, field := range fields {
		if field == EmptyString {
			continue
		}
		index, ok := s.fieldsMap[field]
		if !ok {
			continue
		}
		deleted[index] = &struct{}{}
	}
	length := len(s.fields)
	result := make([]string, 0, length)
	for index, field := range s.fields {
		if _, ok := deleted[index]; ok {
			delete(s.fieldsMap, field)
			delete(s.fieldsArgs, index)
		} else {
			result = append(result, field)
		}
	}
	s.fields = result
	return s
}

func (s *queryField) DelAll() QueryField {
	s.fields = make([]string, 0, 1<<5)
	s.fieldsMap = make(map[string]int, 1<<5)
	s.fieldsArgs = make(map[int][]interface{}, 1<<5)
	return s
}

func (s *queryField) Len() int {
	return len(s.fields)
}

func (s *queryField) Get() ([]string, map[int][]interface{}) {
	return s.fields, s.fieldsArgs
}

func (s *queryField) Set(fields []string, fieldsArgs map[int][]interface{}) QueryField {
	fieldsMap := make(map[string]int, 1<<5)
	for i, field := range fields {
		fieldsMap[field] = i
	}
	s.fields, s.fieldsMap, s.fieldsArgs = fields, fieldsMap, fieldsArgs
	return s
}

// JoinRequire Constructing conditions for join queries.
type JoinRequire func(leftAlias string, rightAlias string) (prepare string, args []interface{})

// QueryJoinTable Constructing table for join queries.
type QueryJoinTable interface {
	CmdTable

	/* the table used for join query only supports querying the field list without any parameters */

	AddQueryField(fields ...string) QueryJoinTable

	DelQueryField(fields ...string) QueryJoinTable

	DelAllQueryField() QueryJoinTable

	ExistsQueryField() bool

	GetQueryField() []string

	GetQueryFieldString() string
}

type queryJoinTable struct {
	CmdTable
	queryField QueryField
}

func (s *queryJoinTable) AddQueryField(fields ...string) QueryJoinTable {
	s.queryField.AddFields(fields...)
	return s
}

func (s *queryJoinTable) DelQueryField(fields ...string) QueryJoinTable {
	s.queryField.DelFields(fields...)
	return s
}

func (s *queryJoinTable) DelAllQueryField() QueryJoinTable {
	s.queryField.DelAll()
	return s
}

func (s *queryJoinTable) ExistsQueryField() bool {
	return s.queryField.Len() > 0
}

func (s *queryJoinTable) GetQueryField() []string {
	if s.CmdTable == nil {
		return nil
	}
	alias := s.GetAlias()
	if alias == EmptyString {
		return nil
	}
	prefix := fmt.Sprintf("%s.", alias)
	columns, _ := s.queryField.Get()
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

func (s *queryJoinTable) GetQueryFieldString() string {
	return strings.Join(s.GetQueryField(), ", ")
}

func (s *Way) QueryJoinTable(table string, alias string, args ...interface{}) QueryJoinTable {
	return &queryJoinTable{
		CmdTable:   NewCmdTable(table, args).Alias(alias),
		queryField: s.QueryField(),
	}
}

// QueryJoin Constructing multi-table join queries.
type QueryJoin interface {
	Cmd

	GetMaster() QueryJoinTable

	SetMaster(master QueryJoinTable) QueryJoin

	NewTable(table string, alias string, args ...interface{}) QueryJoinTable

	NewSubquery(subquery Cmd, alias string) QueryJoinTable

	On(requires ...func(leftAlias string, rightAlias string) Cmd) JoinRequire

	Using(using []string, requires ...func(leftAlias string, rightAlias string) Cmd) JoinRequire

	OnEqual(leftColumn string, rightColumn string, requires ...func(leftAlias string, rightAlias string) Cmd) JoinRequire

	Join(joinTypeString string, leftTable QueryJoinTable, rightTable QueryJoinTable, joinRequire JoinRequire) QueryJoin

	InnerJoin(leftTable QueryJoinTable, rightTable QueryJoinTable, joinRequire JoinRequire) QueryJoin

	LeftJoin(leftTable QueryJoinTable, rightTable QueryJoinTable, joinRequire JoinRequire) QueryJoin

	RightJoin(leftTable QueryJoinTable, rightTable QueryJoinTable, joinRequire JoinRequire) QueryJoin

	// Where Don't forget to prefix the specific columns with the table name?
	Where(where func(where Filter)) QueryJoin

	// QueryExtendFields The queried column uses a conditional statement or calls a function (not the direct column name of the table); the table alias prefix is not automatically added.
	QueryExtendFields(custom func(fields QueryField)) QueryJoin
}

type joinQueryTable struct {
	joinType    string
	rightTable  QueryJoinTable
	joinRequire Cmd
}

type queryJoin struct {
	master QueryJoinTable
	joins  []*joinQueryTable
	filter Filter
	// queryExtendFields Here you can add a list of fields that cannot be set with parameters in the join query table; the table alias prefix is not automatically added.
	queryExtendFields QueryField

	way *Way
}

func (s *Way) QueryJoin() QueryJoin {
	tmp := &queryJoin{
		joins:             make([]*joinQueryTable, 0, 1<<3),
		filter:            F(),
		queryExtendFields: s.QueryField(),
		way:               s,
	}
	return tmp
}

func (s *queryJoin) GetMaster() QueryJoinTable {
	return s.master
}

func (s *queryJoin) SetMaster(master QueryJoinTable) QueryJoin {
	if master != nil && !master.IsEmpty() {
		s.master = master
	}
	return s
}

func (s *queryJoin) Cmd() (prepare string, args []interface{}) {
	columns := s.way.QueryField()
	if s.master.ExistsQueryField() {
		if column := s.master.GetQueryFieldString(); column != EmptyString {
			columns.Add(column)
		}
	}

	for _, tmp := range s.joins {
		if tmp.rightTable.ExistsQueryField() {
			if column := tmp.rightTable.GetQueryFieldString(); column != EmptyString {
				columns.Add(column)
			}
		}
	}
	if s.queryExtendFields != nil && s.queryExtendFields.Len() > 0 {
		extendColumnsPrepare, extendColumnsArgs := s.queryExtendFields.Cmd()
		columns.Add(extendColumnsPrepare, extendColumnsArgs...)
	}

	selectColumnsPrepare, selectColumnsArgs := columns.Cmd()
	if selectColumnsArgs != nil {
		args = append(args, selectColumnsArgs...)
	}

	b := getStringBuilder()
	defer putStringBuilder(b)

	b.WriteString("SELECT ")
	b.WriteString(selectColumnsPrepare)
	b.WriteString(" FROM ")
	masterPrepare, masterArgs := s.master.Cmd()
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
		joinPrepare, joinArgs := tmp.rightTable.Cmd()
		b.WriteString(joinPrepare)
		b.WriteString(SqlSpace)
		args = append(args, joinArgs...)
		if tmp.joinRequire != nil {
			joinRequirePrepare, joinRequireArgs := tmp.joinRequire.Cmd()
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

func (s *queryJoin) NewTable(table string, alias string, args ...interface{}) QueryJoinTable {
	return s.way.QueryJoinTable(table, alias, args...)
}

func (s *queryJoin) NewSubquery(subquery Cmd, alias string) QueryJoinTable {
	prepare, args := subquery.Cmd()
	return s.NewTable(prepare, alias, args...)
}

// On For `... JOIN ON ...`
func (s *queryJoin) On(requires ...func(leftAlias string, rightAlias string) Cmd) JoinRequire {
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
			prepare, param = script.Cmd()
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
func (s *queryJoin) Using(using []string, requires ...func(leftAlias string, rightAlias string) Cmd) JoinRequire {
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
			prepare, param = script.Cmd()
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
func (s *queryJoin) OnEqual(leftColumn string, rightColumn string, requires ...func(leftAlias string, rightAlias string) Cmd) JoinRequire {
	onRequire := make([]func(leftAlias string, rightAlias string) Cmd, 0, len(requires)+1)
	onEqual := func(leftAlias string, rightAlias string) Cmd {
		if leftColumn == EmptyString || rightColumn == EmptyString {
			return nil
		}
		return NewCmd(fmt.Sprintf("%s = %s", SqlPrefix(leftAlias, leftColumn), SqlPrefix(rightAlias, rightColumn)), nil)
	}
	onRequire = append(onRequire, onEqual)
	onRequire = append(onRequire, requires...)
	return s.On(onRequire...)
}

func (s *queryJoin) Join(joinTypeString string, leftTable QueryJoinTable, rightTable QueryJoinTable, joinRequire JoinRequire) QueryJoin {
	if joinTypeString == EmptyString {
		joinTypeString = SqlJoinInner
	}
	if leftTable == nil || leftTable.IsEmpty() {
		leftTable = s.master
	}
	if rightTable == nil || rightTable.IsEmpty() {
		return s
	}
	join := &joinQueryTable{
		joinType:   joinTypeString,
		rightTable: rightTable,
	}
	if joinRequire != nil {
		joinRequirePrepare, joinRequireArgs := joinRequire(leftTable.GetAlias(), rightTable.GetAlias())
		if joinRequirePrepare != EmptyString {
			join.joinRequire = NewCmd(joinRequirePrepare, joinRequireArgs)
		}
	}
	s.joins = append(s.joins, join)
	return s
}

func (s *queryJoin) InnerJoin(leftTable QueryJoinTable, rightTable QueryJoinTable, joinRequire JoinRequire) QueryJoin {
	return s.Join(SqlJoinInner, leftTable, rightTable, joinRequire)
}

func (s *queryJoin) LeftJoin(leftTable QueryJoinTable, rightTable QueryJoinTable, joinRequire JoinRequire) QueryJoin {
	return s.Join(SqlJoinLeft, leftTable, rightTable, joinRequire)
}

func (s *queryJoin) RightJoin(leftTable QueryJoinTable, rightTable QueryJoinTable, joinRequire JoinRequire) QueryJoin {
	return s.Join(SqlJoinRight, leftTable, rightTable, joinRequire)
}

func (s *queryJoin) Where(where func(where Filter)) QueryJoin {
	if where == nil {
		return s
	}
	where(s.filter)
	return s
}

func (s *queryJoin) QueryExtendFields(custom func(fields QueryField)) QueryJoin {
	if s.queryExtendFields == nil {
		s.queryExtendFields = s.way.QueryField()
	}
	if custom != nil {
		custom(s.queryExtendFields)
	}
	return s
}

// QueryGroup Constructing query groups.
type QueryGroup interface {
	IsEmpty

	Cmd

	Group(columns ...string) QueryGroup

	Having(having func(having Filter)) QueryGroup
}

type queryGroup struct {
	group    []string
	groupMap map[string]int
	having   Filter
	way      *Way
}

func (s *queryGroup) IsEmpty() bool {
	return len(s.group) == 0
}

func (s *queryGroup) Cmd() (prepare string, args []interface{}) {
	if s.IsEmpty() {
		return
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString("GROUP BY ")
	b.WriteString(strings.Join(s.way.NameReplaces(s.group), ", "))
	if !s.having.IsEmpty() {
		b.WriteString(" HAVING ")
		having, havingArgs := s.having.Cmd()
		b.WriteString(having)
		if havingArgs != nil {
			args = append(args, havingArgs...)
		}
	}
	prepare = b.String()
	return
}

func (s *queryGroup) Group(columns ...string) QueryGroup {
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

func (s *queryGroup) Having(having func(having Filter)) QueryGroup {
	if having != nil {
		having(s.having)
	}
	return s
}

func (s *Way) QueryGroup() QueryGroup {
	return &queryGroup{
		group:    make([]string, 0, 1<<3),
		groupMap: make(map[string]int, 1<<3),
		having:   F(),
		way:      s,
	}
}

// QueryOrder Constructing query orders.
type QueryOrder interface {
	IsEmpty

	Cmd

	Asc(columns ...string) QueryOrder

	Desc(columns ...string) QueryOrder
}

type queryOrder struct {
	orderBy  []string
	orderMap map[string]int
	way      *Way
}

func (s *queryOrder) IsEmpty() bool {
	return len(s.orderBy) == 0
}

func (s *queryOrder) Cmd() (prepare string, args []interface{}) {
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

func (s *queryOrder) Asc(columns ...string) QueryOrder {
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
		order := s.way.NameReplace(column)
		order = fmt.Sprintf("%s %s", order, SqlAsc)
		s.orderBy = append(s.orderBy, order)
	}
	return s
}

func (s *queryOrder) Desc(columns ...string) QueryOrder {
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
		order := s.way.NameReplace(column)
		order = fmt.Sprintf("%s %s", order, SqlDesc)
		s.orderBy = append(s.orderBy, order)
	}
	return s
}

func (s *Way) QueryOrder() QueryOrder {
	return &queryOrder{
		orderBy:  make([]string, 0, 1<<3),
		orderMap: make(map[string]int, 1<<3),
		way:      s,
	}
}

// QueryLimit Constructing query limits.
type QueryLimit interface {
	IsEmpty

	Cmd

	Limit(limit int64) QueryLimit

	Offset(offset int64) QueryLimit

	Page(page int64, limit ...int64) QueryLimit
}

type queryLimit struct {
	limit  *int64
	offset *int64
}

func (s *queryLimit) IsEmpty() bool {
	return s.limit == nil
}

func (s *queryLimit) Cmd() (prepare string, args []interface{}) {
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

func (s *queryLimit) Limit(limit int64) QueryLimit {
	if limit > 0 {
		s.limit = &limit
	}
	return s
}

func (s *queryLimit) Offset(offset int64) QueryLimit {
	if offset > 0 {
		s.offset = &offset
	}
	return s
}

func (s *queryLimit) Page(page int64, limit ...int64) QueryLimit {
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

func NewQueryLimit() QueryLimit {
	return &queryLimit{}
}

// InsertField Constructing insert fields.
type InsertField interface {
	IsEmpty

	Cmd

	Add(fields ...string) InsertField

	Del(fields ...string) InsertField

	DelAll() InsertField

	DelUseIndex(indexes ...int) InsertField

	FieldIndex(field string) int

	FieldExists(field string) bool

	Len() int

	SetFields(fields []string) InsertField

	GetFields() []string

	GetFieldsMap() map[string]*struct{}
}

type insertField struct {
	fields    []string
	fieldsMap map[string]int
	way       *Way
}

func (s *Way) InsertFieldCmd() InsertField {
	return &insertField{
		fields:    make([]string, 0, 1<<5),
		fieldsMap: make(map[string]int, 1<<5),
		way:       s,
	}
}

func (s *insertField) IsEmpty() bool {
	return len(s.fields) == 0
}

func (s *insertField) Cmd() (prepare string, args []interface{}) {
	if s.IsEmpty() {
		return
	}
	return ConcatString("( ", strings.Join(s.way.NameReplaces(s.fields), ", "), " )"), nil
}

func (s *insertField) Add(fields ...string) InsertField {
	num := len(s.fields)
	for _, column := range fields {
		if column == EmptyString {
			continue
		}
		if _, ok := s.fieldsMap[column]; ok {
			continue
		}
		s.fields = append(s.fields, column)
		s.fieldsMap[column] = num
		num++
	}
	return s
}

func (s *insertField) Del(fields ...string) InsertField {
	deleted := make(map[int]*struct{}, len(fields))
	for _, column := range fields {
		if column == EmptyString {
			continue
		}
		index, ok := s.fieldsMap[column]
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

func (s *insertField) DelAll() InsertField {
	s.fields = make([]string, 0, 1<<5)
	s.fieldsMap = make(map[string]int, 1<<5)
	return s
}

func (s *insertField) DelUseIndex(indexes ...int) InsertField {
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

func (s *insertField) FieldIndex(field string) int {
	index, ok := s.fieldsMap[field]
	if !ok {
		return -1
	}
	return index
}

func (s *insertField) FieldExists(field string) bool {
	return s.FieldIndex(field) >= 0
}

func (s *insertField) SetFields(fields []string) InsertField {
	return s.DelAll().Add(fields...)
}

func (s *insertField) GetFields() []string {
	return s.fields[:]
}

func (s *insertField) GetFieldsMap() map[string]*struct{} {
	result := make(map[string]*struct{}, len(s.fields))
	for _, field := range s.GetFields() {
		result[field] = &struct{}{}
	}
	return result
}

func (s *insertField) Len() int {
	return len(s.fields)
}

// InsertValue Constructing insert values.
type InsertValue interface {
	IsEmpty

	Cmd

	SetSubquery(subquery Cmd) InsertValue

	SetValues(values ...[]interface{}) InsertValue

	Set(index int, value interface{}) InsertValue

	Del(indexes ...int) InsertValue

	LenValues() int

	GetValues() [][]interface{}
}

type insertValue struct {
	subquery Cmd
	values   [][]interface{}
}

func NewInsertValue() InsertValue {
	return &insertValue{}
}

func (s *insertValue) IsEmpty() bool {
	return IsEmptyCmd(s.subquery) && (len(s.values) == 0 || len(s.values[0]) == 0)
}

func (s *insertValue) Cmd() (prepare string, args []interface{}) {
	if s.IsEmpty() {
		return
	}
	if !IsEmptyCmd(s.subquery) {
		prepare, args = s.subquery.Cmd()
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

func (s *insertValue) SetSubquery(subquery Cmd) InsertValue {
	s.subquery = subquery
	return s
}

func (s *insertValue) SetValues(values ...[]interface{}) InsertValue {
	s.values = values
	return s
}

func (s *insertValue) Set(index int, value interface{}) InsertValue {
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

func (s *insertValue) Del(indexes ...int) InsertValue {
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

func (s *insertValue) LenValues() int {
	return len(s.values)
}

func (s *insertValue) GetValues() [][]interface{} {
	return s.values
}

// UpdateSet Constructing update sets.
type UpdateSet interface {
	IsEmpty

	Cmd

	Update(update string, args ...interface{}) UpdateSet

	Set(column string, value interface{}) UpdateSet

	Decr(column string, decr interface{}) UpdateSet

	Incr(column string, incr interface{}) UpdateSet

	SetMap(columnValue map[string]interface{}) UpdateSet

	SetSlice(columns []string, values []interface{}) UpdateSet

	Len() int

	GetUpdate() (updates []string, args [][]interface{})

	UpdateIndex(prepare string) int

	UpdateExists(prepare string) bool
}

type updateSet struct {
	updateExpr []string
	updateArgs [][]interface{}
	updateMap  map[string]int
	way        *Way
}

func (s *Way) UpdateSet() UpdateSet {
	return &updateSet{
		updateExpr: make([]string, 0, 1<<3),
		updateArgs: make([][]interface{}, 0, 1<<3),
		updateMap:  make(map[string]int, 1<<3),
		way:        s,
	}
}

func (s *updateSet) IsEmpty() bool {
	return len(s.updateExpr) == 0
}

func (s *updateSet) Cmd() (prepare string, args []interface{}) {
	if s.IsEmpty() {
		return
	}
	prepare = strings.Join(s.updateExpr, ", ")
	for _, tmp := range s.updateArgs {
		args = append(args, tmp...)
	}
	return
}

func (s *updateSet) beautifyUpdate(update string) string {
	update = strings.TrimSpace(update)
	for strings.Contains(update, "  ") {
		update = strings.ReplaceAll(update, "  ", " ")
	}
	return update
}

func (s *updateSet) Update(update string, args ...interface{}) UpdateSet {
	if update == EmptyString {
		return s
	}
	update = s.beautifyUpdate(update)
	if update == EmptyString {
		return s
	}
	index, ok := s.updateMap[update]
	if ok {
		s.updateExpr[index], s.updateArgs[index] = update, args
		return s
	}
	s.updateMap[update] = len(s.updateExpr)
	s.updateExpr = append(s.updateExpr, update)
	s.updateArgs = append(s.updateArgs, args)
	return s
}

func (s *updateSet) Set(column string, value interface{}) UpdateSet {
	column = s.way.NameReplace(column)
	return s.Update(fmt.Sprintf("%s = %s", column, SqlPlaceholder), value)
}

func (s *updateSet) Decr(column string, decrement interface{}) UpdateSet {
	column = s.way.NameReplace(column)
	return s.Update(fmt.Sprintf("%s = %s - %s", column, column, SqlPlaceholder), decrement)
}

func (s *updateSet) Incr(column string, increment interface{}) UpdateSet {
	s.way.NameReplace(column)
	return s.Update(fmt.Sprintf("%s = %s + %s", column, column, SqlPlaceholder), increment)
}

func (s *updateSet) SetMap(columnValue map[string]interface{}) UpdateSet {
	for column, value := range columnValue {
		s.Set(column, value)
	}
	return s
}

func (s *updateSet) SetSlice(columns []string, values []interface{}) UpdateSet {
	for index, column := range columns {
		s.Set(column, values[index])
	}
	return s
}

func (s *updateSet) Len() int {
	return len(s.updateExpr)
}

func (s *updateSet) GetUpdate() ([]string, [][]interface{}) {
	return s.updateExpr, s.updateArgs
}

func (s *updateSet) UpdateIndex(update string) int {
	update = s.beautifyUpdate(update)
	index, ok := s.updateMap[update]
	if !ok {
		return -1
	}
	return index
}

func (s *updateSet) UpdateExists(update string) bool {
	return s.UpdateIndex(update) >= 0
}

type Helper interface {
	// DriverName Get the driver name.
	DriverName() []byte

	// DataSourceName Get the data source name.
	DataSourceName() []byte

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

func NewMysqlHelper(driverName string, dataSourceName string) *MysqlHelper {
	return &MysqlHelper{
		driverName:     []byte(driverName),
		dataSourceName: []byte(dataSourceName),
	}
}

// PostgresHelper helper for postgresql.
type PostgresHelper struct {
	driverName     []byte
	dataSourceName []byte

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

func NewPostgresHelper(driverName string, dataSourceName string) *PostgresHelper {
	return &PostgresHelper{
		driverName:     []byte(driverName),
		dataSourceName: []byte(dataSourceName),
	}
}

// Sqlite3Helper helper for sqlite3.
type Sqlite3Helper struct {
	driverName     []byte
	dataSourceName []byte

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

func NewSqlite3Helper(driverName string, dataSourceName string) *Sqlite3Helper {
	return &Sqlite3Helper{
		driverName:     []byte(driverName),
		dataSourceName: []byte(dataSourceName),
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
			columns[index] = s.way.NameReplace(adjust(column))
		}
	}
	return s.way.NameReplaces(columns)
}

// ColumnAll Add table name prefix to column names in batches.
func (s *AdjustColumn) ColumnAll(columns ...string) []string {
	if s.alias == EmptyString {
		return s.way.NameReplaces(columns)
	}
	prefix := fmt.Sprintf("%s%s", s.alias, SqlPoint)
	for index, column := range columns {
		if !strings.HasPrefix(column, prefix) {
			columns[index] = fmt.Sprintf("%s%s", prefix, s.way.NameReplace(column))
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
		return SqlAlias(count, s.way.cfg.Replace.Get(DefaultAliasNameCount))
	}
	if length == 1 && counts[0] != EmptyString {
		// only set alias name
		return SqlAlias(count, counts[0])
	}
	// set COUNT function parameters and alias name
	countAlias := s.way.cfg.Replace.Get(DefaultAliasNameCount)
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

	way *Way

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
	return s.WithFunc(fmt.Sprintf("SUM(%s)", s.way.NameReplace(column)))
}

// Max MAX() Returns the maximum value within the window.
func (s *WindowFunc) Max(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("MAX(%s)", s.way.NameReplace(column)))
}

// Min MIN() Returns the minimum value within the window.
func (s *WindowFunc) Min(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("MIN(%s)", s.way.NameReplace(column)))
}

// Avg AVG() Returns the average of all rows in the window.
func (s *WindowFunc) Avg(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("AVG(%s)", s.way.NameReplace(column)))
}

// Count COUNT() Returns the number of rows in the window.
func (s *WindowFunc) Count(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("COUNT(%s)", s.way.NameReplace(column)))
}

// Lag LAG() Returns the value of the row before the current row.
func (s *WindowFunc) Lag(column string, offset int64, defaultValue any) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("LAG(%s, %d, %s)", s.way.NameReplace(column), offset, ArgString(s.Helper, defaultValue)))
}

// Lead LEAD() Returns the value of a row after the current row.
func (s *WindowFunc) Lead(column string, offset int64, defaultValue any) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("LEAD(%s, %d, %s)", s.way.NameReplace(column), offset, ArgString(s.Helper, defaultValue)))
}

// NthValue NTH_VALUE() The Nth value can be returned according to the specified order. This is very useful when you need to get data at a specific position.
func (s *WindowFunc) NthValue(column string, LineNumber int64) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("NTH_VALUE(%s, %d)", s.way.NameReplace(column), LineNumber))
}

// FirstValue FIRST_VALUE() Returns the value of the first row in the window.
func (s *WindowFunc) FirstValue(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("FIRST_VALUE(%s)", s.way.NameReplace(column)))
}

// LastValue LAST_VALUE() Returns the value of the last row in the window.
func (s *WindowFunc) LastValue(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("LAST_VALUE(%s)", s.way.NameReplace(column)))
}

// Partition The OVER clause defines window partitions so that the window function is calculated independently in each partition.
func (s *WindowFunc) Partition(column ...string) *WindowFunc {
	s.partition = append(s.partition, s.way.NameReplaces(column)...)
	return s
}

// Asc Define the sorting within the partition so that the window function is calculated in order.
func (s *WindowFunc) Asc(column string) *WindowFunc {
	s.order = append(s.order, fmt.Sprintf("%s %s", s.way.NameReplace(column), SqlAsc))
	return s
}

// Desc Define the sorting within the partition so that the window function is calculated in descending order.
func (s *WindowFunc) Desc(column string) *WindowFunc {
	s.order = append(s.order, fmt.Sprintf("%s %s", s.way.NameReplace(column), SqlDesc))
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
		way:    way,
		alias:  LastNotEmptyString(aliases),
	}
}

// CmdQuery execute the built SQL statement and scan query result.
func CmdQuery(ctx context.Context, way *Way, cmd Cmd, query func(rows *sql.Rows) (err error)) error {
	prepare, args := cmd.Cmd()
	return way.QueryContext(ctx, query, prepare, args...)
}

// CmdGet execute the built SQL statement and scan query result.
func CmdGet(ctx context.Context, way *Way, cmd Cmd, result interface{}) error {
	prepare, args := cmd.Cmd()
	return way.TakeAllContext(ctx, result, prepare, args...)
}

// CmdScanAll execute the built SQL statement and scan all from the query results.
func CmdScanAll(ctx context.Context, way *Way, cmd Cmd, custom func(rows *sql.Rows) error) error {
	return CmdQuery(ctx, way, cmd, func(rows *sql.Rows) error {
		return way.ScanAll(rows, custom)
	})
}

// CmdScanOne execute the built SQL statement and scan at most once from the query results.
func CmdScanOne(ctx context.Context, way *Way, cmd Cmd, dest ...interface{}) error {
	return CmdQuery(ctx, way, cmd, func(rows *sql.Rows) error {
		return way.ScanOne(rows, dest...)
	})
}

// CmdViewMap execute the built SQL statement and scan all from the query results.
func CmdViewMap(ctx context.Context, way *Way, cmd Cmd) (result []map[string]interface{}, err error) {
	err = CmdQuery(ctx, way, cmd, func(rows *sql.Rows) (err error) {
		result, err = ScanViewMap(rows)
		return
	})
	return
}
