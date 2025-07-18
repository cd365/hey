package hey

import (
	"context"
	"database/sql"
	"strings"
	"sync"
)

var (
	stringBuilder = &sync.Pool{
		New: func() any {
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

// Replace For replacement identifiers in SQL statements.
// Replace by default, concurrent reads and writes are not safe.
// If you need concurrent read and write security, you can implement Replace by yourself.
type Replace interface {
	Get(key string) string

	Set(key string, value string) Replace

	Del(key string) Replace

	Map() map[string]string

	Use(mapping map[string]string) Replace

	// Gets Batch getting.
	Gets(keys []string) []string

	// Sets Batch setting.
	Sets(mapping map[string]string) Replace
}

type replace struct {
	maps map[string]string
}

func (s *replace) Get(key string) string {
	value, ok := s.maps[key]
	if ok {
		return value
	}
	return key
}

func (s *replace) Set(key string, value string) Replace {
	s.maps[key] = value
	return s
}

func (s *replace) Del(key string) Replace {
	delete(s.maps, key)
	return s
}

func (s *replace) Map() map[string]string {
	return s.maps
}

func (s *replace) Use(mapping map[string]string) Replace {
	s.maps = mapping
	return s
}

func (s *replace) Gets(keys []string) []string {
	length := len(keys)
	if length == 0 {
		return keys
	}
	replaced := make([]string, length)
	for i := range length {
		if value, ok := s.maps[keys[i]]; ok {
			replaced[i] = value
		} else {
			replaced[i] = keys[i]
		}
	}
	return replaced
}

func (s *replace) Sets(mapping map[string]string) Replace {
	for key, value := range mapping {
		s.Set(key, value)
	}
	return s
}

func NewReplace() Replace {
	return &replace{
		maps: make(map[string]string, 256),
	}
}

// Cmder Used to build SQL expression and its corresponding parameter list.
type Cmder interface {
	// Cmd Get a list of script statements and their corresponding parameters.
	Cmd() (prepare string, args []any)
}

type cmdSql struct {
	prepare string
	args    []any
}

func (s *cmdSql) Cmd() (prepare string, args []any) {
	if s.prepare != EmptyString {
		prepare, args = s.prepare, s.args
	}
	return
}

// NewCmder Construct a Cmder using SQL statements and parameter lists.
func NewCmder(prepare string, args []any) Cmder {
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
	args := make([]any, 0, 32)
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
func RowsScanStructAll[V any](ctx context.Context, way *Way, scan func(rows *sql.Rows, v *V) error, prepare string, args ...any) ([]*V, error) {
	length := 16
	if ctx == nil {
		ctx = context.Background()
	} else {
		if tmp := ctx.Value(RowsScanStructAllMakeSliceLength); tmp != nil {
			if intValue, ok := tmp.(int); ok && intValue > 0 && intValue <= 10000 {
				length = intValue
			}
		}
	}
	var err error
	result := make([]*V, 0, length)
	err = way.QueryContext(ctx, func(rows *sql.Rows) error {
		index := 0
		values := make([]V, length)
		for rows.Next() {
			if err = scan(rows, &values[index]); err != nil {
				return err
			}
			result = append(result, &values[index])
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
func RowsScanStructOne[V any](ctx context.Context, way *Way, scan func(rows *sql.Rows, v *V) error, prepare string, args ...any) (*V, error) {
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
		return nil, ErrNoRows
	}
	return &tmp, nil
}

// RowsScanStructAllCmder Rows scan to any struct, based on struct scan data.
func RowsScanStructAllCmder[V any](ctx context.Context, way *Way, scan func(rows *sql.Rows, v *V) error, cmder Cmder) ([]*V, error) {
	if IsEmptyCmder(cmder) {
		return nil, nil
	}
	prepare, args := cmder.Cmd()
	return RowsScanStructAll(ctx, way, scan, prepare, args...)
}

// RowsScanStructOneCmder Rows scan to any struct, based on struct scan data.
func RowsScanStructOneCmder[V any](ctx context.Context, way *Way, scan func(rows *sql.Rows, v *V) error, cmder Cmder) (*V, error) {
	if IsEmptyCmder(cmder) {
		return nil, nil
	}
	prepare, args := cmder.Cmd()
	return RowsScanStructOne(ctx, way, scan, prepare, args...)
}

func MergeAssoc[K comparable, V any](values ...map[K]V) map[K]V {
	length := len(values)
	result := make(map[K]V)
	for i := range length {
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

func MergeArray[V any](values ...[]V) []V {
	length := len(values)
	result := make([]V, 0)
	for i := range length {
		if i == 0 {
			result = values[i]
			continue
		}
		result = append(result, values[i]...)
	}
	return result
}

func AssocToAssoc[K comparable, V any, X comparable, Y any](values map[K]V, fc func(k K, v V) (X, Y)) map[X]Y {
	if fc == nil {
		return nil
	}
	result := make(map[X]Y, len(values))
	for key, value := range values {
		k, v := fc(key, value)
		result[k] = v
	}
	return result
}

func AssocToArray[K comparable, V any, W any](values map[K]V, fc func(k K, v V) W) []W {
	if fc == nil {
		return nil
	}
	length := len(values)
	result := make([]W, length)
	for index, value := range values {
		result = append(result, fc(index, value))
	}
	return result
}

func ArrayToAssoc[V any, K comparable, W any](values []V, fc func(v V) (K, W)) map[K]W {
	if fc == nil {
		return nil
	}
	length := len(values)
	result := make(map[K]W, length)
	for i := range length {
		k, v := fc(values[i])
		result[k] = v
	}
	return result
}

func ArrayToArray[V any, W any](values []V, fc func(k int, v V) W) []W {
	if fc == nil {
		return nil
	}
	result := make([]W, len(values))
	for index, value := range values {
		result[index] = fc(index, value)
	}
	return result
}

func AssocDiscard[K comparable, V any](values map[K]V, discard func(k K, v V) bool) map[K]V {
	if values == nil || discard == nil {
		return values
	}
	result := make(map[K]V, len(values))
	for index, value := range values {
		if !discard(index, value) {
			result[index] = value
		}
	}
	return result
}

func ArrayDiscard[V any](values []V, discard func(k int, v V) bool) []V {
	if values == nil || discard == nil {
		return values
	}
	result := make([]V, 0, len(values))
	for index, value := range values {
		if !discard(index, value) {
			result = append(result, value)
		}
	}
	return result
}

// InsertOnConflictUpdateSet Implement the following SQL statement:
// INSERT INTO ... ON CONFLICT ( column_a[, column_b, column_c...] ) DO UPDATE SET column1 = EXCLUDED.column1, column2 = EXCLUDED.column2, column3 = EXCLUDED.column3, column4 = 'fixed value' ...
type InsertOnConflictUpdateSet interface {
	UpdateSet

	// Excluded Construct the update expression column1 = EXCLUDED.column1, column2 = EXCLUDED.column2, column3 = EXCLUDED.column3 ...
	// This is how the `new` data is accessed that causes the conflict.
	Excluded(columns ...string) InsertOnConflictUpdateSet
}

type insertOnConflictUpdateSet struct {
	UpdateSet

	way *Way
}

func (s *insertOnConflictUpdateSet) Excluded(columns ...string) InsertOnConflictUpdateSet {
	for _, column := range columns {
		tmp := s.way.Replace(column)
		s.Update(ConcatString(tmp, SqlSpace, SqlEqual, SqlSpace, SqlExcluded, SqlPoint, tmp))
	}
	return s
}

func NewInsertOnConflictUpdateSet(way *Way) InsertOnConflictUpdateSet {
	tmp := &insertOnConflictUpdateSet{
		way: way,
	}
	tmp.UpdateSet = NewUpdateSet(way)
	return tmp
}

// InsertOnConflict Implement the following SQL statement:
// INSERT INTO ... ON CONFLICT (column_a[, column_b, column_c...]) DO NOTHING /* If a conflict occurs, the insert operation is ignored. */
// INSERT INTO ... ON CONFLICT (column_a[, column_b, column_c...]) DO UPDATE SET column1 = EXCLUDED.column1, column2 = EXCLUDED.column2, column3 = EXCLUDED.column3, column4 = 'fixed value' ... /* If a conflict occurs, the existing row is updated with the new value */
type InsertOnConflict interface {
	// OnConflict The column causing the conflict, such as a unique key or primary key, which can be a single column or multiple columns.
	OnConflict(onConflicts ...string) InsertOnConflict

	// Do The SQL statement that needs to be executed when a data conflict occurs. By default, nothing is done.
	Do(prepare string, args ...any) InsertOnConflict

	// DoUpdateSet SQL update statements executed when data conflicts occur.
	DoUpdateSet(fc func(u InsertOnConflictUpdateSet)) InsertOnConflict

	// Cmd The SQL statement and its parameter list that are finally executed.
	Cmd() (prepare string, args []any)

	// InsertOnConflict Executes the SQL statement constructed by the current object.
	InsertOnConflict() (int64, error)
}

type insertOnConflict struct {
	way *Way
	ctx context.Context

	insertPrepare     string
	insertPrepareArgs []any

	onConflicts []string

	onConflictsDoPrepare     string
	onConflictsDoPrepareArgs []any

	onConflictsDoUpdateSet InsertOnConflictUpdateSet
}

func (s *insertOnConflict) Context(ctx context.Context) InsertOnConflict {
	s.ctx = ctx
	return s
}

func (s *insertOnConflict) OnConflict(onConflicts ...string) InsertOnConflict {
	s.onConflicts = onConflicts
	return s
}

func (s *insertOnConflict) Do(prepare string, args ...any) InsertOnConflict {
	s.onConflictsDoPrepare, s.onConflictsDoPrepareArgs = strings.TrimSpace(prepare), args
	return s
}

func (s *insertOnConflict) DoUpdateSet(fc func(u InsertOnConflictUpdateSet)) InsertOnConflict {
	tmp := s.onConflictsDoUpdateSet
	if tmp == nil {
		s.onConflictsDoUpdateSet = NewInsertOnConflictUpdateSet(s.way)
		tmp = s.onConflictsDoUpdateSet
	}
	fc(tmp)
	return s
}

func (s *insertOnConflict) Cmd() (prepare string, args []any) {
	if s.insertPrepare == EmptyString || len(s.onConflicts) == 0 {
		return
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(s.insertPrepare)
	args = append(args, s.insertPrepareArgs...)
	b.WriteString(ConcatString(SqlSpace, SqlOn, SqlSpace, SqlConflict, SqlSpace))
	b.WriteString(ParcelPrepare(strings.Join(s.way.Replaces(s.onConflicts), SqlConcat)))
	b.WriteString(SqlSpace)
	b.WriteString(SqlDo)
	b.WriteString(SqlSpace)
	doPrepare, doPrepareArgs := SqlNothing, make([]any, 0)
	if s.onConflictsDoPrepare != EmptyString {
		doPrepare, doPrepareArgs = s.onConflictsDoPrepare, s.onConflictsDoPrepareArgs
	} else {
		if s.onConflictsDoUpdateSet != nil && s.onConflictsDoUpdateSet.Len() > 0 {
			tmpPrepare, tmpPrepareArgs := s.onConflictsDoUpdateSet.Cmd()
			bus := getStringBuilder()
			defer putStringBuilder(bus)
			b.WriteString(ConcatString(SqlUpdate, SqlSpace, SqlSet, SqlSpace))
			bus.WriteString(tmpPrepare)
			doPrepare = bus.String()
			doPrepareArgs = tmpPrepareArgs
		}
	}
	b.WriteString(doPrepare)
	args = append(args, doPrepareArgs...)
	prepare = b.String()
	return
}

func (s *insertOnConflict) InsertOnConflict() (int64, error) {
	ctx := s.ctx
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.way.cfg.TransactionMaxDuration)
		defer cancel()
	}
	prepare, args := s.Cmd()
	return s.way.ExecContext(ctx, prepare, args...)
}

func NewInsertOnConflict(way *Way, insertPrepare string, insertArgs []any) InsertOnConflict {
	return &insertOnConflict{
		way:               way,
		insertPrepare:     insertPrepare,
		insertPrepareArgs: insertArgs,
	}
}
