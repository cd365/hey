package hey

import (
	"reflect"
	"testing"
)

func TestAnyToSQLNamedPrimitive(t *testing.T) {
	type namedBool bool
	type namedInt int32
	type namedUint uint16
	type namedFloat float32
	type namedString string

	stringValue := namedString("column_name")
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{name: "bool", value: namedBool(true), want: "true"},
		{name: "int", value: namedInt(-12), want: "-12"},
		{name: "uint", value: namedUint(12), want: "12"},
		{name: "float", value: namedFloat(1.25), want: "1.25"},
		{name: "string", value: namedString("column_name"), want: "column_name"},
		{name: "pointer", value: &stringValue, want: "column_name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := AnyToSQL(tt.value)
			if script.Prepare != tt.want {
				t.Fatalf("prepare = %q, want %q", script.Prepare, tt.want)
			}
		})
	}
}

func TestParcelSQLRequiresCompleteOuterParentheses(t *testing.T) {
	partial := NewSQL("( SELECT id FROM allowed ) UNION SELECT id FROM public_ids", 1)
	result := ParcelSQL(partial)

	if result.Prepare != "( ( SELECT id FROM allowed ) UNION SELECT id FROM public_ids )" {
		t.Fatalf("unexpected prepare: %q", result.Prepare)
	}
	if !reflect.DeepEqual(result.Args, []any{1}) {
		t.Fatalf("unexpected args: %#v", result.Args)
	}
	if partial.Prepare != "( SELECT id FROM allowed ) UNION SELECT id FROM public_ids" {
		t.Fatalf("input SQL was modified: %q", partial.Prepare)
	}

	complete := ParcelSQL(NewSQL("( SELECT ')' AS value /* ( */ )"))
	if complete.Prepare != "( SELECT ')' AS value /* ( */ )" {
		t.Fatalf("already parenthesized SQL changed: %q", complete.Prepare)
	}
}

func TestBindingDeepNestedNoAliasing(t *testing.T) {
	type A struct {
		X string `db:"x"`
		Y string `db:"y"`
	}
	type B struct {
		A A
	}
	type C struct {
		B B
	}
	type Root struct {
		C C
	}

	var b bindScanStruct
	b.init()
	b.binding(reflect.TypeOf(Root{}), nil, "db")

	x, ok := b.indirect["x"]
	if !ok {
		t.Fatalf("missing indirect mapping for x: %#v", b.indirect)
	}
	y, ok := b.indirect["y"]
	if !ok {
		t.Fatalf("missing indirect mapping for y: %#v", b.indirect)
	}
	wantX := []int{0, 0, 0, 0} // Root.C -> C.B -> B.A -> A.X
	wantY := []int{0, 0, 0, 1} // Root.C -> C.B -> B.A -> A.Y
	if !reflect.DeepEqual(x, wantX) {
		t.Errorf("x mapping = %v, want %v", x, wantX)
	}
	if !reflect.DeepEqual(y, wantY) {
		t.Errorf("y mapping = %v, want %v", y, wantY)
	}
}

// Self-referential structs must not cause infinite recursion / stack overflow.
func TestObjectInsertSelfReferentialStruct(t *testing.T) {
	type Node struct {
		Value string `db:"value"`
		Next  *Node  `db:"-"`
	}
	root := &Node{Value: "root", Next: &Node{Value: "child"}}

	columns, values, category := ObjectInsert(root, "db", nil, nil)
	if category != CategoryInsertOne {
		t.Fatalf("category = %v, want CategoryInsertOne", category)
	}
	if !reflect.DeepEqual(columns, []string{"value"}) {
		t.Fatalf("columns = %v, want [value]", columns)
	}
	if len(values) != 1 || len(values[0]) != 1 || values[0][0] != "root" {
		t.Fatalf("values = %v, want [[root]]", values)
	}
}

func TestObjectModifySelfReferentialStruct(t *testing.T) {
	type Node struct {
		Value string `db:"value"`
		Next  *Node  `db:"-"`
	}
	root := &Node{Value: "root", Next: &Node{Value: "child"}}

	columns, values := ObjectModify(root, "db")
	if !reflect.DeepEqual(columns, []string{"value"}) {
		t.Fatalf("columns = %v, want [value]", columns)
	}
	if len(values) != 1 || values[0] != "root" {
		t.Fatalf("values = %v, want [root]", values)
	}
}

// A nil pointer element in a slice must be skipped without leaving a nil hole.
func TestObjectInsertSliceNilElement(t *testing.T) {
	type Row struct {
		Name string `db:"name"`
	}
	rows := []*Row{nil, {Name: "b"}, {Name: "c"}}

	columns, values, category := ObjectInsert(rows, "db", nil, nil)
	if category != CategoryInsertAll {
		t.Fatalf("category = %v, want CategoryInsertAll", category)
	}
	if !reflect.DeepEqual(columns, []string{"name"}) {
		t.Fatalf("columns = %v, want [name]", columns)
	}
	if len(values) != 2 {
		t.Fatalf("values has %d rows, want 2 (nil element skipped): %v", len(values), values)
	}
	for i, row := range values {
		if len(row) != 1 {
			t.Fatalf("row %d has %d columns, want 1: %v", i, len(row), row)
		}
	}
}

// Duplicate db tags spanning nested struct levels must be deduplicated for every
// row, not just the first, so each row's value count matches the column count.
func TestObjectInsertSliceDuplicateNestedTag(t *testing.T) {
	type Address struct {
		City string `db:"city"`
	}
	type User struct {
		Name    string `db:"name"`
		City    string `db:"city"` // duplicate tag with Address.City
		Address Address
	}
	users := []User{
		{Name: "a", City: "NY", Address: Address{City: "LA"}},
		{Name: "b", City: "SF", Address: Address{City: "SD"}},
	}

	columns, values, category := ObjectInsert(users, "db", nil, nil)
	if category != CategoryInsertAll {
		t.Fatalf("category = %v, want CategoryInsertAll", category)
	}
	if !reflect.DeepEqual(columns, []string{"name", "city"}) {
		t.Fatalf("columns = %v, want [name city]", columns)
	}
	if len(values) != 2 {
		t.Fatalf("values has %d rows, want 2: %v", len(values), values)
	}
	for i, row := range values {
		if len(row) != len(columns) {
			t.Fatalf("row %d has %d values vs %d columns: %v", i, len(row), len(columns), row)
		}
	}
	// The first occurrence wins: city comes from the outer field, not Address.City.
	if !reflect.DeepEqual(values[1], []any{"b", "SF"}) {
		t.Fatalf("row 1 = %v, want [b SF]", values[1])
	}
}

// A pointer field that changed between a concrete value and nil must be updated:
// non-nil -> nil sets NULL, nil -> non-nil sets the value. Unchanged fields are skipped.
func TestStructUpdatePointerNil(t *testing.T) {
	type Row struct {
		Name *string `db:"name"`
		Age  *int    `db:"age"`
		City string  `db:"city"`
		Addr *string `db:"addr"`
	}
	name := "a"
	age := 1
	old := &Row{Name: &name, Age: &age, City: "NY", Addr: nil}

	addr := "x"
	latest := &Row{Name: nil, Age: &age, City: "SF", Addr: &addr}

	columns, values := StructUpdate(old, latest, "db")
	wantColumns := []string{"name", "city", "addr"}
	wantValues := []any{nil, "SF", "x"}
	if !reflect.DeepEqual(columns, wantColumns) {
		t.Fatalf("columns = %v, want %v", columns, wantColumns)
	}
	if !reflect.DeepEqual(values, wantValues) {
		t.Fatalf("values = %v, want %v", values, wantValues)
	}
}

// A multi-level pointer whose inner pointer is nil must still be reported as a column
// (nil value) by ObjectObtain, matching the single-level pointer path, so StructUpdate
// can detect NULL transitions instead of silently dropping the field.
func TestObjectObtainMultiLevelPointerNil(t *testing.T) {
	type Row struct {
		Name **string `db:"name"`
		Age  **int    `db:"age"`
	}
	name := "a"
	namePtr := &name
	age := 1
	agePtr := &age
	row := &Row{Name: &namePtr, Age: &agePtr} // inner non-nil

	columns, values := ObjectObtain(row, "db")
	if !reflect.DeepEqual(columns, []string{"name", "age"}) {
		t.Fatalf("columns = %v, want [name age]", columns)
	}
	if len(values) != 2 || values[0] != "a" || values[1] != 1 {
		t.Fatalf("values = %#v, want [a 1]", values)
	}

	// Inner pointer nil: still emitted with a nil value (not dropped).
	nilRow := &Row{Name: nil, Age: &agePtr}
	columns, values = ObjectObtain(nilRow, "db")
	if !reflect.DeepEqual(columns, []string{"name", "age"}) {
		t.Fatalf("columns = %v, want [name age]", columns)
	}
	if len(values) != 2 || values[0] != nil || values[1] != 1 {
		t.Fatalf("values = %#v, want [nil 1]", values)
	}
}

// Compare must emit a parameterized NULL update for a pointer field that became nil.
func TestUpdateSetComparePointerNull(t *testing.T) {
	way := NewWay()
	u := newSQLUpdateSet(way)
	type Row struct {
		Name *string `db:"name"`
		Age  *int    `db:"age"`
	}
	name := "a"
	age := 1
	old := &Row{Name: &name, Age: &age}
	latest := &Row{Name: nil, Age: &age}
	u.Compare(old, latest)
	exprs, args := u.GetUpdate()
	if !reflect.DeepEqual(exprs, []string{"name = ?"}) {
		t.Fatalf("exprs = %v, want [name = ?]", exprs)
	}
	if len(args) != 1 || len(args[0]) != 1 || args[0][0] != nil {
		t.Fatalf("args = %#v, want [[nil]]", args)
	}
}

// Repeated Set on the same column must overwrite (last write wins), and Remove must
// keep the remaining expressions, args and index map consistent.
func TestUpdateSetRepeatSetThenRemove(t *testing.T) {
	way := NewWay()
	u := newSQLUpdateSet(way)

	u.Set("name", "a")
	u.Set("name", "b") // overwrites the previous name value
	u.Set("age", 1)
	u.Set("city", "SF")

	exprs, args := u.GetUpdate()
	wantExprs := []string{"name = ?", "age = ?", "city = ?"}
	wantArgs := [][]any{{"b"}, {1}, {"SF"}}
	if !reflect.DeepEqual(exprs, wantExprs) {
		t.Fatalf("before remove: exprs = %v, want %v", exprs, wantExprs)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("before remove: args = %#v, want %#v", args, wantArgs)
	}

	u.Remove("name")

	exprs, args = u.GetUpdate()
	wantExprs = []string{"age = ?", "city = ?"}
	wantArgs = [][]any{{1}, {"SF"}}
	if !reflect.DeepEqual(exprs, wantExprs) {
		t.Fatalf("after remove: exprs = %v, want %v", exprs, wantExprs)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("after remove: args = %#v, want %#v", args, wantArgs)
	}

	// The index map must still be consistent: setting an existing column again after
	// Remove should overwrite in place rather than duplicate or panic.
	u.Set("city", "LA")
	exprs, args = u.GetUpdate()
	wantExprs = []string{"age = ?", "city = ?"}
	wantArgs = [][]any{{1}, {"LA"}}
	if !reflect.DeepEqual(exprs, wantExprs) {
		t.Fatalf("after re-set: exprs = %v, want %v", exprs, wantExprs)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("after re-set: args = %#v, want %#v", args, wantArgs)
	}
}

// A nil nested struct pointer is skipped (not flattened), matching ObjectObtain and
// ObjectModify: its columns are not part of the column set.
func TestObjectInsertNilNestedStructSkipped(t *testing.T) {
	type Address struct {
		City string `db:"city"`
	}
	type User struct {
		Name    string `db:"name"`
		Address *Address
	}
	columns, values, category := ObjectInsert(&User{Name: "a", Address: nil}, "db", nil, nil)
	if category != CategoryInsertOne {
		t.Fatalf("category = %v, want CategoryInsertOne", category)
	}
	if !reflect.DeepEqual(columns, []string{"name"}) {
		t.Fatalf("columns = %v, want [name]", columns)
	}
	if len(values) != 1 || !reflect.DeepEqual(values[0], []any{"a"}) {
		t.Fatalf("values = %v, want [[a]]", values)
	}
}

func TestRowsScanNilResult(t *testing.T) {
	if err := RowsScan(nil, nil, "db"); err == nil {
		t.Fatal("RowsScan(nil, nil, ...) should return an error, not panic")
	}
}

// Nested structs are flattened up to two levels by default; the third level is skipped.
func TestObjectInsertDepthTwoLevels(t *testing.T) {
	type Geo struct {
		Lat float64 `db:"lat"`
	}
	type Address struct {
		City string `db:"city"`
		Geo  Geo
	}
	type User struct {
		Name    string `db:"name"`
		Address Address
	}
	columns, values, category := ObjectInsert(&User{Name: "a", Address: Address{City: "x", Geo: Geo{Lat: 1.0}}}, "db", nil, nil)
	if category != CategoryInsertOne {
		t.Fatalf("category = %v, want CategoryInsertOne", category)
	}
	if !reflect.DeepEqual(columns, []string{"name", "city"}) {
		t.Fatalf("columns = %v, want [name city]", columns)
	}
	if len(values) != 1 || !reflect.DeepEqual(values[0], []any{"a", "x"}) {
		t.Fatalf("values = %v, want [[a x]]", values)
	}
}

// depth == 0 means unbounded recursion, so the third level is flattened too.
func TestObjectInsertDepthUnbounded(t *testing.T) {
	i := poolGetObjectInsert()
	defer poolPutObjectInsert(i)
	i.depth = 0

	type Geo struct {
		Lat float64 `db:"lat"`
	}
	type Address struct {
		City string `db:"city"`
		Geo  Geo
	}
	type User struct {
		Name    string `db:"name"`
		Address Address
	}
	columns, values, category := i.Insert(&User{Name: "a", Address: Address{City: "x", Geo: Geo{Lat: 1.0}}}, "db", nil, nil)
	if category != CategoryInsertOne {
		t.Fatalf("category = %v, want CategoryInsertOne", category)
	}
	if !reflect.DeepEqual(columns, []string{"name", "city", "lat"}) {
		t.Fatalf("columns = %v, want [name city lat]", columns)
	}
	if len(values) != 1 || !reflect.DeepEqual(values[0], []any{"a", "x", 1.0}) {
		t.Fatalf("values = %v, want [[a x 1]]", values)
	}
}

// Slice elements may be multi-level pointers to any struct.
func TestObjectInsertSliceMultiLevelPointer(t *testing.T) {
	type Row struct {
		Name string `db:"name"`
	}
	a := &Row{Name: "a"}
	pa := &a
	b := &Row{Name: "b"}
	pb := &b
	columns, values, category := ObjectInsert([]**Row{pa, pb}, "db", nil, nil)
	if category != CategoryInsertAll {
		t.Fatalf("category = %v, want CategoryInsertAll", category)
	}
	if !reflect.DeepEqual(columns, []string{"name"}) {
		t.Fatalf("columns = %v, want [name]", columns)
	}
	if len(values) != 2 || !reflect.DeepEqual(values[0], []any{"a"}) || !reflect.DeepEqual(values[1], []any{"b"}) {
		t.Fatalf("values = %v, want [[a] [b]]", values)
	}
}

// A subsequent row that misses a fixed column (nil nested struct) makes the whole
// multi-row insert empty.
func TestObjectInsertSliceMissingColumnEmpty(t *testing.T) {
	type Address struct {
		City string `db:"city"`
	}
	type User struct {
		Name    string `db:"name"`
		Address *Address
	}
	rows := []User{
		{Name: "a", Address: &Address{City: "x"}},
		{Name: "b", Address: nil},
	}
	columns, values, category := ObjectInsert(rows, "db", nil, nil)
	if category != CategoryInsertUnknown {
		t.Fatalf("category = %v, want CategoryInsertUnknown", category)
	}
	if columns != nil || values != nil {
		t.Fatalf("columns = %v, values = %v, want nil and nil", columns, values)
	}
}

// A []map[string]any row missing a key makes the whole insert empty.
func TestObjectInsertMapSliceMissingKeyEmpty(t *testing.T) {
	rows := []map[string]any{
		{"a": 1, "b": 2},
		{"a": 3},
	}
	columns, values, category := ObjectInsert(rows, "db", nil, nil)
	if category != CategoryInsertUnknown {
		t.Fatalf("category = %v, want CategoryInsertUnknown", category)
	}
	if columns != nil || values != nil {
		t.Fatalf("columns = %v, values = %v, want nil and nil", columns, values)
	}
}
