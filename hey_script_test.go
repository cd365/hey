package hey

import (
	"reflect"
	"testing"
)

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
