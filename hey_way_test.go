package hey

import "testing"

func assert(value any, prepare string) {
	switch v := value.(type) {
	case *SQL:
		if v.Prepare != prepare {
			panic("assert failed\nexpected: " + prepare + "\nactual: " + v.Prepare)
		}
	case Maker:
		script := v.ToSQL()
		if script.Prepare != prepare {
			panic("assert failed\nexpected: " + prepare + "\nactual: " + script.Prepare)
		}
	default:
		panic("unexpected data")
	}
}

var way *Way

func init() {
	opts := make([]Option, 0, 1<<3)
	if false {
		opts = append(opts, WithConfig(nil))
		opts = append(opts, WithDatabase(nil))
		opts = append(opts, WithTrack(nil))
		opts = append(opts, WithReader(nil))
	}
	way = NewWay(opts...)
}

func TestPrepare63236JsonbOperator(t *testing.T) {
	cases := map[string]string{
		"data ? 'key'":                          "data ? 'key'",
		"data ? 'key' AND id = ?":               "data ? 'key' AND id = $1",
		"data ?| array['a','b']":                "data ?| array['a','b']",
		"data ?& array['a','b']":                "data ?& array['a','b']",
		"SELECT * FROM t WHERE a = ?":           "SELECT * FROM t WHERE a = $1",
		"SELECT * FROM t WHERE a = ? AND b = ?": "SELECT * FROM t WHERE a = $1 AND b = $2",
		"SELECT '?' FROM t":                     "SELECT '?' FROM t",
	}
	for in, want := range cases {
		if got := prepare63236(in); got != want {
			t.Errorf("prepare63236(%q) = %q, want %q", in, got, want)
		}
	}
}
