package hey

import (
	"testing"

	"github.com/cd365/hey/v8/cst"
)

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

func TestPrepareDollar(t *testing.T) {
	cases := map[string]string{
		// The '?' character is inert: it is no longer a placeholder, so JSONB
		// operators work natively without any heuristic detection.
		"data ? 'key'":                "data ? 'key'",
		"data ? 'key' AND id = ?":     "data ? 'key' AND id = ?",
		"data ?| array['a','b']":      "data ?| array['a','b']",
		"data ?& array['a','b']":      "data ?& array['a','b']",
		"SELECT * FROM t WHERE a = ?": "SELECT * FROM t WHERE a = ?",
		"SELECT '?' FROM t":           "SELECT '?' FROM t",
		"SELECT E'a\\'b?' FROM t":     "SELECT E'a\\'b?' FROM t",
		// The placeholder marker '$?' is rewritten to '$n'.
		"SELECT * FROM t WHERE a = $?":            "SELECT * FROM t WHERE a = $1",
		"SELECT * FROM t WHERE a = $? AND b = $?": "SELECT * FROM t WHERE a = $1 AND b = $2",
		"data ? 'key' AND id = $?":                "data ? 'key' AND id = $1",
		// Marker sequences inside string literals and comments are not placeholders.
		"SELECT '$?' FROM t": "SELECT '$?' FROM t",
		"SELECT '$?', $?":    "SELECT '$?', $1",
		"/* $? */ $?":        "/* $? */ $1",
		"-- $? \n $?":        "-- $? \n $1",
		// Dollar-quoted strings are not treated specially.
		"$$?$$ $?":          "$$1$$ $2",
		"$tag$ $? $tag$ $?": "$tag$ $1 $tag$ $2",
	}
	for in, want := range cases {
		if got := prepareDollar(in); got != want {
			t.Errorf("prepareDollar(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMergeConfigManualPrepare(t *testing.T) {
	base := ConfigDefault()
	// A Manual with only DatabaseType set must not lose the Prepare converter.
	for _, dbType := range []cst.DatabaseType{cst.Postgresql, cst.Mysql, cst.Sqlite} {
		cfg := mergeConfig(base, &Config{Manual: Manual{DatabaseType: dbType}})
		if cfg.Manual.Prepare == nil {
			t.Errorf("database type %q: Prepare is nil", dbType)
			continue
		}
		script := "data ? 'key' AND id = $?"
		want := "data ? 'key' AND id = ?"
		if dbType == cst.Postgresql {
			want = "data ? 'key' AND id = $1"
		}
		if got := cfg.Manual.Prepare(script); got != want {
			t.Errorf("database type %q: Prepare(%q) = %q, want %q", dbType, script, got, want)
		}
		if cfg.Manual.InsertOneReturningId == nil {
			t.Errorf("database type %q: InsertOneReturningId is nil", dbType)
		}
	}
	// A user-provided Prepare is preserved as-is.
	custom := func(prepare string) string { return prepare }
	cfg := mergeConfig(base, &Config{Manual: Manual{DatabaseType: cst.Mysql, Prepare: custom}})
	if got := cfg.Manual.Prepare("id = $?"); got != "id = $?" {
		t.Errorf("custom Prepare was not preserved: got %q", got)
	}
	// An unset Manual keeps the base configuration untouched.
	cfg = mergeConfig(base, &Config{})
	if cfg.Manual.Prepare == nil {
		t.Error("base Manual.Prepare was lost")
	}
}

func TestPrepareQuestionMark(t *testing.T) {
	cases := map[string]string{
		// The placeholder marker '$?' is rewritten to '?'.
		"SELECT * FROM t WHERE a = $?":            "SELECT * FROM t WHERE a = ?",
		"SELECT * FROM t WHERE a = $? AND b = $?": "SELECT * FROM t WHERE a = ? AND b = ?",
		// Plain '$' and numbered parameters are left untouched.
		"SELECT $1, $2, $ FROM t": "SELECT $1, $2, $ FROM t",
		// Marker sequences inside string literals and comments are not placeholders.
		"SELECT '$?' FROM t": "SELECT '$?' FROM t",
		// Backslash is not an escape character, only '' is, so the literal ends at
		// the quote right after the backslash.
		"SELECT E'a\\'b$?' FROM t": "SELECT E'a\\'b?' FROM t",
		"/* $? */ $?":              "/* $? */ ?",
		"-- $? \n $?":              "-- $? \n ?",
		// Dollar-quoted strings are not treated specially.
		"$$?$$ $?":          "$?$$ ?",
		"$tag$ $? $tag$ $?": "$tag$ ? $tag$ ?",
		// The '?' character is inert.
		"data ? 'key' AND id = $?": "data ? 'key' AND id = ?",
	}
	for in, want := range cases {
		if got := prepareQuestionMark(in); got != want {
			t.Errorf("prepareQuestionMark(%q) = %q, want %q", in, got, want)
		}
	}
}
