package version

import (
	"testing"
)

// --- Parse ---

func TestParse_Valid(t *testing.T) {
	tests := []struct {
		input string
		want  Version
	}{
		{"v0.0.0", Version{0, 0, 0}},
		{"v1.0.0", Version{1, 0, 0}},
		{"v1.2.3", Version{1, 2, 3}},
		{"v0.1.0", Version{0, 1, 0}},
		{"v10.20.30", Version{10, 20, 30}},
		{"v100.200.300", Version{100, 200, 300}},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("Parse(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParse_Invalid(t *testing.T) {
	tests := []string{
		"",
		"1.2.3",        // missing v prefix
		"v1.2",         // only two components
		"v1",           // one component
		"v1.2.3.4",     // four components
		"v1.2.x",       // non-numeric
		"v-1.2.3",      // negative
		"va.b.c",       // letters
		"V1.2.3",       // uppercase V
		"v 1.2.3",      // space
		"v1.2.3-rc1",   // pre-release (not supported)
		"v1.2.3+build", // build metadata (not supported)
	}
	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := Parse(input)
			if err == nil {
				t.Fatalf("Parse(%q) expected error, got nil", input)
			}
		})
	}
}

// --- String ---

func TestVersion_String(t *testing.T) {
	v := Version{1, 2, 3}
	if s := v.String(); s != "v1.2.3" {
		t.Fatalf("String() = %q, want %q", s, "v1.2.3")
	}
}

func TestVersion_String_Roundtrip(t *testing.T) {
	inputs := []string{"v0.0.0", "v1.2.3", "v10.20.30"}
	for _, input := range inputs {
		v, err := Parse(input)
		if err != nil {
			t.Fatalf("Parse(%q) error: %v", input, err)
		}
		if s := v.String(); s != input {
			t.Fatalf("roundtrip: Parse(%q).String() = %q", input, s)
		}
	}
}

// --- Compare ---

func TestVersion_Compare(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"v1.0.0", "v1.0.0", 0},
		{"v1.0.0", "v2.0.0", -1},
		{"v2.0.0", "v1.0.0", 1},
		{"v1.0.0", "v1.1.0", -1},
		{"v1.1.0", "v1.0.0", 1},
		{"v1.0.0", "v1.0.1", -1},
		{"v1.0.1", "v1.0.0", 1},
		{"v1.2.3", "v1.2.3", 0},
		// major wins over minor/patch
		{"v2.0.0", "v1.9.9", 1},
		// minor wins over patch
		{"v1.2.0", "v1.1.9", 1},
	}
	for _, tt := range tests {
		t.Run(tt.a+"_vs_"+tt.b, func(t *testing.T) {
			a := mustParse(t, tt.a)
			b := mustParse(t, tt.b)
			if got := a.Compare(b); got != tt.want {
				t.Fatalf("%s.Compare(%s) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestVersion_LessThan(t *testing.T) {
	a := mustParse(t, "v1.0.0")
	b := mustParse(t, "v2.0.0")
	if !a.LessThan(b) {
		t.Fatal("expected v1.0.0 < v2.0.0")
	}
	if b.LessThan(a) {
		t.Fatal("expected !(v2.0.0 < v1.0.0)")
	}
	if a.LessThan(a) {
		t.Fatal("expected !(v1.0.0 < v1.0.0)")
	}
}

func TestVersion_Equal(t *testing.T) {
	a := mustParse(t, "v1.2.3")
	b := mustParse(t, "v1.2.3")
	c := mustParse(t, "v1.2.4")
	if !a.Equal(b) {
		t.Fatal("expected v1.2.3 == v1.2.3")
	}
	if a.Equal(c) {
		t.Fatal("expected v1.2.3 != v1.2.4")
	}
}

func TestVersion_GreaterThan(t *testing.T) {
	a := mustParse(t, "v2.0.0")
	b := mustParse(t, "v1.0.0")
	if !a.GreaterThan(b) {
		t.Fatal("expected v2.0.0 > v1.0.0")
	}
	if b.GreaterThan(a) {
		t.Fatal("expected !(v1.0.0 > v2.0.0)")
	}
}

// --- Constraint Parsing ---

func TestParseConstraint_Exact(t *testing.T) {
	c, err := ParseConstraint("v1.2.3")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !c.Match(mustParse(t, "v1.2.3")) {
		t.Fatal("exact constraint should match v1.2.3")
	}
	if c.Match(mustParse(t, "v1.2.4")) {
		t.Fatal("exact constraint should not match v1.2.4")
	}
}

func TestParseConstraint_GreaterEqual(t *testing.T) {
	c, err := ParseConstraint(">=v1.2.0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !c.Match(mustParse(t, "v1.2.0")) {
		t.Fatal(">=v1.2.0 should match v1.2.0")
	}
	if !c.Match(mustParse(t, "v1.3.0")) {
		t.Fatal(">=v1.2.0 should match v1.3.0")
	}
	if !c.Match(mustParse(t, "v2.0.0")) {
		t.Fatal(">=v1.2.0 should match v2.0.0")
	}
	if c.Match(mustParse(t, "v1.1.9")) {
		t.Fatal(">=v1.2.0 should not match v1.1.9")
	}
}

func TestParseConstraint_Greater(t *testing.T) {
	c, err := ParseConstraint(">v1.0.0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Match(mustParse(t, "v1.0.0")) {
		t.Fatal(">v1.0.0 should not match v1.0.0 (strict)")
	}
	if !c.Match(mustParse(t, "v1.0.1")) {
		t.Fatal(">v1.0.0 should match v1.0.1")
	}
}

func TestParseConstraint_LessEqual(t *testing.T) {
	c, err := ParseConstraint("<=v2.0.0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !c.Match(mustParse(t, "v2.0.0")) {
		t.Fatal("<=v2.0.0 should match v2.0.0")
	}
	if !c.Match(mustParse(t, "v1.9.9")) {
		t.Fatal("<=v2.0.0 should match v1.9.9")
	}
	if c.Match(mustParse(t, "v2.0.1")) {
		t.Fatal("<=v2.0.0 should not match v2.0.1")
	}
}

func TestParseConstraint_Less(t *testing.T) {
	c, err := ParseConstraint("<v2.0.0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Match(mustParse(t, "v2.0.0")) {
		t.Fatal("<v2.0.0 should not match v2.0.0 (strict)")
	}
	if !c.Match(mustParse(t, "v1.9.9")) {
		t.Fatal("<v2.0.0 should match v1.9.9")
	}
}

func TestParseConstraint_Range(t *testing.T) {
	c, err := ParseConstraint(">=v1.0.0,<v2.0.0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !c.Match(mustParse(t, "v1.0.0")) {
		t.Fatal("range should match v1.0.0 (lower bound inclusive)")
	}
	if !c.Match(mustParse(t, "v1.5.0")) {
		t.Fatal("range should match v1.5.0 (in range)")
	}
	if !c.Match(mustParse(t, "v1.9.9")) {
		t.Fatal("range should match v1.9.9 (just under upper)")
	}
	if c.Match(mustParse(t, "v2.0.0")) {
		t.Fatal("range should not match v2.0.0 (upper bound exclusive)")
	}
	if c.Match(mustParse(t, "v0.9.9")) {
		t.Fatal("range should not match v0.9.9 (below lower)")
	}
}

func TestParseConstraint_Empty(t *testing.T) {
	c, err := ParseConstraint("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// empty constraint matches everything
	if !c.Match(mustParse(t, "v0.0.1")) {
		t.Fatal("empty constraint should match anything")
	}
	if !c.Match(mustParse(t, "v99.99.99")) {
		t.Fatal("empty constraint should match anything")
	}
}

func TestParseConstraint_Invalid(t *testing.T) {
	tests := []string{
		">=1.2.3",  // missing v
		">=vx.y.z", // non-numeric
		"~v1.2.3",  // unsupported operator
		"==v1.2.3", // double equals not supported
	}
	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := ParseConstraint(input)
			if err == nil {
				t.Fatalf("ParseConstraint(%q) expected error", input)
			}
		})
	}
}

func TestConstraint_String(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"v1.2.3", "v1.2.3"},
		{">=v1.0.0", ">=v1.0.0"},
		{">=v1.0.0,<v2.0.0", ">=v1.0.0,<v2.0.0"},
		{"", "*"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			c, err := ParseConstraint(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := c.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- Validate ---

func TestValidate_Valid(t *testing.T) {
	if err := Validate("v1.2.3"); err != nil {
		t.Fatalf("Validate(v1.2.3) unexpected error: %v", err)
	}
}

func TestValidate_Invalid(t *testing.T) {
	if err := Validate("1.2.3"); err == nil {
		t.Fatal("Validate(1.2.3) expected error")
	}
}

// --- Helpers ---

func mustParse(t *testing.T, s string) Version {
	t.Helper()
	v, err := Parse(s)
	if err != nil {
		t.Fatalf("mustParse(%q): %v", s, err)
	}
	return v
}
