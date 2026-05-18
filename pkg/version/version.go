// Package version provides strict semver parsing, comparison, and constraint
// matching for jb-mesh tool versions.
//
// All tool versions must follow the format vMAJOR.MINOR.PATCH (e.g., v1.2.3).
// The "v" prefix is required.
//
// Constraints support exact, range, and minimum version matching:
//
//	v1.2.3        — exact match
//	>=v1.2.0      — minimum version
//	>=v1.0.0,<v2  — range (multiple comma-separated constraints)
package version

import (
	"fmt"
	"strconv"
	"strings"
)

// Version represents a parsed semantic version (vMAJOR.MINOR.PATCH).
type Version struct {
	Major int
	Minor int
	Patch int
}

// Parse parses a version string in the format "vMAJOR.MINOR.PATCH".
// The "v" prefix is required. Returns an error if the format is invalid.
func Parse(s string) (Version, error) {
	if s == "" {
		return Version{}, fmt.Errorf("version string is empty")
	}
	if !strings.HasPrefix(s, "v") {
		return Version{}, fmt.Errorf("version %q must start with 'v'", s)
	}

	rest := s[1:]
	parts := strings.Split(rest, ".")
	if len(parts) != 3 {
		return Version{}, fmt.Errorf("version %q must have exactly 3 components (vMAJOR.MINOR.PATCH)", s)
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil || major < 0 {
		return Version{}, fmt.Errorf("version %q: invalid major component %q", s, parts[0])
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil || minor < 0 {
		return Version{}, fmt.Errorf("version %q: invalid minor component %q", s, parts[1])
	}
	patch, err := strconv.Atoi(parts[2])
	if err != nil || patch < 0 {
		return Version{}, fmt.Errorf("version %q: invalid patch component %q", s, parts[2])
	}

	return Version{Major: major, Minor: minor, Patch: patch}, nil
}

// String returns the version in canonical "vMAJOR.MINOR.PATCH" format.
func (v Version) String() string {
	return fmt.Sprintf("v%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// Compare returns -1 if v < other, 0 if v == other, 1 if v > other.
func (v Version) Compare(other Version) int {
	if v.Major != other.Major {
		if v.Major < other.Major {
			return -1
		}
		return 1
	}
	if v.Minor != other.Minor {
		if v.Minor < other.Minor {
			return -1
		}
		return 1
	}
	if v.Patch != other.Patch {
		if v.Patch < other.Patch {
			return -1
		}
		return 1
	}
	return 0
}

// LessThan returns true if v < other.
func (v Version) LessThan(other Version) bool {
	return v.Compare(other) < 0
}

// Equal returns true if v == other.
func (v Version) Equal(other Version) bool {
	return v.Compare(other) == 0
}

// GreaterThan returns true if v > other.
func (v Version) GreaterThan(other Version) bool {
	return v.Compare(other) > 0
}

// Constraint represents a version constraint that can match against versions.
// A constraint is one or more individual comparisons joined by commas (AND logic).
type Constraint struct {
	checks []check
}

type check struct {
	op      string // "=", ">=", ">", "<=", "<"
	version Version
}

// ParseConstraint parses a version constraint string.
// Supported formats:
//
//	"v1.2.3"             — exact match
//	">=v1.2.0"           — greater than or equal
//	">v1.0.0"            — strictly greater than
//	"<=v2.0.0"           — less than or equal
//	"<v2.0.0"            — strictly less than
//	">=v1.0.0,<v2.0.0"   — range (comma = AND)
//
// An empty string returns a constraint that matches any version.
func ParseConstraint(s string) (Constraint, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Constraint{}, nil // matches everything
	}

	parts := strings.Split(s, ",")
	checks := make([]check, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		c, err := parseOneCheck(part)
		if err != nil {
			return Constraint{}, err
		}
		checks = append(checks, c)
	}
	return Constraint{checks: checks}, nil
}

func parseOneCheck(s string) (check, error) {
	var op string
	var versionStr string

	if strings.HasPrefix(s, ">=") {
		op = ">="
		versionStr = strings.TrimSpace(s[2:])
	} else if strings.HasPrefix(s, "<=") {
		op = "<="
		versionStr = strings.TrimSpace(s[2:])
	} else if strings.HasPrefix(s, ">") {
		op = ">"
		versionStr = strings.TrimSpace(s[1:])
	} else if strings.HasPrefix(s, "<") {
		op = "<"
		versionStr = strings.TrimSpace(s[1:])
	} else {
		op = "="
		versionStr = s
	}

	v, err := Parse(versionStr)
	if err != nil {
		return check{}, fmt.Errorf("constraint %q: %w", s, err)
	}
	return check{op: op, version: v}, nil
}

// Match returns true if the given version satisfies all constraint checks.
// An empty constraint (no checks) matches any version.
func (c Constraint) Match(v Version) bool {
	for _, ch := range c.checks {
		if !ch.match(v) {
			return false
		}
	}
	return true
}

func (ch check) match(v Version) bool {
	cmp := v.Compare(ch.version)
	switch ch.op {
	case "=":
		return cmp == 0
	case ">=":
		return cmp >= 0
	case ">":
		return cmp > 0
	case "<=":
		return cmp <= 0
	case "<":
		return cmp < 0
	default:
		return false
	}
}

// String returns the constraint as a string.
func (c Constraint) String() string {
	if len(c.checks) == 0 {
		return "*"
	}
	parts := make([]string, len(c.checks))
	for i, ch := range c.checks {
		if ch.op == "=" {
			parts[i] = ch.version.String()
		} else {
			parts[i] = ch.op + ch.version.String()
		}
	}
	return strings.Join(parts, ",")
}

// Validate checks if a version string is a valid semver format (vN.N.N).
// Returns nil if valid, or an error describing the problem.
func Validate(s string) error {
	_, err := Parse(s)
	return err
}
