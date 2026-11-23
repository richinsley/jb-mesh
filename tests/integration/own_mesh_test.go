package integration

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestNoTestReachesAMeshItDidNotStart reads every test file in the repository and fails if
// one could reach a mesh by default: by connecting to the default client port, by reading
// the general NATS_URL variable, or by advertising or browsing under the real mDNS service
// type. A test that wants another mesh must be told its address in a variable of its own.
//
// Files behind a build tag are not part of a plain `go test ./...` and are not read.
func TestNoTestReachesAMeshItDidNotStart(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("the repository root was not found from here: %v", err)
	}
	rules := []struct {
		why string
		re  *regexp.Regexp
	}{
		{"connects to the default client port", regexp.MustCompile(`nats\.Connect\(\s*"nats://(localhost|127\.0\.0\.1):4222`)},
		{"falls back to the default client port", regexp.MustCompile(`=\s*"nats://(localhost|127\.0\.0\.1):4222"`)},
		{"reads the general NATS_URL variable", regexp.MustCompile(`Getenv\("NATS_URL"\)`)},
	}
	discovery := regexp.MustCompile(`NewDiscovery\(DiscoveryConfig\{([^}]*)\}`)
	starts := regexp.MustCompile(`\.(Start|Browse)\(`)
	checked := 0
	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if name := info.Name(); name == ".git" || name == "node_modules" || name == "dist" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") || filepath.Base(path) == "own_mesh_test.go" {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(raw)
		if strings.HasPrefix(text, "//go:build") {
			return nil
		}
		checked++
		rel, _ := filepath.Rel(root, path)
		for _, rule := range rules {
			if loc := rule.re.FindStringIndex(text); loc != nil {
				t.Errorf("%s:%d %s", rel, 1+strings.Count(text[:loc[0]], "\n"), rule.why)
			}
		}
		if starts.MatchString(text) {
			for _, m := range discovery.FindAllStringSubmatchIndex(text, -1) {
				body := text[m[2]:m[3]]
				if strings.Contains(body, "NodeName") && !strings.Contains(body, "ServiceType") {
					// A discovery that is only constructed is harmless; one in a file that starts
					// or browses must not use the real service type.
					line := 1 + strings.Count(text[:m[0]], "\n")
					if line > 0 && discoveryIsStarted(text, m[0]) {
						t.Errorf("%s:%d advertises or browses under the real mDNS service type", rel, line)
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked < 20 {
		t.Fatalf("only %d test files were read; the walk is not seeing the repository", checked)
	}
}

// discoveryIsStarted reports whether the test function holding the construction at pos
// starts or browses with anything.
func discoveryIsStarted(text string, pos int) bool {
	start := strings.LastIndex(text[:pos], "\nfunc ")
	if start < 0 {
		start = 0
	}
	end := strings.Index(text[pos:], "\nfunc ")
	if end < 0 {
		end = len(text) - pos
	}
	fn := text[start : pos+end]
	return strings.Contains(fn, ".Start(") || strings.Contains(fn, ".Browse(")
}
