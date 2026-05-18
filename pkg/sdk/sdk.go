package sdk

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const PythonSDKSubdir = "sdk/python/jb-service"

var SDKPackageSpecs = map[string]struct{}{
	"jb-service": {},
	"git+https://github.com/richinsley/jb-service.git":                                 {},
	"git+https://github.com/richinsley/jb-mesh.git#subdirectory=sdk/python/jb-service": {},
}

// LocalPythonSDKPath returns the in-repository jb-service SDK path when it is
// available next to the jb-mesh source tree. This is the visible source of truth
// used by local preflight/install until binary embedding is wired in.
func LocalPythonSDKPath() (string, bool) {
	candidates := make([]string, 0, 8)

	if wd, err := os.Getwd(); err == nil {
		for dir := wd; ; dir = filepath.Dir(dir) {
			candidates = append(candidates, filepath.Join(dir, PythonSDKSubdir))
			if parent := filepath.Dir(dir); parent == dir {
				break
			}
		}
	}

	if _, file, _, ok := runtime.Caller(0); ok {
		for dir := filepath.Dir(file); ; dir = filepath.Dir(dir) {
			candidates = append(candidates, filepath.Join(dir, PythonSDKSubdir))
			if parent := filepath.Dir(dir); parent == dir {
				break
			}
		}
	}

	seen := map[string]struct{}{}
	for _, candidate := range candidates {
		candidate = filepath.Clean(candidate)
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		if _, err := os.Stat(filepath.Join(candidate, "pyproject.toml")); err == nil {
			return candidate, true
		}
	}
	return "", false
}

// RewritePythonSDKPackages replaces known public/bundled SDK package specs with
// the local visible SDK path when available. Other packages are preserved.
func RewritePythonSDKPackages(packages []string) []string {
	local, ok := LocalPythonSDKPath()
	if !ok {
		return append([]string(nil), packages...)
	}
	out := make([]string, len(packages))
	for i, pkg := range packages {
		trimmed := strings.TrimSpace(pkg)
		if _, isSDK := SDKPackageSpecs[trimmed]; isSDK {
			out[i] = local
		} else {
			out[i] = pkg
		}
	}
	return out
}
