package main

import "testing"

func TestServiceClassName(t *testing.T) {
	tests := map[string]string{
		"my-service":      "MyService",
		"starter-dogfood": "StarterDogfoodService",
		"adns":            "AdnsService",
		"already-service": "AlreadyService",
	}
	for in, want := range tests {
		if got := serviceClassName(in); got != want {
			t.Fatalf("serviceClassName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestValidateServiceName(t *testing.T) {
	good := []string{"my-service", "adns", "svc-2"}
	for _, name := range good {
		if err := validateServiceName(name); err != nil {
			t.Fatalf("validateServiceName(%q) unexpected error: %v", name, err)
		}
	}

	bad := []string{"MyService", "bad_name", "-oops", "spaces bad"}
	for _, name := range bad {
		if err := validateServiceName(name); err == nil {
			t.Fatalf("validateServiceName(%q) expected error", name)
		}
	}
}
