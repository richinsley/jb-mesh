package main

import (
	"reflect"
	"testing"
)

func TestSplitCSV(t *testing.T) {
	got := splitCSV(" node, kind ,tool ")
	want := []string{"node", "kind", "tool"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("splitCSV = %#v want %#v", got, want)
	}
}
