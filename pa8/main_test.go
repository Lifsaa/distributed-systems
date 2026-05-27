package main

import (
	"reflect"
	"testing"
)

func TestNormalizeBoolFlagPairs(t *testing.T) {
	got := normalizeBoolFlagPairs([]string{
		"-id", "1",
		"-leader", "true",
		"-peers", "2=localhost:5002,3=localhost:5003",
	})
	want := []string{
		"-id", "1",
		"-leader=true",
		"-peers", "2=localhost:5002,3=localhost:5003",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalized args = %#v, want %#v", got, want)
	}
}
