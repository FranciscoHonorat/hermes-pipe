package stage

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestParseLiteral(t *testing.T) {
	tests := map[string]any{
		"true": true, "false": false, "null": nil, "42": json.Number("42"), "-1.5": json.Number("-1.5"),
		`"a b"`: "a b", "palavra": "palavra", ` x `: "x", `"quebrado`: `"quebrado`,
	}
	for in, want := range tests {
		if got := ParseLiteral(in); got != want {
			t.Errorf("%q: got %#v want %#v", in, got, want)
		}
	}
}

func TestCompare(t *testing.T) {
	tests := []struct {
		a, b any
		c    int
		ok   bool
	}{
		{json.Number("1"), json.Number("2"), -1, true},
		{2.0, json.Number("2"), 0, true},
		{int64(3), 2, 1, true},
		{"a", "b", -1, true},
		{true, false, 1, true},
		{false, true, -1, true},
		{true, true, 0, true},
		{nil, nil, 0, true},
		{nil, "x", 0, false},
		{"1", json.Number("1"), 0, false},
		{json.Number("1"), "1", 0, false},
		{true, "true", 0, false},
		{[]any{}, []any{}, 0, false},
	}
	for _, tt := range tests {
		c, ok := Compare(tt.a, tt.b)
		if c != tt.c || ok != tt.ok {
			t.Errorf("Compare(%#v,%#v) = %d,%v", tt.a, tt.b, c, ok)
		}
	}
}

func TestKeyString(t *testing.T) {
	tests := []struct {
		v       any
		present bool
		want    string
	}{
		{"x", true, "x"}, {json.Number("5"), true, "5"}, {nil, true, "null"}, {true, true, "true"},
		{[]any{"a"}, true, `["a"]`}, {map[string]any{"k": 1}, true, `{"k":1}`}, {"x", false, ""},
	}
	for _, tt := range tests {
		if got := KeyString(tt.v, tt.present); got != tt.want {
			t.Errorf("%#v: %q", tt.v, got)
		}
	}
}

func TestFatal(t *testing.T) {
	base := errors.New("disco")
	err := Fatal(base)
	var f interface{ Fatal() bool }
	if !errors.As(err, &f) || !f.Fatal() || !errors.Is(err, base) || err.Error() != "disco" {
		t.Fatal("Fatal não marca o erro")
	}
	var s interface{ Stop() bool }
	stop := error(&StopError{Reason: "r"})
	if !errors.As(stop, &s) || !s.Stop() || stop.Error() != "r" {
		t.Fatal("StopError")
	}
}
