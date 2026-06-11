package comparator

import (
	"testing"

	"github.com/litmuschaos/litmus-go/pkg/cerrors"
)

const ec = cerrors.ErrorTypeGeneric

// newModel builds a fully-populated Model via the builder API.
func newModel(a, b interface{}, operator string) *Model {
	return FirstValue(a).
		SecondValue(b).
		Criteria(operator).
		RunCount(1).
		ProbeName("test-probe").
		ProbeVerbosity("info")
}

func TestModelBuilder(t *testing.T) {
	m := RunCount(3)
	if m.rc != 3 {
		t.Errorf("RunCount: expected 3, got %d", m.rc)
	}
	m = FirstValue("a").SecondValue("b").Criteria("equal").ProbeName("p").ProbeVerbosity("debug")
	if m.a != "a" || m.b != "b" || m.operator != "equal" || m.probeName != "p" || m.probeVerbosity != "debug" {
		t.Errorf("builder did not set fields correctly: %+v", m)
	}
}

func TestCompareString(t *testing.T) {
	tests := []struct {
		name     string
		a, b     string
		operator string
		wantErr  bool
	}{
		{"equal match", "foo", "foo", "equal", false},
		{"Equal match capitalized", "foo", "foo", "Equal", false},
		{"equal mismatch", "foo", "bar", "equal", true},
		{"notEqual match", "foo", "bar", "notEqual", false},
		{"NotEqual mismatch", "foo", "foo", "NotEqual", true},
		{"contains match", "hello world", "world", "contains", false},
		{"contains mismatch", "hello", "xyz", "contains", true},
		{"matches valid regex", "abc123", "^abc[0-9]+$", "matches", false},
		{"matches mismatch", "abc", "^[0-9]+$", "matches", true},
		{"matches invalid regex", "abc", "[", "matches", true},
		{"notMatches match", "abc", "^[0-9]+$", "notMatches", false},
		{"notMatches mismatch", "123", "^[0-9]+$", "notMatches", true},
		{"notMatches invalid regex", "abc", "[", "notMatches", true},
		{"oneOf match", "b", "a,b,c", "oneOf", false},
		{"OneOf mismatch", "z", "a,b,c", "OneOf", true},
		{"unsupported operator", "a", "b", "wat", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newModel(tt.a, tt.b, tt.operator).CompareString(ec)
			if (err != nil) != tt.wantErr {
				t.Errorf("CompareString(%q,%q,%q) err=%v wantErr=%v", tt.a, tt.b, tt.operator, err, tt.wantErr)
			}
		})
	}
}

func TestCompareString_VerbosityNonInfo(t *testing.T) {
	// exercise the non-info verbosity log branch
	m := FirstValue("x").SecondValue("x").Criteria("equal").RunCount(5).ProbeName("p").ProbeVerbosity("debug")
	if err := m.CompareString(ec); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestCompareInt(t *testing.T) {
	tests := []struct {
		name     string
		a, b     string
		operator string
		wantErr  bool
	}{
		{">= equal", "5", "5", ">=", false},
		{">= greater", "6", "5", ">=", false},
		{">= less", "4", "5", ">=", true},
		{"<= equal", "5", "5", "<=", false},
		{"<= less", "4", "5", "<=", false},
		{"<= greater", "6", "5", "<=", true},
		{"> true", "6", "5", ">", false},
		{"> false", "5", "5", ">", true},
		{"< true", "4", "5", "<", false},
		{"< false", "5", "5", "<", true},
		{"== true", "5", "5", "==", false},
		{"== false", "4", "5", "==", true},
		{"!= true", "4", "5", "!=", false},
		{"!= false", "5", "5", "!=", true},
		{"oneOf match", "2", "1,2,3", "oneOf", false},
		{"OneOf mismatch", "9", "1,2,3", "OneOf", true},
		{"between within", "5", "1,10", "between", false},
		{"between lower edge", "1", "1,10", "between", false},
		{"between upper edge", "10", "1,10", "between", false},
		{"between outside", "11", "1,10", "between", true},
		{"between too few limits", "5", "1", "between", true},
		{"unsupported operator", "5", "5", "??", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newModel(tt.a, tt.b, tt.operator).CompareInt(ec)
			if (err != nil) != tt.wantErr {
				t.Errorf("CompareInt(%q,%q,%q) err=%v wantErr=%v", tt.a, tt.b, tt.operator, err, tt.wantErr)
			}
		})
	}
}

func TestCompareFloat(t *testing.T) {
	tests := []struct {
		name     string
		a, b     string
		operator string
		wantErr  bool
	}{
		{">= equal", "5.5", "5.5", ">=", false},
		{">= greater", "6.1", "5.5", ">=", false},
		{">= less", "4.9", "5.5", ">=", true},
		{"<= less", "4.0", "5.5", "<=", false},
		{"<= greater", "6.0", "5.5", "<=", true},
		{"> true", "6.0", "5.5", ">", false},
		{"> false", "5.5", "5.5", ">", true},
		{"< true", "4.0", "5.5", "<", false},
		{"< false", "5.5", "5.5", "<", true},
		{"== true", "5.5", "5.5", "==", false},
		{"== false", "4.0", "5.5", "==", true},
		{"!= true", "4.0", "5.5", "!=", false},
		{"!= false", "5.5", "5.5", "!=", true},
		{"oneOf match", "2.5", "1.5,2.5,3.5", "oneOf", false},
		{"OneOf mismatch", "9.9", "1.5,2.5", "OneOf", true},
		{"between within", "5.0", "1.0,10.0", "between", false},
		{"between outside", "11.0", "1.0,10.0", "between", true},
		{"between too few limits", "5.0", "1.0", "between", true},
		{"unsupported operator", "5.0", "5.0", "??", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newModel(tt.a, tt.b, tt.operator).CompareFloat(ec)
			if (err != nil) != tt.wantErr {
				t.Errorf("CompareFloat(%q,%q,%q) err=%v wantErr=%v", tt.a, tt.b, tt.operator, err, tt.wantErr)
			}
		})
	}
}

func TestStringSetValues(t *testing.T) {
	var s String
	s.setValues("actual", "x,y,z")
	if s.a != "actual" || len(s.c) != 3 || s.b != "" {
		t.Errorf("expected list form, got a=%q b=%q c=%v", s.a, s.b, s.c)
	}
	var s2 String
	s2.setValues("actual", "single")
	if s2.b != "single" || s2.c != nil {
		t.Errorf("expected single form, got b=%q c=%v", s2.b, s2.c)
	}
}
