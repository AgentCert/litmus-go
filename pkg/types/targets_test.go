package types

import "testing"

// GetTargets indexed val[2] unconditionally, so any TARGETS entry with fewer than
// three colon-separated fields panicked with an unrecovered index-out-of-range —
// killing the experiment pod before it could write a ChaosResult. Every input here
// must return, not panic.
func TestGetTargets_MalformedInputDoesNotPanic(t *testing.T) {
	cases := []struct {
		name    string
		targets string
		want    int // expected number of parsed AppDetails
	}{
		{"empty", "", 0},
		{"kind only", "deployment", 0},
		{"kind and namespace only", "deployment:default", 0},
		{"trailing separator", "deployment:default:[nginx];", 1},
		{"one good one malformed", "deployment:default:[nginx];statefulset:other", 1},
		{"only separators", ";;;", 0},
		{"well formed by label", "deployment:default:[app=nginx]", 1},
		{"well formed by name", "deployment:default:[nginx-1,nginx-2]", 1},
		{"empty target list", "deployment:default:[]", 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := GetTargets(tc.targets) // must not panic
			if len(got) != tc.want {
				t.Errorf("GetTargets(%q) returned %d entries, want %d: %#v", tc.targets, len(got), tc.want, got)
			}
		})
	}
}

// Labels is populated only when the third field contains "=", Names otherwise.
// Callers that read Labels[0] must therefore tolerate a nil Labels slice.
func TestGetTargets_LabelsVersusNames(t *testing.T) {
	byLabel := GetTargets("deployment:default:[app=nginx]")
	if len(byLabel) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(byLabel))
	}
	if len(byLabel[0].Labels) != 1 || byLabel[0].Labels[0] != "app=nginx" {
		t.Errorf("expected Labels=[app=nginx], got %#v", byLabel[0].Labels)
	}
	if len(byLabel[0].Names) != 0 {
		t.Errorf("expected no Names when targeting by label, got %#v", byLabel[0].Names)
	}

	byName := GetTargets("deployment:default:[nginx-1,nginx-2]")
	if len(byName) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(byName))
	}
	// This is the nil slice that made app[0].Labels[0] panic in all seven
	// getAppDetails() copies under pkg/generic/*/environment/.
	if len(byName[0].Labels) != 0 {
		t.Errorf("expected nil Labels when targeting by name, got %#v", byName[0].Labels)
	}
	if len(byName[0].Names) != 2 {
		t.Errorf("expected 2 Names, got %#v", byName[0].Names)
	}

	// An empty list parses to nil for both — also a panic source.
	empty := GetTargets("deployment:default:[]")
	if len(empty) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(empty))
	}
	if len(empty[0].Labels) != 0 || len(empty[0].Names) != 0 {
		t.Errorf("expected both slices nil for an empty list, got labels=%#v names=%#v", empty[0].Labels, empty[0].Names)
	}
}

// Getenv treats an empty environment value as absent and substitutes the default.
// The whole ""-versus-"0" sentinel bug class rests on this behaviour, so pin it.
func TestGetenv_EmptyValueYieldsDefault(t *testing.T) {
	const key = "LITMUS_TEST_SENTINEL_KEY"

	t.Setenv(key, "")
	if got := Getenv(key, "0"); got != "0" {
		t.Errorf("an empty env value must fall back to the default: got %q, want %q", got, "0")
	}

	t.Setenv(key, "42")
	if got := Getenv(key, "0"); got != "42" {
		t.Errorf("a set env value must win: got %q, want %q", got, "42")
	}
}
