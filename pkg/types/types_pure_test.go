package types

import (
	"os"
	"reflect"
	"testing"

	"github.com/litmuschaos/chaos-operator/api/litmuschaos/v1alpha1"
	"github.com/litmuschaos/litmus-go/pkg/cerrors"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", nil},
		{"brackets empty", "[]", nil},
		{"single", "a", []string{"a"}},
		{"list with brackets", "[a,b,c]", []string{"a", "b", "c"}},
		{"list no brackets", "a,b", []string{"a", "b"}},
		{"trims whitespace then splits", "  [x,y]  ", []string{"x", "y"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parse(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parse(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestGetTargets(t *testing.T) {
	if got := GetTargets(""); got != nil {
		t.Errorf("empty input should return nil, got %v", got)
	}

	got := GetTargets("deployment:ns1:app=nginx;pod:ns2:[p1,p2]")
	if len(got) != 2 {
		t.Fatalf("expected 2 targets, got %d (%v)", len(got), got)
	}

	if got[0].Kind != "deployment" || got[0].Namespace != "ns1" {
		t.Errorf("target[0] kind/ns wrong: %+v", got[0])
	}
	// "app=nginx" contains "=" so it should be parsed as Labels, not Names
	if !reflect.DeepEqual(got[0].Labels, []string{"app=nginx"}) || got[0].Names != nil {
		t.Errorf("target[0] expected label form, got %+v", got[0])
	}

	if got[1].Kind != "pod" || got[1].Namespace != "ns2" {
		t.Errorf("target[1] kind/ns wrong: %+v", got[1])
	}
	if !reflect.DeepEqual(got[1].Names, []string{"p1", "p2"}) || got[1].Labels != nil {
		t.Errorf("target[1] expected name form, got %+v", got[1])
	}
}

func TestGetenv(t *testing.T) {
	const key = "LITMUS_TEST_GETENV"
	os.Unsetenv(key)
	if got := Getenv(key, "fallback"); got != "fallback" {
		t.Errorf("unset key should return default, got %q", got)
	}
	os.Setenv(key, "real")
	defer os.Unsetenv(key)
	if got := Getenv(key, "fallback"); got != "real" {
		t.Errorf("set key should return value, got %q", got)
	}
}

func TestGetChaosResultVerdictEvent(t *testing.T) {
	v, typ := GetChaosResultVerdictEvent(v1alpha1.ResultVerdictPassed)
	if v != string(v1alpha1.ResultVerdictPassed) || typ != "Normal" {
		t.Errorf("passed verdict: got (%q,%q)", v, typ)
	}
	v, typ = GetChaosResultVerdictEvent(v1alpha1.ResultVerdictFailed)
	if v != string(v1alpha1.ResultVerdictFailed) || typ != "Warning" {
		t.Errorf("failed verdict: got (%q,%q)", v, typ)
	}
}

func TestSetResultAttributes(t *testing.T) {
	tests := []struct {
		name     string
		engine   string
		exp      string
		instance string
		wantName string
	}{
		{"exp only", "", "pod-delete", "", "pod-delete"},
		{"engine+exp", "eng", "pod-delete", "", "eng-pod-delete"},
		{"engine+exp+instance", "eng", "pod-delete", "5", "eng-pod-delete-5"},
		{"exp+instance no engine", "", "pod-delete", "9", "pod-delete-9"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rd := &ResultDetails{}
			cd := ChaosDetails{EngineName: tt.engine, ExperimentName: tt.exp, InstanceID: tt.instance}
			SetResultAttributes(rd, cd)
			if rd.Name != tt.wantName {
				t.Errorf("Name = %q, want %q", rd.Name, tt.wantName)
			}
			if rd.Verdict != "Awaited" || rd.Phase != "Running" || rd.PassedProbeCount != 0 {
				t.Errorf("unexpected defaults: %+v", rd)
			}
		})
	}
}

func TestSetResultAfterCompletion(t *testing.T) {
	// error phase with non-helper code -> ErrorOutput populated
	rd := &ResultDetails{}
	SetResultAfterCompletion(rd, v1alpha1.ResultVerdictFailed, v1alpha1.ResultPhaseError, "boom", cerrors.ErrorTypeGeneric)
	if rd.ErrorOutput == nil {
		t.Fatal("expected ErrorOutput to be set")
	}
	if rd.ErrorOutput.Reason != "boom" || rd.ErrorOutput.ErrorCode != string(cerrors.ErrorTypeGeneric) {
		t.Errorf("unexpected ErrorOutput %+v", rd.ErrorOutput)
	}

	// helper-pod-failed code is excluded
	rd2 := &ResultDetails{}
	SetResultAfterCompletion(rd2, v1alpha1.ResultVerdictFailed, v1alpha1.ResultPhaseError, "boom", cerrors.ErrorTypeHelperPodFailed)
	if rd2.ErrorOutput != nil {
		t.Errorf("helper-pod-failed should not populate ErrorOutput, got %+v", rd2.ErrorOutput)
	}

	// non-error phase -> no ErrorOutput
	rd3 := &ResultDetails{}
	SetResultAfterCompletion(rd3, v1alpha1.ResultVerdictPassed, v1alpha1.ResultPhaseCompleted, "", cerrors.ErrorTypeGeneric)
	if rd3.ErrorOutput != nil {
		t.Errorf("completed phase should not populate ErrorOutput")
	}
}

func TestSetEngineEventAttributes(t *testing.T) {
	ed := &EventDetails{}
	cd := &ChaosDetails{EngineName: "eng", ChaosUID: "uid-1"}
	SetEngineEventAttributes(ed, "Reason", "Message", "Normal", cd)
	if ed.Reason != "Reason" || ed.Message != "Message" || ed.Type != "Normal" || ed.ResourceName != "eng" || string(ed.ResourceUID) != "uid-1" {
		t.Errorf("unexpected engine event attrs %+v", ed)
	}
}

func TestSetResultEventAttributes(t *testing.T) {
	ed := &EventDetails{}
	rd := &ResultDetails{Name: "res", ResultUID: "ruid"}
	SetResultEventAttributes(ed, "R", "M", "Warning", rd)
	if ed.Reason != "R" || ed.Message != "M" || ed.Type != "Warning" || ed.ResourceName != "res" || string(ed.ResourceUID) != "ruid" {
		t.Errorf("unexpected result event attrs %+v", ed)
	}
}

func TestParseDuration(t *testing.T) {
	d, err := parseDuration("")
	if err != nil || d != 0 {
		t.Errorf("empty duration: got (%v,%v)", d, err)
	}
	d, err = parseDuration("  ")
	if err != nil || d != 0 {
		t.Errorf("whitespace duration: got (%v,%v)", d, err)
	}
	d, err = parseDuration("5s")
	if err != nil || d.Seconds() != 5 {
		t.Errorf("5s duration: got (%v,%v)", d, err)
	}
	if _, err = parseDuration("notaduration"); err == nil {
		t.Error("expected parse error for invalid duration")
	}
}

func TestParseProbeTimeouts(t *testing.T) {
	probe := v1alpha1.ProbeAttributes{
		Name: "p1",
		Type: "httpProbe",
		RunProperties: v1alpha1.RunProperty{
			ProbeTimeout:         "5s",
			Interval:             "1s",
			ProbePollingInterval: "2s",
			InitialDelay:         "",
			EvaluationTimeout:    "10s",
		},
	}
	to, err := parseProbeTimeouts(probe)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if to.ProbeTimeout.Seconds() != 5 || to.Interval.Seconds() != 1 || to.ProbePollingInterval.Seconds() != 2 || to.InitialDelay != 0 || to.EvaluationTimeout.Seconds() != 10 {
		t.Errorf("unexpected timeouts %+v", to)
	}

	// invalid field surfaces an error
	bad := v1alpha1.ProbeAttributes{
		Name: "p2", Type: "cmdProbe",
		RunProperties: v1alpha1.RunProperty{ProbeTimeout: "garbage"},
	}
	if _, err := parseProbeTimeouts(bad); err == nil {
		t.Error("expected error for invalid ProbeTimeout")
	}
}

func TestGenerateError(t *testing.T) {
	err := generateError("myprobe", "httpProbe", "Interval", os.ErrInvalid)
	if err == nil {
		t.Fatal("expected error")
	}
	var cerr cerrors.Error
	if c, ok := err.(cerrors.Error); ok {
		cerr = c
	} else {
		t.Fatalf("expected cerrors.Error, got %T", err)
	}
	if cerr.ErrorCode != cerrors.ErrorTypeGeneric {
		t.Errorf("expected GENERIC_ERROR, got %s", cerr.ErrorCode)
	}
}
