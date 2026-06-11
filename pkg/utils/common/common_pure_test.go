package common

import (
	"strconv"
	"testing"

	"github.com/litmuschaos/chaos-operator/api/litmuschaos/v1alpha1"
	"github.com/litmuschaos/litmus-go/pkg/types"
	apiv1 "k8s.io/api/core/v1"
)

func TestGetStatusMessage(t *testing.T) {
	tests := []struct {
		name         string
		defaultCheck bool
		defaultMsg   string
		probeStatus  string
		want         string
	}{
		{"defaults only", true, "all checks passed", "", "all checks passed"},
		{"defaults plus probes", true, "all checks passed", "1/1", "all checks passed, Probes: 1/1"},
		{"skipped no probes", false, "ignored", "", "Skipped the default checks"},
		{"skipped with probes", false, "ignored", "2/2", "Probes: 2/2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetStatusMessage(tt.defaultCheck, tt.defaultMsg, tt.probeStatus); got != tt.want {
				t.Errorf("GetStatusMessage() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetRandomSequence(t *testing.T) {
	if got := GetRandomSequence("serial"); got != "serial" {
		t.Errorf("non-random passthrough: got %q", got)
	}
	if got := GetRandomSequence("Parallel"); got != "Parallel" {
		t.Errorf("non-random passthrough: got %q", got)
	}
	// random must resolve to one of the two known values
	for i := 0; i < 20; i++ {
		got := GetRandomSequence("random")
		if got != "serial" && got != "parallel" {
			t.Fatalf("random produced unexpected value %q", got)
		}
	}
	// case-insensitive
	got := GetRandomSequence("RANDOM")
	if got != "serial" && got != "parallel" {
		t.Fatalf("RANDOM produced unexpected value %q", got)
	}
}

func TestValidateRange(t *testing.T) {
	// single value passes through unchanged
	if got := ValidateRange("42"); got != "42" {
		t.Errorf("single value: got %q want 42", got)
	}
	// invalid (3 parts) returns "0"
	if got := ValidateRange("1-2-3"); got != "0" {
		t.Errorf("invalid range: got %q want 0", got)
	}
	// range returns a value within [lb, ub]
	for i := 0; i < 20; i++ {
		got := ValidateRange("5-10")
		n, err := strconv.Atoi(got)
		if err != nil {
			t.Fatalf("range produced non-int %q", got)
		}
		if n < 5 || n > 10 {
			t.Fatalf("range value %d outside [5,10]", n)
		}
	}
}

func TestSubStringExistsInSlice(t *testing.T) {
	slice := []string{"alpha", "beta", "gamma"}
	if !SubStringExistsInSlice("xbetax", slice) {
		t.Error("expected substring match for 'xbetax'")
	}
	if SubStringExistsInSlice("zzz", slice) {
		t.Error("expected no match for 'zzz'")
	}
	if SubStringExistsInSlice("anything", nil) {
		t.Error("expected no match against nil slice")
	}
}

func TestContains(t *testing.T) {
	if !Contains("b", []string{"a", "b", "c"}) {
		t.Error("expected string slice to contain 'b'")
	}
	if Contains("z", []string{"a", "b"}) {
		t.Error("did not expect 'z'")
	}
	if !Contains(2, []int{1, 2, 3}) {
		t.Error("expected int slice to contain 2")
	}
	if Contains("x", nil) {
		t.Error("nil slice should never contain")
	}
}

func TestRandomInterval(t *testing.T) {
	if err := RandomInterval("abc"); err == nil {
		t.Error("expected error for bad input")
	}
	if err := RandomInterval("0"); err == nil {
		t.Error("expected error for upper bound below 1")
	}
	if err := RandomInterval("1"); err != nil {
		t.Errorf("expected valid single interval, got %v", err)
	}
	if err := RandomInterval("1-3"); err != nil {
		t.Errorf("expected valid range interval, got %v", err)
	}
}

func TestFilterBasedOnPercentage(t *testing.T) {
	list := []string{"a", "b", "c", "d", "e"}
	// 100% should select all elements (as a set)
	got := FilterBasedOnPercentage(100, list)
	if len(got) != len(list) {
		t.Errorf("100%% should yield %d elements, got %d", len(list), len(got))
	}
	// low percentage always yields at least 1 (Maximum(1, ...))
	got = FilterBasedOnPercentage(1, list)
	if len(got) < 1 {
		t.Errorf("expected at least 1 element, got %d", len(got))
	}
	// every returned element must come from the source list
	src := map[string]bool{"a": true, "b": true, "c": true, "d": true, "e": true}
	for _, v := range got {
		if !src[v] {
			t.Errorf("returned element %q not in source list", v)
		}
	}
}

func TestSetEnv(t *testing.T) {
	d := &ENVDetails{}
	d.SetEnv("KEY", "value").SetEnv("EMPTY", "")
	if len(d.ENV) != 1 {
		t.Fatalf("expected 1 env (empty value skipped), got %d", len(d.ENV))
	}
	if d.ENV[0].Name != "KEY" || d.ENV[0].Value != "value" {
		t.Errorf("unexpected env entry %+v", d.ENV[0])
	}
}

func TestSetEnvFromDownwardAPI(t *testing.T) {
	d := &ENVDetails{}
	d.SetEnvFromDownwardAPI("", "")
	if len(d.ENV) != 0 {
		t.Error("expected no env added when apiVersion/fieldPath empty")
	}
	d.SetEnvFromDownwardAPI("v1", "metadata.name")
	if len(d.ENV) != 1 || d.ENV[0].Name != "POD_NAME" || d.ENV[0].ValueFrom == nil {
		t.Errorf("expected POD_NAME downward env, got %+v", d.ENV)
	}
	if d.ENV[0].ValueFrom.FieldRef.FieldPath != "metadata.name" {
		t.Errorf("unexpected field path %q", d.ENV[0].ValueFrom.FieldRef.FieldPath)
	}
}

func TestGetContainerNames(t *testing.T) {
	cd := &types.ChaosDetails{ExperimentName: "pod-delete"}
	got := GetContainerNames(cd)
	if len(got) != 1 || got[0] != "pod-delete" {
		t.Errorf("expected [pod-delete], got %v", got)
	}
	cd.SideCar = []types.SideCar{{Name: "sidecar-1"}, {Name: "sidecar-2"}}
	got = GetContainerNames(cd)
	want := []string{"pod-delete", "sidecar-1", "sidecar-2"}
	if len(got) != 3 {
		t.Fatalf("expected 3 names, got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("name[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestBuildSidecarAndVolumes(t *testing.T) {
	cd := &types.ChaosDetails{
		SideCar: []types.SideCar{
			{
				Name:  "sc1",
				Image: "img:1",
				Secrets: []v1alpha1.Secret{
					{Name: "shared", MountPath: "/a"},
				},
			},
			{
				Name:  "sc2",
				Image: "img:2",
				Secrets: []v1alpha1.Secret{
					{Name: "shared", MountPath: "/b"}, // duplicate secret name
					{Name: "unique", MountPath: "/c"},
				},
			},
		},
	}

	sidecars := BuildSidecar(cd)
	if len(sidecars) != 2 {
		t.Fatalf("expected 2 sidecar containers, got %d", len(sidecars))
	}
	if sidecars[0].Name != "sc1" || sidecars[0].Image != "img:1" {
		t.Errorf("unexpected first sidecar %+v", sidecars[0])
	}
	if len(sidecars[0].VolumeMounts) != 1 || sidecars[0].VolumeMounts[0].Name != "shared" {
		t.Errorf("unexpected volume mounts %+v", sidecars[0].VolumeMounts)
	}

	vols := GetSidecarVolumes(cd)
	// "shared" must be de-duplicated -> 2 unique volumes
	if len(vols) != 2 {
		t.Fatalf("expected 2 unique volumes, got %d (%v)", len(vols), vols)
	}
	names := map[string]bool{}
	for _, v := range vols {
		names[v.Name] = true
		if v.VolumeSource.Secret == nil || v.VolumeSource.Secret.SecretName != v.Name {
			t.Errorf("volume %q missing secret source", v.Name)
		}
	}
	if !names["shared"] || !names["unique"] {
		t.Errorf("expected shared+unique volumes, got %v", names)
	}
}

func TestHelperFailedError(t *testing.T) {
	// underlying error -> propagated
	if err := HelperFailedError(assertErr{}, "lbl", "ns", true); err == nil {
		t.Error("expected propagated error")
	}
	// nil err, podLevel true
	err := HelperFailedError(nil, "lbl", "ns", true)
	if err == nil {
		t.Fatal("expected helper-pod-failed error")
	}
	// nil err, podLevel false
	err = HelperFailedError(nil, "lbl", "ns", false)
	if err == nil {
		t.Fatal("expected generic helper error")
	}
}

type assertErr struct{}

func (assertErr) Error() string { return "boom" }

// ensure apiv1 import is exercised
var _ = apiv1.EnvVar{}
