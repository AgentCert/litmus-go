package result

import (
	"testing"

	"github.com/litmuschaos/chaos-operator/api/litmuschaos/v1alpha1"
	"github.com/litmuschaos/litmus-go/pkg/types"
)

// A Pass that nothing justified is the "free 100%" bug: updateResultAttributes turns any
// Pass into probeSuccessPercentage=100, so a fault that merely injected cleanly used to
// certify an agent that did nothing at all.
func TestDowngradeUngradedPass(t *testing.T) {
	cases := []struct {
		name    string
		details types.ResultDetails
		want    v1alpha1.ResultVerdict
	}{
		{
			"ungraded pass with no probes -> N/A",
			types.ResultDetails{Verdict: v1alpha1.ResultVerdictPassed},
			VerdictNotApplicable,
		},
		{
			"pass graded by a built-in assertion stays a pass",
			types.ResultDetails{Verdict: v1alpha1.ResultVerdictPassed, Graded: true},
			v1alpha1.ResultVerdictPassed,
		},
		{
			"pass backed by an explicit probe stays a pass",
			types.ResultDetails{
				Verdict:      v1alpha1.ResultVerdictPassed,
				ProbeDetails: []*types.ProbeDetails{{Name: "healthcheck"}},
			},
			v1alpha1.ResultVerdictPassed,
		},
		{
			"failures are never rewritten",
			types.ResultDetails{Verdict: v1alpha1.ResultVerdictFailed},
			v1alpha1.ResultVerdictFailed,
		},
		{
			"aborts are never rewritten",
			types.ResultDetails{Verdict: v1alpha1.ResultVerdictStopped},
			v1alpha1.ResultVerdictStopped,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			details := c.details
			downgradeUngradedPass(&details)
			if details.Verdict != c.want {
				t.Fatalf("verdict = %q, want %q", details.Verdict, c.want)
			}
		})
	}
}

// An ungraded run must explain itself: with no probes declared, the synthetic entry is the
// only record of why a verdict was reached once the ChaosEngine is garbage-collected.
func TestDowngradeUngradedPassExplainsItself(t *testing.T) {
	details := types.ResultDetails{Verdict: v1alpha1.ResultVerdictPassed}
	downgradeUngradedPass(&details)
	if details.GradingDetail == "" {
		t.Fatal("expected a reason to be recorded for the downgrade")
	}
}

func TestGradingProbeStatus(t *testing.T) {
	cases := []struct {
		name    string
		details types.ResultDetails
		wantOK  bool
		want    v1alpha1.ProbeVerdict
	}{
		{
			"nothing recorded -> no synthetic entry",
			types.ResultDetails{},
			false,
			"",
		},
		{
			"graded pass",
			types.ResultDetails{Graded: true, GradingDetail: "restored", Verdict: v1alpha1.ResultVerdictPassed},
			true,
			v1alpha1.ProbeVerdictPassed,
		},
		{
			"graded failure",
			types.ResultDetails{Graded: true, GradingDetail: "not restored", Verdict: v1alpha1.ResultVerdictFailed},
			true,
			v1alpha1.ProbeVerdictFailed,
		},
		{
			"ungraded",
			types.ResultDetails{GradingDetail: "unsupported kind", Verdict: VerdictNotApplicable},
			true,
			v1alpha1.ProbeVerdictNA,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			status, ok := gradingProbeStatus(&c.details)
			if ok != c.wantOK {
				t.Fatalf("ok = %v, want %v", ok, c.wantOK)
			}
			if !ok {
				return
			}
			if status.Status.Verdict != c.want {
				t.Fatalf("probe verdict = %q, want %q", status.Status.Verdict, c.want)
			}
			if status.Status.Description != c.details.GradingDetail {
				t.Fatalf("description = %q, want %q", status.Status.Description, c.details.GradingDetail)
			}
			if status.Name != RecoveryProbeName {
				t.Fatalf("name = %q, want %q", status.Name, RecoveryProbeName)
			}
		})
	}
}

func TestMarkGradedAndUngraded(t *testing.T) {
	var passed types.ResultDetails
	MarkGraded(&passed, true, "restored")
	if passed.Verdict != v1alpha1.ResultVerdictPassed || !passed.Graded {
		t.Fatalf("MarkGraded(pass) = %+v", passed)
	}

	var failed types.ResultDetails
	MarkGraded(&failed, false, "not restored")
	if failed.Verdict != v1alpha1.ResultVerdictFailed || !failed.Graded {
		t.Fatalf("MarkGraded(fail) = %+v", failed)
	}

	var ungraded types.ResultDetails
	MarkUngraded(&ungraded, "nothing to assert")
	if ungraded.Verdict != VerdictNotApplicable || ungraded.Graded {
		t.Fatalf("MarkUngraded = %+v", ungraded)
	}
}
