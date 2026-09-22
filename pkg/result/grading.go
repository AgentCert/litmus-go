package result

import (
	"github.com/litmuschaos/chaos-operator/api/litmuschaos/v1alpha1"
	"github.com/litmuschaos/litmus-go/pkg/types"
)

// This file defines what it means for a chaos run to be *graded* in an agent
// certification context, which is a different question from the one upstream
// LitmusChaos answers.
//
// Upstream, a fault that injects cleanly reports Verdict=Pass, and
// updateResultAttributes turns any Pass into probeSuccessPercentage=100. That is
// correct for resilience testing ("did the fault run, did the app survive") but
// wrong here: the thing under test is the AGENT, and "the fault injected fine"
// says nothing about whether the agent detected or remediated anything. Left
// alone it awards a free 100% to every experiment built from faults that declare
// no probes -- which is currently every fault in the catalog.
//
// So a terminal Pass is only honoured when something actually evaluated the
// agent: an explicit probe, or a built-in recovery assertion (see
// pkg/itbench/common's defaultRecoveryAssertion). Anything else is reported as
// VerdictNotApplicable, which the control plane removes from the resiliency-score
// denominator rather than scoring as either a pass or a failure.

const (
	// VerdictNotApplicable marks a run that completed but could not be graded on
	// agent remediation. v1alpha1 has no constant for it (ResultVerdict is a bare
	// string type); the value matches the "n/a" case in updateResultAttributes and
	// the "N/A" the GraphQL server excludes from scoring.
	VerdictNotApplicable v1alpha1.ResultVerdict = "N/A"

	// RecoveryProbeName is the name of the synthetic probe entry used to publish the
	// built-in recovery assertion's outcome. Faults declare no probes, so without
	// this the ChaosResult's probeStatuses is always empty and a verdict has no
	// recorded justification anywhere -- neither in the CR, nor in the Argo node,
	// nor (once the ChaosEngine is garbage-collected) in the cluster at all.
	RecoveryProbeName = "agent-recovery-assertion"
	// RecoveryProbeType/Mode mirror the shape of a real probe entry so existing
	// ChaosResult consumers render it without special-casing.
	RecoveryProbeType = "BuiltInProbe"
	RecoveryProbeMode = "OnChaos"
)

// MarkGraded records that an agent-remediation assertion ran and decided the run.
// detail explains the decision and is surfaced on the ChaosResult.
func MarkGraded(resultDetails *types.ResultDetails, passed bool, detail string) {
	resultDetails.Graded = true
	resultDetails.GradingDetail = detail
	if passed {
		resultDetails.Verdict = v1alpha1.ResultVerdictPassed
		return
	}
	resultDetails.Verdict = v1alpha1.ResultVerdictFailed
}

// MarkUngraded records that nothing could evaluate the agent's remediation, so the
// run must be excluded from scoring instead of counted as a pass or a failure.
func MarkUngraded(resultDetails *types.ResultDetails, detail string) {
	resultDetails.Graded = false
	resultDetails.GradingDetail = detail
	resultDetails.Verdict = VerdictNotApplicable
}

// gradingProbeStatus renders the grading outcome as a probe entry, or returns false
// when nothing recorded one.
func gradingProbeStatus(resultDetails *types.ResultDetails) (v1alpha1.ProbeStatuses, bool) {
	if resultDetails.GradingDetail == "" {
		return v1alpha1.ProbeStatuses{}, false
	}

	verdict := v1alpha1.ProbeVerdictNA
	if resultDetails.Graded {
		verdict = v1alpha1.ProbeVerdictPassed
		if resultDetails.Verdict == v1alpha1.ResultVerdictFailed {
			verdict = v1alpha1.ProbeVerdictFailed
		}
	}

	return v1alpha1.ProbeStatuses{
		Name: RecoveryProbeName,
		Type: RecoveryProbeType,
		Mode: RecoveryProbeMode,
		Status: v1alpha1.ProbeStatus{
			Verdict:     verdict,
			Description: resultDetails.GradingDetail,
		},
	}, true
}

// downgradeUngradedPass rewrites a terminal Pass that no probe and no built-in
// assertion ever justified into VerdictNotApplicable. Returning Pass here is what
// produces the free 100%; returning Fail instead would punish the agent for a gap
// in our measurement, so the run is excluded from scoring entirely.
func downgradeUngradedPass(resultDetails *types.ResultDetails) {
	if resultDetails.Verdict != v1alpha1.ResultVerdictPassed {
		return
	}
	if resultDetails.Graded || len(resultDetails.ProbeDetails) != 0 {
		return
	}
	resultDetails.Verdict = VerdictNotApplicable
	if resultDetails.GradingDetail == "" {
		resultDetails.GradingDetail = "fault injected successfully but nothing graded the agent's " +
			"remediation: this fault declares no probe and has no built-in recovery assertion, " +
			"so the run is excluded from the resiliency score. Add a probe to the ChaosEngine to grade it."
	}
}
