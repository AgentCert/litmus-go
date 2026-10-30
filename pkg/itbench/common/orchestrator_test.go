package common

import (
	"context"
	"testing"
	"time"

	"github.com/litmuschaos/litmus-go/pkg/types"
)

// TestRevertContext_SurvivesParentCancellation is what keeps an aborted run from leaving the
// target permanently mutated: every helper's post-hold revert runs on this context, so it
// must stay usable after the experiment's own ctx has been cancelled by SIGTERM.
func TestRevertContext_SurvivesParentCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()

	revertCtx, revertCancel := RevertContext()
	defer revertCancel()

	if parent.Err() == nil {
		t.Fatal("test setup: parent context should already be cancelled")
	}
	if err := revertCtx.Err(); err != nil {
		t.Fatalf("RevertContext is already done (%v); the revert API calls would fail", err)
	}
	deadline, ok := revertCtx.Deadline()
	if !ok {
		t.Fatal("RevertContext has no deadline; a hung revert would block the abort grace period forever")
	}
	if remaining := time.Until(deadline); remaining <= 0 || remaining > RevertTimeout+time.Second {
		t.Fatalf("RevertContext deadline is %v away, want (0, %v]", remaining, RevertTimeout)
	}
}

// TestAbortRevertGraceExceedsRevertTimeout guards the ordering the abort path depends on:
// the watcher must not force-exit while a revert, or the recovery verification that
// follows it, is still in flight.
func TestAbortRevertGraceExceedsRevertTimeout(t *testing.T) {
	for _, details := range []*types.ChaosDetails{{}, {Timeout: 600}} {
		grace := abortRevertGrace(details)
		floor := RevertTimeout + RecoveryBudget(details)
		if grace <= floor {
			t.Fatalf("abortRevertGrace=%v must exceed revert+recovery=%v, else a slow revert is killed mid-flight", grace, floor)
		}
	}
}
