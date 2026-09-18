package common

import (
	"context"
	"testing"
	"time"

	"github.com/litmuschaos/litmus-go/pkg/types"
)

// TestRunMidChaosHook_NoHookIsNoop makes sure calling RunMidChaosHook with nothing
// installed (the state for every fault outside of Run(), and for Run() itself whenever
// there are no probes) never panics -- HoldChaos calls it unconditionally.
func TestRunMidChaosHook_NoHookIsNoop(t *testing.T) {
	setMidChaosHook(nil)
	RunMidChaosHook(context.Background())
}

// TestRunMidChaosHook_InvokesInstalledHook confirms the install/run/clear cycle
// orchestrator.Run() relies on: a hook set via setMidChaosHook fires exactly once per
// RunMidChaosHook call, and is inert again once cleared.
func TestRunMidChaosHook_InvokesInstalledHook(t *testing.T) {
	calls := 0
	setMidChaosHook(func(ctx context.Context) { calls++ })
	defer setMidChaosHook(nil)

	RunMidChaosHook(context.Background())
	RunMidChaosHook(context.Background())
	if calls != 2 {
		t.Fatalf("hook called %d times, want 2", calls)
	}

	setMidChaosHook(nil)
	RunMidChaosHook(context.Background())
	if calls != 2 {
		t.Fatalf("hook fired after being cleared: calls=%d, want 2", calls)
	}
}

// TestHoldChaos_RunsHookAfterTheHold is the behavior every itbench patch helper depends
// on: HoldChaos must not return (letting the caller proceed to revert) until both the
// ChaosDuration hold AND the mid-chaos hook have completed, and the hook must see the
// fault as still "held" -- i.e. it runs before the caller's own revert step, never after.
func TestHoldChaos_RunsHookAfterTheHold(t *testing.T) {
	hookRan := false
	setMidChaosHook(func(ctx context.Context) { hookRan = true })
	defer setMidChaosHook(nil)

	start := time.Now()
	HoldChaos(context.Background(), &types.ChaosDetails{ChaosDuration: 1})
	elapsed := time.Since(start)

	if !hookRan {
		t.Fatal("HoldChaos returned without running the mid-chaos hook")
	}
	if elapsed < time.Second {
		t.Fatalf("HoldChaos returned after %v, want >= 1s (the hold)", elapsed)
	}
}

// TestHoldChaos_ContextCancelSkipsHookAndReturns covers the abort path (SIGTERM relayed by
// the dispatcher's signal-aware ctx): HoldChaos must return promptly so the caller's revert
// still runs, and must NOT evaluate recovery -- the agent never got its full remediation
// window, and Run() records a Stopped verdict for the run regardless.
func TestHoldChaos_ContextCancelSkipsHookAndReturns(t *testing.T) {
	hookRan := false
	setMidChaosHook(func(ctx context.Context) { hookRan = true })
	defer setMidChaosHook(nil)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	HoldChaos(ctx, &types.ChaosDetails{ChaosDuration: 300}) // would block 300s without the cancel
	elapsed := time.Since(start)

	if elapsed >= 300*time.Second {
		t.Fatalf("HoldChaos did not honor ctx cancellation, took %v", elapsed)
	}
	if hookRan {
		t.Fatal("HoldChaos evaluated recovery after an abort; the hold was cut short so the check is meaningless")
	}
}

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
// the watcher must not force-exit while a revert that is merely slow is still in flight.
func TestAbortRevertGraceExceedsRevertTimeout(t *testing.T) {
	if abortRevertGrace <= RevertTimeout {
		t.Fatalf("abortRevertGrace=%v must exceed RevertTimeout=%v, else a slow revert is killed mid-flight", abortRevertGrace, RevertTimeout)
	}
}
