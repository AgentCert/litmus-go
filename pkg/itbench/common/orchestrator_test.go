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

// TestHoldChaos_ContextCancelDuringHoldStillRunsHook confirms the abort-watcher path (ctx
// cancellation via SIGTERM) still gives the hook a chance to run -- Sleep returns early on
// ctx.Done(), and HoldChaos must still call the hook afterward so a killed run doesn't
// silently skip probe evaluation.
func TestHoldChaos_ContextCancelDuringHoldStillRunsHook(t *testing.T) {
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

	if !hookRan {
		t.Fatal("HoldChaos did not run the mid-chaos hook after ctx cancellation")
	}
	if elapsed >= 300*time.Second {
		t.Fatalf("HoldChaos did not honor ctx cancellation, took %v", elapsed)
	}
}
