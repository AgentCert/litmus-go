// Package common provides the shared litmus-go SDK plumbing (ChaosResult lifecycle,
// probes, abort-watcher, events, generic target resolution) reused by every fault under
// experiments/itbench/. Each fault only implements its own InjectFunc; this package
// gives it full SDK-conformant behavior (real ChaosResult reporting, real probe
// execution, real TARGETS-derived target resolution) for free -- the same behavior the
// built-in experiments (pod-delete, node-taint, ...) get, instead of the raw-shell-script
// approach the itbench faults previously used (see chaos-charts/ITBENCH_HANDOFF.md).
package common

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/litmuschaos/chaos-operator/api/litmuschaos/v1alpha1"
	"github.com/litmuschaos/litmus-go/pkg/cerrors"
	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/events"
	"github.com/litmuschaos/litmus-go/pkg/log"
	"github.com/litmuschaos/litmus-go/pkg/probe"
	"github.com/litmuschaos/litmus-go/pkg/result"
	"github.com/litmuschaos/litmus-go/pkg/status"
	"github.com/litmuschaos/litmus-go/pkg/types"
	litmusCommon "github.com/litmuschaos/litmus-go/pkg/utils/common"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktypes "k8s.io/apimachinery/pkg/types"
)

// InjectFunc performs one fault's actual mutation (resolve target, capture original
// state, patch, hold for chaosDetails.ChaosDuration, revert) and returns nil on success.
type InjectFunc func(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error

// midChaosHook, when set, is invoked by HoldChaos (called from every itbench patch
// helper's hold-then-revert point in fieldpatch.go/containerpatch.go) at the instant the
// fault has been held for its full ChaosDuration but BEFORE the helper reverts it -- the
// last moment the injected fault is still live.
//
// Run() uses it to log the target's state at that instant (see defaultRecoveryAssertion).
// That is a record only: like the upstream generic faults, an itbench verdict says
// whether the fault injected and reverted cleanly, never what the agent under test did.
// The agent is judged by the certifier from its own trace.
var (
	midChaosHook   func(ctx context.Context)
	midChaosHookMu sync.Mutex
)

// setMidChaosHook installs (or, passed nil, clears) the mid-chaos probe hook. Guarded by
// a mutex only for safety against concurrent Run() calls in the same process (the
// itbench-experiment binary only ever runs one at a time in practice, via -name).
func setMidChaosHook(fn func(ctx context.Context)) {
	midChaosHookMu.Lock()
	midChaosHook = fn
	midChaosHookMu.Unlock()
}

// RunMidChaosHook invokes the currently-installed mid-chaos hook, if any. It is a no-op
// (and therefore safe to call unconditionally) when Run() has no probes to evaluate --
// setMidChaosHook is only ever called when resultDetails.ProbeDetails is non-empty.
func RunMidChaosHook(ctx context.Context) {
	midChaosHookMu.Lock()
	fn := midChaosHook
	midChaosHookMu.Unlock()
	if fn != nil {
		fn(ctx)
	}
}

// HoldChaos blocks for the fault's ChaosDuration (honoring ctx cancellation, same as
// Sleep) and then runs the mid-chaos hook. Every itbench patch helper calls this in place
// of a bare Sleep(ctx, chaosDetails.ChaosDuration) at its hold-then-revert point, so the
// helper's own revert logic (unchanged) always still runs immediately afterward -- a
// probe failure here never leaves the target un-reverted.
func HoldChaos(ctx context.Context, chaosDetails *types.ChaosDetails) {
	Sleep(ctx, chaosDetails.ChaosDuration)
	if ctx.Err() != nil {
		// Aborted mid-hold: skip the end-of-window observation (the window never ended)
		// and fall straight through to the caller's revert, which runs on RevertContext
		// and therefore still works.
		log.Info("[Abort]: hold interrupted -- skipping the end-of-window observation and reverting the fault now")
		return
	}
	RunMidChaosHook(ctx)
}

// RevertTimeout bounds each fault's post-hold revert.
var RevertTimeout = 90 * time.Second

// abortRevertGrace is how long the abort watcher lets an in-flight revert finish before
// force-exiting. Must exceed RevertTimeout so a revert that is merely slow is not killed.
var abortRevertGrace = RevertTimeout + 30*time.Second

// RevertContext returns the context a fault's post-hold revert must use instead of the
// experiment's own ctx. On SIGTERM the experiment ctx is cancelled, which would make every
// revert API call fail with "context canceled" and leave the target permanently mutated --
// a workflow stop would break the cluster rather than just ending the run.
func RevertContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), RevertTimeout)
}

// defaultRecoveryAssertion reports whether the SDK-resolved target
// (chaosDetails.AppDetail[0]) looks restored at the end of the fault hold, before the
// fault reverts it. Run() only logs the result as a record of the target's state; it
// never decides the verdict.
//
//   - deployment/statefulset : status.readyReplicas >= 1
//   - pod                    : at least one matching pod is Ready
//   - service                : the Service's Endpoints has >= 1 ready address
//   - horizontalpodautoscaler: spec targets no longer match the injected values
//   - any other kind         : not checked -- can't generically tell
//
// graded=false means nothing could be checked (check disabled, no target resolved,
// unsupported kind, or the lookup itself failed); ok is meaningless then. Teardown
// experiments (uninstall-*) are not checked either. Gated by
// ITBENCH_DEFAULT_RECOVERY_CHECK (default on; "false"/"0"/"no" disables).
func defaultRecoveryAssertion(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) (graded, ok bool, detail string) {
	switch v := strings.ToLower(strings.TrimSpace(os.Getenv("ITBENCH_DEFAULT_RECOVERY_CHECK"))); v {
	case "false", "0", "no", "off":
		return true, true, "recovery assertion disabled via ITBENCH_DEFAULT_RECOVERY_CHECK"
	}
	if strings.HasPrefix(strings.ToLower(chaosDetails.ExperimentName), "uninstall-") {
		return true, true, "teardown experiment -- not graded on remediation"
	}
	if len(chaosDetails.AppDetail) == 0 {
		return false, false, "no target resolved (TARGETS unset) -- nothing to assert"
	}
	app := chaosDetails.AppDetail[0]
	switch strings.ToLower(app.Kind) {
	case "deployment", "deployments", "statefulset", "statefulsets":
		gvr, err := WorkloadGVRForKind(app.Kind)
		if err != nil {
			return false, false, "unknown workload kind: " + err.Error()
		}
		obj, err := ResolveTarget(ctx, cs, gvr, chaosDetails)
		if err != nil {
			return false, false, "could not resolve target workload: " + err.Error()
		}
		ready, _, _ := unstructured.NestedInt64(obj.Object, "status", "readyReplicas")
		if ready >= 1 {
			return true, true, fmt.Sprintf("%s/%s readyReplicas=%d -- target restored", gvr.Resource, obj.GetName(), ready)
		}
		return true, false, fmt.Sprintf("%s/%s still has readyReplicas=%d -- target not restored", gvr.Resource, obj.GetName(), ready)
	case "pod", "pods":
		pods, err := ResolveTargets(ctx, cs, GVRPods, chaosDetails)
		if err != nil || len(pods) == 0 {
			return false, false, "could not resolve target pods"
		}
		for _, p := range pods {
			conds, _, _ := unstructured.NestedSlice(p.Object, "status", "conditions")
			for _, c := range conds {
				if m, _ := c.(map[string]interface{}); m["type"] == "Ready" && m["status"] == "True" {
					return true, true, fmt.Sprintf("pod %s is Ready -- target restored", p.GetName())
				}
			}
		}
		return true, false, "no target pod is Ready -- target not restored"
	case "service", "services":
		svc, err := ResolveTarget(ctx, cs, GVRServices, chaosDetails)
		if err != nil {
			return true, false, "target Service not found -- not restored: " + err.Error()
		}
		epsGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "endpoints"}
		eps, err := cs.DynamicClient.Resource(epsGVR).Namespace(svc.GetNamespace()).Get(ctx, svc.GetName(), metav1.GetOptions{})
		if err != nil {
			return true, false, fmt.Sprintf("Service %s has no Endpoints object -- not restored", svc.GetName())
		}
		subsets, _, _ := unstructured.NestedSlice(eps.Object, "subsets")
		for _, s := range subsets {
			if m, _ := s.(map[string]interface{}); m != nil {
				if addrs, ok := m["addresses"].([]interface{}); ok && len(addrs) > 0 {
					return true, true, fmt.Sprintf("Service %s has %d ready endpoint address(es) -- target restored", svc.GetName(), len(addrs))
				}
			}
		}
		return true, false, fmt.Sprintf("Service %s has 0 ready endpoint addresses -- not restored", svc.GetName())
	case "horizontalpodautoscaler", "horizontalpodautoscalers", "hpa":
		return hpaRecoveryAssertion(ctx, cs, chaosDetails)
	default:
		return false, false, "no built-in recovery assertion for kind " + app.Kind
	}
}

// hpaRecoveryAssertion grades misconfigured-kubernetes-horizontal-pod-autoscaler, whose
// damage is a *configuration* value rather than an unready workload -- readyReplicas stays
// healthy throughout, so the generic workload assertion would report a free pass.
//
// The fault sets the HPA's cpu/memory utilization targets to CPU_UTILIZATION_PERCENT /
// MEMORY_UTILIZATION_PERCENT. The agent has remediated it when the live targets no longer
// match those injected values. With neither env var set there is nothing to compare
// against, so the run is ungraded rather than passed.
func hpaRecoveryAssertion(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) (graded, ok bool, detail string) {
	injected := map[string]string{}
	for resourceName, envName := range map[string]string{
		"cpu":    "CPU_UTILIZATION_PERCENT",
		"memory": "MEMORY_UTILIZATION_PERCENT",
	} {
		if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
			injected[resourceName] = v
		}
	}
	if len(injected) == 0 {
		return false, false, "no injected HPA utilization values (CPU_/MEMORY_UTILIZATION_PERCENT unset) -- nothing to compare against"
	}

	obj, err := ResolveTarget(ctx, cs, GVRHPA, chaosDetails)
	if err != nil {
		return false, false, "could not resolve target HorizontalPodAutoscaler: " + err.Error()
	}

	metrics, _, _ := unstructured.NestedSlice(obj.Object, "spec", "metrics")
	var stillInjected []string
	for _, m := range metrics {
		entry, _ := m.(map[string]interface{})
		if entry == nil {
			continue
		}
		resourceName, _, _ := unstructured.NestedString(entry, "resource", "name")
		want, tracked := injected[strings.ToLower(resourceName)]
		if !tracked {
			continue
		}
		live, found, _ := unstructured.NestedInt64(entry, "resource", "target", "averageUtilization")
		if found && strconv.FormatInt(live, 10) == want {
			stillInjected = append(stillInjected, fmt.Sprintf("%s=%d", resourceName, live))
		}
	}
	if len(stillInjected) > 0 {
		return true, false, fmt.Sprintf("HPA %s still carries the injected utilization target(s) %s -- not remediated",
			obj.GetName(), strings.Join(stillInjected, ", "))
	}
	return true, true, fmt.Sprintf("HPA %s no longer matches the injected utilization target(s) -- remediated", obj.GetName())
}

// recordAbort writes the "Stopped" ChaosResult and the abort events -- the same bookkeeping
// litmusCommon.AbortWatcher does, minus its os.Exit(1). The exit is what makes the upstream
// watcher unusable here: it kills the goroutine parked in HoldChaos, so the fault's revert
// (which lives after the hold, inside InjectFunc) never runs.
func recordAbort(cs clients.ClientSets, resultDetails *types.ResultDetails, chaosDetails *types.ChaosDetails, eventsDetails *types.EventDetails) {
	types.SetResultAfterCompletion(resultDetails, "Stopped", "Stopped", "Chaos injection stopped!", cerrors.ErrorTypeExperimentAborted)
	if err := result.ChaosResult(chaosDetails, cs, resultDetails, "EOT"); err != nil {
		log.Errorf("[Abort]: Failed to update result, err: %v", err)
	}

	msg := chaosDetails.ExperimentName + " experiment has been aborted"
	types.SetEngineEventAttributes(eventsDetails, types.Summary, msg, "Warning", chaosDetails)
	if err := events.GenerateEvents(eventsDetails, cs, chaosDetails, "ChaosEngine"); err != nil {
		log.Errorf("[Abort]: Failed to create chaosengine summary event, err: %v", err)
	}
	types.SetResultEventAttributes(eventsDetails, types.AbortVerdict, msg, "Warning", resultDetails)
	if err := events.GenerateEvents(eventsDetails, cs, chaosDetails, "ChaosResult"); err != nil {
		log.Errorf("[Abort]: Failed to create chaosresult abort event, err: %v", err)
	}
}

// watchAbort is the itbench replacement for litmusCommon.AbortWatcher. On SIGTERM (relayed
// by the signal-aware ctx the dispatcher builds) it does NOT exit immediately: the fault's
// hold returns early, its revert runs on RevertContext, and Run() then records the abort on
// the main goroutine. The hard exit here is only the escape hatch for a revert that hangs
// past the grace period.
func watchAbort(ctx context.Context, done <-chan struct{}, cs clients.ClientSets, resultDetails *types.ResultDetails, chaosDetails *types.ChaosDetails, eventsDetails *types.EventDetails) {
	select {
	case <-ctx.Done():
	case <-done:
		return
	}
	log.Info("[Abort]: terminated signal received -- waiting for the fault to revert before exiting")
	select {
	case <-done:
	case <-time.After(abortRevertGrace):
		log.Errorf("[Abort]: revert did not finish within %s -- exiting anyway; the target may still be mutated", abortRevertGrace)
		recordAbort(cs, resultDetails, chaosDetails, eventsDetails)
		os.Exit(1)
	}
}

// Run drives the full litmus-go experiment lifecycle around a fault-specific InjectFunc:
// env parsing, SOT/EOT ChaosResult create+patch, pre/post-chaos probes, the abort-watcher
// goroutine, and ChaosEngine/ChaosResult events. Mirrors experiments/generic/pod-delete's
// orchestration (the upstream reference shape) exactly, parameterized by InjectFunc so it
// is shared by all itbench faults instead of being copy-pasted per fault.
func Run(ctx context.Context, cs clients.ClientSets, inject InjectFunc) {
	resultDetails := types.ResultDetails{}
	eventsDetails := types.EventDetails{}
	chaosDetails := types.ChaosDetails{}

	log.Infof("[PreReq]: Getting the ENV for the %v experiment", os.Getenv("EXPERIMENT_NAME"))
	types.InitialiseChaosVariables(&chaosDetails)
	types.SetResultAttributes(&resultDetails, chaosDetails)

	if chaosDetails.EngineName != "" {
		if err := litmusCommon.GetValuesFromChaosEngine(&chaosDetails, cs, &resultDetails); err != nil {
			log.Errorf("Unable to initialize the probes, err: %v", err)
			return
		}
	}

	log.Infof("[PreReq]: Updating the chaos result of %v experiment (SOT)", chaosDetails.ExperimentName)
	if err := result.ChaosResult(&chaosDetails, cs, &resultDetails, "SOT"); err != nil {
		log.Errorf("Unable to create the chaosresult, err: %v", err)
		result.RecordAfterFailure(&chaosDetails, &resultDetails, err, cs, &eventsDetails)
		return
	}
	if err := result.SetResultUID(&resultDetails, cs, &chaosDetails); err != nil {
		log.Errorf("Unable to set the result uid, err: %v", err)
		result.RecordAfterFailure(&chaosDetails, &resultDetails, err, cs, &eventsDetails)
		return
	}

	msg := "experiment: " + chaosDetails.ExperimentName + ", Result: Awaited"
	types.SetResultEventAttributes(&eventsDetails, types.AwaitedVerdict, msg, "Normal", &resultDetails)
	if err := events.GenerateEvents(&eventsDetails, cs, &chaosDetails, "ChaosResult"); err != nil {
		log.Errorf("failed to create %v event inside chaosresult", types.AwaitedVerdict)
	}

	log.InfoWithValues("The application information is as follows", logrus.Fields{
		"Targets":        litmusCommon.GetAppDetailsForLogging(chaosDetails.AppDetail),
		"Chaos Duration": chaosDetails.ChaosDuration,
	})

	// Abort handling is watchAbort's, not litmusCommon.AbortWatcher's -- the latter's
	// os.Exit(1) would kill the goroutine parked in the fault's hold and skip its revert.
	experimentDone := make(chan struct{})
	defer close(experimentDone)
	go watchAbort(ctx, experimentDone, cs, &resultDetails, &chaosDetails, &eventsDetails)
	if chaosDetails.DefaultHealthCheck {
		log.Info("[Status]: Verify that the AUT (Application Under Test) is running (pre-chaos)")
		if err := status.AUTStatusCheck(cs, &chaosDetails); err != nil {
			log.Errorf("Application status check failed, err: %v", err)
			result.RecordAfterFailure(&chaosDetails, &resultDetails, err, cs, &eventsDetails)
			return
		}
	}

	if chaosDetails.EngineName != "" && len(resultDetails.ProbeDetails) != 0 {
		if err := probe.RunProbes(ctx, &chaosDetails, cs, &resultDetails, "PreChaos", &eventsDetails); err != nil {
			log.Errorf("Probe Failed, err: %v", err)
			result.RecordAfterFailure(&chaosDetails, &resultDetails, err, cs, &eventsDetails)
			return
		}
	}

	// Record the target's state at the end of the fault window, just before the fault
	// reverts itself (HoldChaos runs this hook from every itbench patch helper). It is
	// logged only and never affects the verdict.
	if chaosDetails.EngineName != "" {
		setMidChaosHook(func(hookCtx context.Context) {
			_, _, why := defaultRecoveryAssertion(hookCtx, cs, &chaosDetails)
			log.Infof("[Observation]: end of fault window, before revert (recorded only, not graded): %s", why)
		})
		defer setMidChaosHook(nil)
	}

	chaosDetails.Phase = types.ChaosInjectPhase
	injectErr := inject(ctx, cs, &chaosDetails)
	if ctx.Err() != nil {
		// Aborted. inject() has already reverted (its revert runs on RevertContext, which
		// survives the cancellation), so the target is clean; record Stopped and stop.
		if injectErr != nil {
			log.Errorf("Chaos injection returned during abort, err: %v", injectErr)
		}
		log.Info("[Abort]: fault reverted -- recording Stopped verdict")
		recordAbort(cs, &resultDetails, &chaosDetails, &eventsDetails)
		return
	}
	if injectErr != nil {
		log.Errorf("Chaos injection failed, err: %v", injectErr)
		result.RecordAfterFailure(&chaosDetails, &resultDetails, injectErr, cs, &eventsDetails)
		return
	}

	// The fault injected, held and reverted cleanly: that is a Pass, exactly as in the
	// upstream generic faults. Only explicit probes or the post-chaos health check
	// below can still change it.
	log.Infof("[Confirmation]: %v chaos has been injected successfully", chaosDetails.ExperimentName)
	resultDetails.Verdict = v1alpha1.ResultVerdictPassed
	chaosDetails.Phase = types.PostChaosPhase

	if chaosDetails.DefaultHealthCheck {
		log.Info("[Status]: Verify that the AUT (Application Under Test) is running (post-chaos)")
		if err := status.AUTStatusCheck(cs, &chaosDetails); err != nil {
			log.Errorf("Application status check failed, err: %v", err)
			result.RecordAfterFailure(&chaosDetails, &resultDetails, err, cs, &eventsDetails)
			return
		}
	}

	// Explicit probes keep upstream semantics: the PostChaos phase runs after the fault
	// has reverted (EOT/Edge probes, and the end of Continuous ones), and a failing
	// probe fails the run.
	if chaosDetails.EngineName != "" && len(resultDetails.ProbeDetails) != 0 {
		if err := probe.RunProbes(ctx, &chaosDetails, cs, &resultDetails, "PostChaos", &eventsDetails); err != nil {
			log.Errorf("Probes Failed, err: %v", err)
			result.RecordAfterFailure(&chaosDetails, &resultDetails, err, cs, &eventsDetails)
			return
		}
	}

	log.Infof("[The End]: Updating the chaos result of %v experiment (EOT)", chaosDetails.ExperimentName)
	if err := result.ChaosResult(&chaosDetails, cs, &resultDetails, "EOT"); err != nil {
		log.Errorf("Unable to update the chaosresult, err: %v", err)
		result.RecordAfterFailure(&chaosDetails, &resultDetails, err, cs, &eventsDetails)
		return
	}

	msg = "experiment: " + chaosDetails.ExperimentName + ", Result: " + string(resultDetails.Verdict)
	reason, eventType := types.GetChaosResultVerdictEvent(resultDetails.Verdict)
	types.SetResultEventAttributes(&eventsDetails, reason, msg, eventType, &resultDetails)
	events.GenerateEvents(&eventsDetails, cs, &chaosDetails, "ChaosResult")

	if chaosDetails.EngineName != "" {
		summaryMsg := chaosDetails.ExperimentName + " experiment has been " + string(resultDetails.Verdict) + "ed"
		types.SetEngineEventAttributes(&eventsDetails, types.Summary, summaryMsg, "Normal", &chaosDetails)
		events.GenerateEvents(&eventsDetails, cs, &chaosDetails, "ChaosEngine")
	}
}

// ResolveTarget returns the single object matching chaosDetails.AppDetail[0] -- the
// namespace plus either explicit Names or a Labels selector, both derived by the litmus-go
// SDK itself (types.InitialiseChaosVariables -> types.GetTargets) from the TARGETS env var
// the chaos-operator populates from ChaosEngine.spec.appinfo. This is the real SDK
// mechanism the itbench faults previously assumed existed as APP_NAMESPACE/APP_LABEL/
// APP_KIND env vars (it doesn't -- see the fault-catalog investigation); TARGETS is.
func ResolveTarget(ctx context.Context, cs clients.ClientSets, gvr schema.GroupVersionResource, chaosDetails *types.ChaosDetails) (*unstructured.Unstructured, error) {
	if len(chaosDetails.AppDetail) == 0 {
		return nil, fmt.Errorf("no target resolved: TARGETS env var was empty/unset (expected kind:namespace:label-or-[names])")
	}
	app := chaosDetails.AppDetail[0]
	rc := cs.DynamicClient.Resource(gvr).Namespace(app.Namespace)

	if len(app.Names) > 0 {
		return rc.Get(ctx, strings.TrimSpace(app.Names[0]), metav1.GetOptions{})
	}

	sel := labels.Set{}
	for _, l := range app.Labels {
		kv := strings.SplitN(strings.TrimSpace(l), "=", 2)
		if len(kv) == 2 {
			sel[kv[0]] = kv[1]
		}
	}
	list, err := rc.List(ctx, metav1.ListOptions{LabelSelector: sel.String()})
	if err != nil {
		return nil, fmt.Errorf("failed listing %s in ns=%s label=%s: %w", gvr.Resource, app.Namespace, sel.String(), err)
	}
	if len(list.Items) == 0 {
		return nil, fmt.Errorf("no %s found in ns=%s matching label %s", gvr.Resource, app.Namespace, sel.String())
	}
	return &list.Items[0], nil
}

// ResolveTargets is ResolveTarget but returns every matching object (for faults that
// operate on all matched pods rather than a single parent workload, e.g. the ephemeral-
// container freeze fault).
func ResolveTargets(ctx context.Context, cs clients.ClientSets, gvr schema.GroupVersionResource, chaosDetails *types.ChaosDetails) ([]unstructured.Unstructured, error) {
	if len(chaosDetails.AppDetail) == 0 {
		return nil, fmt.Errorf("no target resolved: TARGETS env var was empty/unset (expected kind:namespace:label-or-[names])")
	}
	app := chaosDetails.AppDetail[0]
	rc := cs.DynamicClient.Resource(gvr).Namespace(app.Namespace)

	if len(app.Names) > 0 {
		var items []unstructured.Unstructured
		for _, n := range app.Names {
			obj, err := rc.Get(ctx, strings.TrimSpace(n), metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			items = append(items, *obj)
		}
		return items, nil
	}

	sel := labels.Set{}
	for _, l := range app.Labels {
		kv := strings.SplitN(strings.TrimSpace(l), "=", 2)
		if len(kv) == 2 {
			sel[kv[0]] = kv[1]
		}
	}
	list, err := rc.List(ctx, metav1.ListOptions{LabelSelector: sel.String()})
	if err != nil {
		return nil, fmt.Errorf("failed listing %s in ns=%s label=%s: %w", gvr.Resource, app.Namespace, sel.String(), err)
	}
	if len(list.Items) == 0 {
		return nil, fmt.Errorf("no %s found in ns=%s matching label %s", gvr.Resource, app.Namespace, sel.String())
	}
	return list.Items, nil
}

// JSONPatch applies an RFC 6902 JSON-patch document to the named namespaced object.
func JSONPatch(ctx context.Context, cs clients.ClientSets, gvr schema.GroupVersionResource, namespace, name string, patch []byte) error {
	_, err := cs.DynamicClient.Resource(gvr).Namespace(namespace).Patch(ctx, name, ktypes.JSONPatchType, patch, metav1.PatchOptions{})
	return err
}

// Sleep blocks for the given number of seconds, honoring ctx cancellation (SIGTERM via
// the abort-watcher) so a killed experiment doesn't hang the hold period.
func Sleep(ctx context.Context, seconds int) {
	if seconds <= 0 {
		return
	}
	select {
	case <-time.After(time.Duration(seconds) * time.Second):
	case <-ctx.Done():
	}
}

// GVR shorthands for the resource kinds the itbench fault catalog touches.
var (
	GVRDeployments           = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	GVRStatefulSets          = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}
	GVRServices              = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
	GVRPods                  = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	GVRConfigMaps            = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	GVRSecrets               = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}
	GVRNodes                 = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}
	GVRResourceQuotas        = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "resourcequotas"}
	GVRPersistentVolumeClaim = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumeclaims"}
	GVRHPA                   = schema.GroupVersionResource{Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"}
	GVRNetworkPolicies       = schema.GroupVersionResource{Group: "networking.k8s.io", Version: "v1", Resource: "networkpolicies"}
	GVRPriorityClasses       = schema.GroupVersionResource{Group: "scheduling.k8s.io", Version: "v1", Resource: "priorityclasses"}
)

// WorkloadGVRForKind maps an AppDetail.Kind string ("deployment"/"statefulset") to its GVR.
func WorkloadGVRForKind(kind string) (schema.GroupVersionResource, error) {
	switch strings.ToLower(kind) {
	case "deployment", "deployments":
		return GVRDeployments, nil
	case "statefulset", "statefulsets":
		return GVRStatefulSets, nil
	default:
		return schema.GroupVersionResource{}, fmt.Errorf("unsupported workload kind %q (expected deployment or statefulset)", kind)
	}
}
