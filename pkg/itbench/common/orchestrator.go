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
	"encoding/json"
	"fmt"
	"os"
	"strings"
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktypes "k8s.io/apimachinery/pkg/types"
)

// InjectFunc performs one fault's actual mutation (resolve target, capture original
// state, patch, hold for chaosDetails.ChaosDuration, revert) and returns nil on success.
type InjectFunc func(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error

// HoldChaos verifies the initial effect, waits for the configured window, and
// returns to the injector's rollback. Agent remediation during the window is allowed.
func HoldChaos(ctx context.Context, details *types.ChaosDetails) {
	if v := verification(ctx); v != nil {
		v.hold(ctx)
		return
	}
	Sleep(ctx, details.ChaosDuration)
}

// RevertTimeout bounds each fault's post-hold revert.
var RevertTimeout = 90 * time.Second

// abortRevertGrace is how long the abort watcher lets an in-flight revert finish before
// force-exiting. It must exceed the revert itself plus the recovery verification that
// follows it, so a run that is merely waiting for a rollout is not killed mid-flight.
func abortRevertGrace(details *types.ChaosDetails) time.Duration {
	return RevertTimeout + RecoveryBudget(details) + 30*time.Second
}

// RevertContext returns the context a fault's post-hold revert must use instead of the
// experiment's own ctx. On SIGTERM the experiment ctx is cancelled, which would make every
// revert API call fail with "context canceled" and leave the target permanently mutated --
// a workflow stop would break the cluster rather than just ending the run.
func RevertContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), RevertTimeout)
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
	grace := abortRevertGrace(chaosDetails)
	select {
	case <-done:
	case <-time.After(grace):
		log.Errorf("[Abort]: revert did not finish within %s -- exiting anyway; the target may still be mutated", grace)
		result.RecordAfterFailure(chaosDetails, resultDetails, cerrors.Error{ErrorCode: cerrors.ErrorTypeChaosRevert, Reason: "abort cleanup exceeded its deadline; recovery was not verified"}, cs, eventsDetails)
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

	var lifecycle *faultVerification
	if !strings.HasPrefix(chaosDetails.ExperimentName, "uninstall-") {
		var err error
		lifecycle, err = prepareVerification(ctx, cs, &chaosDetails)
		if err != nil {
			log.Errorf("Chaos injection was not attempted, err: %v", err)
			result.RecordAfterFailure(&chaosDetails, &resultDetails, friendlyError(err, cerrors.ErrorTypeTargetSelection), cs, &eventsDetails)
			return
		}
		ctx = context.WithValue(ctx, verificationKey{}, lifecycle)
		lifecycle.onChaos = func(hookCtx context.Context) error {
			if len(resultDetails.ProbeDetails) == 0 {
				return nil
			}
			return probe.RunProbes(hookCtx, &chaosDetails, cs, &resultDetails, "DuringChaos", &eventsDetails)
		}
		lifecycle.stopProbes = func() error {
			if len(resultDetails.ProbeDetails) == 0 {
				return nil
			}
			chaosDetails.ProbeContext.CancelFunc()
			waitCtx, cancel := context.WithTimeout(context.Background(), RevertTimeout)
			defer cancel()
			return probe.WaitForBackgroundProbes(waitCtx, &resultDetails)
		}
	}
	chaosDetails.Phase = types.ChaosInjectPhase
	injectErr := inject(ctx, cs, &chaosDetails)
	if lifecycle != nil {
		injectErr = lifecycle.finish(ctx, injectErr)
	}
	if ctx.Err() != nil {
		// Aborted. inject() has already reverted (its revert runs on RevertContext, which
		// survives the cancellation), so the target is clean; record Stopped and stop.
		if injectErr != nil {
			log.Errorf("Chaos injection returned during abort, err: %v", injectErr)
		}
		if lifecycle != nil && lifecycle.recoveryErr != nil {
			result.RecordAfterFailure(&chaosDetails, &resultDetails, friendlyError(injectErr, cerrors.ErrorTypeChaosRevert), cs, &eventsDetails)
			return
		}
		log.Info("[Abort]: recovery verified; recording Stopped verdict")
		recordAbort(cs, &resultDetails, &chaosDetails, &eventsDetails)
		return
	}
	if injectErr != nil {
		log.Errorf("Chaos injection failed, err: %v", injectErr)
		result.RecordAfterFailure(&chaosDetails, &resultDetails, friendlyError(injectErr, cerrors.ErrorTypeChaosInject), cs, &eventsDetails)
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
func targetSelectionError(gvr schema.GroupVersionResource, app types.AppDetails, reason string) error {
	return cerrors.Error{ErrorCode: cerrors.ErrorTypeTargetSelection,
		Target: fmt.Sprintf("%s in namespace %s", gvr.Resource, app.Namespace), Reason: reason}
}

func ResolveTarget(ctx context.Context, cs clients.ClientSets, gvr schema.GroupVersionResource, details *types.ChaosDetails) (*unstructured.Unstructured, error) {
	objects, err := ResolveTargets(ctx, cs, gvr, details)
	if err != nil {
		return nil, err
	}
	if len(objects) != 1 {
		return nil, targetSelectionError(gvr, details.AppDetail[0], fmt.Sprintf("expected exactly one target, found %d; choose a unique name or label selector", len(objects)))
	}
	return &objects[0], nil
}

// ResolveTargets never broadens an empty selector to every object. A workload
// name is resolved to its own pod selector when a pod-based fault needs its pods.
func ResolveTargets(ctx context.Context, cs clients.ClientSets, gvr schema.GroupVersionResource, details *types.ChaosDetails) ([]unstructured.Unstructured, error) {
	if len(details.AppDetail) != 1 {
		return nil, targetSelectionError(gvr, types.AppDetails{}, "provide exactly one TARGETS entry with a namespace and names or labels")
	}
	app := details.AppDetail[0]
	if strings.TrimSpace(app.Namespace) == "" {
		return nil, targetSelectionError(gvr, app, "target namespace is required")
	}
	rc := cs.DynamicClient.Resource(gvr).Namespace(app.Namespace)
	if len(app.Names) > 0 {
		if len(app.Labels) > 0 {
			return nil, targetSelectionError(gvr, app, "choose target names or labels, not both")
		}
		var objects []unstructured.Unstructured
		seen := map[string]bool{}
		for _, raw := range app.Names {
			name := strings.TrimSpace(raw)
			if name == "" {
				return nil, targetSelectionError(gvr, app, "target names must not be empty")
			}
			if seen[name] {
				continue
			}
			seen[name] = true
			if gvr == GVRPods && app.Kind != "pod" && app.Kind != "pods" {
				workloadGVR, err := WorkloadGVRForKind(app.Kind)
				if err != nil {
					return nil, err
				}
				workload, err := cs.DynamicClient.Resource(workloadGVR).Namespace(app.Namespace).Get(ctx, name, metav1.GetOptions{})
				if err != nil {
					return nil, targetSelectionError(gvr, app, err.Error())
				}
				selector, found, err := unstructured.NestedMap(workload.Object, "spec", "selector")
				if err != nil || !found {
					return nil, targetSelectionError(gvr, app, "workload has no pod selector")
				}
				rawSelector := &metav1.LabelSelector{}
				encoded, err := json.Marshal(selector)
				if err == nil {
					err = json.Unmarshal(encoded, rawSelector)
				}
				if err != nil {
					return nil, targetSelectionError(gvr, app, "workload pod selector is invalid")
				}
				parsed, err := metav1.LabelSelectorAsSelector(rawSelector)
				if err != nil || parsed.Empty() {
					return nil, targetSelectionError(gvr, app, "workload pod selector is empty or invalid")
				}
				pods, err := rc.List(ctx, metav1.ListOptions{LabelSelector: parsed.String()})
				if err != nil {
					return nil, targetSelectionError(gvr, app, err.Error())
				}
				objects = append(objects, pods.Items...)
				continue
			}
			object, err := rc.Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return nil, targetSelectionError(gvr, app, err.Error())
			}
			objects = append(objects, *object)
		}
		if len(objects) == 0 {
			return nil, targetSelectionError(gvr, app, "no objects matched the requested target names")
		}
		return objects, nil
	}
	selector, err := labels.Parse(strings.Join(app.Labels, ","))
	if err != nil || selector.Empty() {
		return nil, targetSelectionError(gvr, app, "a valid, nonempty target label selector is required")
	}
	objects, err := rc.List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, targetSelectionError(gvr, app, err.Error())
	}
	if len(objects.Items) == 0 {
		return nil, targetSelectionError(gvr, app, fmt.Sprintf("no %s match %s; install the required resource or choose an applicable target", gvr.Resource, selector.String()))
	}
	return objects.Items, nil
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
