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
	"strings"
	"time"

	"github.com/litmuschaos/chaos-operator/api/litmuschaos/v1alpha1"
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

	go litmusCommon.AbortWatcher(chaosDetails.ExperimentName, cs, &resultDetails, &chaosDetails, &eventsDetails)

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

	chaosDetails.Phase = types.ChaosInjectPhase
	if err := inject(ctx, cs, &chaosDetails); err != nil {
		log.Errorf("Chaos injection failed, err: %v", err)
		result.RecordAfterFailure(&chaosDetails, &resultDetails, err, cs, &eventsDetails)
		return
	}

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
