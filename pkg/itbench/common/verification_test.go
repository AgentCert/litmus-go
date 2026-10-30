package common

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/litmuschaos/litmus-go/pkg/types"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

// chaosDetails builds the ChaosDetails the orchestrator would hand to the lifecycle for
// one fault, with a target expressed the way TARGETS delivers it.
func chaosDetails(fault, kind, ns, name string) *types.ChaosDetails {
	app := types.AppDetails{Namespace: ns, Kind: kind}
	if name != "" {
		app.Names = []string{name}
	}
	return &types.ChaosDetails{
		ExperimentName: fault,
		ChaosDuration:  1,
		Timeout:        2,
		AppDetail:      []types.AppDetails{app},
	}
}

// TestPrepareVerification_RejectsUnusableConfiguration keeps a run from reaching the
// cluster at all when the inputs could not describe a real fault. Each of these would
// otherwise mutate something and then report a verdict nobody can trust.
func TestPrepareVerification_RejectsUnusableConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name    string
		details *types.ChaosDetails
		want    string
	}{
		{
			name: "zero duration",
			details: func() *types.ChaosDetails {
				d := chaosDetails("scaled-to-zero-kubernetes-workload", "deployment", "ns1", "app")
				d.ChaosDuration = 0
				return d
			}(),
			want: "TOTAL_CHAOS_DURATION",
		},
		{
			name: "no target",
			details: func() *types.ChaosDetails {
				d := chaosDetails("scaled-to-zero-kubernetes-workload", "deployment", "ns1", "app")
				d.AppDetail = nil
				return d
			}(),
			want: "exactly one application target",
		},
		{
			name:    "unknown fault has no verifier",
			details: chaosDetails("some-fault-nobody-implemented", "deployment", "ns1", "app"),
			want:    "no behavior verifier",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs := fakeCS(readyDeployment("app", "ns1"))
			_, err := prepareVerification(context.Background(), cs, tc.details)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error mentioning %q", err, tc.want)
			}
		})
	}
}

// TestObservedTarget_FeatureFlagResolvesItsRealWorkload covers the fault whose TARGETS
// entry names "configmap" only to carry a namespace. Resolving that kind as a workload
// fails, so the fault could never be injected at all.
func TestObservedTarget_FeatureFlagResolvesItsRealWorkload(t *testing.T) {
	details := chaosDetails("opentelemetry-demo-feature-flag", "configmap", "otel-demo", "")
	observed := observedTarget(details)

	if _, err := targetGVR(observed); err != nil {
		t.Fatalf("feature flag target is unresolvable: %v", err)
	}
	app := observed.AppDetail[0]
	if app.Kind != "deployment" || len(app.Names) != 1 || app.Names[0] != FlagdDeployment {
		t.Fatalf("observed target = %+v, want the %s deployment", app, FlagdDeployment)
	}
	if app.Namespace != "otel-demo" {
		t.Fatalf("namespace = %q, want it carried over from TARGETS", app.Namespace)
	}
	if observedTarget(chaosDetails("scaled-to-zero-kubernetes-workload", "deployment", "ns1", "app")).AppDetail[0].Kind != "deployment" {
		t.Fatal("ordinary workload faults must keep their declared target")
	}
}

// TestPrepareVerification_NamespaceScopedFaultNeedsNoWorkload covers the API-surge fault,
// which owns no workload: demanding one would fail a run before it injected anything.
func TestPrepareVerification_NamespaceScopedFaultNeedsNoWorkload(t *testing.T) {
	details := chaosDetails("kubernetes-api-server-request-surge", "", "sock-shop", "")
	v, err := prepareVerification(context.Background(), fakeCS(), details)
	if err != nil {
		t.Fatalf("namespace-scoped fault was rejected: %v", err)
	}
	if v.baseline != nil {
		t.Fatal("a namespace-scoped fault must not bind to a workload baseline")
	}
	if v.ns() != "sock-shop" {
		t.Fatalf("ns() = %q, want sock-shop", v.ns())
	}

	details.AppDetail[0].Namespace = "  "
	if _, err := prepareVerification(context.Background(), fakeCS(), details); err == nil {
		t.Fatal("a namespace-scoped fault without a namespace has no target at all")
	}
}

// TestWithoutServiceAccountMount_IgnoresAdmissionDefaults is the regression for faults
// whose mutation lands on a list the API server also appends to. Comparing a pod against
// the template verbatim never matches, so the effect could never be observed and the
// fault was reported Failed even when it worked.
func TestWithoutServiceAccountMount_IgnoresAdmissionDefaults(t *testing.T) {
	template := []interface{}{
		map[string]interface{}{"name": "worker", "image": "busybox", "volumeMounts": []interface{}{
			map[string]interface{}{"name": "tmp-volume", "mountPath": "/tmp"},
		}},
	}
	pod := []interface{}{
		map[string]interface{}{"name": "worker", "image": "busybox", "volumeMounts": []interface{}{
			map[string]interface{}{"name": "tmp-volume", "mountPath": "/tmp"},
			map[string]interface{}{"name": "kube-api-access-796q8", "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount", "readOnly": true},
		}},
	}
	if equivalent(template, pod) {
		t.Fatal("test setup: the raw values must differ, otherwise this proves nothing")
	}
	if !equivalent(withoutServiceAccountMount(template), withoutServiceAccountMount(pod)) {
		t.Fatal("the injected init container is present in the pod but was not recognised")
	}

	volumes := []interface{}{map[string]interface{}{"name": "kube-api-access-796q8"}, map[string]interface{}{"name": "fault-volume"}}
	stripped, _ := withoutServiceAccountMount(volumes).([]interface{})
	if len(stripped) != 1 {
		t.Fatalf("stripped volumes = %v, want only the fault's own volume", stripped)
	}

	// A genuinely different value must still be reported as different.
	other := []interface{}{map[string]interface{}{"name": "worker", "image": "alpine"}}
	if equivalent(withoutServiceAccountMount(template), withoutServiceAccountMount(other)) {
		t.Fatal("different container specs must not compare equal")
	}
}

// TestPodHasMutation_AcceptsAdmittedPod drives the same regression through the check the
// effect loop actually calls.
func TestPodHasMutation_AcceptsAdmittedPod(t *testing.T) {
	baseline := readyDeployment("catalogue", "sock-shop")
	mutated := baseline.DeepCopy()
	injected := []interface{}{map[string]interface{}{
		"name": "worker", "image": "busybox", "command": []interface{}{"sleep", "infinity"},
	}}
	if err := unstructured.SetNestedSlice(mutated.Object, injected, "spec", "template", "spec", "initContainers"); err != nil {
		t.Fatal(err)
	}

	v := &faultVerification{
		baseline: baseline,
		details:  chaosDetails("hanging-kubernetes-workload-init-container", "deployment", "sock-shop", "catalogue"),
		paths:    [][]string{{"spec", "template", "spec", "initContainers"}},
	}

	admitted := &unstructured.Unstructured{Object: map[string]interface{}{"spec": map[string]interface{}{
		"initContainers": []interface{}{map[string]interface{}{
			"name": "worker", "image": "busybox", "command": []interface{}{"sleep", "infinity"},
			"volumeMounts": []interface{}{map[string]interface{}{
				"name": "kube-api-access-xyz", "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount",
			}},
		}},
	}}}
	if !v.podHasMutation(mutated, admitted) {
		t.Fatal("a pod created from the mutated template must be recognised as carrying the fault")
	}

	stale := &unstructured.Unstructured{Object: map[string]interface{}{"spec": map[string]interface{}{}}}
	if v.podHasMutation(mutated, stale) {
		t.Fatal("a pod predating the mutation must not be treated as faulted")
	}
}

// TestSnapshotLookup_ReportsMissingBaselineInsteadOfPanicking covers the prerequisite the
// cordon and feature-flag checks depend on. Addressing captures positionally crashed the
// experiment once an injector tracked a temporary resource of its own.
func TestSnapshotLookup_ReportsMissingBaselineInsteadOfPanicking(t *testing.T) {
	v := &faultVerification{}
	if _, err := v.snapshot(GVRNodes, "node-1"); err == nil {
		t.Fatal("a missing capture must be reported, not indexed into")
	}

	v.checks = append(v.checks, resourceSnapshot{gvr: GVRConfigMaps, namespace: "ns1", name: FlagdConfigMap, object: nil})
	_, err := v.snapshot(GVRConfigMaps, FlagdConfigMap)
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("got %v, want an actionable missing-prerequisite error", err)
	}

	v.checks = append(v.checks, resourceSnapshot{gvr: GVRNodes, name: "node-1", object: readyDeployment("x", "")})
	if _, err := v.snapshot(GVRNodes, "node-1"); err != nil {
		t.Fatalf("captured baseline was not found: %v", err)
	}
}

// TestRestoredPaths_ServiceSemantics guards the deleted-Service recovery check: a
// recreated Service gets a new ClusterIP, which is not a restoration failure, but a
// headless Service turning into a routed one is.
func TestRestoredPaths_ServiceSemantics(t *testing.T) {
	service := func(clusterIP string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "v1", "kind": "Service",
			"metadata": map[string]interface{}{"name": "carts", "namespace": "sock-shop"},
			"spec": map[string]interface{}{
				"clusterIP": clusterIP,
				"selector":  map[string]interface{}{"name": "carts"},
				"ports":     []interface{}{map[string]interface{}{"port": int64(80)}},
			},
		}}
	}
	paths := [][]string{{"spec"}}

	if err := restoredPaths(service("10.96.1.1"), service("10.96.9.9"), paths, true); err != nil {
		t.Fatalf("a recreated Service with a fresh ClusterIP is restored: %v", err)
	}
	if err := restoredPaths(service("None"), service("10.96.9.9"), paths, true); err == nil {
		t.Fatal("a headless Service recreated as a routed one is NOT restored")
	}
	if err := restoredPaths(service("None"), service("None"), paths, true); err != nil {
		t.Fatalf("a headless Service recreated headless is restored: %v", err)
	}
}

// TestRestoredPaths_DetectsReplacedTarget stops a run from calling a different object of
// the same name a successful recovery.
func TestRestoredPaths_DetectsReplacedTarget(t *testing.T) {
	original := readyDeployment("carts", "sock-shop")
	original.SetUID("uid-before")
	replaced := original.DeepCopy()
	replaced.SetUID("uid-after")

	if err := restoredPaths(original, replaced, [][]string{{"spec", "replicas"}}, false); err == nil {
		t.Fatal("a replaced target must not count as the original restored")
	}
}

// TestFinish_RequiresTheInjectorToHaveHeld keeps an injector that returned early -- and
// so never exposed its fault to anything -- from being reported as a successful run.
func TestFinish_RequiresTheInjectorToHaveHeld(t *testing.T) {
	cs := fakeCS(readyDeployment("carts", "sock-shop"))
	v := &faultVerification{
		cs:      cs,
		details: chaosDetails("scaled-to-zero-kubernetes-workload", "deployment", "sock-shop", "carts"),
		gvr:     GVRDeployments,
	}
	v.details.Timeout = 1
	v.baseline = readyDeployment("carts", "sock-shop")

	err := v.finish(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "without observing and holding") {
		t.Fatalf("got %v, want the run to fail because the fault was never held", err)
	}
}

// readyDeployment is a converged, healthy Deployment: the baseline every fault requires
// before it is allowed to inject.
func readyDeployment(name, ns string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]interface{}{
			"name": name, "namespace": ns, "generation": int64(1),
			"labels": map[string]interface{}{"name": name},
		},
		"spec": map[string]interface{}{
			"replicas": int64(1),
			"selector": map[string]interface{}{"matchLabels": map[string]interface{}{"name": name}},
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{"labels": map[string]interface{}{"name": name}},
				"spec": map[string]interface{}{
					"containers": []interface{}{map[string]interface{}{"name": name, "image": "app:1"}},
				},
			},
		},
		"status": map[string]interface{}{
			"replicas": int64(1), "readyReplicas": int64(1),
			"updatedReplicas": int64(1), "observedGeneration": int64(1),
		},
	}}
}

// TestReady_RejectsUnconvergedRollout is what separates "the revert API call succeeded"
// from "the application is actually working again".
func TestReady_RejectsUnconvergedRollout(t *testing.T) {
	converged := readyDeployment("carts", "sock-shop")
	midRollout := readyDeployment("carts", "sock-shop")
	if err := unstructured.SetNestedField(midRollout.Object, int64(0), "status", "readyReplicas"); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		object  *unstructured.Unstructured
		wantErr bool
	}{
		{"converged", converged, false},
		{"no ready replicas", midRollout, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &faultVerification{
				cs:       fakeCS(tc.object),
				gvr:      GVRDeployments,
				baseline: tc.object,
				details:  chaosDetails("scaled-to-zero-kubernetes-workload", "deployment", "sock-shop", "carts"),
			}
			err := v.ready(context.Background())
			if tc.wantErr != (err != nil) {
				t.Fatalf("ready() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// TestWaitForCheck_SurfacesTheLastObservation keeps a timeout from hiding the concrete
// reason the fault or the recovery was never seen.
func TestWaitForCheck_SurfacesTheLastObservation(t *testing.T) {
	err := WaitForCheck(context.Background(), 50*time.Millisecond, "fault effect was not observed", func(context.Context) error {
		return errContainerStillRunning
	})
	if err == nil {
		t.Fatal("a check that never passes must fail")
	}
	for _, want := range []string{"fault effect was not observed", errContainerStillRunning.Error()} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %q", err, want)
		}
	}

	calls := 0
	if err := WaitForCheck(context.Background(), time.Second, "should pass", func(context.Context) error {
		calls++
		if calls < 2 {
			return errContainerStillRunning
		}
		return nil
	}); err != nil {
		t.Fatalf("a check that passes on retry must succeed: %v", err)
	}
}

// TestCapture_RecordsAbsenceWithoutFailing lets a fault declare a prerequisite that does
// not exist yet, so the missing-prerequisite error is raised where it can be explained.
func TestCapture_RecordsAbsenceWithoutFailing(t *testing.T) {
	v := &faultVerification{cs: fakeCS()}
	if err := v.capture(context.Background(), GVRConfigMaps, "ns1", FlagdConfigMap, nil); err != nil {
		t.Fatalf("capturing an absent resource must not be an error: %v", err)
	}
	if len(v.checks) != 1 || v.checks[0].object != nil {
		t.Fatalf("absence was not recorded: %+v", v.checks)
	}
	if err := v.capture(context.Background(), GVRConfigMaps, "ns1", "", nil); err == nil {
		t.Fatal("an unnamed prerequisite cannot be captured")
	}
}

var errContainerStillRunning = errors.New("target still has its original configuration")

// TestRecoveryBudgetExceedsRevert keeps post-revert convergence from being cut short.
// Restoring configuration is one API call; recovering behavior is a controller rollout,
// so recovery must not be bounded by the revert budget.
func TestRecoveryBudgetExceedsRevert(t *testing.T) {
	if got := RecoveryBudget(nil); got <= RevertTimeout {
		t.Fatalf("default recovery budget %v must exceed RevertTimeout %v", got, RevertTimeout)
	}
	if got := RecoveryBudget(&types.ChaosDetails{Timeout: 600}); got != 600*time.Second {
		t.Fatalf("recovery budget = %v, want STATUS_CHECK_TIMEOUT to be honoured", got)
	}
}

// TestReadyEndpoints_FallsBackWhenEndpointSlicesAreUnreadable keeps a cluster whose RBAC
// predates EndpointSlices from making every Service-targeting fault unrunnable.
func TestReadyEndpoints_FallsBackWhenEndpointSlicesAreUnreadable(t *testing.T) {
	service := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1", "kind": "Service",
		"metadata": map[string]interface{}{"name": "carts", "namespace": "sock-shop"},
	}}
	endpoints := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1", "kind": "Endpoints",
		"metadata": map[string]interface{}{"name": "carts", "namespace": "sock-shop"},
		"subsets": []interface{}{map[string]interface{}{
			"addresses": []interface{}{map[string]interface{}{"ip": "10.244.0.63"}},
		}},
	}}
	cs := fakeCS(endpoints)
	cs.DynamicClient.(*dynamicfake.FakeDynamicClient).PrependReactor("list", "endpointslices",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "discovery.k8s.io", Resource: "endpointslices"}, "", errors.New("no access"))
		})

	count, err := readyEndpoints(context.Background(), cs, service)
	if err != nil {
		t.Fatalf("Service readiness must fall back to the Endpoints API: %v", err)
	}
	if count != 1 {
		t.Fatalf("ready endpoint addresses = %d, want 1", count)
	}
}

// TestNamespaceScopedFaultsAreExplicit documents that only a fault owning no workload
// may skip target resolution. Widening this silently would stop checking real targets.
func TestNamespaceScopedFaultsAreExplicit(t *testing.T) {
	if !namespaceScoped("kubernetes-api-server-request-surge") {
		t.Fatal("the API surge fault owns no workload and must be namespace-scoped")
	}
	for _, name := range []string{
		"scaled-to-zero-kubernetes-workload", "deleted-kubernetes-service",
		"cordoned-kubernetes-worker-node", "priority-kubernetes-workload-priority-preemption",
		"ingress-port-blocking-network-policy", "insufficient-kubernetes-resource-quota",
	} {
		if namespaceScoped(name) {
			t.Fatalf("%s resolves a real target and must not skip verification", name)
		}
	}
}

// TestQuotaRejected_RecognisesBothAdmissionMessages is the regression for a working
// quota fault reported as failed: the admission plugin says "failed quota" when the
// target declares none of the resources the quota tracks, and "exceeded quota" only
// when it declares resources that do not fit. Most workloads declare none.
func TestQuotaRejected_RecognisesBothAdmissionMessages(t *testing.T) {
	t.Setenv("QUOTA_NAME", "memory")

	event := func(name, message string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "v1", "kind": "Event",
			"metadata":       map[string]interface{}{"name": name, "namespace": "sock-shop"},
			"message":        message,
			"involvedObject": map[string]interface{}{"kind": "ReplicaSet", "name": name},
		}}
	}

	for _, tc := range []struct {
		name    string
		event   *unstructured.Unstructured
		wantErr bool
	}{
		{
			name:  "target declares no resources the quota tracks",
			event: event("payment-58c88f455c", `Error creating: pods "payment-58c88f455c-gc7w5" is forbidden: failed quota: memory: must specify limits.cpu for: payment`),
		},
		{
			name:  "target declares resources that do not fit",
			event: event("payment-58c88f455c", `Error creating: pods "payment-x" is forbidden: exceeded quota: memory, requested: limits.memory=64Mi, used: limits.memory=0, limited: limits.memory=32Mi`),
		},
		{
			name:    "rejection belongs to a different workload",
			event:   event("catalogue-7d794b584f", `Error creating: pods "catalogue-x" is forbidden: failed quota: memory: must specify limits.cpu`),
			wantErr: true,
		},
		{
			name:    "unrelated failure is not a quota rejection",
			event:   event("payment-58c88f455c", `Error creating: pods "payment-x" is forbidden: unrelated admission failure`),
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs := fakeCS(tc.event)
			v := &faultVerification{
				cs:       cs,
				baseline: readyDeployment("payment", "sock-shop"),
				details:  chaosDetails("insufficient-kubernetes-resource-quota", "deployment", "sock-shop", "payment"),
			}
			err := v.quotaRejected(context.Background())
			if tc.wantErr != (err != nil) {
				t.Fatalf("quotaRejected() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
