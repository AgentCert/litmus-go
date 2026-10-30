package common

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/litmuschaos/litmus-go/pkg/cerrors"
	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/log"
	"github.com/litmuschaos/litmus-go/pkg/types"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type verificationKey struct{}

// A lifecycle belongs to one experiment invocation. Observing an agent is not
// part of this contract: baseline, actual injected effect, then restored state.
type faultVerification struct {
	cs               clients.ClientSets
	details          *types.ChaosDetails
	gvr              schema.GroupVersionResource
	baseline         *unstructured.Unstructured
	namespace        string
	baselineReplicas int64
	paths            [][]string
	pods             []unstructured.Unstructured
	address          string
	checks           []resourceSnapshot
	held             bool
	err              error
	recoveryErr      error
	onChaos          func(context.Context) error
	stopProbes       func() error
	effectOverride   func(context.Context) error
	recoveryOverride func(context.Context) error
}

type resourceSnapshot struct {
	gvr             schema.GroupVersionResource
	namespace, name string
	object          *unstructured.Unstructured
	paths           [][]string
}

func verification(ctx context.Context) *faultVerification {
	v, _ := ctx.Value(verificationKey{}).(*faultVerification)
	return v
}

// WaitForCheck bounds controller convergence and retains the last concrete
// failure. API calls receive the same deadline as the poll loop.
func WaitForCheck(ctx context.Context, timeout time.Duration, description string, check func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var last error
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%s: %w (last observation: %v)", description, err, last)
		}
		if last = check(ctx); last == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w (last observation: %v)", description, ctx.Err(), last)
		case <-time.After(time.Second):
		}
	}
}

func (v *faultVerification) timeout() time.Duration {
	if v.details.Timeout > 0 {
		return time.Duration(v.details.Timeout) * time.Second
	}
	return 180 * time.Second
}

func prepareVerification(ctx context.Context, cs clients.ClientSets, details *types.ChaosDetails) (*faultVerification, error) {
	if details.ChaosDuration <= 0 {
		return nil, fmt.Errorf("TOTAL_CHAOS_DURATION must be a positive integer")
	}
	if len(details.AppDetail) != 1 {
		return nil, fmt.Errorf("provide exactly one application target")
	}
	details = observedTarget(details)
	if namespaceScoped(details.ExperimentName) {
		// This fault owns no workload: it creates its own load generator and is
		// measured entirely through that helper. Resolving (and health-checking) an
		// unrelated workload would fail a run the fault never depended on.
		namespace := strings.TrimSpace(details.AppDetail[0].Namespace)
		if namespace == "" {
			return nil, fmt.Errorf("target namespace is required")
		}
		return &faultVerification{cs: cs, details: details, namespace: namespace}, nil
	}
	gvr, err := targetGVR(details)
	if err != nil {
		return nil, err
	}
	target, err := ResolveTarget(ctx, cs, gvr, details)
	if err != nil {
		return nil, err
	}
	v := &faultVerification{cs: cs, details: details, gvr: gvr, baseline: target.DeepCopy()}
	v.paths, err = mutationPaths(details.ExperimentName, target)
	if err != nil {
		return nil, err
	}
	if err := WaitForCheck(ctx, v.timeout(), "target baseline is not healthy", v.ready); err != nil {
		return nil, err
	}
	if gvr == GVRDeployments || gvr == GVRStatefulSets || gvr == GVRPods {
		v.pods, err = ResolveTargets(ctx, cs, GVRPods, details)
		if err != nil {
			return nil, err
		}
	}
	switch details.ExperimentName {
	case "misconfigured-kubernetes-horizontal-pod-autoscaler":
		v.baselineReplicas, _, _ = unstructured.NestedInt64(target.Object, "status", "desiredReplicas")
	case "deleted-kubernetes-service", "invalid-kubernetes-service-selector", "modified-target-port-kubernetes-service",
		"ingress-port-blocking-network-policy", "chaos-mesh-http-abort-replacement", "valkey-workload-changed-password", "chaos-mesh-pod-failure-replacement", "chaos-mesh-http-body-tamper-replacement":
		v.address, err = v.serviceAddress(ctx)
		if err != nil {
			return nil, err
		}
		if err := tcpReachable(ctx, v.address); err != nil {
			return nil, fmt.Errorf("target connection must work before injection: %w", err)
		}
	case "opentelemetry-demo-feature-flag":
		err = v.capture(ctx, GVRConfigMaps, target.GetNamespace(), FlagdConfigMap, [][]string{{"data", FlagdDataKey}})
	case "insufficient-kubernetes-resource-quota":
		err = v.capture(ctx, GVRResourceQuotas, target.GetNamespace(), os.Getenv("QUOTA_NAME"), [][]string{{"spec", "hard"}})
	case "cordoned-kubernetes-worker-node":
		if len(v.pods) == 0 {
			return nil, fmt.Errorf("no scheduled target pod")
		}
		node, _, _ := unstructured.NestedString(v.pods[0].Object, "spec", "nodeName")
		if node == "" {
			return nil, fmt.Errorf("target pod is not scheduled")
		}
		if err = v.capture(ctx, GVRNodes, "", node, [][]string{{"spec", "unschedulable"}}); err == nil {
			snapshot, lookupErr := v.snapshot(GVRNodes, node)
			if lookupErr != nil {
				return nil, lookupErr
			}
			if cordoned, _, _ := unstructured.NestedBool(snapshot.object.Object, "spec", "unschedulable"); cordoned {
				return nil, fmt.Errorf("target node is already cordoned; no new fault can be injected")
			}
		}
	}
	if err != nil {
		return nil, err
	}
	return v, nil
}

// FlagdDeployment, FlagdConfigMap and FlagdDataKey name the objects the
// opentelemetry-demo-feature-flag fault acts on. The ChaosEngine CRD restricts
// appinfo.appkind to workload kinds, so that fault's TARGETS entry carries kind
// "configmap" purely to pass its namespace through; the objects themselves are fixed.
const (
	FlagdDeployment = "flagd"
	FlagdConfigMap  = "flagd-config"
	FlagdDataKey    = "demo.flagd.json"
)

// namespaceScoped reports whether a fault acts on a namespace rather than on a
// resolved workload, so the lifecycle must not demand a target object for it.
func namespaceScoped(name string) bool {
	return name == "kubernetes-api-server-request-surge"
}

// ns is the namespace the lifecycle observes, which for a namespace-scoped fault is
// the only target it has.
func (v *faultVerification) ns() string {
	if v.baseline != nil {
		return v.baseline.GetNamespace()
	}
	return v.namespace
}

// observedTarget returns the target description the lifecycle must observe. A fault
// whose TARGETS entry names a kind that is not the object it acts on (the feature flag
// declares "configmap" but mutates a ConfigMap and restarts a Deployment) would
// otherwise fail target resolution before injecting anything.
func observedTarget(details *types.ChaosDetails) *types.ChaosDetails {
	if details.ExperimentName != "opentelemetry-demo-feature-flag" {
		return details
	}
	observed := *details
	observed.AppDetail = []types.AppDetails{{
		Namespace: details.AppDetail[0].Namespace,
		Kind:      "deployment",
		Names:     []string{FlagdDeployment},
	}}
	return &observed
}

// snapshot returns the captured baseline for one resource by identity rather than by
// position: injectors append their own temporary resources to the same slice, so an
// index captured during preparation does not stay valid.
func (v *faultVerification) snapshot(gvr schema.GroupVersionResource, name string) (*resourceSnapshot, error) {
	for i := range v.checks {
		if v.checks[i].gvr == gvr && v.checks[i].name == name {
			if v.checks[i].object == nil {
				return nil, fmt.Errorf("%s %q does not exist; it must be present before this fault can be injected", gvr.Resource, name)
			}
			return &v.checks[i], nil
		}
	}
	return nil, fmt.Errorf("no baseline was captured for %s %q", gvr.Resource, name)
}

func targetGVR(details *types.ChaosDetails) (schema.GroupVersionResource, error) {
	switch details.ExperimentName {
	case "deleted-kubernetes-service", "invalid-kubernetes-service-selector", "modified-target-port-kubernetes-service":
		return GVRServices, nil
	case "misconfigured-kubernetes-horizontal-pod-autoscaler":
		return GVRHPA, nil
	}
	if strings.EqualFold(details.AppDetail[0].Kind, "pod") {
		return GVRPods, nil
	}
	return WorkloadGVRForKind(details.AppDetail[0].Kind)
}

func mutationPaths(name string, target *unstructured.Unstructured) ([][]string, error) {
	var fields []string
	switch name {
	case "scaled-to-zero-kubernetes-workload":
		return [][]string{{"spec", "replicas"}}, nil
	case "nonexistent-kubernetes-workload-node":
		return [][]string{{"spec", "template", "spec", "nodeSelector"}}, nil
	case "unschedulable-k8s-workload-pod-anti-affinity-rule":
		return [][]string{{"spec", "template", "spec", "affinity"}}, nil
	case "failing-name-resolution-kubernetes-workload-dns-policy":
		return [][]string{{"spec", "template", "spec", "dnsPolicy"}, {"spec", "template", "spec", "dnsConfig"}}, nil
	case "crashing-kubernetes-workload-init-container", "hanging-kubernetes-workload-init-container":
		return [][]string{{"spec", "template", "spec", "initContainers"}}, nil
	case "nonexistent-kubernetes-workload-container-image", "unsupported-architecture-k8s-workload-container-image":
		fields = []string{"image"}
	case "invalid-kubernetes-workload-container-command":
		fields = []string{"command", "args"}
	case "modified-k8s-workload-container-environment-variable":
		fields = []string{"env"}
	case "misconfigured-k8s-workload-container-readiness-probe":
		fields = []string{"readinessProbe"}
	case "insufficient-kubernetes-workload-container-resources", "valkey-workload-out-of-memory", "unassigned-k8s-workload-container-resource-limits":
		fields = []string{"resources"}
	case "valkey-workload-changed-password":
		fields = []string{"command", "args", "env"}
	case "nonexistent-kubernetes-workload-persistent-volume-claim":
		fields = []string{"volumeMounts"}
	case "misconfigured-kubernetes-horizontal-pod-autoscaler":
		return [][]string{{"spec", "metrics"}}, nil
	case "invalid-kubernetes-service-selector":
		return [][]string{{"spec", "selector"}}, nil
	case "modified-target-port-kubernetes-service":
		return [][]string{{"spec", "ports"}}, nil
	case "deleted-kubernetes-service":
		return [][]string{{"spec"}}, nil
	case "insufficient-kubernetes-resource-quota":
		return [][]string{{"spec", "template", "metadata", "annotations"}}, nil
	case "opentelemetry-demo-feature-flag":
		return [][]string{{"spec", "template", "metadata", "annotations"}}, nil
	case "ingress-port-blocking-network-policy", "chaos-mesh-http-abort-replacement",
		"chaos-mesh-http-body-tamper-replacement", "chaos-mesh-pod-failure-replacement", "kubernetes-api-server-request-surge",
		"priority-kubernetes-workload-priority-preemption", "cordoned-kubernetes-worker-node":
		return nil, nil
	default:
		return nil, fmt.Errorf("fault %q has no behavior verifier", name)
	}
	container := TargetContainerName(target.GetName())
	index, err := ContainerIndex(target, container)
	if err != nil && os.Getenv("TARGET_CONTAINER") == "" {
		if i, _, ok := soleContainer(target); ok {
			index, err = i, nil
		}
	}
	if err != nil {
		return nil, err
	}
	var paths [][]string
	for _, field := range fields {
		paths = append(paths, []string{"spec", "template", "spec", "containers", strconv.Itoa(index), field})
	}
	if name == "nonexistent-kubernetes-workload-persistent-volume-claim" {
		paths = append(paths, []string{"spec", "template", "spec", "volumes"})
	}
	return paths, nil
}

func (v *faultVerification) capture(ctx context.Context, gvr schema.GroupVersionResource, namespace, name string, paths [][]string) error {
	if name == "" {
		return fmt.Errorf("resource name is required for %s", gvr.Resource)
	}
	object, err := v.cs.DynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	v.checks = append(v.checks, resourceSnapshot{gvr: gvr, namespace: namespace, name: name, object: object, paths: paths})
	return nil
}

func (v *faultVerification) current(ctx context.Context) (*unstructured.Unstructured, error) {
	return v.cs.DynamicClient.Resource(v.gvr).Namespace(v.baseline.GetNamespace()).Get(ctx, v.baseline.GetName(), metav1.GetOptions{})
}

func (v *faultVerification) ready(ctx context.Context) error {
	object, err := v.current(ctx)
	if err != nil {
		return err
	}
	if v.gvr == GVRServices {
		_, err := readyEndpoints(ctx, v.cs, object)
		return err
	}
	if v.gvr == GVRHPA {
		conditions, _, _ := unstructured.NestedSlice(object.Object, "status", "conditions")
		for _, condition := range conditions {
			m, _ := condition.(map[string]interface{})
			if m["type"] == "ScalingActive" && m["status"] == "True" {
				return nil
			}
		}
		return fmt.Errorf("HPA has no active resource metrics; check its target and resource requests")
	}
	if v.gvr == GVRPods {
		if podReady(object) {
			return nil
		}
		return fmt.Errorf("target pod is not Ready")
	}
	desired, found, _ := unstructured.NestedInt64(object.Object, "spec", "replicas")
	if !found {
		desired = 1
	}
	ready, _, _ := unstructured.NestedInt64(object.Object, "status", "readyReplicas")
	updated, _, _ := unstructured.NestedInt64(object.Object, "status", "updatedReplicas")
	actual, _, _ := unstructured.NestedInt64(object.Object, "status", "replicas")
	observed, _, _ := unstructured.NestedInt64(object.Object, "status", "observedGeneration")
	if desired < 1 || ready != desired || updated != desired || actual != desired || observed < object.GetGeneration() {
		return fmt.Errorf("%s/%s rollout not converged: desired=%d ready=%d updated=%d actual=%d", v.gvr.Resource, object.GetName(), desired, ready, updated, actual)
	}
	return nil
}

// nodeNameOf returns the node the first resolved target pod is scheduled on, which is
// the node the cordon fault captured a baseline for.
func nodeNameOf(pods []unstructured.Unstructured) string {
	if len(pods) == 0 {
		return ""
	}
	node, _, _ := unstructured.NestedString(pods[0].Object, "spec", "nodeName")
	return node
}

func podReady(pod *unstructured.Unstructured) bool {
	conditions, _, _ := unstructured.NestedSlice(pod.Object, "status", "conditions")
	for _, condition := range conditions {
		m, _ := condition.(map[string]interface{})
		if m["type"] == "Ready" && m["status"] == "True" {
			return true
		}
	}
	return false
}

// readyEndpoints counts the addresses a Service actually routes to. EndpointSlices are
// the current source, but a cluster whose RBAC predates them must not make every
// Service fault unrunnable, so the superseded Endpoints API is used as a fallback.
func readyEndpoints(ctx context.Context, cs clients.ClientSets, service *unstructured.Unstructured) (int, error) {
	gvr := schema.GroupVersionResource{Group: "discovery.k8s.io", Version: "v1", Resource: "endpointslices"}
	slices, err := cs.DynamicClient.Resource(gvr).Namespace(service.GetNamespace()).List(ctx, metav1.ListOptions{LabelSelector: "kubernetes.io/service-name=" + service.GetName()})
	if err != nil {
		if !apierrors.IsForbidden(err) && !apierrors.IsNotFound(err) {
			return 0, err
		}
		return legacyReadyEndpoints(ctx, cs, service)
	}
	count := 0
	for _, slice := range slices.Items {
		endpoints, _, _ := unstructured.NestedSlice(slice.Object, "endpoints")
		for _, endpoint := range endpoints {
			m, _ := endpoint.(map[string]interface{})
			ready, found, _ := unstructured.NestedBool(m, "conditions", "ready")
			addresses, _, _ := unstructured.NestedStringSlice(m, "addresses")
			if (!found || ready) && len(addresses) > 0 {
				count += len(addresses)
			}
		}
	}
	if count == 0 {
		return 0, fmt.Errorf("Service %s has no ready endpoint addresses", service.GetName())
	}
	return count, nil
}

func legacyReadyEndpoints(ctx context.Context, cs clients.ClientSets, service *unstructured.Unstructured) (int, error) {
	gvr := schema.GroupVersionResource{Version: "v1", Resource: "endpoints"}
	endpoints, err := cs.DynamicClient.Resource(gvr).Namespace(service.GetNamespace()).Get(ctx, service.GetName(), metav1.GetOptions{})
	if err != nil {
		return 0, err
	}
	count := 0
	subsets, _, _ := unstructured.NestedSlice(endpoints.Object, "subsets")
	for _, raw := range subsets {
		subset, _ := raw.(map[string]interface{})
		addresses, _, _ := unstructured.NestedSlice(subset, "addresses")
		count += len(addresses)
	}
	if count == 0 {
		return 0, fmt.Errorf("Service %s has no ready endpoint addresses", service.GetName())
	}
	return count, nil
}

func (v *faultVerification) serviceAddress(ctx context.Context) (string, error) {
	service := v.baseline
	if v.gvr != GVRServices {
		object, err := ResolveTarget(ctx, v.cs, GVRServices, v.details)
		if err != nil {
			return "", err
		}
		service = object
	}
	ports, _, _ := unstructured.NestedSlice(service.Object, "spec", "ports")
	for _, raw := range ports {
		port, _ := raw.(map[string]interface{})
		protocol, _ := port["protocol"].(string)
		if protocol != "" && protocol != "TCP" {
			continue
		}
		if configured := os.Getenv("TARGET_PORT"); v.details.ExperimentName == "chaos-mesh-http-abort-replacement" && configured != fmt.Sprint(port["targetPort"]) && configured != fmt.Sprint(port["port"]) {
			continue
		}
		return net.JoinHostPort(service.GetName()+"."+service.GetNamespace()+".svc", fmt.Sprint(port["port"])), nil
	}
	return "", fmt.Errorf("target Service has no applicable TCP port")
}

func tcpReachable(ctx context.Context, address string) error {
	connection, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	return connection.Close()
}

func equivalent(before, after interface{}) bool {
	// API round trips omit empty lists/maps. That is equivalent to absence.
	empty := func(v interface{}) bool {
		if v == nil {
			return true
		}
		r := reflect.ValueOf(v)
		return (r.Kind() == reflect.Map || r.Kind() == reflect.Slice) && r.Len() == 0
	}
	if empty(before) && empty(after) {
		return true
	}
	if a, ok := before.(string); ok && strings.HasPrefix(strings.TrimSpace(a), "{") {
		if b, ok := after.(string); ok {
			var x, y interface{}
			if json.Unmarshal([]byte(a), &x) == nil && json.Unmarshal([]byte(b), &y) == nil {
				return reflect.DeepEqual(x, y)
			}
		}
	}
	a, _ := json.Marshal(before)
	b, _ := json.Marshal(after)
	return string(a) == string(b)
}

func restoredPaths(original, current *unstructured.Unstructured, paths [][]string, freshService bool) error {
	if !freshService && original.GetUID() != "" && original.GetUID() != current.GetUID() {
		return fmt.Errorf("target identity changed during experiment")
	}
	for _, path := range paths {
		before, _ := readNested(original.Object, path)
		after, _ := readNested(current.Object, path)
		if freshService && len(path) == 1 && path[0] == "spec" {
			original = original.DeepCopy()
			current = current.DeepCopy()
			ip, _, _ := unstructured.NestedString(original.Object, "spec", "clusterIP")
			if ip != "None" {
				for _, key := range []string{"clusterIP", "clusterIPs"} {
					unstructured.RemoveNestedField(original.Object, "spec", key)
					unstructured.RemoveNestedField(current.Object, "spec", key)
				}
			}
			before, _ = readNested(original.Object, path)
			after, _ = readNested(current.Object, path)
		}
		if !equivalent(before, after) {
			return fmt.Errorf("original configuration not restored at %s", strings.Join(path, "."))
		}
	}
	return nil
}

func (v *faultVerification) recovered(ctx context.Context) error {
	if v.recoveryOverride != nil {
		if err := v.recoveryOverride(ctx); err != nil {
			return err
		}
	}
	if v.baseline != nil {
		current, err := v.current(ctx)
		if err != nil {
			return err
		}
		if err := restoredPaths(v.baseline, current, v.paths, v.details.ExperimentName == "deleted-kubernetes-service"); err != nil {
			return err
		}
	}
	for _, snapshot := range v.checks {
		current, err := v.cs.DynamicClient.Resource(snapshot.gvr).Namespace(snapshot.namespace).Get(ctx, snapshot.name, metav1.GetOptions{})
		if snapshot.object == nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return err
			}
			return fmt.Errorf("temporary %s/%s still exists", snapshot.gvr.Resource, snapshot.name)
		}
		if err != nil {
			return err
		}
		if err := restoredPaths(snapshot.object, current, snapshot.paths, false); err != nil {
			return err
		}
	}
	if err := v.helpersGone(ctx); err != nil {
		return err
	}
	if v.baseline != nil {
		if err := v.ready(ctx); err != nil {
			return err
		}
	}
	if v.address != "" {
		return tcpReachable(ctx, v.address)
	}
	return nil
}

func (v *faultVerification) effect(ctx context.Context) error {
	if v.effectOverride != nil {
		return v.effectOverride(ctx)
	}
	name := v.details.ExperimentName
	if v.baseline == nil {
		return v.helperEffect(ctx)
	}
	current, err := v.current(ctx)
	if name == "deleted-kubernetes-service" && apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if name == "deleted-kubernetes-service" {
		return fmt.Errorf("target Service is still present")
	}
	if len(v.paths) > 0 && restoredPaths(v.baseline, current, v.paths, false) == nil {
		return fmt.Errorf("target still has its original configuration")
	}
	switch name {
	case "scaled-to-zero-kubernetes-workload":
		pods, err := ResolveTargets(ctx, v.cs, GVRPods, v.details)
		if err != nil { // An empty result is the expected effect; API errors are not.
			app := v.details.AppDetail[0]
			selector := strings.Join(app.Labels, ",")
			if selector == "" {
				return v.zeroPodsByWorkload(ctx, current)
			}
			list, listErr := v.cs.DynamicClient.Resource(GVRPods).Namespace(app.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
			if listErr != nil {
				return listErr
			}
			if len(list.Items) == 0 {
				return nil
			}
			return err
		}
		if len(pods) != 0 {
			return fmt.Errorf("target still has %d pods", len(pods))
		}
		return nil
	case "invalid-kubernetes-service-selector", "modified-target-port-kubernetes-service", "ingress-port-blocking-network-policy", "chaos-mesh-http-abort-replacement":
		if err := tcpReachable(ctx, v.address); err != nil {
			return nil
		}
		return fmt.Errorf("target connection still succeeds")
	case "misconfigured-kubernetes-horizontal-pod-autoscaler":
		return v.autoscalerReacted(ctx)
	case "cordoned-kubernetes-worker-node":
		snapshot, err := v.snapshot(GVRNodes, nodeNameOf(v.pods))
		if err != nil {
			return err
		}
		node, err := v.cs.DynamicClient.Resource(GVRNodes).Get(ctx, snapshot.name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		cordoned, _, _ := unstructured.NestedBool(node.Object, "spec", "unschedulable")
		if cordoned {
			return nil
		}
		return fmt.Errorf("target node is still schedulable")
	case "opentelemetry-demo-feature-flag":
		snapshot, err := v.snapshot(GVRConfigMaps, FlagdConfigMap)
		if err != nil {
			return err
		}
		cm, err := v.cs.DynamicClient.Resource(GVRConfigMaps).Namespace(snapshot.namespace).Get(ctx, snapshot.name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if restoredPaths(snapshot.object, cm, snapshot.paths, false) == nil {
			return fmt.Errorf("feature flag data did not change")
		}
		return v.ready(ctx)
	case "insufficient-kubernetes-resource-quota":
		return v.quotaRejected(ctx)
	case "priority-kubernetes-workload-priority-preemption":
		return v.preempted(ctx)
	case "kubernetes-api-server-request-surge", "chaos-mesh-http-body-tamper-replacement", "chaos-mesh-pod-failure-replacement":
		return v.helperEffect(ctx)
	case "valkey-workload-changed-password":
		return v.passwordRejected(ctx)
	}
	pods, err := ResolveTargets(ctx, v.cs, GVRPods, v.details)
	if err != nil {
		return err
	}
	for _, pod := range pods {
		if !v.podHasMutation(current, &pod) {
			continue
		}
		if v.podEffect(ctx, &pod) {
			return nil
		}
	}
	return fmt.Errorf("no target pod has demonstrated the expected %s behavior", name)
}

func (v *faultVerification) zeroPodsByWorkload(ctx context.Context, workload *unstructured.Unstructured) error {
	selector, _, _ := unstructured.NestedStringMap(workload.Object, "spec", "selector", "matchLabels")
	if len(selector) == 0 {
		return fmt.Errorf("workload has no pod selector")
	}
	var terms []string
	for key, value := range selector {
		terms = append(terms, key+"="+value)
	}
	list, err := v.cs.DynamicClient.Resource(GVRPods).Namespace(workload.GetNamespace()).List(ctx, metav1.ListOptions{LabelSelector: strings.Join(terms, ",")})
	if err != nil {
		return err
	}
	if len(list.Items) > 0 {
		return fmt.Errorf("target still has %d pods", len(list.Items))
	}
	return nil
}

func (v *faultVerification) podHasMutation(current, pod *unstructured.Unstructured) bool {
	for _, path := range v.paths {
		if len(path) < 3 || path[0] != "spec" || path[1] != "template" {
			continue
		}
		before, _ := readNested(v.baseline.Object, path)
		changed, _ := readNested(current.Object, path)
		if equivalent(before, changed) {
			continue
		}
		actual, _ := readNested(pod.Object, path[2:])
		if !equivalent(withoutServiceAccountMount(actual), withoutServiceAccountMount(changed)) {
			return false
		}
	}
	return true
}

// withoutServiceAccountMount drops the projected service-account token the API server
// adds to every Pod at admission. It never appears on the workload's pod template, so
// comparing a pod's volumes, volumeMounts or initContainers against the template the
// fault mutated would otherwise never match and no effect could ever be observed.
func withoutServiceAccountMount(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, nested := range typed {
			cleaned := withoutServiceAccountMount(nested)
			// Stripping the only mount leaves an empty list where the pod template
			// has no key at all. Absent and empty mean the same thing here, and
			// keeping one would make every comparison fail.
			if list, ok := cleaned.([]interface{}); ok && len(list) == 0 {
				continue
			}
			out[key] = cleaned
		}
		return out
	case []interface{}:
		out := make([]interface{}, 0, len(typed))
		for _, element := range typed {
			if isServiceAccountMount(element) {
				continue
			}
			out = append(out, withoutServiceAccountMount(element))
		}
		return out
	default:
		return value
	}
}

func isServiceAccountMount(element interface{}) bool {
	entry, ok := element.(map[string]interface{})
	if !ok {
		return false
	}
	if path, _ := entry["mountPath"].(string); path == "/var/run/secrets/kubernetes.io/serviceaccount" {
		return true
	}
	name, _ := entry["name"].(string)
	return strings.HasPrefix(name, "kube-api-access-")
}

func (v *faultVerification) podEffect(ctx context.Context, pod *unstructured.Unstructured) bool {
	name := v.details.ExperimentName
	switch name {
	case "hanging-kubernetes-workload-init-container", "crashing-kubernetes-workload-init-container":
		statuses, _, _ := unstructured.NestedSlice(pod.Object, "status", "initContainerStatuses")
		key := "HANG_CONTAINER_NAME"
		if strings.HasPrefix(name, "crashing-") {
			key = "INIT_CONTAINER_NAME"
		}
		for _, raw := range statuses {
			status, _ := raw.(map[string]interface{})
			if status["name"] != os.Getenv(key) {
				continue
			}
			if key == "HANG_CONTAINER_NAME" {
				_, running, _ := unstructured.NestedMap(status, "state", "running")
				return running
			}
			exit, found, _ := unstructured.NestedInt64(status, "lastState", "terminated", "exitCode")
			if !found {
				exit, found, _ = unstructured.NestedInt64(status, "state", "terminated", "exitCode")
			}
			return found && exit != 0
		}
		return false
	case "nonexistent-kubernetes-workload-node", "unschedulable-k8s-workload-pod-anti-affinity-rule", "nonexistent-kubernetes-workload-persistent-volume-claim":
		conditions, _, _ := unstructured.NestedSlice(pod.Object, "status", "conditions")
		for _, raw := range conditions {
			condition, _ := raw.(map[string]interface{})
			message := strings.ToLower(fmt.Sprint(condition["message"]))
			if condition["type"] != "PodScheduled" || condition["status"] != "False" {
				continue
			}
			if strings.Contains(name, "anti-affinity") {
				return strings.Contains(message, "anti-affinity")
			}
			if strings.Contains(name, "persistent-volume") {
				return strings.Contains(message, "persistentvolumeclaim") || strings.Contains(message, "unbound immediate")
			}
			return strings.Contains(message, "selector") || strings.Contains(message, "affinity")
		}
		return false
	case "unassigned-k8s-workload-container-resource-limits":
		return true // Actual pod configuration was compared above.
	case "failing-name-resolution-kubernetes-workload-dns-policy":
		servers, _, _ := unstructured.NestedStringSlice(pod.Object, "spec", "dnsConfig", "nameservers")
		if len(servers) != 1 {
			return false
		}
		lookupCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(servers[0], "53"))
		}}
		_, err := resolver.LookupHost(lookupCtx, "kubernetes.default.svc.cluster.local")
		return err != nil && ctx.Err() == nil
	case "modified-k8s-workload-container-environment-variable":
		address := os.Getenv("BAD_ENV_VALUE")
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return false
		}
		if !strings.Contains(host, ".") {
			host += "." + pod.GetNamespace() + ".svc"
		}
		return tcpReachable(ctx, net.JoinHostPort(host, port)) != nil && ctx.Err() == nil
	}
	statuses, _, _ := unstructured.NestedSlice(pod.Object, "status", "containerStatuses")
	container := TargetContainerName(v.baseline.GetName())
	if _, fallback, ok := soleContainer(v.baseline); ok && os.Getenv("TARGET_CONTAINER") == "" {
		container = fallback
	}
	for _, raw := range statuses {
		status, _ := raw.(map[string]interface{})
		if status["name"] != container {
			continue
		}
		reason, _, _ := unstructured.NestedString(status, "state", "waiting", "reason")
		message, _, _ := unstructured.NestedString(status, "state", "waiting", "message")
		exit, failed, _ := unstructured.NestedInt64(status, "lastState", "terminated", "exitCode")
		switch name {
		case "nonexistent-kubernetes-workload-container-image":
			return reason == "ErrImagePull" || reason == "ImagePullBackOff"
		case "unsupported-architecture-k8s-workload-container-image":
			return strings.Contains(strings.ToLower(message), "manifest") || strings.Contains(strings.ToLower(message), "architecture") || strings.Contains(strings.ToLower(message), "exec format")
		case "invalid-kubernetes-workload-container-command":
			return reason == "RunContainerError" || reason == "CreateContainerError" || (failed && exit != 0)
		case "insufficient-kubernetes-workload-container-resources", "valkey-workload-out-of-memory":
			return (failed && exit != 0) || reason == "RunContainerError" || reason == "CreateContainerError"
		case "misconfigured-k8s-workload-container-readiness-probe":
			_, running, _ := unstructured.NestedMap(status, "state", "running")
			return running && status["ready"] == false && v.readinessFailed(ctx, pod)
		}
	}
	return false
}

func (v *faultVerification) events(ctx context.Context) ([]unstructured.Unstructured, error) {
	list, err := v.cs.DynamicClient.Resource(schema.GroupVersionResource{Version: "v1", Resource: "events"}).Namespace(v.ns()).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (v *faultVerification) readinessFailed(ctx context.Context, pod *unstructured.Unstructured) bool {
	events, err := v.events(ctx)
	if err != nil {
		return false
	}
	for _, event := range events {
		uid, _, _ := unstructured.NestedString(event.Object, "involvedObject", "uid")
		message, _, _ := unstructured.NestedString(event.Object, "message")
		if uid == string(pod.GetUID()) && strings.Contains(strings.ToLower(message), "readiness probe failed") {
			return true
		}
	}
	return false
}

// autoscalerReacted requires the lowered utilization targets to actually drive the
// autoscaler rather than merely to have been written: it must ask for more replicas
// than it did at baseline. Checking that it is still live would only repeat the
// precondition prepareVerification already enforced.
func (v *faultVerification) autoscalerReacted(ctx context.Context) error {
	if err := v.ready(ctx); err != nil {
		return err
	}
	current, err := v.current(ctx)
	if err != nil {
		return err
	}
	desired, _, _ := unstructured.NestedInt64(current.Object, "status", "desiredReplicas")
	if desired > v.baselineReplicas {
		return nil
	}
	return fmt.Errorf("autoscaler still targets %d replicas (baseline %d); the injected utilization target has not changed its decision", desired, v.baselineReplicas)
}

func (v *faultVerification) quotaRejected(ctx context.Context) error {
	events, err := v.events(ctx)
	if err != nil {
		return err
	}
	// The admission plugin words its rejection two ways, and both mean the injected
	// quota blocked the pod: "exceeded quota" when the pod declares resources that do
	// not fit, and "failed quota" when it declares none of the resources the quota
	// tracks. Matching only the first missed every target whose containers set no
	// requests or limits -- the common case -- and reported a working fault as failed.
	quota := os.Getenv("QUOTA_NAME")
	rejections := []string{"exceeded quota: " + quota, "failed quota: " + quota}
	for _, event := range events {
		message, _, _ := unstructured.NestedString(event.Object, "message")
		name, _, _ := unstructured.NestedString(event.Object, "involvedObject", "name")
		if !strings.HasPrefix(name, v.baseline.GetName()+"-") {
			continue
		}
		for _, rejection := range rejections {
			if strings.Contains(message, rejection) {
				return nil
			}
		}
	}
	return fmt.Errorf("no target admission attempt has been rejected by the injected quota %q", quota)
}

func (v *faultVerification) preempted(ctx context.Context) error {
	events, err := v.events(ctx)
	if err != nil {
		return err
	}
	for _, event := range events {
		reason, _, _ := unstructured.NestedString(event.Object, "reason")
		uid, _, _ := unstructured.NestedString(event.Object, "involvedObject", "uid")
		if reason != "Preempted" {
			continue
		}
		for _, pod := range v.pods {
			if uid == string(pod.GetUID()) {
				return nil
			}
		}
	}
	return fmt.Errorf("the selected target has not been preempted")
}

func (v *faultVerification) passwordRejected(ctx context.Context) error {
	connection, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "tcp", v.address)
	if err != nil {
		return err
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	if _, err := connection.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		return err
	}
	response, err := bufio.NewReader(connection).ReadString('\n')
	if err != nil {
		return err
	}
	if strings.HasPrefix(response, "-NOAUTH") {
		return nil
	}
	return fmt.Errorf("Valkey still accepts unauthenticated requests")
}

func (v *faultVerification) helperEffect(ctx context.Context) error {
	selector := "app.kubernetes.io/name=" + os.Getenv("SURGE_WORKLOAD_NAME")
	switch v.details.ExperimentName {
	case "chaos-mesh-http-body-tamper-replacement":
		selector = "chaos-injector=" + v.details.ExperimentName
	case "chaos-mesh-pod-failure-replacement":
		return fmt.Errorf("freeze helper did not register verified process state")
	}
	pods, err := v.cs.DynamicClient.Resource(GVRPods).Namespace(v.ns()).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return err
	}
	if v.cs.KubeClient == nil {
		return fmt.Errorf("helper log client is unavailable")
	}
	verified, aggregateRate := 0, float64(0)
	for _, pod := range pods.Items {
		if !podReady(&pod) {
			continue
		}
		logCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		tail := int64(5)
		logs, err := v.cs.KubeClient.CoreV1().Pods(pod.GetNamespace()).GetLogs(pod.GetName(), &corev1.PodLogOptions{TailLines: &tail}).DoRaw(logCtx)
		cancel()
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(logs), "\n") {
			var sample struct {
				Rate   float64 `json:"successfulRate"`
				Status int     `json:"status"`
			}
			if json.Unmarshal([]byte(line), &sample) != nil {
				continue
			}
			if v.details.ExperimentName == "kubernetes-api-server-request-surge" {
				aggregateRate += sample.Rate
				verified++
				break
			}
			if sample.Status == 400 || sample.Status == 422 || sample.Status >= 500 {
				return nil
			}
		}
	}
	if v.details.ExperimentName == "kubernetes-api-server-request-surge" {
		expected, _ := strconv.Atoi(os.Getenv("REQUESTS_PER_SECOND"))
		replicas, _ := strconv.Atoi(os.Getenv("SURGE_REPLICAS"))
		if expected > 0 && verified == replicas && aggregateRate >= 0.8*float64(expected) {
			return nil
		}
		return fmt.Errorf("API load not delivered: verified helpers=%d/%d successful rate=%.1f/s expected=%d/s", verified, replicas, aggregateRate, expected)
	}
	return fmt.Errorf("no helper request has demonstrated the intended target response")
}

func (v *faultVerification) helpersGone(ctx context.Context) error {
	var selectors []string
	switch v.details.ExperimentName {
	case "kubernetes-api-server-request-surge":
		selectors = []string{"app.kubernetes.io/name=" + os.Getenv("SURGE_WORKLOAD_NAME")}
	case "chaos-mesh-http-body-tamper-replacement":
		selectors = []string{"chaos-injector=" + v.details.ExperimentName}
	case "priority-kubernetes-workload-priority-preemption":
		selectors = []string{"app=" + os.Getenv("DECOY_NAME")}
	}
	for _, selector := range selectors {
		namespace := v.ns()
		if ns := os.Getenv("DECOY_NAMESPACE"); ns != "" && strings.HasPrefix(selector, "app=") {
			namespace = ns
		}
		pods, err := v.cs.DynamicClient.Resource(GVRPods).Namespace(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return err
		}
		if len(pods.Items) > 0 {
			return fmt.Errorf("temporary helper pods still exist")
		}
	}
	return nil
}

func (v *faultVerification) hold(ctx context.Context) {
	v.held = true
	if err := WaitForCheck(ctx, v.timeout(), "fault effect was not observed", v.effect); err != nil {
		v.err = err
		return
	}
	log.Infof("[Effect]: %s behavior verified; holding for %ds", v.details.ExperimentName, v.details.ChaosDuration)
	if v.onChaos != nil {
		if err := v.onChaos(ctx); err != nil {
			v.err = err
			return
		}
	}
	if v.stopProbes != nil {
		defer func() { v.err = errors.Join(v.err, v.stopProbes()) }()
	}
	Sleep(ctx, v.details.ChaosDuration)
}

func (v *faultVerification) finish(ctx context.Context, injectErr error) error {
	if !v.held && injectErr == nil {
		injectErr = fmt.Errorf("injector returned without observing and holding its fault")
	}
	// Recovery waits for controllers, not just for the revert API call: pods are
	// recreated, images are pulled and endpoints republished. That legitimately takes
	// longer than RevertTimeout, so it gets its own budget on a context that survives
	// the cancellation an abort puts on ctx.
	budget := RecoveryBudget(v.details)
	recoveryCtx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	recoveryErr := WaitForCheck(recoveryCtx, budget, "fault recovery was not verified", v.recovered)
	v.recoveryErr = recoveryErr
	if recoveryErr == nil {
		log.Infof("[Recovery]: %s original state and health verified", v.details.ExperimentName)
	}
	if recoveryErr != nil {
		return friendlyError(errors.Join(injectErr, v.err, recoveryErr), cerrors.ErrorTypeChaosRevert)
	}
	return errors.Join(injectErr, v.err)
}

// RecoveryBudget bounds post-revert convergence. It follows STATUS_CHECK_TIMEOUT rather
// than RevertTimeout because restoring configuration is an API call while recovering
// behavior is a controller rollout.
func RecoveryBudget(details *types.ChaosDetails) time.Duration {
	if details != nil && details.Timeout > 0 {
		return time.Duration(details.Timeout) * time.Second
	}
	return 180 * time.Second
}

// RegisterFaultChecks supplies effect/recovery checks for a helper with its own
// observable protocol. Registration is not success; the lifecycle runs both.
func RegisterFaultChecks(ctx context.Context, effect, recovery func(context.Context) error) {
	if v := verification(ctx); v != nil {
		v.effectOverride = effect
		v.recoveryOverride = recovery
	}
}

func friendlyError(err error, code cerrors.ErrorType) error {
	if err == nil {
		return nil
	}
	if cerrors.IsUserFriendly(err) {
		return err
	}
	return cerrors.Error{ErrorCode: code, Reason: err.Error()}
}

// TrackTemporaryResource makes cleanup part of the mandatory recovery check.
func TrackTemporaryResource(ctx context.Context, gvr schema.GroupVersionResource, namespace, name string) {
	if v := verification(ctx); v != nil {
		v.checks = append(v.checks, resourceSnapshot{gvr: gvr, namespace: namespace, name: name})
	}
}

// TargetPodSelector uses the complete workload selector, including expressions.
func TargetPodSelector(ctx context.Context, cs clients.ClientSets, details *types.ChaosDetails) (map[string]interface{}, error) {
	gvr, err := targetGVR(details)
	if err != nil {
		return nil, err
	}
	target, err := ResolveTarget(ctx, cs, gvr, details)
	if err != nil {
		return nil, err
	}
	selector, found, err := unstructured.NestedMap(target.Object, "spec", "selector")
	if err != nil || !found || len(selector) == 0 {
		return nil, fmt.Errorf("target workload has no pod selector")
	}
	return selector, nil
}
