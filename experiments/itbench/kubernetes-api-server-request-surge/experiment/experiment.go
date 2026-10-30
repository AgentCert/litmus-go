// Package experiment implements kubernetes-api-server-request-surge: deploys a
// synthetic load-generator Deployment (+ dedicated ServiceAccount/Role/RoleBinding) that
// hammers the kube-apiserver's pods endpoint for the target namespace, holds, deletes all
// four objects. Cluster-wide blast radius: kube-apiserver latency degrades for every
// tenant, not just the target namespace -- same warning the original script carried.
package experiment

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/log"
	"github.com/litmuschaos/litmus-go/pkg/types"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	gvrRoles           = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
	gvrRoleBindings    = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}
	gvrServiceAccounts = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "serviceaccounts"}
)

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) (retErr error) {
	if len(chaosDetails.AppDetail) == 0 {
		return fmt.Errorf("no target namespace resolved: TARGETS env var was empty/unset")
	}
	namespace := chaosDetails.AppDetail[0].Namespace

	workloadName := os.Getenv("SURGE_WORKLOAD_NAME")
	// Discarding these parse errors would silently degrade a 1000 req/s surge to the
	// rpsPerPod<1 clamp of 1 req/s -- not a detectable API-server surge, yet still Passed.
	requestsPerSecond, err := strconv.Atoi(strings.TrimSpace(os.Getenv("REQUESTS_PER_SECOND")))
	if err != nil || requestsPerSecond <= 0 {
		return fmt.Errorf("REQUESTS_PER_SECOND must be a positive integer, got %q", os.Getenv("REQUESTS_PER_SECOND"))
	}
	surgeReplicas, err := strconv.Atoi(strings.TrimSpace(os.Getenv("SURGE_REPLICAS")))
	if err != nil || surgeReplicas <= 0 {
		return fmt.Errorf("SURGE_REPLICAS must be a positive integer, got %q", os.Getenv("SURGE_REPLICAS"))
	}
	if surgeReplicas > requestsPerSecond || requestsPerSecond%surgeReplicas != 0 {
		return fmt.Errorf("REQUESTS_PER_SECOND must divide evenly across SURGE_REPLICAS, with at least one request per helper")
	}
	rpsPerPod := requestsPerSecond / surgeReplicas
	if rpsPerPod > 10000 {
		return fmt.Errorf("load per helper exceeds 10000 requests/second")
	}
	if workloadName == "" {
		return fmt.Errorf("SURGE_WORKLOAD_NAME is required")
	}
	self, err := itbench.ThisExperimentPod(ctx, cs)
	if err != nil {
		return err
	}
	surgeImage := helperImage(self)

	labels := map[string]interface{}{"litmuschaos.io/fault": "kubernetes-api-server-request-surge"}

	sa := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1", "kind": "ServiceAccount",
		"metadata": map[string]interface{}{"name": workloadName, "namespace": namespace, "labels": labels},
	}}
	role := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
		"metadata": map[string]interface{}{"name": workloadName, "namespace": namespace, "labels": labels},
		"rules": []interface{}{
			map[string]interface{}{"apiGroups": []interface{}{""}, "resources": []interface{}{"pods"}, "verbs": []interface{}{"get", "list"}},
		},
	}}
	roleBinding := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
		"metadata": map[string]interface{}{"name": workloadName, "namespace": namespace, "labels": labels},
		"roleRef": map[string]interface{}{
			"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": workloadName,
		},
		"subjects": []interface{}{
			map[string]interface{}{"kind": "ServiceAccount", "name": workloadName, "namespace": namespace},
		},
	}}

	deployment := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]interface{}{
			"name": workloadName, "namespace": namespace,
			"labels": map[string]interface{}{"litmuschaos.io/fault": "kubernetes-api-server-request-surge", "app.kubernetes.io/name": workloadName},
		},
		"spec": map[string]interface{}{
			"replicas": int64(surgeReplicas),
			"selector": map[string]interface{}{"matchLabels": map[string]interface{}{"app.kubernetes.io/name": workloadName}},
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{"labels": map[string]interface{}{"app.kubernetes.io/name": workloadName}},
				"spec": map[string]interface{}{
					"serviceAccountName": workloadName,
					"containers": []interface{}{
						map[string]interface{}{
							"name":  "load-generator",
							"image": surgeImage,
							"env": []interface{}{
								map[string]interface{}{"name": "RPS", "value": fmt.Sprintf("%d", rpsPerPod)},
								map[string]interface{}{"name": "LOAD_NAMESPACE", "value": namespace},
							},
							"command": []interface{}{"/itbench-experiment"},
							"args":    []interface{}{"-api-load"},
							"resources": map[string]interface{}{
								"requests": map[string]interface{}{"cpu": "50m", "memory": "64Mi"},
								"limits":   map[string]interface{}{"cpu": "1", "memory": "128Mi"},
							},
						},
					},
				},
			},
		},
	}}

	// The helpers are created after the server resolved the manifest's images, so
	// they inherit this experiment's registry credentials rather than going without.
	// The pod spec is mutated in place: unstructured.NestedMap deep-copies, which
	// panics on any value that is not already JSON-shaped.
	if podSpec := itbench.NestedObject(deployment.Object, "spec", "template", "spec"); podSpec != nil {
		self.ApplyTo(podSpec, surgeImage)
	}

	log.Infof("Injecting: deploying %s (%d replicas, ~%d req/s/pod, ~%d req/s aggregate target) using image %s into %s", workloadName, surgeReplicas, rpsPerPod, requestsPerSecond, surgeImage, namespace)
	saClient := cs.DynamicClient.Resource(gvrServiceAccounts).Namespace(namespace)
	roleClient := cs.DynamicClient.Resource(gvrRoles).Namespace(namespace)
	rbClient := cs.DynamicClient.Resource(gvrRoleBindings).Namespace(namespace)
	deployClient := cs.DynamicClient.Resource(itbench.GVRDeployments).Namespace(namespace)

	// Each object is registered for deletion the moment it exists: all four share one fixed
	// name, so anything left behind by a partial create or an abort makes every later run
	// fail with AlreadyExists.
	var cleanups []func(ctx context.Context) error
	defer func() {
		revertCtx, cancel := itbench.RevertContext()
		defer cancel()
		log.Infof("Reverting: deleting %s surge workload and its RBAC from %s", workloadName, namespace)
		for i := len(cleanups) - 1; i >= 0; i-- {
			retErr = errors.Join(retErr, cleanups[i](revertCtx))
		}
	}()

	create := func(rc namespacedClient, obj *unstructured.Unstructured, kind string) error {
		if _, err := rc.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("creating %s: %w", kind, err)
		}
		kindGVR := map[string]schema.GroupVersionResource{"serviceaccount": gvrServiceAccounts, "role": gvrRoles, "rolebinding": gvrRoleBindings, "surge deployment": itbench.GVRDeployments}[kind]
		itbench.TrackTemporaryResource(ctx, kindGVR, namespace, workloadName)
		cleanups = append(cleanups, func(c context.Context) error { return deleteIgnoreNotFound(c, rc, workloadName) })
		return nil
	}

	if err := create(saClient, sa, "serviceaccount"); err != nil {
		return err
	}
	if err := create(roleClient, role, "role"); err != nil {
		return err
	}
	if err := create(rbClient, roleBinding, "rolebinding"); err != nil {
		return err
	}
	if err := create(deployClient, deployment, "surge deployment"); err != nil {
		return err
	}

	itbench.HoldChaos(ctx, chaosDetails)
	return nil
}

type namespacedClient interface {
	Create(ctx context.Context, obj *unstructured.Unstructured, opts metav1.CreateOptions, subresources ...string) (*unstructured.Unstructured, error)
	Delete(ctx context.Context, name string, opts metav1.DeleteOptions, subresources ...string) error
}

// helperImage resolves the image the load generators must run. The generator is
// this same binary invoked with -api-load, so the image has to be the experiment's
// own: SURGE_IMAGE used to name a general-purpose kubectl image left over from the
// original shell-script generator, which has no /itbench-experiment to execute, so
// every helper pod failed to start and the surge delivered no load at all.
// Inheriting it from the running pod keeps the helper on the same build and the
// same registry as the experiment, whichever registry that is.
func helperImage(self itbench.ExperimentPod) string {
	if configured := strings.TrimSpace(os.Getenv("SURGE_IMAGE")); configured != "" {
		return configured
	}
	return self.Image
}

func deleteIgnoreNotFound(ctx context.Context, rc interface {
	Delete(ctx context.Context, name string, opts metav1.DeleteOptions, subresources ...string) error
}, name string) error {
	if err := rc.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("deleting %s: %w", name, err)
	}
	return nil
}
