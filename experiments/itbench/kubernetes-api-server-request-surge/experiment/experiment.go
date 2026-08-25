// Package experiment implements kubernetes-api-server-request-surge: deploys a
// synthetic load-generator Deployment (+ dedicated ServiceAccount/Role/RoleBinding) that
// hammers the kube-apiserver's pods endpoint for the target namespace, holds, deletes all
// four objects. Cluster-wide blast radius: kube-apiserver latency degrades for every
// tenant, not just the target namespace -- same warning the original script carried.
package experiment

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/log"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
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

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	if len(chaosDetails.AppDetail) == 0 {
		return fmt.Errorf("no target namespace resolved: TARGETS env var was empty/unset")
	}
	namespace := chaosDetails.AppDetail[0].Namespace

	workloadName := os.Getenv("SURGE_WORKLOAD_NAME")
	surgeImage := os.Getenv("SURGE_IMAGE")
	requestsPerSecond, _ := strconv.Atoi(os.Getenv("REQUESTS_PER_SECOND"))
	surgeReplicas, _ := strconv.Atoi(os.Getenv("SURGE_REPLICAS"))
	if surgeReplicas <= 0 {
		surgeReplicas = 1
	}
	rpsPerPod := requestsPerSecond / surgeReplicas
	if rpsPerPod < 1 {
		rpsPerPod = 1
	}

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

	loadScript := `set -eu
echo "=== API Request Surge Load Generator ==="
echo "Target: kube-apiserver (namespace ` + namespace + ` pods endpoint)"
echo "Requests per second (this pod): ${RPS}"
echo "Duration: ${DURATION}s"
END=$(( $(date +%s) + ${DURATION} ))
while [ "$(date +%s)" -lt "${END}" ]; do
  i=0
  while [ "${i}" -lt "${RPS}" ]; do
    kubectl get --raw="/api/v1/namespaces/` + namespace + `/pods" >/dev/null 2>&1 &
    i=$((i + 1))
  done
  wait
done
echo "load generation complete"
`
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
								map[string]interface{}{"name": "DURATION", "value": fmt.Sprintf("%d", chaosDetails.ChaosDuration)},
							},
							"command": []string{"/bin/sh", "-c"},
							"args":    []string{loadScript},
							"resources": map[string]interface{}{
								"requests": map[string]interface{}{"cpu": "50m", "memory": "64Mi"},
								"limits":   map[string]interface{}{"cpu": "200m", "memory": "128Mi"},
							},
						},
					},
				},
			},
		},
	}}

	log.Infof("Injecting: deploying %s (%d replicas, ~%d req/s/pod, ~%d req/s aggregate target) into %s", workloadName, surgeReplicas, rpsPerPod, requestsPerSecond, namespace)
	saClient := cs.DynamicClient.Resource(gvrServiceAccounts).Namespace(namespace)
	roleClient := cs.DynamicClient.Resource(gvrRoles).Namespace(namespace)
	rbClient := cs.DynamicClient.Resource(gvrRoleBindings).Namespace(namespace)
	deployClient := cs.DynamicClient.Resource(itbench.GVRDeployments).Namespace(namespace)

	if _, err := saClient.Create(ctx, sa, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("creating serviceaccount: %w", err)
	}
	if _, err := roleClient.Create(ctx, role, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("creating role: %w", err)
	}
	if _, err := rbClient.Create(ctx, roleBinding, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("creating rolebinding: %w", err)
	}
	if _, err := deployClient.Create(ctx, deployment, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("creating surge deployment: %w", err)
	}

	itbench.Sleep(ctx, chaosDetails.ChaosDuration)

	log.Infof("Reverting: deleting %s surge workload and its RBAC from %s", workloadName, namespace)
	deleteIgnoreNotFound(ctx, deployClient, workloadName)
	deleteIgnoreNotFound(ctx, rbClient, workloadName)
	deleteIgnoreNotFound(ctx, roleClient, workloadName)
	deleteIgnoreNotFound(ctx, saClient, workloadName)
	return nil
}

func deleteIgnoreNotFound(ctx context.Context, rc interface {
	Delete(ctx context.Context, name string, opts metav1.DeleteOptions, subresources ...string) error
}, name string) {
	if err := rc.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		log.Errorf("failed to delete %s (non-fatal): %v", name, err)
	}
}
