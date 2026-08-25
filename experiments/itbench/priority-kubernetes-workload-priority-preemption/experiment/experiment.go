// Package experiment implements priority-kubernetes-workload-priority-preemption: creates
// a temporary high-priority PriorityClass and a decoy Deployment sized to
// DECOY_MEMORY_PERCENT of the target pod's node's allocatable memory, pinned to that same
// node via nodeSelector -- forcing the scheduler to preempt lower-priority pods already
// there, including the target's. Holds, deletes the decoy and its pods (force, since a
// Deployment delete alone can leave pods behind briefly) and the PriorityClass.
package experiment

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/log"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/types"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	pods, err := itbench.ResolveTargets(ctx, cs, itbench.GVRPods, chaosDetails)
	if err != nil {
		return err
	}
	nodeName, _, _ := unstructured.NestedString(pods[0].Object, "spec", "nodeName")
	if nodeName == "" {
		return fmt.Errorf("target pod %s has no assigned node (not yet scheduled)", pods[0].GetName())
	}
	log.Infof("Target pod=%s is scheduled on node=%s", pods[0].GetName(), nodeName)

	node, err := cs.DynamicClient.Resource(itbench.GVRNodes).Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	allocMemRaw, _, _ := unstructured.NestedString(node.Object, "status", "allocatable", "memory")
	allocMemKi, err := parseKiValue(allocMemRaw)
	if err != nil {
		return fmt.Errorf("parsing node allocatable memory %q: %w", allocMemRaw, err)
	}
	decoyMemPercent, _ := strconv.Atoi(os.Getenv("DECOY_MEMORY_PERCENT"))
	decoyMemKi := allocMemKi * int64(decoyMemPercent) / 100
	decoyMemoryRequest := fmt.Sprintf("%dKi", decoyMemKi)
	log.Infof("Node allocatable memory=%s; decoy will request %s (%d%%), pinned to node=%s", allocMemRaw, decoyMemoryRequest, decoyMemPercent, nodeName)

	priorityClassName := os.Getenv("PRIORITY_CLASS_NAME")
	priorityValue, _ := strconv.Atoi(os.Getenv("PRIORITY_VALUE"))
	decoyName := os.Getenv("DECOY_NAME")
	decoyNamespace := os.Getenv("DECOY_NAMESPACE")
	if decoyNamespace == "" {
		decoyNamespace = chaosDetails.AppDetail[0].Namespace
	}
	decoyImage := os.Getenv("DECOY_IMAGE")
	decoyCPURequest := os.Getenv("DECOY_CPU_REQUEST")

	priorityClass := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion":    "scheduling.k8s.io/v1",
		"kind":          "PriorityClass",
		"metadata":      map[string]interface{}{"name": priorityClassName},
		"value":         int64(priorityValue),
		"description":   "Temporary priority class created by the priority-kubernetes-workload-priority-preemption chaos fault",
		"globalDefault": false,
	}}

	decoy := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]interface{}{
			"name": decoyName, "namespace": decoyNamespace,
			"labels": map[string]interface{}{"app": decoyName, "chaos.itbench.io/role": "priority-preemption-decoy"},
		},
		"spec": map[string]interface{}{
			"replicas": int64(1),
			"selector": map[string]interface{}{"matchLabels": map[string]interface{}{"app": decoyName}},
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{"labels": map[string]interface{}{"app": decoyName}},
				"spec": map[string]interface{}{
					"priorityClassName": priorityClassName,
					"nodeSelector":      map[string]interface{}{"kubernetes.io/hostname": nodeName},
					"containers": []interface{}{
						map[string]interface{}{
							"name":  "decoy",
							"image": decoyImage,
							"resources": map[string]interface{}{
								"requests": map[string]interface{}{"cpu": decoyCPURequest, "memory": decoyMemoryRequest},
							},
						},
					},
				},
			},
		},
	}}

	log.Infof("Injecting: creating higher-priority PriorityClass %s (value=%d)", priorityClassName, priorityValue)
	pcClient := cs.DynamicClient.Resource(itbench.GVRPriorityClasses)
	if _, err := pcClient.Create(ctx, priorityClass, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("creating priorityclass: %w", err)
	}

	log.Infof("Injecting: creating decoy pressure Deployment %s in %s", decoyName, decoyNamespace)
	decoyClient := cs.DynamicClient.Resource(itbench.GVRDeployments).Namespace(decoyNamespace)
	if _, err := decoyClient.Create(ctx, decoy, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("creating decoy deployment: %w", err)
	}

	itbench.Sleep(ctx, chaosDetails.ChaosDuration)

	log.Infof("Reverting: deleting decoy Deployment %s and PriorityClass %s", decoyName, priorityClassName)
	if err := decoyClient.Delete(ctx, decoyName, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		log.Errorf("failed to delete decoy deployment (non-fatal): %v", err)
	}
	if err := deleteDecoyPods(ctx, cs, decoyNamespace, decoyName); err != nil {
		log.Errorf("failed to force-delete decoy pods (non-fatal): %v", err)
	}
	if err := pcClient.Delete(ctx, priorityClassName, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		log.Errorf("failed to delete priorityclass (non-fatal): %v", err)
	}
	return nil
}

func deleteDecoyPods(ctx context.Context, cs clients.ClientSets, namespace, decoyName string) error {
	podClient := cs.DynamicClient.Resource(itbench.GVRPods).Namespace(namespace)
	list, err := podClient.List(ctx, metav1.ListOptions{LabelSelector: "app=" + decoyName})
	if err != nil {
		return err
	}
	zero := int64(0)
	for _, p := range list.Items {
		if err := podClient.Delete(ctx, p.GetName(), metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil && !k8serrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// parseKiValue parses a Kubernetes quantity string of the form "<N>Ki" into N.
func parseKiValue(s string) (int64, error) {
	s = strings.TrimSuffix(s, "Ki")
	return strconv.ParseInt(s, 10, 64)
}
