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

	"github.com/litmuschaos/litmus-go/pkg/cerrors"
	"github.com/litmuschaos/litmus-go/pkg/clients"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/log"
	"github.com/litmuschaos/litmus-go/pkg/types"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
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
	allocMem, err := resource.ParseQuantity(allocMemRaw)
	if err != nil {
		return fmt.Errorf("parsing node allocatable memory %q: %w", allocMemRaw, err)
	}
	// Discarding this parse error would make the decoy request 0Ki, so it schedules
	// alongside everything and preempts nothing -- while still reporting success.
	decoyMemPercent, err := strconv.Atoi(strings.TrimSpace(os.Getenv("DECOY_MEMORY_PERCENT")))
	if err != nil || decoyMemPercent <= 0 || decoyMemPercent > 100 {
		return fmt.Errorf("DECOY_MEMORY_PERCENT must be an integer in 1-100, got %q", os.Getenv("DECOY_MEMORY_PERCENT"))
	}
	// The decoy has to be sized against what the node has left, not against a fixed
	// share of its total: a fraction of allocatable fits in the free space of a large
	// or lightly loaded node, so the scheduler simply places it and preempts nothing.
	requested, err := requestedMemory(ctx, cs, nodeName)
	if err != nil {
		return err
	}
	targetRequest := podMemoryRequest(&pods[0])
	if targetRequest <= 0 {
		return cerrors.Error{ErrorCode: cerrors.ErrorTypeTargetSelection,
			Target: fmt.Sprintf("pod %s in namespace %s", pods[0].GetName(), pods[0].GetNamespace()),
			Reason: "target declares no memory request, so evicting it frees no capacity and the scheduler can never preempt it; choose a target with resources.requests.memory set"}
	}
	free := allocMem.Value() - requested
	if free < 0 {
		free = 0
	}
	// Strictly more than the free space (so it cannot simply be placed), and no more
	// than the free space plus the target's own request (so evicting the target is
	// enough to admit it).
	decoyMemBytes := free + targetRequest
	ceiling := allocMem.Value() / 100 * int64(decoyMemPercent)
	if decoyMemBytes > ceiling {
		return cerrors.Error{ErrorCode: cerrors.ErrorTypeTargetSelection,
			Target: fmt.Sprintf("node %s", nodeName),
			Reason: fmt.Sprintf("forcing preemption needs a decoy of %dMi but DECOY_MEMORY_PERCENT caps it at %dMi (%d%% of allocatable); free the node, choose a larger target, or raise DECOY_MEMORY_PERCENT",
				decoyMemBytes/(1024*1024), ceiling/(1024*1024), decoyMemPercent)}
	}
	decoyMemoryRequest := fmt.Sprintf("%dKi", decoyMemBytes/1024)
	log.Infof("Node allocatable=%dMi requested=%dMi free=%dMi; target requests %dMi, so the decoy requests %s to force its eviction (node=%s)",
		allocMem.Value()/(1024*1024), requested/(1024*1024), free/(1024*1024), targetRequest/(1024*1024), decoyMemoryRequest, nodeName)

	priorityClassName := os.Getenv("PRIORITY_CLASS_NAME")
	// A PriorityClass with value 0 equals the default pod priority, so nothing is preempted.
	priorityValue, err := strconv.Atoi(strings.TrimSpace(os.Getenv("PRIORITY_VALUE")))
	if err != nil || priorityValue <= 0 {
		return fmt.Errorf("PRIORITY_VALUE must be a positive integer, got %q", os.Getenv("PRIORITY_VALUE"))
	}
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
	// The PriorityClass is cluster-scoped and the decoy outlives a namespace
	// teardown, so both removals are asserted by the shared recovery check rather
	// than trusted to a best-effort delete.
	itbench.TrackTemporaryResource(ctx, itbench.GVRPriorityClasses, "", priorityClassName)
	// Deferred from here on: the PriorityClass is cluster-scoped, so one left behind by a
	// failed decoy create or an abort survives the namespace teardown and makes every later
	// run fail with AlreadyExists.
	defer func() {
		revertCtx, cancel := itbench.RevertContext()
		defer cancel()
		log.Infof("Reverting: deleting PriorityClass %s", priorityClassName)
		if err := pcClient.Delete(revertCtx, priorityClassName, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
			log.Errorf("failed to delete priorityclass (non-fatal): %v", err)
		}
	}()

	// The decoy is created after the server resolved the manifest's images, so it
	// inherits this experiment's registry credentials rather than going without.
	if self, err := itbench.ThisExperimentPod(ctx, cs); err == nil {
		if podSpec := itbench.NestedObject(decoy.Object, "spec", "template", "spec"); podSpec != nil {
			self.ApplyTo(podSpec, "")
		}
	} else {
		log.Warnf("could not inherit registry credentials for the decoy (%v); it may fail to pull %s", err, decoyImage)
	}

	log.Infof("Injecting: creating decoy pressure Deployment %s in %s", decoyName, decoyNamespace)
	decoyClient := cs.DynamicClient.Resource(itbench.GVRDeployments).Namespace(decoyNamespace)
	if _, err := decoyClient.Create(ctx, decoy, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("creating decoy deployment: %w", err)
	}
	itbench.TrackTemporaryResource(ctx, itbench.GVRDeployments, decoyNamespace, decoyName)
	defer func() {
		revertCtx, cancel := itbench.RevertContext()
		defer cancel()
		log.Infof("Reverting: deleting decoy Deployment %s", decoyName)
		if err := decoyClient.Delete(revertCtx, decoyName, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
			log.Errorf("failed to delete decoy deployment (non-fatal): %v", err)
		}
		if err := deleteDecoyPods(revertCtx, cs, decoyNamespace, decoyName); err != nil {
			log.Errorf("failed to force-delete decoy pods (non-fatal): %v", err)
		}
	}()

	itbench.HoldChaos(ctx, chaosDetails)
	return nil
}

// requestedMemory totals the memory requests of every pod already placed on the node.
// Terminated pods hold no capacity, so they are excluded.
func requestedMemory(ctx context.Context, cs clients.ClientSets, nodeName string) (int64, error) {
	pods, err := cs.DynamicClient.Resource(itbench.GVRPods).Namespace("").List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		return 0, fmt.Errorf("reading the pods already scheduled on node %s: %w", nodeName, err)
	}
	var total int64
	for i := range pods.Items {
		phase, _, _ := unstructured.NestedString(pods.Items[i].Object, "status", "phase")
		if phase == "Succeeded" || phase == "Failed" {
			continue
		}
		total += podMemoryRequest(&pods.Items[i])
	}
	return total, nil
}

// podMemoryRequest totals one pod's container memory requests, which is what the
// scheduler reserves for it and therefore what evicting it would release.
func podMemoryRequest(pod *unstructured.Unstructured) int64 {
	containers, _, _ := unstructured.NestedSlice(pod.Object, "spec", "containers")
	var total int64
	for _, raw := range containers {
		container, _ := raw.(map[string]interface{})
		value, found, _ := unstructured.NestedString(container, "resources", "requests", "memory")
		if !found {
			continue
		}
		quantity, err := resource.ParseQuantity(value)
		if err != nil {
			continue
		}
		total += quantity.Value()
	}
	return total
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
