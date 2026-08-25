// Package experiment implements insufficient-kubernetes-resource-quota: tightens (or
// creates) a namespace ResourceQuota (QUOTA_NAME) to insufficient hard limits, triggers a
// rollout restart on the target workload so new pods hit admission, holds, then restores
// the quota to its original values -- or deletes it entirely if it didn't exist before
// injection. Namespace-scoped: affects every workload in the target namespace, not just
// the resolved target (same blast radius as the original script).
package experiment

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/log"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/types"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	if len(chaosDetails.AppDetail) == 0 {
		return fmt.Errorf("no target resolved: TARGETS env var was empty/unset")
	}
	workloadGVR, err := itbench.WorkloadGVRForKind(chaosDetails.AppDetail[0].Kind)
	if err != nil {
		return err
	}
	target, err := itbench.ResolveTarget(ctx, cs, workloadGVR, chaosDetails)
	if err != nil {
		return err
	}
	namespace := target.GetNamespace()
	quotaName := os.Getenv("QUOTA_NAME")

	hard := map[string]interface{}{
		"requests.cpu":    os.Getenv("QUOTA_HARD_REQUESTS_CPU"),
		"requests.memory": os.Getenv("QUOTA_HARD_REQUESTS_MEMORY"),
		"limits.cpu":      os.Getenv("QUOTA_HARD_LIMITS_CPU"),
		"limits.memory":   os.Getenv("QUOTA_HARD_LIMITS_MEMORY"),
	}

	rqClient := cs.DynamicClient.Resource(itbench.GVRResourceQuotas).Namespace(namespace)
	existing, getErr := rqClient.Get(ctx, quotaName, metav1.GetOptions{})
	existed := getErr == nil
	if getErr != nil && !k8serrors.IsNotFound(getErr) {
		return getErr
	}

	var originalHard map[string]interface{}
	if existed {
		originalHard, _, _ = unstructured.NestedMap(existing.Object, "spec", "hard")
		log.Infof("Injecting: patching ResourceQuota %s/%s hard limits to %v", namespace, quotaName, hard)
		if err := unstructured.SetNestedMap(existing.Object, hard, "spec", "hard"); err != nil {
			return err
		}
		if _, err := rqClient.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("patching resourcequota: %w", err)
		}
	} else {
		log.Infof("Injecting: creating ResourceQuota %s/%s with hard limits %v (did not exist)", namespace, quotaName, hard)
		rq := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ResourceQuota",
			"metadata": map[string]interface{}{
				"name":      quotaName,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{"hard": hard},
		}}
		if _, err := rqClient.Create(ctx, rq, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("creating resourcequota: %w", err)
		}
	}

	log.Infof("Triggering new pod admission attempts: rollout restart %s/%s", workloadGVR.Resource, target.GetName())
	if err := triggerRolloutRestart(ctx, cs, workloadGVR, namespace, target.GetName()); err != nil {
		log.Errorf("rollout restart failed (non-fatal, quota is still applied): %v", err)
	}

	itbench.Sleep(ctx, chaosDetails.ChaosDuration)

	if existed {
		log.Infof("Reverting: restoring ResourceQuota %s/%s hard limits to %v", namespace, quotaName, originalHard)
		current, err := rqClient.Get(ctx, quotaName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("re-fetching resourcequota for revert: %w", err)
		}
		if err := unstructured.SetNestedMap(current.Object, originalHard, "spec", "hard"); err != nil {
			return err
		}
		if _, err := rqClient.Update(ctx, current, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("reverting resourcequota: %w", err)
		}
	} else {
		log.Infof("Reverting: deleting ResourceQuota %s/%s (did not exist before injection)", namespace, quotaName)
		if err := rqClient.Delete(ctx, quotaName, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
			return fmt.Errorf("deleting resourcequota: %w", err)
		}
	}
	return nil
}

// triggerRolloutRestart mimics `kubectl rollout restart` by setting the standard
// restartedAt annotation on the pod template, forcing a new ReplicaSet/Pod generation.
// Reads-merges-writes the whole annotations map (rather than a JSON-patch "add" at a
// possibly-absent path) since spec.template.metadata.annotations may not exist yet.
func triggerRolloutRestart(ctx context.Context, cs clients.ClientSets, gvr schema.GroupVersionResource, namespace, name string) error {
	rc := cs.DynamicClient.Resource(gvr).Namespace(namespace)
	obj, err := rc.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	annotations, _, _ := unstructured.NestedStringMap(obj.Object, "spec", "template", "metadata", "annotations")
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339)
	if err := unstructured.SetNestedStringMap(obj.Object, annotations, "spec", "template", "metadata", "annotations"); err != nil {
		return err
	}
	_, err = rc.Update(ctx, obj, metav1.UpdateOptions{})
	return err
}
