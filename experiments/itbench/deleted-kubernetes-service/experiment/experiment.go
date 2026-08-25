// Package experiment implements deleted-kubernetes-service: captures the target
// Service's manifest, deletes it, holds, then recreates it from the captured manifest
// (stripped of server-generated/immutable fields so it can be re-applied as a brand-new
// object; clusterIP/clusterIPs dropped so Kubernetes allocates a fresh address rather
// than trying to reclaim one already released back to the pool).
package experiment

import (
	"context"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/log"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

// appkind is CRD-validated to a fixed enum and rejects "service", so this fault always
// hardcodes the real target kind, trusting TARGETS only for namespace/label.
func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	target, err := itbench.ResolveTarget(ctx, cs, itbench.GVRServices, chaosDetails)
	if err != nil {
		return err
	}
	name, namespace := target.GetName(), target.GetNamespace()
	log.Infof("Target resolved: services/%s (ns=%s)", name, namespace)

	backup := target.DeepCopy()
	unstructured.RemoveNestedField(backup.Object, "status")
	unstructured.RemoveNestedField(backup.Object, "metadata", "resourceVersion")
	unstructured.RemoveNestedField(backup.Object, "metadata", "uid")
	unstructured.RemoveNestedField(backup.Object, "metadata", "selfLink")
	unstructured.RemoveNestedField(backup.Object, "metadata", "generation")
	unstructured.RemoveNestedField(backup.Object, "metadata", "creationTimestamp")
	unstructured.RemoveNestedField(backup.Object, "metadata", "managedFields")
	unstructured.RemoveNestedField(backup.Object, "metadata", "annotations", "kubectl.kubernetes.io/last-applied-configuration")
	unstructured.RemoveNestedField(backup.Object, "spec", "clusterIP")
	unstructured.RemoveNestedField(backup.Object, "spec", "clusterIPs")

	svcClient := cs.DynamicClient.Resource(itbench.GVRServices).Namespace(namespace)

	log.Infof("Injecting: deleting Service %s", name)
	if err := svcClient.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		return err
	}

	itbench.Sleep(ctx, chaosDetails.ChaosDuration)

	log.Infof("Reverting: recreating Service %s from captured manifest", name)
	if _, err := svcClient.Create(ctx, backup, metav1.CreateOptions{}); err != nil {
		return err
	}
	return nil
}
