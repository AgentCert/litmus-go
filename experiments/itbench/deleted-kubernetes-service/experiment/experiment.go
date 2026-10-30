// Package experiment implements deleted-kubernetes-service: captures the target
// Service's manifest, deletes it, holds, then recreates it from the captured manifest
// (stripped of server-generated/immutable fields so it can be re-applied as a brand-new
// object; clusterIP/clusterIPs dropped so Kubernetes allocates a fresh address rather
// than trying to reclaim one already released back to the pool).
package experiment

import (
	"context"
	"errors"
	"fmt"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/log"
	"github.com/litmuschaos/litmus-go/pkg/types"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

// appkind is CRD-validated to a fixed enum and rejects "service", so this fault always
// hardcodes the real target kind, trusting TARGETS only for namespace/label.
func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) (retErr error) {
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
	clusterIP, _, _ := unstructured.NestedString(backup.Object, "spec", "clusterIP")
	if clusterIP != "None" {
		unstructured.RemoveNestedField(backup.Object, "spec", "clusterIP")
		unstructured.RemoveNestedField(backup.Object, "spec", "clusterIPs")
	}

	svcClient := cs.DynamicClient.Resource(itbench.GVRServices).Namespace(namespace)

	log.Infof("Injecting: deleting Service %s", name)
	if err := svcClient.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		return err
	}
	// Deferred: a failure or an abort between here and the hold's end would otherwise
	// delete the Service permanently, since nothing else recreates it.
	defer func() {
		revertCtx, cancel := itbench.RevertContext()
		defer cancel()
		log.Infof("Reverting: recreating Service %s from captured manifest", name)
		err := itbench.WaitForCheck(revertCtx, itbench.RevertTimeout, "recreating Service "+name, func(c context.Context) error {
			_, err := svcClient.Create(c, backup, metav1.CreateOptions{})
			if !k8serrors.IsAlreadyExists(err) {
				return err
			}
			existing, err := svcClient.Get(c, name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if existing.GetDeletionTimestamp() != nil {
				return fmt.Errorf("original Service is still terminating")
			}
			return nil // The shared post-revert check validates the full Service spec.
		})
		if err != nil {
			retErr = errors.Join(retErr, err)
		}
	}()

	itbench.HoldChaos(ctx, chaosDetails)
	return nil
}
