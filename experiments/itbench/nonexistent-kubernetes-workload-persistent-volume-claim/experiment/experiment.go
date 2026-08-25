// Package experiment implements nonexistent-kubernetes-workload-persistent-volume-claim:
// creates a PVC with an invalid StorageClass, additively mounts it onto the target
// container (volumeMount + pod-level volume, appended rather than replacing any existing
// entries), holds, then removes exactly the appended entries and deletes the PVC.
package experiment

import (
	"context"
	"fmt"
	"os"

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

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	target, idx, _, err := itbench.ResolveTargetWorkloadAndContainer(ctx, cs, chaosDetails)
	if err != nil {
		return err
	}
	namespace := target.GetNamespace()
	pvcName := fmt.Sprintf("%s-fault-pvc", target.GetName())
	mountPath := os.Getenv("PVC_MOUNT_PATH")
	storageClass := os.Getenv("INVALID_STORAGE_CLASS")

	pvc := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata":   map[string]interface{}{"name": pvcName, "namespace": namespace},
		"spec": map[string]interface{}{
			"accessModes":      []interface{}{"ReadWriteOnce"},
			"resources":        map[string]interface{}{"requests": map[string]interface{}{"storage": "25Mi"}},
			"storageClassName": storageClass,
		},
	}}
	pvcClient := cs.DynamicClient.Resource(itbench.GVRPersistentVolumeClaim).Namespace(namespace)
	log.Infof("Creating PVC %s with invalid StorageClass %s", pvcName, storageClass)
	if _, err := pvcClient.Create(ctx, pvc, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("creating pvc: %w", err)
	}

	log.Infof("Patching %s to mount fault PVC at %s", target.GetName(), mountPath)
	err = itbench.AppendAndRemoveWorkloadArrayItems(ctx, cs, chaosDetails, []itbench.ArrayAppendSpec{
		{
			Path: []string{"spec", "template", "spec", "containers", fmt.Sprintf("%d", idx), "volumeMounts"},
			Item: map[string]interface{}{"name": "fault-volume", "mountPath": mountPath},
		},
		{
			Path: []string{"spec", "template", "spec", "volumes"},
			Item: map[string]interface{}{"name": "fault-volume", "persistentVolumeClaim": map[string]interface{}{"claimName": pvcName}},
		},
	})
	if err != nil {
		return err
	}

	log.Infof("Reverting: deleting fault PVC %s", pvcName)
	if err := pvcClient.Delete(ctx, pvcName, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("deleting pvc: %w", err)
	}
	return nil
}
