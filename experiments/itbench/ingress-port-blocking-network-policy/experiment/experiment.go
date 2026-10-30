// Package experiment implements ingress-port-blocking-network-policy: creates a deny-all-
// ingress NetworkPolicy selecting the target's own label, holds, deletes it.
package experiment

import (
	"context"
	"fmt"
	"os"
	"time"

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

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) (retErr error) {
	selector, err := itbench.TargetPodSelector(ctx, cs, chaosDetails)
	if err != nil {
		return err
	}
	namespace := chaosDetails.AppDetail[0].Namespace

	netpolName := os.Getenv("NETPOL_NAME")
	if netpolName == "" {
		netpolName = fmt.Sprintf("itbench-ingress-block-%s", "target")
	}

	// Each run owns only its unique policy; never delete an existing user's policy.
	netpolName = fmt.Sprintf("%.35s-%d", netpolName, time.Now().UnixNano())

	netpol := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "networking.k8s.io/v1",
		"kind":       "NetworkPolicy",
		"metadata": map[string]interface{}{
			"name":      netpolName,
			"namespace": namespace,
			"labels":    map[string]interface{}{"litmuschaos.io/fault": "ingress-port-blocking-network-policy"},
		},
		"spec": map[string]interface{}{
			"podSelector": selector,
			"policyTypes": []interface{}{"Ingress"},
			"ingress":     []interface{}{},
		},
	}}

	netpolClient := cs.DynamicClient.Resource(itbench.GVRNetworkPolicies).Namespace(namespace)

	log.Infof("Injecting NetworkPolicy %s in %s", netpolName, namespace)
	if _, err := netpolClient.Create(ctx, netpol, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("creating networkpolicy: %w", err)
	}
	itbench.TrackTemporaryResource(ctx, itbench.GVRNetworkPolicies, namespace, netpolName)
	defer func() {
		revertCtx, cancel := itbench.RevertContext()
		defer cancel()
		log.Infof("Reverting: deleting NetworkPolicy %s", netpolName)
		if err := netpolClient.Delete(revertCtx, netpolName, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
			log.Errorf("failed to delete NetworkPolicy %s -- the target stays blocked: %v", netpolName, err)
			if retErr == nil {
				retErr = fmt.Errorf("deleting networkpolicy: %w", err)
			}
		}
	}()

	itbench.HoldChaos(ctx, chaosDetails)
	return nil
}
