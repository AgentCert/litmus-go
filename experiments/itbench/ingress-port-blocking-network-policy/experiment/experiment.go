// Package experiment implements ingress-port-blocking-network-policy: creates a deny-all-
// ingress NetworkPolicy selecting the target's own label, holds, deletes it.
package experiment

import (
	"context"
	"fmt"
	"os"
	"strings"

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
	if len(chaosDetails.AppDetail) == 0 || len(chaosDetails.AppDetail[0].Labels) == 0 {
		return fmt.Errorf("no target label resolved: TARGETS env var was empty/unset or had no label selector")
	}
	namespace := chaosDetails.AppDetail[0].Namespace
	label := chaosDetails.AppDetail[0].Labels[0]
	kv := strings.SplitN(label, "=", 2)
	if len(kv) != 2 {
		return fmt.Errorf("target label %q is not in key=value form", label)
	}

	netpolName := os.Getenv("NETPOL_NAME")
	if netpolName == "" {
		netpolName = fmt.Sprintf("itbench-ingress-block-%s", strings.NewReplacer("/", "-", ".", "-").Replace(kv[1]))
	}

	netpol := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "networking.k8s.io/v1",
		"kind":       "NetworkPolicy",
		"metadata": map[string]interface{}{
			"name":      netpolName,
			"namespace": namespace,
			"labels":    map[string]interface{}{"litmuschaos.io/fault": "ingress-port-blocking-network-policy"},
		},
		"spec": map[string]interface{}{
			"podSelector": map[string]interface{}{
				"matchLabels": map[string]interface{}{kv[0]: kv[1]},
			},
			"policyTypes": []interface{}{"Ingress"},
			"ingress":     []interface{}{},
		},
	}}

	netpolClient := cs.DynamicClient.Resource(itbench.GVRNetworkPolicies).Namespace(namespace)
	log.Infof("Injecting: creating deny-all-ingress NetworkPolicy %s selecting %s=%s in %s", netpolName, kv[0], kv[1], namespace)
	if _, err := netpolClient.Create(ctx, netpol, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("creating networkpolicy: %w", err)
	}

	itbench.Sleep(ctx, chaosDetails.ChaosDuration)

	log.Infof("Reverting: deleting NetworkPolicy %s", netpolName)
	if err := netpolClient.Delete(ctx, netpolName, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("deleting networkpolicy: %w", err)
	}
	return nil
}
