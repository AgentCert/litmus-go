// Package experiment implements chaos-mesh-http-abort-replacement: creates a NetworkPolicy
// that denies ingress on TARGET_PORT/TARGET_PROTOCOL to the target's pods while
// re-allowing every other discovered container port (approximates Chaos Mesh HTTPChaos
// abort=true on a specific port, rather than a blanket deny-all), holds, deletes it.
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
	targetPort := os.Getenv("TARGET_PORT")

	pods, err := itbench.ResolveTargets(ctx, cs, itbench.GVRPods, chaosDetails)
	if err != nil {
		return err
	}
	otherPorts := discoverOtherContainerPorts(pods[0].Object, targetPort)
	log.Infof("Discovered other container ports to keep open: %v (blocking %s)", otherPorts, targetPort)

	netpolName := os.Getenv("NETPOL_NAME")
	if netpolName == "" {
		netpolName = fmt.Sprintf("itbench-http-abort-%s", strings.NewReplacer("/", "-", ".", "-").Replace(kv[1]))
	}

	var ingress []interface{}
	if len(otherPorts) == 0 {
		ingress = []interface{}{}
	} else {
		var ports []interface{}
		for _, p := range otherPorts {
			ports = append(ports, map[string]interface{}{"protocol": "TCP", "port": p})
		}
		ingress = []interface{}{map[string]interface{}{"ports": ports}}
	}

	netpol := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "networking.k8s.io/v1",
		"kind":       "NetworkPolicy",
		"metadata": map[string]interface{}{
			"name":      netpolName,
			"namespace": namespace,
			"labels":    map[string]interface{}{"litmuschaos.io/fault": "chaos-mesh-http-abort-replacement"},
		},
		"spec": map[string]interface{}{
			"podSelector": map[string]interface{}{
				"matchLabels": map[string]interface{}{kv[0]: kv[1]},
			},
			"policyTypes": []interface{}{"Ingress"},
			"ingress":     ingress,
		},
	}}

	netpolClient := cs.DynamicClient.Resource(itbench.GVRNetworkPolicies).Namespace(namespace)
	log.Infof("Injecting: NetworkPolicy %s denies ingress on port %s to %s=%s in %s", netpolName, targetPort, kv[0], kv[1], namespace)
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

func discoverOtherContainerPorts(pod map[string]interface{}, targetPort string) []int64 {
	var result []int64
	containers, _, _ := unstructured.NestedSlice(pod, "spec", "containers")
	for _, c := range containers {
		cm, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		ports, _, _ := unstructured.NestedSlice(cm, "ports")
		for _, p := range ports {
			pm, ok := p.(map[string]interface{})
			if !ok {
				continue
			}
			cp, ok := pm["containerPort"].(int64)
			if !ok {
				continue
			}
			if fmt.Sprintf("%d", cp) != targetPort {
				result = append(result, cp)
			}
		}
	}
	return result
}
