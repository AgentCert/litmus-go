// Package experiment implements chaos-mesh-http-abort-replacement: creates a NetworkPolicy
// that denies ingress on TARGET_PORT/TARGET_PROTOCOL to the target's pods while
// re-allowing every other discovered container port (approximates Chaos Mesh HTTPChaos
// abort=true on a specific port, rather than a blanket deny-all), holds, deletes it.
package experiment

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
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

	// An empty/garbage TARGET_PORT matches no containerPort, so every port ends up in the
	// "keep open" allow-list below and the policy blocks nothing while still reporting success.
	targetPort := strings.TrimSpace(os.Getenv("TARGET_PORT"))
	if n, err := strconv.Atoi(targetPort); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("TARGET_PORT must be a port number in 1-65535, got %q", targetPort)
	}
	protocol := strings.ToUpper(strings.TrimSpace(os.Getenv("TARGET_PROTOCOL")))
	if protocol == "" {
		protocol = "TCP"
	}
	switch protocol {
	case "TCP", "UDP", "SCTP":
	default:
		return fmt.Errorf("TARGET_PROTOCOL must be TCP, UDP or SCTP, got %q", protocol)
	}

	pods, err := itbench.ResolveTargets(ctx, cs, itbench.GVRPods, chaosDetails)
	if err != nil {
		return err
	}
	otherPorts := discoverOtherContainerPorts(pods[0].Object, targetPort)
	log.Infof("Discovered other container ports to keep open: %v (blocking %s/%s)", otherPorts, protocol, targetPort)

	netpolName := os.Getenv("NETPOL_NAME")
	if netpolName == "" {
		netpolName = fmt.Sprintf("itbench-http-abort-%s", "target")
	}

	// Each run owns only its unique policy; never delete an existing user's policy.
	netpolName = fmt.Sprintf("%.35s-%d", netpolName, time.Now().UnixNano())

	var ingress []interface{}
	if len(otherPorts) == 0 {
		ingress = []interface{}{}
	} else {
		var ports []interface{}
		for _, p := range otherPorts {
			ports = append(ports, map[string]interface{}{"protocol": protocol, "port": p})
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
			"podSelector": selector,
			"policyTypes": []interface{}{"Ingress"},
			"ingress":     ingress,
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
