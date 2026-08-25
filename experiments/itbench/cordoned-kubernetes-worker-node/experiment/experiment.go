// Package experiment implements cordoned-kubernetes-worker-node: finds the node hosting
// the target pod(s) and cordons it (spec.unschedulable=true), holds, then uncordons it --
// unless it was already cordoned before injection (a pre-existing maintenance state),
// in which case it's left as-is rather than blindly uncordoned.
package experiment

import (
	"context"
	"fmt"

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
	pods, err := itbench.ResolveTargets(ctx, cs, itbench.GVRPods, chaosDetails)
	if err != nil {
		return err
	}
	nodeName, _, _ := unstructured.NestedString(pods[0].Object, "spec", "nodeName")
	if nodeName == "" {
		return fmt.Errorf("target pod %s has no assigned node (not yet scheduled)", pods[0].GetName())
	}
	log.Infof("Target node=%s", nodeName)

	nodeClient := cs.DynamicClient.Resource(itbench.GVRNodes)
	node, err := nodeClient.Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	origUnschedulable, _, _ := unstructured.NestedBool(node.Object, "spec", "unschedulable")

	log.Infof("Injecting: cordoning node %s", nodeName)
	if err := setUnschedulable(ctx, cs, nodeName, true); err != nil {
		return fmt.Errorf("cordoning node: %w", err)
	}

	itbench.Sleep(ctx, chaosDetails.ChaosDuration)

	if origUnschedulable {
		log.Infof("Node %s was already cordoned before injection; leaving it cordoned", nodeName)
		return nil
	}
	log.Infof("Reverting: uncordoning node %s", nodeName)
	if err := setUnschedulable(ctx, cs, nodeName, false); err != nil {
		return fmt.Errorf("uncordoning node: %w", err)
	}
	return nil
}

func setUnschedulable(ctx context.Context, cs clients.ClientSets, nodeName string, value bool) error {
	nodeClient := cs.DynamicClient.Resource(itbench.GVRNodes)
	node, err := nodeClient.Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := unstructured.SetNestedField(node.Object, value, "spec", "unschedulable"); err != nil {
		return err
	}
	_, err = nodeClient.Update(ctx, node, metav1.UpdateOptions{})
	return err
}
