// Package experiment implements nonexistent-kubernetes-workload-node: sets the target pod
// template's nodeSelector to a hostname that doesn't exist (NONEXISTENT_NODE_NAME),
// making the pod unschedulable/Pending, holds, restores.
package experiment

import (
	"context"
	"os"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/types"
)

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	nodeName := os.Getenv("NONEXISTENT_NODE_NAME")
	nodeSelector := map[string]interface{}{"kubernetes.io/hostname": nodeName}
	return itbench.PatchWorkloadField(ctx, cs, chaosDetails, []string{"spec", "template", "spec", "nodeSelector"}, "/spec/template/spec/nodeSelector", nodeSelector)
}
