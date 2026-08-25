// Package experiment implements the scaled-to-zero-kubernetes-workload itbench fault:
// scales the target Deployment/StatefulSet to 0 replicas, holds for ChaosDuration, then
// restores the original replica count.
package experiment

import (
	"context"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/types"
)

// Run executes the experiment.
func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	return itbench.PatchWorkloadField(ctx, cs, chaosDetails, []string{"spec", "replicas"}, "/spec/replicas", int64(0))
}
