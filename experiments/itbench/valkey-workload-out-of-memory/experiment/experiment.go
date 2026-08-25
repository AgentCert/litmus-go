// Package experiment implements valkey-workload-out-of-memory: clamps the target
// container's memory limit and request to MEMORY_LIMIT to force OOMKilled/
// CrashLoopBackOff, holds, restores. Merges into resources.limits/requests rather than
// replacing them wholesale, so any pre-existing cpu limit/request survives revert intact.
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
	memLimit := os.Getenv("MEMORY_LIMIT")
	return itbench.MergeContainerMapFields(ctx, cs, chaosDetails, []itbench.MapMergeSpec{
		{Path: []string{"resources", "limits"}, Key: "memory", Value: memLimit},
		{Path: []string{"resources", "requests"}, Key: "memory", Value: memLimit},
	})
}
