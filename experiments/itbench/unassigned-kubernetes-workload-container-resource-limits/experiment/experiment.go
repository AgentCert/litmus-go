// Package experiment implements unassigned-kubernetes-workload-container-resource-limits:
// removes the target container's resources.limits entirely (no ceiling on cpu/memory
// consumption), holds, restores.
package experiment

import (
	"context"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/types"
)

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	return itbench.RemoveContainerField(ctx, cs, chaosDetails, []string{"resources", "limits"})
}
