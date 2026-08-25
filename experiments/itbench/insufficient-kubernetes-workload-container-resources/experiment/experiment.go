// Package experiment implements insufficient-kubernetes-workload-container-resources:
// patches the target container's whole resources block to insufficient values
// (FAULT_CPU_REQUEST/LIMIT, FAULT_MEMORY_REQUEST/LIMIT), holds, restores.
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
	resources := map[string]interface{}{
		"limits": map[string]interface{}{
			"cpu":    os.Getenv("FAULT_CPU_LIMIT"),
			"memory": os.Getenv("FAULT_MEMORY_LIMIT"),
		},
		"requests": map[string]interface{}{
			"cpu":    os.Getenv("FAULT_CPU_REQUEST"),
			"memory": os.Getenv("FAULT_MEMORY_REQUEST"),
		},
	}
	return itbench.PatchContainerField(ctx, cs, chaosDetails, []string{"resources"}, resources)
}
