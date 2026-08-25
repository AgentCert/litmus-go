// Package experiment implements nonexistent-kubernetes-workload-container-image: sets the
// target container's image to a tag that doesn't exist (INVALID_IMAGE), holds, restores.
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
	invalidImage := os.Getenv("INVALID_IMAGE")
	return itbench.PatchContainerField(ctx, cs, chaosDetails, []string{"image"}, invalidImage)
}
