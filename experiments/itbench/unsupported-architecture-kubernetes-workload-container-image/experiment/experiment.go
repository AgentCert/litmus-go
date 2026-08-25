// Package experiment implements unsupported-architecture-kubernetes-workload-container-image:
// sets the target container's image to one published for an unsupported architecture
// (INVALID_ARCH_IMAGE, e.g. an arm64-only image on an amd64 node), holds, restores.
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
	invalidArchImage := os.Getenv("INVALID_ARCH_IMAGE")
	return itbench.PatchContainerField(ctx, cs, chaosDetails, []string{"image"}, invalidArchImage)
}
