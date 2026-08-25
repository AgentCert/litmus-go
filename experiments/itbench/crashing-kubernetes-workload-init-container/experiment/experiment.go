// Package experiment implements crashing-kubernetes-workload-init-container: appends an
// init container that exits 1 (INIT_CONTAINER_NAME/IMAGE, BAD_SCRIPT) to the target's pod
// template, holds, removes exactly the appended entry.
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
	initContainer := map[string]interface{}{
		"name":    os.Getenv("INIT_CONTAINER_NAME"),
		"image":   os.Getenv("INIT_CONTAINER_IMAGE"),
		"command": []string{"/bin/sh", "-c", os.Getenv("BAD_SCRIPT")},
	}
	return itbench.AppendAndRemoveInitContainer(ctx, cs, chaosDetails, initContainer)
}
