// Package experiment implements hanging-kubernetes-workload-init-container: appends an
// init container that never exits (HANG_CONTAINER_NAME/IMAGE, HANG_COMMAND, e.g. "sleep
// infinity") to the target's pod template, holds, removes exactly the appended entry.
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
		"name":    os.Getenv("HANG_CONTAINER_NAME"),
		"image":   os.Getenv("HANG_CONTAINER_IMAGE"),
		"command": []string{"/bin/sh"},
		"args":    []string{"-c", os.Getenv("HANG_COMMAND")},
	}
	return itbench.AppendAndRemoveInitContainer(ctx, cs, chaosDetails, initContainer)
}
