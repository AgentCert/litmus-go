// Package experiment implements modified-target-port-kubernetes-service: replaces the
// target Service's first port's targetPort with an unreachable value
// (BROKEN_TARGET_PORT), holds, restores. Assumes a single-port Service at index 0 (same
// assumption the original script made, verified only against otel-demo's single-port
// components).
package experiment

import (
	"context"
	"os"
	"strconv"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/types"
)

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	port, err := strconv.Atoi(os.Getenv("BROKEN_TARGET_PORT"))
	if err != nil {
		return err
	}
	return itbench.PatchField(ctx, cs, itbench.GVRServices, chaosDetails, []string{"spec", "ports", "0", "targetPort"}, "/spec/ports/0/targetPort", int64(port))
}
