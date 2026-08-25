// Package experiment implements misconfigured-kubernetes-workload-container-readiness-probe:
// sets the target container's readinessProbe to an unreachable httpGet (PROBE_PATH,
// PROBE_PORT), holds, restores.
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
	port, err := strconv.Atoi(os.Getenv("PROBE_PORT"))
	if err != nil {
		return err
	}
	probe := map[string]interface{}{
		"failureThreshold": int64(3),
		"httpGet": map[string]interface{}{
			"path":   os.Getenv("PROBE_PATH"),
			"port":   int64(port),
			"scheme": "HTTP",
		},
		"periodSeconds":    int64(5),
		"successThreshold": int64(1),
		"timeoutSeconds":   int64(3),
	}
	return itbench.PatchContainerField(ctx, cs, chaosDetails, []string{"readinessProbe"}, probe)
}
