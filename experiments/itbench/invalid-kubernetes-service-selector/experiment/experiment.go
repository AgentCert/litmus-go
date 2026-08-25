// Package experiment implements invalid-kubernetes-service-selector: merges an invalid
// app.kubernetes.io/name key into the target Service's selector (RANDOM_SUFFIX), holds,
// restores the exact original selector.
package experiment

import (
	"context"
	"fmt"
	"os"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/types"
)

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

// appkind is CRD-validated to a fixed enum (deployment|statefulset|daemonset|
// deploymentconfig|rollout|job) -- "service" is rejected at admission time, so the
// ChaosEngine submission uses a dummy valid appkind and this fault always hardcodes the
// real target kind, trusting TARGETS only for namespace/label (same convention every
// itbench fault script used).
func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	suffix := os.Getenv("RANDOM_SUFFIX")
	if suffix == "" {
		suffix = "1"
	}
	invalidValue := fmt.Sprintf("invalid-workload-%s", suffix)
	return itbench.MergeField(ctx, cs, itbench.GVRServices, chaosDetails, []string{"spec", "selector"}, "app.kubernetes.io/name", invalidValue)
}
