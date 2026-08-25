// Package experiment implements misconfigured-kubernetes-horizontal-pod-autoscaler:
// replaces the target HPA's spec.metrics with unrealistically low CPU/memory utilization
// targets (CPU_UTILIZATION_PERCENT, MEMORY_UTILIZATION_PERCENT), holds, restores.
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

func resourceMetric(name string, pct int64) map[string]interface{} {
	return map[string]interface{}{
		"type": "Resource",
		"resource": map[string]interface{}{
			"name": name,
			"target": map[string]interface{}{
				"type":               "Utilization",
				"averageUtilization": pct,
			},
		},
	}
}

// appkind is CRD-validated to a fixed enum (deployment|statefulset|daemonset|
// deploymentconfig|rollout|job) -- "horizontalpodautoscaler" is rejected at admission
// time, so this fault always hardcodes the real target kind, trusting TARGETS only for
// namespace/label.
func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	cpuPct, err := strconv.ParseInt(os.Getenv("CPU_UTILIZATION_PERCENT"), 10, 64)
	if err != nil {
		return err
	}
	memPct, err := strconv.ParseInt(os.Getenv("MEMORY_UTILIZATION_PERCENT"), 10, 64)
	if err != nil {
		return err
	}
	metrics := []interface{}{resourceMetric("cpu", cpuPct), resourceMetric("memory", memPct)}
	return itbench.PatchField(ctx, cs, itbench.GVRHPA, chaosDetails, []string{"spec", "metrics"}, "/spec/metrics", metrics)
}
