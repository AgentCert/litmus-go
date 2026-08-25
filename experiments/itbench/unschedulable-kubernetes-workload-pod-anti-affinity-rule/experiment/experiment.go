// Package experiment implements unschedulable-kubernetes-workload-pod-anti-affinity-rule:
// sets an unsatisfiable required podAntiAffinity rule against the workload's own label
// (or ANTI_AFFINITY_LABEL if set) on TOPOLOGY_KEY, holds, restores.
package experiment

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/types"
)

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	affinityLabel := os.Getenv("ANTI_AFFINITY_LABEL")
	if affinityLabel == "" && len(chaosDetails.AppDetail) > 0 && len(chaosDetails.AppDetail[0].Labels) > 0 {
		affinityLabel = chaosDetails.AppDetail[0].Labels[0]
	}
	kv := strings.SplitN(affinityLabel, "=", 2)
	if len(kv) != 2 {
		return fmt.Errorf("ANTI_AFFINITY_LABEL/target label %q is not in key=value form", affinityLabel)
	}
	topologyKey := os.Getenv("TOPOLOGY_KEY")

	podAntiAffinity := map[string]interface{}{
		"requiredDuringSchedulingIgnoredDuringExecution": []interface{}{
			map[string]interface{}{
				"labelSelector": map[string]interface{}{
					"matchExpressions": []interface{}{
						map[string]interface{}{
							"key":      kv[0],
							"operator": "In",
							"values":   []string{kv[1]},
						},
					},
				},
				"topologyKey": topologyKey,
			},
		},
	}
	// Patched at the "affinity" level (not the narrower "affinity.podAntiAffinity") and
	// merged rather than replaced wholesale: "affinity" itself is not always present on a
	// pod spec (unlike e.g. "resources", it's not a default-initialized struct), so a
	// JSON-patch "add" at the deeper path would fail if the parent key is entirely
	// missing. Merging at the "affinity" level also preserves any pre-existing
	// nodeAffinity/podAffinity instead of clobbering them.
	return itbench.MergeWorkloadMapField(ctx, cs, chaosDetails, []string{"spec", "template", "spec", "affinity"}, "podAntiAffinity", podAntiAffinity)
}
