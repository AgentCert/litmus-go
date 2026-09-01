// Package experiment implements the uninstall-application ITBench teardown step: tears
// the target application down completely after a run. Authored as a real litmus-go SDK
// experiment (via pkg/itbench/common.Run) so it reports a proper ChaosResult (SOT/EOT,
// verdict Pass on success) instead of the operator/UI always showing Fail/0%.
//
// Teardown is two-stage so it is complete regardless of what actually ran or succeeded
// during the experiment:
//  1. Remove the app's Helm release (release-scoped: also catches the chart's
//     cluster-scoped objects -- ClusterRole/ClusterRoleBinding -- which a namespace
//     delete alone would leave behind).
//  2. Delete the namespace, which cascades to every remaining namespaced object:
//     fault leftovers (NetworkPolicies, patched workloads), orphaned PVCs, leftover
//     ChaosEngine/ChaosResult CRs, a half-installed agent release, etc.
//
// Env (from ChaosEngine.spec.experiments[].spec.components.env):
//
//	FOLDER    -- the app Helm release name (== AppHub chart folder / namespace by convention)
//	NAMESPACE -- the namespace to tear down
package experiment

import (
	"context"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/log"
	"github.com/litmuschaos/litmus-go/pkg/types"
)

// Run executes the experiment.
func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	release := types.Getenv("FOLDER", "")
	namespace := types.Getenv("NAMESPACE", "")
	log.Infof("[uninstall-application] release=%q namespace=%q", release, namespace)

	if release != "" && namespace != "" {
		if err := itbench.UninstallHelmRelease(ctx, cs.KubeClient, cs.DynamicClient, release, namespace); err != nil {
			// Non-fatal: the namespace delete below is the real catch-all. Log and continue.
			log.Warnf("[uninstall-application] helm-release sweep reported: %v (continuing to namespace delete)", err)
		}
	}
	return itbench.DeleteNamespace(ctx, cs.KubeClient, namespace)
}
