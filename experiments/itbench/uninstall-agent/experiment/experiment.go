// Package experiment implements the uninstall-agent ITBench teardown step: removes the
// AI agent's Helm release from the cluster after a run. It is authored as a real
// litmus-go SDK experiment (via pkg/itbench/common.Run) rather than a raw helm-wrapper
// binary so it reports a proper ChaosResult -- SOT/EOT, verdict Pass on success -- instead
// of the operator/UI always showing Fail/0% regardless of outcome.
//
// It is release-scoped, not namespace-scoped: the agent frequently shares the target
// application's namespace, so this deletes only the objects Helm recorded for release
// FOLDER (plus Helm's own release-state Secrets). Full namespace teardown is the
// uninstall-application step's job.
//
// Env (from ChaosEngine.spec.experiments[].spec.components.env):
//
//	FOLDER    -- the agent Helm release name (== AgentHub chart folder by convention)
//	NAMESPACE -- the namespace the agent release was installed into
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
	log.Infof("[uninstall-agent] release=%q namespace=%q", release, namespace)
	return itbench.UninstallHelmRelease(ctx, cs.KubeClient, cs.DynamicClient, release, namespace)
}
