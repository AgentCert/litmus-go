// Command itbench-experiment is the dispatcher binary for the itbench custom fault
// catalog (chaos-charts/faults/itbench/) -- mirrors bin/experiment/experiment.go's own
// -name-flag/switch pattern, kept separate so the itbench faults build into their own
// slim image rather than growing the upstream dispatcher.
package main

import (
	"context"
	"flag"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/log"

	httpAbort "github.com/litmuschaos/litmus-go/experiments/itbench/chaos-mesh-http-abort-replacement/experiment"
	httpBodyTamper "github.com/litmuschaos/litmus-go/experiments/itbench/chaos-mesh-http-body-tamper-replacement/experiment"
	podFailure "github.com/litmuschaos/litmus-go/experiments/itbench/chaos-mesh-pod-failure-replacement/experiment"
	cordonNode "github.com/litmuschaos/litmus-go/experiments/itbench/cordoned-kubernetes-worker-node/experiment"
	crashingInit "github.com/litmuschaos/litmus-go/experiments/itbench/crashing-kubernetes-workload-init-container/experiment"
	deletedService "github.com/litmuschaos/litmus-go/experiments/itbench/deleted-kubernetes-service/experiment"
	dnsPolicy "github.com/litmuschaos/litmus-go/experiments/itbench/failing-name-resolution-kubernetes-workload-dns-policy/experiment"
	hangingInit "github.com/litmuschaos/litmus-go/experiments/itbench/hanging-kubernetes-workload-init-container/experiment"
	ingressBlock "github.com/litmuschaos/litmus-go/experiments/itbench/ingress-port-blocking-network-policy/experiment"
	resourceQuota "github.com/litmuschaos/litmus-go/experiments/itbench/insufficient-kubernetes-resource-quota/experiment"
	insufficientResources "github.com/litmuschaos/litmus-go/experiments/itbench/insufficient-kubernetes-workload-container-resources/experiment"
	serviceSelector "github.com/litmuschaos/litmus-go/experiments/itbench/invalid-kubernetes-service-selector/experiment"
	invalidCommand "github.com/litmuschaos/litmus-go/experiments/itbench/invalid-kubernetes-workload-container-command/experiment"
	apiServerSurge "github.com/litmuschaos/litmus-go/experiments/itbench/kubernetes-api-server-request-surge/experiment"
	readinessProbe "github.com/litmuschaos/litmus-go/experiments/itbench/misconfigured-k8s-workload-container-readiness-probe/experiment"
	hpaMisconfig "github.com/litmuschaos/litmus-go/experiments/itbench/misconfigured-kubernetes-horizontal-pod-autoscaler/experiment"
	modifiedEnvVar "github.com/litmuschaos/litmus-go/experiments/itbench/modified-k8s-workload-container-environment-variable/experiment"
	targetPort "github.com/litmuschaos/litmus-go/experiments/itbench/modified-target-port-kubernetes-service/experiment"
	nonexistentImage "github.com/litmuschaos/litmus-go/experiments/itbench/nonexistent-kubernetes-workload-container-image/experiment"
	nonexistentNode "github.com/litmuschaos/litmus-go/experiments/itbench/nonexistent-kubernetes-workload-node/experiment"
	nonexistentPVC "github.com/litmuschaos/litmus-go/experiments/itbench/nonexistent-kubernetes-workload-persistent-volume-claim/experiment"
	featureFlag "github.com/litmuschaos/litmus-go/experiments/itbench/opentelemetry-demo-feature-flag/experiment"
	priorityPreemption "github.com/litmuschaos/litmus-go/experiments/itbench/priority-kubernetes-workload-priority-preemption/experiment"
	scaledToZero "github.com/litmuschaos/litmus-go/experiments/itbench/scaled-to-zero-kubernetes-workload/experiment"
	unassignedLimits "github.com/litmuschaos/litmus-go/experiments/itbench/unassigned-k8s-workload-container-resource-limits/experiment"
	uninstallAgent "github.com/litmuschaos/litmus-go/experiments/itbench/uninstall-agent/experiment"
	uninstallApplication "github.com/litmuschaos/litmus-go/experiments/itbench/uninstall-application/experiment"
	antiAffinity "github.com/litmuschaos/litmus-go/experiments/itbench/unschedulable-k8s-workload-pod-anti-affinity-rule/experiment"
	unsupportedArchImage "github.com/litmuschaos/litmus-go/experiments/itbench/unsupported-architecture-k8s-workload-container-image/experiment"
	valkeyPassword "github.com/litmuschaos/litmus-go/experiments/itbench/valkey-workload-changed-password/experiment"
	valkeyOOM "github.com/litmuschaos/litmus-go/experiments/itbench/valkey-workload-out-of-memory/experiment"
)

func main() {
	cs := clients.ClientSets{}

	// experimentName must be registered before GenerateClientSetFromKubeConfig(), which
	// itself registers -kubeconfig and calls flag.Parse() -- matching bin/experiment's
	// own ordering (see pkg/clients/clientset.go's getKubeConfig()).
	freezeID := flag.String("freeze-container-id", "", "freeze an exact container cgroup from a hostPID helper")
	freezeSeconds := flag.Int("freeze-seconds", 0, "independent thaw deadline")
	apiLoad := flag.Bool("api-load", false, "run the bounded API load helper")
	experimentName := flag.String("name", "", "name of the itbench chaos experiment")

	if err := cs.GenerateClientSetFromKubeConfig(); err != nil {
		log.Fatalf("Unable to get the kubeconfig, err: %v", err)
	}

	// Signal-aware: every fault holds on this ctx and reverts on itbench.RevertContext(),
	// so a workflow stop ends the run without leaving the target mutated.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *freezeID != "" {
		if err := itbench.RunFreeze(ctx, *freezeID, time.Duration(*freezeSeconds)*time.Second); err != nil {
			log.Fatalf("container freeze failed: %v", err)
		}
		return
	}
	if *apiLoad {
		rps, err := strconv.Atoi(os.Getenv("RPS"))
		if err != nil {
			log.Fatalf("invalid RPS: %v", err)
		}
		if err := itbench.RunAPILoad(ctx, cs.KubeConfig, os.Getenv("LOAD_NAMESPACE"), rps); err != nil {
			log.Fatalf("API load helper failed: %v", err)
		}
		return
	}

	switch *experimentName {
	case "scaled-to-zero-kubernetes-workload":
		scaledToZero.Run(ctx, cs)
	case "invalid-kubernetes-workload-container-command":
		invalidCommand.Run(ctx, cs)
	case "modified-k8s-workload-container-environment-variable":
		modifiedEnvVar.Run(ctx, cs)
	case "nonexistent-kubernetes-workload-container-image":
		nonexistentImage.Run(ctx, cs)
	case "unsupported-architecture-k8s-workload-container-image":
		unsupportedArchImage.Run(ctx, cs)
	case "insufficient-kubernetes-workload-container-resources":
		insufficientResources.Run(ctx, cs)
	case "misconfigured-k8s-workload-container-readiness-probe":
		readinessProbe.Run(ctx, cs)
	case "unassigned-k8s-workload-container-resource-limits":
		unassignedLimits.Run(ctx, cs)
	case "valkey-workload-out-of-memory":
		valkeyOOM.Run(ctx, cs)
	case "failing-name-resolution-kubernetes-workload-dns-policy":
		dnsPolicy.Run(ctx, cs)
	case "nonexistent-kubernetes-workload-node":
		nonexistentNode.Run(ctx, cs)
	case "unschedulable-k8s-workload-pod-anti-affinity-rule":
		antiAffinity.Run(ctx, cs)
	case "invalid-kubernetes-service-selector":
		serviceSelector.Run(ctx, cs)
	case "modified-target-port-kubernetes-service":
		targetPort.Run(ctx, cs)
	case "misconfigured-kubernetes-horizontal-pod-autoscaler":
		hpaMisconfig.Run(ctx, cs)
	case "insufficient-kubernetes-resource-quota":
		resourceQuota.Run(ctx, cs)
	case "crashing-kubernetes-workload-init-container":
		crashingInit.Run(ctx, cs)
	case "hanging-kubernetes-workload-init-container":
		hangingInit.Run(ctx, cs)
	case "cordoned-kubernetes-worker-node":
		cordonNode.Run(ctx, cs)
	case "deleted-kubernetes-service":
		deletedService.Run(ctx, cs)
	case "valkey-workload-changed-password":
		valkeyPassword.Run(ctx, cs)
	case "chaos-mesh-http-abort-replacement":
		httpAbort.Run(ctx, cs)
	case "ingress-port-blocking-network-policy":
		ingressBlock.Run(ctx, cs)
	case "nonexistent-kubernetes-workload-persistent-volume-claim":
		nonexistentPVC.Run(ctx, cs)
	case "opentelemetry-demo-feature-flag":
		featureFlag.Run(ctx, cs)
	case "chaos-mesh-http-body-tamper-replacement":
		httpBodyTamper.Run(ctx, cs)
	case "kubernetes-api-server-request-surge":
		apiServerSurge.Run(ctx, cs)
	case "priority-kubernetes-workload-priority-preemption":
		priorityPreemption.Run(ctx, cs)
	case "chaos-mesh-pod-failure-replacement":
		podFailure.Run(ctx, cs)
	case "uninstall-agent":
		uninstallAgent.Run(ctx, cs)
	case "uninstall-application":
		uninstallApplication.Run(ctx, cs)
	default:
		log.Fatalf("Unsupported itbench chaos experiment: %v", *experimentName)
	}
}
