// Package experiment implements chaos-mesh-pod-failure-replacement: for each Running pod
// matching TARGETS, attaches an ephemeral debug container that shares the target
// container's (TARGET_CONTAINER) PID namespace, then sends SIGSTOP to PID 1 from inside
// it, holds, then SIGCONT -- all as one command inside the ephemeral container itself, so
// the freeze self-reverts even if this experiment job is interrupted mid-run. Reproduces
// the *effect* of Chaos Mesh's PodChaos action=pod-failure (container frozen in place,
// never deleted/restarted) using LitmusChaos-native primitives, since this deployment runs
// no service mesh / Chaos Mesh transparent proxy.
//
// Ephemeral containers cannot be individually removed once added (a Kubernetes API
// limitation, not a bug here) -- they remain visible (Completed) under `kubectl describe
// pod` until the Pod itself is deleted/recreated. Requires the Ephemeral Containers
// feature (stable/default since Kubernetes 1.25).
package experiment

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/log"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	pods, err := itbench.ResolveTargets(ctx, cs, itbench.GVRPods, chaosDetails)
	if err != nil {
		return err
	}
	targetContainer := os.Getenv("TARGET_CONTAINER")
	debugImage := os.Getenv("DEBUG_IMAGE")
	ts := time.Now().Unix()

	podClient := cs.DynamicClient.Resource(itbench.GVRPods)
	var attached []string
	for _, pod := range pods {
		phase, _, _ := unstructured.NestedString(pod.Object, "status", "phase")
		if phase != "Running" {
			log.Infof("Skipping pod %s (phase=%s, not Running)", pod.GetName(), phase)
			continue
		}
		debugName := fmt.Sprintf("chaos-freeze-%d-%s", ts, pod.GetName())
		if len(debugName) > 63 {
			debugName = debugName[:63]
		}
		log.Infof("Injecting: freezing pod=%s container=%s via ephemeral debug container=%s (image=%s)", pod.GetName(), targetContainer, debugName, debugImage)

		current, err := podClient.Namespace(pod.GetNamespace()).Get(ctx, pod.GetName(), metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("re-fetching pod %s: %w", pod.GetName(), err)
		}
		ephemeralContainers, _, _ := unstructured.NestedSlice(current.Object, "spec", "ephemeralContainers")
		ephemeralContainers = append(ephemeralContainers, map[string]interface{}{
			"name":                     debugName,
			"image":                    debugImage,
			"targetContainerName":      targetContainer,
			"command":                  []interface{}{"sh", "-c"},
			"args":                     []interface{}{fmt.Sprintf("kill -STOP 1; sleep %d; kill -CONT 1", chaosDetails.ChaosDuration)},
			"stdin":                    false,
			"tty":                      false,
			"terminationMessagePolicy": "File",
		})
		if err := unstructured.SetNestedSlice(current.Object, ephemeralContainers, "spec", "ephemeralContainers"); err != nil {
			return err
		}
		if _, err := podClient.Namespace(pod.GetNamespace()).Update(ctx, current, metav1.UpdateOptions{}, "ephemeralcontainers"); err != nil {
			return fmt.Errorf("attaching ephemeral container to pod %s: %w", pod.GetName(), err)
		}
		attached = append(attached, pod.GetName()+"/"+debugName)
	}
	if len(attached) == 0 {
		return fmt.Errorf("no Running pods found matching target label to freeze")
	}

	log.Infof("Holding fault for %ds (each ephemeral debug container independently runs its own STOP -> sleep -> CONT sequence, so the freeze self-reverts even if this job is interrupted)", chaosDetails.ChaosDuration)
	itbench.Sleep(ctx, chaosDetails.ChaosDuration)

	log.Info("Fault duration elapsed. NOTE: ephemeral debug containers cannot be individually removed (Kubernetes API limitation) -- they remain visible (Completed) under 'kubectl describe pod' until the Pod itself is deleted/recreated. This is an expected, documented side effect of this fault's mechanism, not a bug.")
	for _, a := range attached {
		log.Infof("Attached: %s", a)
	}
	return nil
}
