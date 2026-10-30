package common

import (
	"context"
	"fmt"
	"os"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// NestedObject walks an object we constructed ourselves and returns the map at
// path for in-place mutation. unstructured.NestedMap cannot be used for this: it
// deep-copies through DeepCopyJSON, which panics on any value that is not already
// JSON-shaped (a []string command, for example). Returns nil when the path is
// absent or not a map.
func NestedObject(object map[string]interface{}, path ...string) map[string]interface{} {
	current := object
	for _, key := range path {
		next, ok := current[key].(map[string]interface{})
		if !ok {
			return nil
		}
		current = next
	}
	return current
}

// ExperimentPod describes how this experiment's own pod was configured.
//
// A fault that creates helper workloads at run time cannot take their image or
// registry credentials from the chart: by the time the experiment runs, the
// server has already resolved every image in the manifest against IMAGE_REGISTRY
// and IMAGE_MIRROR_NAMESPACE, and attached the pull secret for a private
// registry. Helpers created afterwards are not covered by that pass, so they
// inherit it from the pod that is already running the resolved image instead.
// That keeps a helper on the same build and the same registry as the experiment
// without any fault needing to know which registry is in use.
type ExperimentPod struct {
	Image       string
	PullSecrets []interface{}
}

// ThisExperimentPod reads the pod this experiment is running in. POD_NAME and
// CHAOS_NAMESPACE are set by the chaos-operator on every experiment pod.
func ThisExperimentPod(ctx context.Context, cs clients.ClientSets) (ExperimentPod, error) {
	name, namespace := os.Getenv("POD_NAME"), os.Getenv("CHAOS_NAMESPACE")
	if name == "" || namespace == "" {
		return ExperimentPod{}, fmt.Errorf("POD_NAME and CHAOS_NAMESPACE are not set, so this experiment cannot read its own image or registry credentials")
	}
	pod, err := cs.DynamicClient.Resource(GVRPods).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return ExperimentPod{}, fmt.Errorf("reading this experiment's own pod %s/%s: %w", namespace, name, err)
	}
	var self ExperimentPod
	containers, _, _ := unstructured.NestedSlice(pod.Object, "spec", "containers")
	for _, raw := range containers {
		container, _ := raw.(map[string]interface{})
		if image, _ := container["image"].(string); image != "" {
			self.Image = image
			break
		}
	}
	if self.Image == "" {
		return ExperimentPod{}, fmt.Errorf("this experiment's pod %s/%s declares no image", namespace, name)
	}
	secrets, _, _ := unstructured.NestedSlice(pod.Object, "spec", "imagePullSecrets")
	self.PullSecrets = secrets
	return self, nil
}

// ApplyTo sets the inherited image and pull secrets on one helper pod spec.
// Without the pull secrets a helper cannot pull from a private registry even
// though the experiment that created it could.
func (p ExperimentPod) ApplyTo(podSpec map[string]interface{}, image string) {
	if len(p.PullSecrets) > 0 {
		podSpec["imagePullSecrets"] = p.PullSecrets
	}
	containers, _ := podSpec["containers"].([]interface{})
	for _, raw := range containers {
		if container, ok := raw.(map[string]interface{}); ok && image != "" {
			container["image"] = image
		}
	}
}
