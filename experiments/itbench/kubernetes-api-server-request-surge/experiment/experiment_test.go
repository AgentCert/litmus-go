package experiment

import (
	"context"
	"strings"
	"testing"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func fakeClients(objects ...runtime.Object) clients.ClientSets {
	listKinds := map[schema.GroupVersionResource]string{itbench.GVRPods: "PodList"}
	return clients.ClientSets{
		DynamicClient: dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, objects...),
	}
}

func experimentPod(name, namespace, image string, pullSecrets ...string) *unstructured.Unstructured {
	spec := map[string]interface{}{
		"containers": []interface{}{map[string]interface{}{"name": "experiment", "image": image}},
	}
	if len(pullSecrets) > 0 {
		var secrets []interface{}
		for _, secret := range pullSecrets {
			secrets = append(secrets, map[string]interface{}{"name": secret})
		}
		spec["imagePullSecrets"] = secrets
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]interface{}{"name": name, "namespace": namespace},
		"spec":     spec,
	}}
}

// TestHelperImageDefaultsToThisExperimentsOwnImage is the regression for a surge that
// delivered no load at all: the load generators run this fault's own binary
// (/itbench-experiment -api-load), but the chart defaulted them to a general-purpose
// kubectl image that has no such binary, so every helper pod failed to start.
// Inheriting the image also keeps the helper on whichever registry the server
// already resolved for this experiment.
func TestHelperImageDefaultsToThisExperimentsOwnImage(t *testing.T) {
	t.Setenv("SURGE_IMAGE", "")
	t.Setenv("POD_NAME", "surge-abc123")
	t.Setenv("CHAOS_NAMESPACE", "litmus")

	for _, image := range []string{
		"agentcert/itbench-experiment:dev",
		"infyartifactory.jfrog.io/docker-local/agentcert/itbench-experiment:RELEASE-3",
	} {
		cs := fakeClients(experimentPod("surge-abc123", "litmus", image))
		self, err := itbench.ThisExperimentPod(context.Background(), cs)
		if err != nil {
			t.Fatalf("ThisExperimentPod() error = %v", err)
		}
		if got := helperImage(self); got != image {
			t.Fatalf("helperImage() = %q, want this experiment's own image %q", got, image)
		}
	}
}

// An explicit override still wins: an operator may host the binary elsewhere.
func TestHelperImageHonoursExplicitOverride(t *testing.T) {
	t.Setenv("SURGE_IMAGE", "registry.example.com/itbench:v1")
	self := itbench.ExperimentPod{Image: "agentcert/itbench-experiment:dev"}
	if got := helperImage(self); got != "registry.example.com/itbench:v1" {
		t.Fatalf("helperImage() = %q, want the configured override", got)
	}
}

// Without a readable pod the fault must say so rather than deploy helpers that
// cannot run.
func TestThisExperimentPodReportsWhenItCannotResolve(t *testing.T) {
	t.Setenv("POD_NAME", "")
	t.Setenv("CHAOS_NAMESPACE", "")
	if _, err := itbench.ThisExperimentPod(context.Background(), fakeClients()); err == nil ||
		!strings.Contains(err.Error(), "POD_NAME") {
		t.Fatalf("got %v, want an actionable error naming POD_NAME", err)
	}

	t.Setenv("POD_NAME", "missing-pod")
	t.Setenv("CHAOS_NAMESPACE", "litmus")
	if _, err := itbench.ThisExperimentPod(context.Background(), fakeClients()); err == nil {
		t.Fatal("an unreadable experiment pod must be reported, not silently ignored")
	}
}

// TestHelperInheritsRegistryCredentials covers a private registry: a helper created
// at run time is not covered by the server's manifest pass, so without inheriting
// the pull secret it cannot pull an image the experiment itself could.
func TestHelperInheritsRegistryCredentials(t *testing.T) {
	t.Setenv("POD_NAME", "surge-abc123")
	t.Setenv("CHAOS_NAMESPACE", "litmus")

	cs := fakeClients(experimentPod("surge-abc123", "litmus", "reg.example.com/agentcert/itbench-experiment:dev", "registry-pull"))
	self, err := itbench.ThisExperimentPod(context.Background(), cs)
	if err != nil {
		t.Fatalf("ThisExperimentPod() error = %v", err)
	}

	podSpec := map[string]interface{}{
		"containers": []interface{}{map[string]interface{}{"name": "load-generator", "image": "placeholder"}},
	}
	self.ApplyTo(podSpec, self.Image)

	secrets, _ := podSpec["imagePullSecrets"].([]interface{})
	if len(secrets) != 1 {
		t.Fatalf("imagePullSecrets = %v, want the experiment's own secret", podSpec["imagePullSecrets"])
	}
	if name, _ := secrets[0].(map[string]interface{})["name"].(string); name != "registry-pull" {
		t.Fatalf("pull secret = %q, want registry-pull", name)
	}
	containers, _ := podSpec["containers"].([]interface{})
	if image, _ := containers[0].(map[string]interface{})["image"].(string); image != self.Image {
		t.Fatalf("helper image = %q, want %q", image, self.Image)
	}

	// A public registry needs no secret, and none must be invented.
	plain := map[string]interface{}{"containers": []interface{}{map[string]interface{}{"name": "c"}}}
	itbench.ExperimentPod{Image: "agentcert/itbench-experiment:dev"}.ApplyTo(plain, "agentcert/itbench-experiment:dev")
	if _, found := plain["imagePullSecrets"]; found {
		t.Fatal("no pull secret must be added when the experiment itself needs none")
	}
}

// TestHelperDeploymentSurvivesDeepCopy is the regression for a panic that killed
// the experiment before it injected anything: the helper Deployment is built by
// hand, and any value that is not already JSON-shaped (a []string command, say)
// makes the unstructured helpers panic when they deep-copy it. The dynamic client
// deep-copies on Create, so this must hold for the object as a whole.
func TestHelperDeploymentSurvivesDeepCopy(t *testing.T) {
	deployment := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]interface{}{"name": "workload-scanner", "namespace": "sock-shop"},
		"spec": map[string]interface{}{
			"replicas": int64(5),
			"template": map[string]interface{}{
				"spec": map[string]interface{}{
					"containers": []interface{}{map[string]interface{}{
						"name":    "load-generator",
						"image":   "agentcert/itbench-experiment:dev",
						"command": []interface{}{"/itbench-experiment"},
						"args":    []interface{}{"-api-load"},
					}},
				},
			},
		},
	}}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("helper Deployment is not valid unstructured content: %v", r)
		}
	}()
	_ = deployment.DeepCopy()

	// In-place access must find the pod spec without deep-copying it.
	podSpec := itbench.NestedObject(deployment.Object, "spec", "template", "spec")
	if podSpec == nil {
		t.Fatal("NestedObject did not find the helper pod spec")
	}
	itbench.ExperimentPod{Image: "x", PullSecrets: []interface{}{map[string]interface{}{"name": "registry-pull"}}}.
		ApplyTo(podSpec, "reg.example.com/agentcert/itbench-experiment:dev")

	containers, _ := podSpec["containers"].([]interface{})
	if image, _ := containers[0].(map[string]interface{})["image"].(string); image != "reg.example.com/agentcert/itbench-experiment:dev" {
		t.Fatalf("image = %q, want the mutation applied in place", image)
	}
	if itbench.NestedObject(deployment.Object, "spec", "nope") != nil {
		t.Fatal("an absent path must return nil rather than an empty map")
	}
	if _, err := deployment.MarshalJSON(); err != nil {
		t.Fatalf("helper Deployment does not serialise: %v", err)
	}
}
