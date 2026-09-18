package common

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// deployWithContainer builds a Deployment whose single container carries the supplied
// container fields, so a patch helper can be driven end-to-end against the fake client.
func deployWithContainer(name, ns string, container map[string]interface{}) *unstructured.Unstructured {
	container["name"] = name
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]interface{}{
			"name": name, "namespace": ns,
			"labels": map[string]interface{}{"app": name},
		},
		"spec": map[string]interface{}{
			"template": map[string]interface{}{
				"spec": map[string]interface{}{
					"containers": []interface{}{container},
				},
			},
		},
	}}
}

// TestRemoveContainerField_AlreadyAbsentIsAnError is the difference between a certification
// run that injected nothing and one that reports Passed: for a fault whose injection IS the
// removal, an already-absent field means no chaos was ever applied.
func TestRemoveContainerField_AlreadyAbsentIsAnError(t *testing.T) {
	t.Setenv("TARGET_CONTAINER", "app")
	cs := fakeCS(deployWithContainer("app", "ns1", map[string]interface{}{
		"image": "nginx:1.25",
	}))

	err := RemoveContainerField(context.Background(), cs, cd("deployment", "ns1", "app=app", ""), []string{"resources", "limits"})
	if err == nil {
		t.Fatal("RemoveContainerField returned nil for an already-absent field; the run would report Passed having injected nothing")
	}
	if !strings.Contains(err.Error(), "already absent") {
		t.Fatalf("error %q does not explain that there was nothing to remove", err)
	}
}

// TestRemoveContainerField_RestoresOriginal covers the happy path: the field is removed for
// the hold and put back byte-for-byte afterwards.
func TestRemoveContainerField_RestoresOriginal(t *testing.T) {
	t.Setenv("TARGET_CONTAINER", "app")
	limits := map[string]interface{}{"cpu": "500m", "memory": "256Mi"}
	cs := fakeCS(deployWithContainer("app", "ns1", map[string]interface{}{
		"image":     "nginx:1.25",
		"resources": map[string]interface{}{"limits": limits},
	}))
	details := cd("deployment", "ns1", "app=app", "")
	details.ChaosDuration = 0

	if err := RemoveContainerField(context.Background(), cs, details, []string{"resources", "limits"}); err != nil {
		t.Fatalf("RemoveContainerField: %v", err)
	}

	got, err := cs.DynamicClient.Resource(GVRDeployments).Namespace("ns1").Get(context.Background(), "app", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("re-fetching deployment: %v", err)
	}
	restored, found := readNested(got.Object, []string{"spec", "template", "spec", "containers", "0", "resources", "limits"})
	if !found {
		t.Fatal("resources.limits was not restored after the hold")
	}
	m, _ := restored.(map[string]interface{})
	if m["cpu"] != "500m" || m["memory"] != "256Mi" {
		t.Fatalf("resources.limits restored as %v, want %v", m, limits)
	}
}

// TestMergeContainerMapFields_CreatesMissingAncestors guards the RFC 6902 rule that "add"
// requires its parent to exist: merging into resources/limits on a container that declares
// no resources at all must still apply, and must leave nothing behind on revert.
func TestMergeContainerMapFields_CreatesMissingAncestors(t *testing.T) {
	t.Setenv("TARGET_CONTAINER", "app")
	cs := fakeCS(deployWithContainer("app", "ns1", map[string]interface{}{
		"image": "nginx:1.25",
	}))
	details := cd("deployment", "ns1", "app=app", "")
	details.ChaosDuration = 0

	err := MergeContainerMapFields(context.Background(), cs, details, []MapMergeSpec{
		{Path: []string{"resources", "limits"}, Key: "memory", Value: "1Mi"},
	})
	if err != nil {
		t.Fatalf("MergeContainerMapFields on a container with no resources: %v", err)
	}

	got, gerr := cs.DynamicClient.Resource(GVRDeployments).Namespace("ns1").Get(context.Background(), "app", metav1.GetOptions{})
	if gerr != nil {
		t.Fatalf("re-fetching deployment: %v", gerr)
	}
	if _, found := readNested(got.Object, []string{"spec", "template", "spec", "containers", "0", "resources"}); found {
		t.Fatal("revert left the resources object behind; the target no longer matches its pre-chaos spec")
	}
}
