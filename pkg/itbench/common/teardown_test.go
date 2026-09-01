package common

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

func TestIsMissingAPI(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"notfound", apierrors.NewNotFound(schema.GroupResource{Resource: "ingresses"}, "x"), true},
		{"no matches for kind", errors.New(`no matches for kind "Ingress" in version "networking.k8s.io/v1"`), true},
		{"could not find requested resource", errors.New("the server could not find the requested resource"), true},
		{"real forbidden error", apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "x", errors.New("nope")), false},
		{"transient", errors.New("etcdserver: request timed out"), false},
	}
	for _, c := range cases {
		if got := isMissingAPI(c.err); got != c.want {
			t.Errorf("isMissingAPI(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

func TestDeleteNamespace(t *testing.T) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "target"}}
	kube := k8sfake.NewSimpleClientset(ns)
	ctx := context.Background()

	if err := DeleteNamespace(ctx, kube, "target"); err != nil {
		t.Fatalf("DeleteNamespace(target) unexpected error: %v", err)
	}
	if _, err := kube.CoreV1().Namespaces().Get(ctx, "target", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("namespace still present after delete (get err = %v)", err)
	}
	// Idempotent: deleting an absent namespace is not an error.
	if err := DeleteNamespace(ctx, kube, "target"); err != nil {
		t.Errorf("DeleteNamespace on absent namespace returned error: %v", err)
	}
	// Empty namespace is rejected.
	if err := DeleteNamespace(ctx, kube, ""); err == nil {
		t.Errorf("DeleteNamespace(\"\") should error")
	}
}

func helmDeployment(name, ns, releaseName string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]interface{}{
			"name":        name,
			"namespace":   ns,
			"labels":      map[string]interface{}{"app.kubernetes.io/managed-by": "Helm"},
			"annotations": map[string]interface{}{helmReleaseNameAnno: releaseName, helmReleaseNsAnno: ns},
		},
	}}
}

func TestUninstallHelmRelease(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()

	gvrToListKind := map[schema.GroupVersionResource]string{}
	for _, gvr := range append(append([]schema.GroupVersionResource{}, helmSweptNamespacedGVRs...), helmSweptClusterGVRs...) {
		singular := gvr.Resource[:len(gvr.Resource)-1] // deployments -> deployment
		gvrToListKind[gvr] = singular + "List"
	}

	mine := helmDeployment("agent", "target", "myrel")
	theirs := helmDeployment("other-agent", "target", "otherrel")
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind, mine, theirs)

	relSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "sh.helm.release.v1.myrel.v1", Namespace: "target"},
		Type:       helmReleaseSecretTyp,
	}
	otherSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "some-app-secret", Namespace: "target"},
		Type:       corev1.SecretTypeOpaque,
	}
	kube := k8sfake.NewSimpleClientset(relSecret, otherSecret)

	if err := UninstallHelmRelease(ctx, kube, dyn, "myrel", "target"); err != nil {
		t.Fatalf("UninstallHelmRelease unexpected error: %v", err)
	}

	depGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	if _, err := dyn.Resource(depGVR).Namespace("target").Get(ctx, "agent", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("release-owned Deployment 'agent' should be gone (get err = %v)", err)
	}
	if _, err := dyn.Resource(depGVR).Namespace("target").Get(ctx, "other-agent", metav1.GetOptions{}); err != nil {
		t.Errorf("Deployment 'other-agent' (different release) should survive, got err = %v", err)
	}
	if _, err := kube.CoreV1().Secrets("target").Get(ctx, "sh.helm.release.v1.myrel.v1", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("helm release-state Secret should be gone (get err = %v)", err)
	}
	if _, err := kube.CoreV1().Secrets("target").Get(ctx, "some-app-secret", metav1.GetOptions{}); err != nil {
		t.Errorf("unrelated Secret should survive, got err = %v", err)
	}

	// Missing inputs are rejected.
	if err := UninstallHelmRelease(ctx, kube, dyn, "", "target"); err == nil {
		t.Errorf("UninstallHelmRelease with empty release should error")
	}
}
