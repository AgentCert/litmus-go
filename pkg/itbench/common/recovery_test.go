package common

import (
	"context"
	"testing"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/types"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func deploy(name, ns string, readyReplicas int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]interface{}{
			"name": name, "namespace": ns,
			"labels": map[string]interface{}{"opentelemetry.io/name": name},
		},
		"status": map[string]interface{}{"readyReplicas": readyReplicas},
	}}
}

func svc(name, ns string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1", "kind": "Service",
		"metadata": map[string]interface{}{
			"name": name, "namespace": ns,
			"labels": map[string]interface{}{"app": name},
		},
	}}
}

func endpoints(name, ns string, withAddr bool) *unstructured.Unstructured {
	o := map[string]interface{}{
		"apiVersion": "v1", "kind": "Endpoints",
		"metadata": map[string]interface{}{"name": name, "namespace": ns},
	}
	if withAddr {
		o["subsets"] = []interface{}{map[string]interface{}{
			"addresses": []interface{}{map[string]interface{}{"ip": "10.0.0.1"}},
		}}
	}
	return &unstructured.Unstructured{Object: o}
}

func fakeCS(objs ...runtime.Object) clients.ClientSets {
	scheme := runtime.NewScheme()
	lk := map[schema.GroupVersionResource]string{
		GVRDeployments: "DeploymentList",
		GVRServices:    "ServiceList",
		GVRPods:        "PodList",
		{Group: "", Version: "v1", Resource: "endpoints"}: "EndpointsList",
	}
	return clients.ClientSets{
		DynamicClient: dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, lk, objs...),
	}
}

func cd(kind, ns, label, name string) *types.ChaosDetails {
	ad := types.AppDetails{Namespace: ns, Kind: kind}
	if label != "" {
		ad.Labels = []string{label}
	}
	if name != "" {
		ad.Names = []string{name}
	}
	return &types.ChaosDetails{AppDetail: []types.AppDetails{ad}, ExperimentName: "some-fault"}
}

func TestDefaultRecoveryAssertion(t *testing.T) {
	t.Setenv("ITBENCH_DEFAULT_RECOVERY_CHECK", "") // default = on

	cases := []struct {
		name   string
		cs     clients.ClientSets
		cd     *types.ChaosDetails
		wantOK bool
	}{
		{
			"deployment ready -> pass",
			fakeCS(deploy("accounting", "otel-demo", 1)),
			cd("deployment", "otel-demo", "opentelemetry.io/name=accounting", ""),
			true,
		},
		{
			"deployment 0 ready -> fail",
			fakeCS(deploy("accounting", "otel-demo", 0)),
			cd("deployment", "otel-demo", "opentelemetry.io/name=accounting", ""),
			false,
		},
		{
			"deployment missing -> pass (cannot check, not the agent's fault)",
			fakeCS(),
			cd("deployment", "otel-demo", "opentelemetry.io/name=ghost", ""),
			true,
		},
		{
			"service with endpoints -> pass",
			fakeCS(svc("details", "book-info"), endpoints("details", "book-info", true)),
			cd("service", "book-info", "", "details"),
			true,
		},
		{
			"service, endpoints empty -> fail",
			fakeCS(svc("details", "book-info"), endpoints("details", "book-info", false)),
			cd("service", "book-info", "", "details"),
			false,
		},
		{
			"service deleted -> fail",
			fakeCS(),
			cd("service", "book-info", "", "details"),
			false,
		},
		{
			"unknown kind (configmap) -> pass (no assertion)",
			fakeCS(),
			cd("configmap", "otel-demo", "", "flagd-config"),
			true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ok, detail := defaultRecoveryAssertion(context.Background(), c.cs, c.cd)
			if ok != c.wantOK {
				t.Fatalf("ok=%v want %v (detail: %s)", ok, c.wantOK, detail)
			}
		})
	}
}

func TestDefaultRecoveryAssertion_Disabled(t *testing.T) {
	t.Setenv("ITBENCH_DEFAULT_RECOVERY_CHECK", "false")
	ok, _ := defaultRecoveryAssertion(context.Background(),
		fakeCS(deploy("accounting", "otel-demo", 0)),
		cd("deployment", "otel-demo", "opentelemetry.io/name=accounting", ""))
	if !ok {
		t.Fatal("disabled check must always return ok=true")
	}
}

func TestDefaultRecoveryAssertion_SkipsTeardown(t *testing.T) {
	t.Setenv("ITBENCH_DEFAULT_RECOVERY_CHECK", "")
	c := cd("deployment", "otel-demo", "opentelemetry.io/name=accounting", "")
	c.ExperimentName = "uninstall-agent"
	ok, detail := defaultRecoveryAssertion(context.Background(), fakeCS(deploy("accounting", "otel-demo", 0)), c)
	if !ok {
		t.Fatalf("teardown experiment must be skipped, got fail: %s", detail)
	}
}
