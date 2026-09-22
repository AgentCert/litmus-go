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

func hpa(name, ns string, cpuTarget, memTarget int64) *unstructured.Unstructured {
	metric := func(resource string, target int64) interface{} {
		return map[string]interface{}{
			"type": "Resource",
			"resource": map[string]interface{}{
				"name": resource,
				"target": map[string]interface{}{
					"type":               "Utilization",
					"averageUtilization": target,
				},
			},
		}
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
		"metadata": map[string]interface{}{
			"name": name, "namespace": ns,
			"labels": map[string]interface{}{"app": name},
		},
		"spec": map[string]interface{}{
			"metrics": []interface{}{metric("cpu", cpuTarget), metric("memory", memTarget)},
		},
	}}
}

func fakeCS(objs ...runtime.Object) clients.ClientSets {
	scheme := runtime.NewScheme()
	lk := map[schema.GroupVersionResource]string{
		GVRDeployments: "DeploymentList",
		GVRServices:    "ServiceList",
		GVRPods:        "PodList",
		GVRHPA:         "HorizontalPodAutoscalerList",
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
		name       string
		cs         clients.ClientSets
		cd         *types.ChaosDetails
		wantGraded bool
		wantOK     bool
	}{
		{
			"deployment ready -> pass",
			fakeCS(deploy("accounting", "otel-demo", 1)),
			cd("deployment", "otel-demo", "opentelemetry.io/name=accounting", ""),
			true,
			true,
		},
		{
			"deployment 0 ready -> fail",
			fakeCS(deploy("accounting", "otel-demo", 0)),
			cd("deployment", "otel-demo", "opentelemetry.io/name=accounting", ""),
			true,
			false,
		},
		{
			"deployment missing -> ungraded (cannot check, must not count as a pass)",
			fakeCS(),
			cd("deployment", "otel-demo", "opentelemetry.io/name=ghost", ""),
			false,
			false,
		},
		{
			"service with endpoints -> pass",
			fakeCS(svc("details", "book-info"), endpoints("details", "book-info", true)),
			cd("service", "book-info", "", "details"),
			true,
			true,
		},
		{
			"service, endpoints empty -> fail",
			fakeCS(svc("details", "book-info"), endpoints("details", "book-info", false)),
			cd("service", "book-info", "", "details"),
			true,
			false,
		},
		{
			"service deleted -> fail",
			fakeCS(),
			cd("service", "book-info", "", "details"),
			true,
			false,
		},
		{
			"unknown kind (configmap) -> ungraded, not a free pass",
			fakeCS(),
			cd("configmap", "otel-demo", "", "flagd-config"),
			false,
			false,
		},
		{
			"no target resolved -> ungraded, not a free pass",
			fakeCS(),
			&types.ChaosDetails{ExperimentName: "some-fault"},
			false,
			false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			graded, ok, detail := defaultRecoveryAssertion(context.Background(), c.cs, c.cd)
			if graded != c.wantGraded || ok != c.wantOK {
				t.Fatalf("graded=%v ok=%v want graded=%v ok=%v (detail: %s)", graded, ok, c.wantGraded, c.wantOK, detail)
			}
		})
	}
}

func TestHPARecoveryAssertion(t *testing.T) {
	t.Setenv("ITBENCH_DEFAULT_RECOVERY_CHECK", "")
	t.Setenv("CPU_UTILIZATION_PERCENT", "20")
	t.Setenv("MEMORY_UTILIZATION_PERCENT", "30")

	t.Run("injected targets still in place -> fail", func(t *testing.T) {
		graded, ok, detail := defaultRecoveryAssertion(context.Background(),
			fakeCS(hpa("frontend", "otel-demo", 20, 60)),
			cd("horizontalpodautoscaler", "otel-demo", "", "frontend"))
		if !graded || ok {
			t.Fatalf("graded=%v ok=%v want graded=true ok=false (detail: %s)", graded, ok, detail)
		}
	})

	t.Run("targets corrected -> pass", func(t *testing.T) {
		graded, ok, detail := defaultRecoveryAssertion(context.Background(),
			fakeCS(hpa("frontend", "otel-demo", 80, 75)),
			cd("horizontalpodautoscaler", "otel-demo", "", "frontend"))
		if !graded || !ok {
			t.Fatalf("graded=%v ok=%v want graded=true ok=true (detail: %s)", graded, ok, detail)
		}
	})
}

func TestHPARecoveryAssertion_NoInjectedValues(t *testing.T) {
	t.Setenv("ITBENCH_DEFAULT_RECOVERY_CHECK", "")
	t.Setenv("CPU_UTILIZATION_PERCENT", "")
	t.Setenv("MEMORY_UTILIZATION_PERCENT", "")

	graded, ok, _ := defaultRecoveryAssertion(context.Background(),
		fakeCS(hpa("frontend", "otel-demo", 20, 30)),
		cd("horizontalpodautoscaler", "otel-demo", "", "frontend"))
	if graded || ok {
		t.Fatalf("graded=%v ok=%v want both false when there is nothing to compare against", graded, ok)
	}
}

func TestDefaultRecoveryAssertion_Disabled(t *testing.T) {
	t.Setenv("ITBENCH_DEFAULT_RECOVERY_CHECK", "false")
	graded, ok, _ := defaultRecoveryAssertion(context.Background(),
		fakeCS(deploy("accounting", "otel-demo", 0)),
		cd("deployment", "otel-demo", "opentelemetry.io/name=accounting", ""))
	if !graded || !ok {
		t.Fatal("disabled check must always return graded=true, ok=true")
	}
}

func TestDefaultRecoveryAssertion_SkipsTeardown(t *testing.T) {
	t.Setenv("ITBENCH_DEFAULT_RECOVERY_CHECK", "")
	c := cd("deployment", "otel-demo", "opentelemetry.io/name=accounting", "")
	c.ExperimentName = "uninstall-agent"
	graded, ok, detail := defaultRecoveryAssertion(context.Background(), fakeCS(deploy("accounting", "otel-demo", 0)), c)
	if !graded || !ok {
		t.Fatalf("teardown experiment must be skipped, got graded=%v ok=%v: %s", graded, ok, detail)
	}
}
