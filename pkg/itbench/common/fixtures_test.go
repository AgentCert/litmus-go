package common

import (
	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/types"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func fakeCS(objs ...runtime.Object) clients.ClientSets {
	scheme := runtime.NewScheme()
	lk := map[schema.GroupVersionResource]string{
		GVRDeployments: "DeploymentList",
		GVRServices:    "ServiceList",
		GVRPods:        "PodList",
		GVRHPA:         "HorizontalPodAutoscalerList",
		{Group: "", Version: "v1", Resource: "events"}:                         "EventList",
		{Group: "", Version: "v1", Resource: "endpoints"}:                      "EndpointsList",
		{Group: "discovery.k8s.io", Version: "v1", Resource: "endpointslices"}: "EndpointSliceList",
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
