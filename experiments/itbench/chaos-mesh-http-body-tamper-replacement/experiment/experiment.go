// Package experiment implements chaos-mesh-http-body-tamper-replacement: launches a
// synthetic curl-loop pod sending ITBench's exact tampered request body at the target
// Service on an interval, holds, deletes the pod. ACE has no service mesh / Chaos Mesh
// transparent proxy, so genuine in-flight tampering of real traffic isn't reproducible via
// kubectl alone -- this approximates the effect with synthetic traffic alongside real
// traffic, same honest limitation the original script documented.
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
	if len(chaosDetails.AppDetail) == 0 {
		return fmt.Errorf("no target resolved: TARGETS env var was empty/unset")
	}
	namespace := chaosDetails.AppDetail[0].Namespace

	svcName := os.Getenv("SERVICE_NAME")
	if svcName == "" {
		svcTarget, err := itbench.ResolveTarget(ctx, cs, itbench.GVRServices, chaosDetails)
		if err != nil {
			return fmt.Errorf("no Service found matching target label (set SERVICE_NAME to override): %w", err)
		}
		svcName = svcTarget.GetName()
	}

	servicePort := os.Getenv("SERVICE_PORT")
	requestPath := os.Getenv("REQUEST_PATH")
	requestMethod := os.Getenv("REQUEST_METHOD")
	tamperedBody := os.Getenv("TAMPERED_BODY")
	intervalSeconds := os.Getenv("REQUEST_INTERVAL_SECONDS")
	generatorImage := os.Getenv("GENERATOR_IMAGE")

	url := fmt.Sprintf("http://%s.%s.svc.cluster.local:%s%s", svcName, namespace, servicePort, requestPath)
	genPodName := fmt.Sprintf("chaos-http-body-tamper-%d", time.Now().Unix())
	log.Infof("Target service=%s port=%s path=%s", svcName, servicePort, requestPath)

	pod := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]interface{}{
			"name":      genPodName,
			"namespace": namespace,
			"labels":    map[string]interface{}{"chaos-injector": "chaos-mesh-http-body-tamper-replacement"},
		},
		"spec": map[string]interface{}{
			"restartPolicy": "Never",
			"containers": []interface{}{
				map[string]interface{}{
					"name":  "tamper-generator",
					"image": generatorImage,
					"env": []interface{}{
						map[string]interface{}{"name": "TAMPER_URL", "value": url},
						map[string]interface{}{"name": "TAMPER_BODY", "value": tamperedBody},
						map[string]interface{}{"name": "TAMPER_METHOD", "value": requestMethod},
						map[string]interface{}{"name": "TAMPER_INTERVAL", "value": intervalSeconds},
					},
					"command": []string{"/bin/sh", "-c",
						`while true; do curl -s -o /dev/null -w "%{http_code} " -X "$TAMPER_METHOD" -H "Content-Type: application/json" -d "$TAMPER_BODY" "$TAMPER_URL"; sleep "$TAMPER_INTERVAL"; done`,
					},
				},
			},
		},
	}}

	podClient := cs.DynamicClient.Resource(itbench.GVRPods).Namespace(namespace)
	log.Infof("Injecting: launching request-generator pod %s -> %s", genPodName, url)
	if _, err := podClient.Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("creating generator pod: %w", err)
	}

	log.Info("Waiting for generator pod to become Ready (best-effort)")
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		p, err := podClient.Get(ctx, genPodName, metav1.GetOptions{})
		if err == nil {
			phase, _, _ := unstructured.NestedString(p.Object, "status", "phase")
			if phase == "Running" || phase == "Succeeded" {
				break
			}
		}
		time.Sleep(2 * time.Second)
	}

	itbench.Sleep(ctx, chaosDetails.ChaosDuration)

	log.Info("Reverting: no application state was ever modified -- stopping synthetic traffic by removing the generator pod")
	if err := podClient.Delete(ctx, genPodName, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("deleting generator pod: %w", err)
	}
	return nil
}
