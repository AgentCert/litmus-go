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
	"strconv"
	"strings"
	"time"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/log"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/types"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) (retErr error) {
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

	servicePort := strings.TrimSpace(os.Getenv("SERVICE_PORT"))
	if n, err := strconv.Atoi(servicePort); err != nil || n < 1 || n > 65535 {
		// An empty port yields "http://svc.ns.svc.cluster.local:/path", which curl rejects --
		// the fault would hold for its full duration having sent zero requests.
		return fmt.Errorf("SERVICE_PORT must be a port number in 1-65535, got %q", servicePort)
	}
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
	defer func() {
		revertCtx, cancel := itbench.RevertContext()
		defer cancel()
		log.Info("Reverting: no application state was ever modified -- stopping synthetic traffic by removing the generator pod")
		if err := podClient.Delete(revertCtx, genPodName, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
			log.Errorf("failed to delete generator pod %s: %v", genPodName, err)
			if retErr == nil {
				retErr = fmt.Errorf("deleting generator pod: %w", err)
			}
		}
	}()

	// Without this the fault holds its full duration having sent zero requests (e.g. the
	// generator image is stuck in ImagePullBackOff) and still reports success.
	log.Info("Waiting for the generator pod to start")
	if err := waitForPodRunning(ctx, podClient, genPodName, 60*time.Second); err != nil {
		return err
	}

	itbench.HoldChaos(ctx, chaosDetails)
	return nil
}

func waitForPodRunning(ctx context.Context, podClient dynamic.ResourceInterface, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	lastPhase, lastErr := "", error(nil)
	for time.Now().Before(deadline) {
		p, err := podClient.Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			lastErr = nil
			lastPhase, _, _ = unstructured.NestedString(p.Object, "status", "phase")
			if lastPhase == "Running" || lastPhase == "Succeeded" {
				return nil
			}
			if lastPhase == "Failed" {
				return fmt.Errorf("generator pod %s entered phase Failed -- no synthetic traffic was sent", name)
			}
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("generator pod %s never reached Running within %s (last phase=%q, last error=%v) -- no synthetic traffic was sent", name, timeout, lastPhase, lastErr)
}
