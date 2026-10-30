// Package experiment freezes the selected container from a node-pinned hostPID
// helper. Its signal handler and independent deadline both thaw the container.
package experiment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/types"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func Run(ctx context.Context, cs clients.ClientSets) { itbench.Run(ctx, cs, inject) }

func inject(ctx context.Context, cs clients.ClientSets, details *types.ChaosDetails) (retErr error) {
	pods, err := itbench.ResolveTargets(ctx, cs, itbench.GVRPods, details)
	if err != nil {
		return err
	}
	image, container := strings.TrimSpace(os.Getenv("DEBUG_IMAGE")), strings.TrimSpace(os.Getenv("TARGET_CONTAINER"))
	if image == "" || container == "" {
		return fmt.Errorf("DEBUG_IMAGE must contain the itbench binary and TARGET_CONTAINER must name a running container")
	}
	// Validate every target before freezing the first one.
	var helpers []*unstructured.Unstructured
	for index, pod := range pods {
		node, _, _ := unstructured.NestedString(pod.Object, "spec", "nodeName")
		statuses, _, _ := unstructured.NestedSlice(pod.Object, "status", "containerStatuses")
		id := ""
		for _, raw := range statuses {
			status, _ := raw.(map[string]interface{})
			if status["name"] != container {
				continue
			}
			if _, running, _ := unstructured.NestedMap(status, "state", "running"); !running {
				continue
			}
			runtimeID, _ := status["containerID"].(string)
			parts := strings.SplitN(runtimeID, "://", 2)
			if len(parts) == 2 {
				id = parts[1]
			}
		}
		if node == "" || id == "" {
			return fmt.Errorf("pod %s has no running %s container with a runtime ID", pod.GetName(), container)
		}
		helpers = append(helpers, &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]interface{}{"name": fmt.Sprintf("chaos-freeze-%d-%d", time.Now().UnixNano(), index), "namespace": pod.GetNamespace()},
			"spec": map[string]interface{}{
				"nodeName": node, "hostPID": true, "restartPolicy": "Never", "terminationGracePeriodSeconds": int64(15),
				"containers": []interface{}{map[string]interface{}{
					"name": "freeze", "image": image, "command": []interface{}{"/itbench-experiment"},
					"args":            []interface{}{"-freeze-container-id=" + id, fmt.Sprintf("-freeze-seconds=%d", details.ChaosDuration+int(itbench.RevertTimeout.Seconds())+180)},
					"securityContext": map[string]interface{}{"runAsUser": int64(0), "capabilities": map[string]interface{}{"add": []interface{}{"KILL"}}},
					"resources":       map[string]interface{}{"requests": map[string]interface{}{"cpu": "10m", "memory": "32Mi"}, "limits": map[string]interface{}{"cpu": "100m", "memory": "64Mi"}},
				}},
			},
		}})
	}
	var created []*unstructured.Unstructured
	gone := func(ctx context.Context) error {
		for _, helper := range created {
			pod, err := cs.DynamicClient.Resource(itbench.GVRPods).Namespace(helper.GetNamespace()).Get(ctx, helper.GetName(), metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return err
			}
			return fmt.Errorf("freeze helper %s still exists (phase %v)", helper.GetName(), pod.Object["status"])
		}
		return nil
	}
	defer func() {
		revertCtx, cancel := itbench.RevertContext()
		defer cancel()
		for _, helper := range created {
			err := cs.DynamicClient.Resource(itbench.GVRPods).Namespace(helper.GetNamespace()).Delete(revertCtx, helper.GetName(), metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				retErr = errors.Join(retErr, err)
			}
		}
		retErr = errors.Join(retErr, itbench.WaitForCheck(revertCtx, itbench.RevertTimeout, "freeze helpers did not exit", gone))
	}()
	itbench.RegisterFaultChecks(ctx, func(ctx context.Context) error {
		if cs.KubeClient == nil {
			return fmt.Errorf("freeze helper logs are unavailable")
		}
		for _, helper := range created {
			tail := int64(10)
			raw, err := cs.KubeClient.CoreV1().Pods(helper.GetNamespace()).GetLogs(helper.GetName(), &corev1.PodLogOptions{TailLines: &tail}).DoRaw(ctx)
			if err != nil {
				return err
			}
			frozen := false
			for _, line := range strings.Split(string(raw), "\n") {
				var state struct {
					Frozen bool `json:"frozen"`
				}
				if json.Unmarshal([]byte(line), &state) == nil && state.Frozen {
					frozen = true
				}
			}
			if !frozen {
				return fmt.Errorf("helper %s has not verified stopped target processes", helper.GetName())
			}
		}
		return nil
	}, gone)
	for _, helper := range helpers {
		obj, err := cs.DynamicClient.Resource(itbench.GVRPods).Namespace(helper.GetNamespace()).Create(ctx, helper, metav1.CreateOptions{})
		if err != nil {
			return err
		}
		created = append(created, obj)
	}
	itbench.HoldChaos(ctx, details)
	return nil
}
