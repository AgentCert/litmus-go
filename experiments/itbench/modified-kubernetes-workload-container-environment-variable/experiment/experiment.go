// Package experiment implements modified-kubernetes-workload-container-environment-variable:
// overwrites (or adds) one named env var on the target container (ENV_VAR_NAME,
// BAD_ENV_VALUE), holds, then restores the original value -- or removes the var entirely
// if it did not exist beforehand. Env vars are matched by name within a list, not a fixed
// index, so this needs its own logic rather than the generic field-patch helpers.
package experiment

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/log"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/types"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type jsonPatchOp struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	envVarName := os.Getenv("ENV_VAR_NAME")
	badValue := os.Getenv("BAD_ENV_VALUE")

	target, idx, containerName, err := itbench.ResolveTargetWorkloadAndContainer(ctx, cs, chaosDetails)
	if err != nil {
		return err
	}
	gvr, err := itbench.WorkloadGVRForKind(chaosDetails.AppDetail[0].Kind)
	if err != nil {
		return err
	}
	name, namespace := target.GetName(), target.GetNamespace()

	containers, _, err := unstructured.NestedSlice(target.Object, "spec", "template", "spec", "containers")
	if err != nil {
		return err
	}
	container, ok := containers[idx].(map[string]interface{})
	if !ok {
		return fmt.Errorf("container %q malformed", containerName)
	}
	envList, _, _ := unstructured.NestedSlice(container, "env")

	envIdx := -1
	var origEntry map[string]interface{}
	for i, e := range envList {
		m, ok := e.(map[string]interface{})
		if !ok {
			continue
		}
		if n, _ := m["name"].(string); n == envVarName {
			envIdx = i
			origEntry = m
			break
		}
	}

	basePath := fmt.Sprintf("/spec/template/spec/containers/%d/env", idx)
	existed := envIdx >= 0
	badEntry := map[string]string{"name": envVarName, "value": badValue}

	var injectPatch []byte
	if existed {
		// The whole entry is replaced rather than just its /value: a var sourced from a
		// ConfigMap/Secret has no "value" member, and RFC 6902 "replace" on a missing key
		// is rejected by the API server.
		log.Infof("Injecting: setting %s=%s on %s (was %v)", envVarName, badValue, containerName, origEntry)
		injectPatch, err = json.Marshal([]jsonPatchOp{{Op: "replace", Path: fmt.Sprintf("%s/%d", basePath, envIdx), Value: badEntry}})
	} else {
		log.Infof("Injecting: adding %s=%s on %s (did not exist)", envVarName, badValue, containerName)
		if len(envList) == 0 {
			injectPatch, err = json.Marshal([]jsonPatchOp{{Op: "add", Path: basePath, Value: []map[string]string{badEntry}}})
		} else {
			injectPatch, err = json.Marshal([]jsonPatchOp{{Op: "add", Path: basePath + "/-", Value: badEntry}})
		}
		envIdx = len(envList) // the index it will land at once appended
	}
	if err != nil {
		return err
	}
	if err := itbench.JSONPatch(ctx, cs, gvr, namespace, name, injectPatch); err != nil {
		return fmt.Errorf("patching env var: %w", err)
	}

	itbench.HoldChaos(ctx, chaosDetails)

	revertCtx, cancel := itbench.RevertContext()
	defer cancel()

	var revertPatch []byte
	if existed {
		log.Infof("Reverting: restoring %s on %s", envVarName, containerName)
		revertPatch, err = json.Marshal([]jsonPatchOp{{Op: "replace", Path: fmt.Sprintf("%s/%d", basePath, envIdx), Value: origEntry}})
	} else {
		log.Infof("Reverting: removing %s from %s (it did not exist originally)", envVarName, containerName)
		revertPatch, err = json.Marshal([]jsonPatchOp{{Op: "remove", Path: fmt.Sprintf("%s/%d", basePath, envIdx)}})
	}
	if err != nil {
		return err
	}
	if err := itbench.JSONPatch(revertCtx, cs, gvr, namespace, name, revertPatch); err != nil {
		return fmt.Errorf("reverting env var: %w", err)
	}
	return nil
}
