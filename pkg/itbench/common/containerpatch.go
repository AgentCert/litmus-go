package common

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/log"
	"github.com/litmuschaos/litmus-go/pkg/types"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TargetContainerName resolves which container within the target pod template a
// container-scoped fault should act on: the TARGET_CONTAINER env var if set, otherwise
// the target workload's own name (the convention every itbench fault script already
// used: CONTAINER="${TARGET_CONTAINER:-$TARGET}").
func TargetContainerName(targetName string) string {
	if v := os.Getenv("TARGET_CONTAINER"); v != "" {
		return v
	}
	return targetName
}

// ContainerIndex returns the index of the named container within the resolved
// Deployment/StatefulSet's spec.template.spec.containers list.
func ContainerIndex(target *unstructured.Unstructured, containerName string) (int, error) {
	containers, found, err := unstructured.NestedSlice(target.Object, "spec", "template", "spec", "containers")
	if err != nil {
		return -1, err
	}
	if !found {
		return -1, fmt.Errorf("no containers found on %s", target.GetName())
	}
	for i, c := range containers {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if name, _ := m["name"].(string); name == containerName {
			return i, nil
		}
	}
	return -1, fmt.Errorf("container %q not found on %s", containerName, target.GetName())
}

// ResolveTargetWorkloadAndContainer resolves the target Deployment/StatefulSet via
// TARGETS and locates the container named by TargetContainerName within it. Returns the
// target object, its container index, and the container name (for logging).
func ResolveTargetWorkloadAndContainer(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) (target *unstructured.Unstructured, containerIdx int, containerName string, err error) {
	if len(chaosDetails.AppDetail) == 0 {
		return nil, -1, "", fmt.Errorf("no target resolved: TARGETS env var was empty/unset")
	}
	gvr, err := WorkloadGVRForKind(chaosDetails.AppDetail[0].Kind)
	if err != nil {
		return nil, -1, "", err
	}
	target, err = ResolveTarget(ctx, cs, gvr, chaosDetails)
	if err != nil {
		return nil, -1, "", err
	}
	containerName = TargetContainerName(target.GetName())
	containerIdx, err = ContainerIndex(target, containerName)
	if err != nil {
		return nil, -1, "", err
	}
	log.Infof("Target resolved: %s/%s container=%s (ns=%s)", gvr.Resource, target.GetName(), containerName, target.GetNamespace())
	return target, containerIdx, containerName, nil
}

// PatchContainerField resolves the target Deployment/StatefulSet via TARGETS, locates
// the container named by TargetContainerName within it, captures the current value at
// containerFieldPath (a path relative to that container object -- e.g. []string{"image"}
// or []string{"resources","limits"}; nil if absent), replaces it with newValue, holds for
// chaosDetails.ChaosDuration, then restores the original (or removes the field if it was
// absent). Shared shape behind the container-level itbench faults: image, command/args,
// resource limits/requests, readiness probe.
func PatchContainerField(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails, containerFieldPath []string, newValue interface{}) error {
	if len(chaosDetails.AppDetail) == 0 {
		return fmt.Errorf("no target resolved: TARGETS env var was empty/unset")
	}
	gvr, err := WorkloadGVRForKind(chaosDetails.AppDetail[0].Kind)
	if err != nil {
		return err
	}
	target, idx, _, err := ResolveTargetWorkloadAndContainer(ctx, cs, chaosDetails)
	if err != nil {
		return err
	}
	name, namespace := target.GetName(), target.GetNamespace()

	nestedPath := append([]string{"spec", "template", "spec", "containers", fmt.Sprintf("%d", idx)}, containerFieldPath...)
	jsonPointerPath := fmt.Sprintf("/spec/template/spec/containers/%d/%s", idx, jsonPointerJoin(containerFieldPath))

	return applyFieldPatch(ctx, cs, chaosDetails, gvr, namespace, name, target, nestedPath, jsonPointerPath, newValue)
}

// RemoveContainerField resolves the target container and, if containerFieldPath exists,
// removes it entirely (JSON-patch "remove", matching strategic-merge's "set to null
// deletes the field" semantics), holds, then restores the original value. If the field is
// already absent, this is a no-op (still holds for ChaosDuration, matching the original
// scripts' behavior of running the no-op patch anyway). Used by faults whose injection
// itself is a deletion, not a replacement (e.g. unassigned-resource-limits).
func RemoveContainerField(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails, containerFieldPath []string) error {
	if len(chaosDetails.AppDetail) == 0 {
		return fmt.Errorf("no target resolved: TARGETS env var was empty/unset")
	}
	gvr, err := WorkloadGVRForKind(chaosDetails.AppDetail[0].Kind)
	if err != nil {
		return err
	}
	target, idx, containerName, err := ResolveTargetWorkloadAndContainer(ctx, cs, chaosDetails)
	if err != nil {
		return err
	}
	name, namespace := target.GetName(), target.GetNamespace()

	nestedPath := append([]string{"spec", "template", "spec", "containers", fmt.Sprintf("%d", idx)}, containerFieldPath...)
	jsonPointerPath := fmt.Sprintf("/spec/template/spec/containers/%d/%s", idx, jsonPointerJoin(containerFieldPath))

	original, found := readNested(target.Object, nestedPath)
	if !found {
		log.Infof("Injecting: %s already absent on %s, nothing to remove", jsonPointerPath, containerName)
		HoldChaos(ctx, chaosDetails)
		return nil
	}

	log.Infof("Injecting: removing %s from %s", jsonPointerPath, containerName)
	removePatch, err := json.Marshal([]jsonPatchOp{{Op: "remove", Path: jsonPointerPath}})
	if err != nil {
		return err
	}
	if err := JSONPatch(ctx, cs, gvr, namespace, name, removePatch); err != nil {
		return fmt.Errorf("removing %s: %w", jsonPointerPath, err)
	}

	HoldChaos(ctx, chaosDetails)

	log.Infof("Reverting: restoring %s on %s", jsonPointerPath, containerName)
	restorePatch, err := json.Marshal([]jsonPatchOp{{Op: "add", Path: jsonPointerPath, Value: original}})
	if err != nil {
		return err
	}
	if err := JSONPatch(ctx, cs, gvr, namespace, name, restorePatch); err != nil {
		return fmt.Errorf("restoring %s: %w", jsonPointerPath, err)
	}
	return nil
}

// MapMergeSpec is one "merge key=value into the map at Path" operation for
// MergeContainerMapFields.
type MapMergeSpec struct {
	Path  []string
	Key   string
	Value interface{}
}

// MergeContainerMapField is MergeContainerMapFields for a single field.
func MergeContainerMapField(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails, containerFieldPath []string, key string, value interface{}) error {
	return MergeContainerMapFields(ctx, cs, chaosDetails, []MapMergeSpec{{Path: containerFieldPath, Key: key, Value: value}})
}

// MergeContainerMapFields reads the current map at each spec's Path (empty map if
// absent), sets Key=Value in a copy of it, and replaces/adds the whole map -- all fields
// applied and reverted atomically in one patch. Unlike PatchContainerField (whole-value
// replace), this preserves sibling keys already present in each map -- e.g. patching
// resources.limits.memory without clobbering an existing resources.limits.cpu.
func MergeContainerMapFields(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails, specs []MapMergeSpec) error {
	if len(chaosDetails.AppDetail) == 0 {
		return fmt.Errorf("no target resolved: TARGETS env var was empty/unset")
	}
	gvr, err := WorkloadGVRForKind(chaosDetails.AppDetail[0].Kind)
	if err != nil {
		return err
	}
	target, idx, _, err := ResolveTargetWorkloadAndContainer(ctx, cs, chaosDetails)
	if err != nil {
		return err
	}
	name, namespace := target.GetName(), target.GetNamespace()

	type resolved struct {
		jsonPointerPath string
		original        interface{}
		found           bool
		merged          map[string]interface{}
	}
	items := make([]resolved, len(specs))
	injectOps := make([]jsonPatchOp, len(specs))
	for i, spec := range specs {
		nestedPath := append([]string{"spec", "template", "spec", "containers", fmt.Sprintf("%d", idx)}, spec.Path...)
		jsonPointerPath := fmt.Sprintf("/spec/template/spec/containers/%d/%s", idx, jsonPointerJoin(spec.Path))
		original, found := readNested(target.Object, nestedPath)
		merged := map[string]interface{}{}
		if found {
			if m, ok := original.(map[string]interface{}); ok {
				for k, v := range m {
					merged[k] = v
				}
			}
		}
		merged[spec.Key] = spec.Value
		items[i] = resolved{jsonPointerPath, original, found, merged}
		op := "replace"
		if !found {
			op = "add"
		}
		injectOps[i] = jsonPatchOp{Op: op, Path: jsonPointerPath, Value: merged}
	}

	injectPatch, err := json.Marshal(injectOps)
	if err != nil {
		return err
	}
	log.Infof("Injecting: merging %d map field(s)", len(specs))
	if err := JSONPatch(ctx, cs, gvr, namespace, name, injectPatch); err != nil {
		return fmt.Errorf("patching fields: %w", err)
	}

	HoldChaos(ctx, chaosDetails)

	revertOps := make([]jsonPatchOp, len(items))
	for i, it := range items {
		if it.found {
			revertOps[i] = jsonPatchOp{Op: "replace", Path: it.jsonPointerPath, Value: it.original}
		} else {
			revertOps[i] = jsonPatchOp{Op: "remove", Path: it.jsonPointerPath}
		}
	}
	revertPatch, err := json.Marshal(revertOps)
	if err != nil {
		return err
	}
	log.Infof("Reverting: restoring %d map field(s)", len(specs))
	if err := JSONPatch(ctx, cs, gvr, namespace, name, revertPatch); err != nil {
		return fmt.Errorf("reverting fields: %w", err)
	}
	return nil
}

// AppendAndRemoveInitContainer resolves the target Deployment/StatefulSet via TARGETS,
// appends initContainer to spec.template.spec.initContainers (creating the array if
// entirely absent), holds for chaosDetails.ChaosDuration, then removes exactly that
// appended entry by the index it landed at -- any pre-existing init containers are left
// untouched. Shared shape behind the crashing/hanging init-container faults.
func AppendAndRemoveInitContainer(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails, initContainer map[string]interface{}) error {
	if len(chaosDetails.AppDetail) == 0 {
		return fmt.Errorf("no target resolved: TARGETS env var was empty/unset")
	}
	gvr, err := WorkloadGVRForKind(chaosDetails.AppDetail[0].Kind)
	if err != nil {
		return err
	}
	target, err := ResolveTarget(ctx, cs, gvr, chaosDetails)
	if err != nil {
		return err
	}
	name, namespace := target.GetName(), target.GetNamespace()

	existing, found, err := unstructured.NestedSlice(target.Object, "spec", "template", "spec", "initContainers")
	if err != nil {
		return err
	}
	origCount := 0
	if found {
		origCount = len(existing)
	}
	log.Infof("Target resolved: %s/%s (ns=%s), existing initContainers=%d", gvr.Resource, name, namespace, origCount)

	var injectPatch []byte
	if origCount == 0 {
		// JSON Patch "add .../initContainers/-" requires the array to already exist;
		// when there are no pre-existing init containers the field may be entirely
		// absent, so create it as a single-element array instead.
		injectPatch, err = json.Marshal([]jsonPatchOp{{Op: "add", Path: "/spec/template/spec/initContainers", Value: []interface{}{initContainer}}})
	} else {
		injectPatch, err = json.Marshal([]jsonPatchOp{{Op: "add", Path: "/spec/template/spec/initContainers/-", Value: initContainer}})
	}
	if err != nil {
		return err
	}
	log.Infof("Injecting: appending initContainer %v", initContainer["name"])
	if err := JSONPatch(ctx, cs, gvr, namespace, name, injectPatch); err != nil {
		return fmt.Errorf("appending initContainer: %w", err)
	}

	HoldChaos(ctx, chaosDetails)

	removePath := fmt.Sprintf("/spec/template/spec/initContainers/%d", origCount)
	log.Infof("Reverting: removing appended initContainer at index %d", origCount)
	revertPatch, err := json.Marshal([]jsonPatchOp{{Op: "remove", Path: removePath}})
	if err != nil {
		return err
	}
	if err := JSONPatch(ctx, cs, gvr, namespace, name, revertPatch); err != nil {
		return fmt.Errorf("removing appended initContainer: %w", err)
	}
	return nil
}

// ArrayAppendSpec is one "append Item to the array at Path" operation for
// AppendAndRemoveWorkloadArrayItems.
type ArrayAppendSpec struct {
	Path []string
	Item interface{}
}

// AppendAndRemoveWorkloadArrayItems resolves the target Deployment/StatefulSet via
// TARGETS, appends each spec's Item to the array at its Path (creating the array if
// entirely absent), holds for chaosDetails.ChaosDuration, then removes exactly the
// appended entries by the indices they landed at -- all applied and reverted atomically.
// Generalizes AppendAndRemoveInitContainer to arbitrary array paths (e.g. volumeMounts
// on one container + volumes on the pod spec, added together for the PVC-mount fault).
func AppendAndRemoveWorkloadArrayItems(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails, specs []ArrayAppendSpec) error {
	if len(chaosDetails.AppDetail) == 0 {
		return fmt.Errorf("no target resolved: TARGETS env var was empty/unset")
	}
	gvr, err := WorkloadGVRForKind(chaosDetails.AppDetail[0].Kind)
	if err != nil {
		return err
	}
	target, err := ResolveTarget(ctx, cs, gvr, chaosDetails)
	if err != nil {
		return err
	}
	name, namespace := target.GetName(), target.GetNamespace()

	origCounts := make([]int, len(specs))
	injectOps := make([]jsonPatchOp, len(specs))
	for i, spec := range specs {
		// readNested (not unstructured.NestedSlice) because spec.Path may contain a
		// numeric array-index segment (e.g. containers/<idx>/volumeMounts) --
		// unstructured's own Nested* helpers only navigate map[string]interface{} at
		// every level and error out on an array segment.
		existingRaw, found := readNested(target.Object, spec.Path)
		var existing []interface{}
		if found {
			existing, found = existingRaw.([]interface{})
		}
		if found {
			origCounts[i] = len(existing)
		}
		pointerPath := "/" + jsonPointerJoin(spec.Path)
		if origCounts[i] == 0 {
			injectOps[i] = jsonPatchOp{Op: "add", Path: pointerPath, Value: []interface{}{spec.Item}}
		} else {
			injectOps[i] = jsonPatchOp{Op: "add", Path: pointerPath + "/-", Value: spec.Item}
		}
	}
	injectPatch, err := json.Marshal(injectOps)
	if err != nil {
		return err
	}
	log.Infof("Injecting: appending %d array item(s)", len(specs))
	if err := JSONPatch(ctx, cs, gvr, namespace, name, injectPatch); err != nil {
		return fmt.Errorf("appending array items: %w", err)
	}

	HoldChaos(ctx, chaosDetails)

	revertOps := make([]jsonPatchOp, len(specs))
	for i, spec := range specs {
		pointerPath := fmt.Sprintf("/%s/%d", jsonPointerJoin(spec.Path), origCounts[i])
		revertOps[i] = jsonPatchOp{Op: "remove", Path: pointerPath}
	}
	revertPatch, err := json.Marshal(revertOps)
	if err != nil {
		return err
	}
	log.Infof("Reverting: removing %d appended array item(s)", len(specs))
	if err := JSONPatch(ctx, cs, gvr, namespace, name, revertPatch); err != nil {
		return fmt.Errorf("removing appended array items: %w", err)
	}
	return nil
}

func jsonPointerJoin(path []string) string {
	out := ""
	for i, p := range path {
		if i > 0 {
			out += "/"
		}
		out += p
	}
	return out
}
