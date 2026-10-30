package common

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/log"
	"github.com/litmuschaos/litmus-go/pkg/types"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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

// soleContainer returns the only container of a single-container workload.
func soleContainer(target *unstructured.Unstructured) (int, string, bool) {
	containers, found, err := unstructured.NestedSlice(target.Object, "spec", "template", "spec", "containers")
	if err != nil || !found || len(containers) != 1 {
		return -1, "", false
	}
	m, ok := containers[0].(map[string]interface{})
	if !ok {
		return -1, "", false
	}
	name, _ := m["name"].(string)
	return 0, name, name != ""
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
	if err != nil && os.Getenv("TARGET_CONTAINER") == "" {
		// The workload-name convention does not hold for every app (bookinfo's
		// Deployment details-v1 runs container "details"); a single-container
		// workload is unambiguous, so act on that container.
		if idx, name, ok := soleContainer(target); ok {
			containerIdx, containerName, err = idx, name, nil
		}
	}
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
// already absent it returns an error rather than holding: removing nothing is not an
// injection, and reporting success would grade a run that never mutated the cluster.
// Used by faults whose injection itself is a deletion, not a replacement (e.g.
// unassigned-resource-limits).
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
		// Removing the field IS the injection, so "already absent" means nothing was
		// injected. Holding and returning nil here would report Verdict=Passed for a run
		// that never mutated the cluster.
		return fmt.Errorf("cannot inject: %s is already absent on container %q of %s -- nothing to remove", jsonPointerPath, containerName, name)
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

	revertCtx, cancel := RevertContext()
	defer cancel()

	log.Infof("Reverting: restoring %s on %s", jsonPointerPath, containerName)
	restorePatch, err := json.Marshal([]jsonPatchOp{{Op: "add", Path: jsonPointerPath, Value: original}})
	if err != nil {
		return err
	}
	if err := JSONPatch(revertCtx, cs, gvr, namespace, name, restorePatch); err != nil {
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
func MergeContainerMapFields(ctx context.Context, cs clients.ClientSets, details *types.ChaosDetails, specs []MapMergeSpec) error {
	target, idx, _, err := ResolveTargetWorkloadAndContainer(ctx, cs, details)
	if err != nil {
		return err
	}
	gvr, err := WorkloadGVRForKind(details.AppDetail[0].Kind)
	if err != nil {
		return err
	}
	base := []string{"spec", "template", "spec", "containers", fmt.Sprint(idx)}
	roots := map[string]map[string]interface{}{}
	var order []string
	for _, spec := range specs {
		if len(spec.Path) == 0 || spec.Key == "" {
			return fmt.Errorf("map merge requires a field path and key")
		}
		root := spec.Path[0]
		value, exists := roots[root]
		if !exists {
			original, found := readNested(target.Object, append(append([]string{}, base...), root))
			value = map[string]interface{}{}
			if found {
				copied, ok := runtime.DeepCopyJSONValue(original).(map[string]interface{})
				if !ok {
					return fmt.Errorf("%s must be an object", root)
				}
				value = copied
			}
			roots[root] = value
			order = append(order, root)
		}
		nested := value
		for _, key := range spec.Path[1:] {
			child, present := nested[key]
			if !present {
				child = map[string]interface{}{}
				nested[key] = child
			}
			var ok bool
			nested, ok = child.(map[string]interface{})
			if !ok {
				return fmt.Errorf("map merge parent %s is not an object", key)
			}
		}
		nested[spec.Key] = spec.Value
	}
	fields := make([]FieldSpec, 0, len(order))
	for _, root := range order {
		path := append(append([]string{}, base...), root)
		fields = append(fields, FieldSpec{Path: path, JSONPointerPath: "/" + jsonPointerJoin(path), NewValue: roots[root]})
	}
	// Capture each shared parent once. Cleanup cannot remove the same ancestor
	// twice, and the ordinary multi-field restoration handles API omitempty.
	return applyMultiFieldPatch(ctx, cs, details, gvr, target.GetNamespace(), target.GetName(), target, fields)
}

// AppendAndRemoveInitContainer resolves the target Deployment/StatefulSet via TARGETS,
// appends initContainer to spec.template.spec.initContainers (creating the array if
// entirely absent), holds for chaosDetails.ChaosDuration, then removes exactly that
// appended entry by the index it landed at -- any pre-existing init containers are left
// untouched. Shared shape behind the crashing/hanging init-container faults.
func AppendAndRemoveInitContainer(ctx context.Context, cs clients.ClientSets, details *types.ChaosDetails, item map[string]interface{}) error {
	return AppendAndRemoveWorkloadArrayItems(ctx, cs, details, []ArrayAppendSpec{{Path: []string{"spec", "template", "spec", "initContainers"}, Item: item}})
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

	fields := make([]FieldSpec, 0, len(specs))
	seen := map[string]bool{}
	for _, spec := range specs {
		pointer := "/" + jsonPointerJoin(spec.Path)
		if seen[pointer] {
			return fmt.Errorf("duplicate array patch path %s", pointer)
		}
		seen[pointer] = true
		raw, found := readNested(target.Object, spec.Path)
		var items []interface{}
		if found {
			var ok bool
			items, ok = raw.([]interface{})
			if !ok {
				return fmt.Errorf("%s must be an array", pointer)
			}
		}
		if added, ok := spec.Item.(map[string]interface{}); ok && added["name"] != nil {
			for _, raw := range items {
				existing, _ := raw.(map[string]interface{})
				if existing["name"] == added["name"] {
					return fmt.Errorf("%s already contains an item named %v", pointer, added["name"])
				}
			}
		}
		updated := append(append([]interface{}{}, items...), spec.Item)
		fields = append(fields, FieldSpec{Path: spec.Path, JSONPointerPath: pointer, NewValue: updated})
	}
	return applyMultiFieldPatch(ctx, cs, chaosDetails, gvr, namespace, name, target, fields)
}

func jsonPointerJoin(path []string) string {
	out := ""
	for i, p := range path {
		if i > 0 {
			out += "/"
		}
		out += strings.NewReplacer("~", "~0", "/", "~1").Replace(p)
	}
	return out
}
