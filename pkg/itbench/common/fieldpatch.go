package common

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/log"
	"github.com/litmuschaos/litmus-go/pkg/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type jsonPatchOp struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

// readNested navigates obj by path, where each segment is either a map key or (if the
// current value is a slice) a numeric index -- generalizes unstructured.NestedFieldNoCopy
// to also index into arrays, needed for spec.template.spec.containers[i].<field>.
func readNested(obj interface{}, path []string) (interface{}, bool) {
	cur := obj
	for _, seg := range path {
		switch v := cur.(type) {
		case map[string]interface{}:
			next, ok := v[seg]
			if !ok {
				return nil, false
			}
			cur = next
		case []interface{}:
			i, err := strconv.Atoi(seg)
			if err != nil || i < 0 || i >= len(v) {
				return nil, false
			}
			cur = v[i]
		default:
			return nil, false
		}
	}
	return cur, true
}

// buildRevertOp decides the correct revert JSON-patch op for one field, re-checking the
// object's CURRENT state rather than trusting the pre-inject "found" alone: the API
// server's typed round-trip can silently drop an empty slice/map back to "absent" (Go's
// `omitempty`) even though our own inject patch explicitly added it, which would make a
// blind "remove" on revert fail with an invalid-request error (RFC 6902 remove requires
// the target to currently exist). Restoring a real original value always uses "add"
// (idempotent whether or not the key is currently present); restoring "absent" only
// emits "remove" if the field is actually there right now, otherwise it's skipped
// (skip=true).
func buildRevertOp(ctx context.Context, cs clients.ClientSets, gvr schema.GroupVersionResource, namespace, name string, fieldPath []string, jsonPointerPath string, original interface{}, found bool) (op jsonPatchOp, skip bool, err error) {
	if found {
		return jsonPatchOp{Op: "add", Path: jsonPointerPath, Value: original}, false, nil
	}
	current, err := cs.DynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return jsonPatchOp{}, false, fmt.Errorf("re-fetching %s/%s to check current state before revert: %w", gvr.Resource, name, err)
	}
	_, currentlyFound := readNested(current.Object, fieldPath)
	if !currentlyFound {
		return jsonPatchOp{}, true, nil
	}
	return jsonPatchOp{Op: "remove", Path: jsonPointerPath}, false, nil
}

// applyFieldPatch is the shared capture/inject/hold/revert engine: reads the current
// value at fieldPath inside target (nil if absent), JSON-patches jsonPointerPath to
// newValue, holds for chaosDetails.ChaosDuration, then restores the original value (or
// removes the field if it was absent beforehand).
func applyFieldPatch(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails, gvr schema.GroupVersionResource, namespace, name string, target *unstructured.Unstructured, fieldPath []string, jsonPointerPath string, newValue interface{}) error {
	original, found := readNested(target.Object, fieldPath)

	injectOp := jsonPatchOp{Op: "replace", Path: jsonPointerPath, Value: newValue}
	if !found {
		injectOp.Op = "add"
	}
	injectPatch, err := json.Marshal([]jsonPatchOp{injectOp})
	if err != nil {
		return err
	}
	log.Infof("Injecting: patching %s to %v", jsonPointerPath, newValue)
	if err := JSONPatch(ctx, cs, gvr, namespace, name, injectPatch); err != nil {
		return fmt.Errorf("patching %s: %w", jsonPointerPath, err)
	}

	Sleep(ctx, chaosDetails.ChaosDuration)

	revertOp, skip, err := buildRevertOp(ctx, cs, gvr, namespace, name, fieldPath, jsonPointerPath, original, found)
	if err != nil {
		return err
	}
	if skip {
		log.Infof("Reverting: %s already back to absent, nothing to do", jsonPointerPath)
		return nil
	}
	revertPatch, err := json.Marshal([]jsonPatchOp{revertOp})
	if err != nil {
		return err
	}
	log.Infof("Reverting: restoring %s", jsonPointerPath)
	if err := JSONPatch(ctx, cs, gvr, namespace, name, revertPatch); err != nil {
		return fmt.Errorf("reverting %s: %w", jsonPointerPath, err)
	}
	return nil
}

// FieldSpec is one field to patch as part of a multi-field atomic patch (e.g. command +
// args together on the same container).
type FieldSpec struct {
	Path            []string    // path into the object, for reading the original value
	JSONPointerPath string      // RFC 6902 pointer for the patch operation
	NewValue        interface{} // value to inject
}

// applyMultiFieldPatch is applyFieldPatch generalized to N fields patched/reverted
// atomically in a single JSON-patch document (used when faults must change several
// fields on the same object together, e.g. command+args).
func applyMultiFieldPatch(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails, gvr schema.GroupVersionResource, namespace, name string, target *unstructured.Unstructured, fields []FieldSpec) error {
	type captured struct {
		original interface{}
		found    bool
	}
	origs := make([]captured, len(fields))
	injectOps := make([]jsonPatchOp, len(fields))
	for i, f := range fields {
		original, found := readNested(target.Object, f.Path)
		origs[i] = captured{original, found}
		op := "replace"
		if !found {
			op = "add"
		}
		injectOps[i] = jsonPatchOp{Op: op, Path: f.JSONPointerPath, Value: f.NewValue}
	}
	injectPatch, err := json.Marshal(injectOps)
	if err != nil {
		return err
	}
	log.Infof("Injecting: patching %d field(s)", len(fields))
	if err := JSONPatch(ctx, cs, gvr, namespace, name, injectPatch); err != nil {
		return fmt.Errorf("patching fields: %w", err)
	}

	Sleep(ctx, chaosDetails.ChaosDuration)

	var current *unstructured.Unstructured
	var revertOps []jsonPatchOp
	for i, f := range fields {
		if origs[i].found {
			revertOps = append(revertOps, jsonPatchOp{Op: "add", Path: f.JSONPointerPath, Value: origs[i].original})
			continue
		}
		// Re-check current presence rather than trusting pre-inject "found": the API
		// server's typed round-trip can silently drop an empty slice/map back to
		// "absent" (Go's `omitempty`) even though our own inject patch explicitly
		// added it, which would make a blind "remove" fail (RFC 6902 requires the
		// target to currently exist).
		if current == nil {
			current, err = cs.DynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("re-fetching %s/%s to check current state before revert: %w", gvr.Resource, name, err)
			}
		}
		if _, currentlyFound := readNested(current.Object, f.Path); currentlyFound {
			revertOps = append(revertOps, jsonPatchOp{Op: "remove", Path: f.JSONPointerPath})
		}
	}
	if len(revertOps) == 0 {
		log.Info("Reverting: all fields already back to absent, nothing to do")
		return nil
	}
	revertPatch, err := json.Marshal(revertOps)
	if err != nil {
		return err
	}
	log.Infof("Reverting: restoring %d field(s)", len(revertOps))
	if err := JSONPatch(ctx, cs, gvr, namespace, name, revertPatch); err != nil {
		return fmt.Errorf("reverting fields: %w", err)
	}
	return nil
}

// PatchContainerFields is PatchContainerField for multiple fields on the same container,
// applied and reverted atomically (e.g. command+args together).
func PatchContainerFields(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails, fields []ContainerFieldSpec) error {
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

	specs := make([]FieldSpec, len(fields))
	for i, f := range fields {
		specs[i] = FieldSpec{
			Path:            append([]string{"spec", "template", "spec", "containers", fmt.Sprintf("%d", idx)}, f.Path...),
			JSONPointerPath: fmt.Sprintf("/spec/template/spec/containers/%d/%s", idx, jsonPointerJoin(f.Path)),
			NewValue:        f.NewValue,
		}
	}
	return applyMultiFieldPatch(ctx, cs, chaosDetails, gvr, namespace, name, target, specs)
}

// ContainerFieldSpec is FieldSpec for PatchContainerFields, where Path is relative to the
// container object (see PatchContainerField).
type ContainerFieldSpec struct {
	Path     []string
	NewValue interface{}
}

// PatchWorkloadField resolves the target Deployment/StatefulSet via TARGETS and applies
// applyFieldPatch at the pod-template level (fieldPath rooted at "spec"...). Shared shape
// behind faults that patch a top-level pod-template field: replicas, dnsPolicy,
// nodeSelector, affinity.
func PatchWorkloadField(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails, fieldPath []string, jsonPointerPath string, newValue interface{}) error {
	if len(chaosDetails.AppDetail) == 0 {
		return fmt.Errorf("no target resolved: TARGETS env var was empty/unset")
	}
	gvr, err := WorkloadGVRForKind(chaosDetails.AppDetail[0].Kind)
	if err != nil {
		return err
	}
	return PatchField(ctx, cs, gvr, chaosDetails, fieldPath, jsonPointerPath, newValue)
}

// PatchField is PatchWorkloadField for an explicitly-given GVR -- used for target kinds
// that aren't a Deployment/StatefulSet (services, configmaps, secrets, HPAs,
// resourcequotas, ...).
func PatchField(ctx context.Context, cs clients.ClientSets, gvr schema.GroupVersionResource, chaosDetails *types.ChaosDetails, fieldPath []string, jsonPointerPath string, newValue interface{}) error {
	target, err := ResolveTarget(ctx, cs, gvr, chaosDetails)
	if err != nil {
		return err
	}
	name, namespace := target.GetName(), target.GetNamespace()
	log.Infof("Target resolved: %s/%s (ns=%s)", gvr.Resource, name, namespace)
	return applyFieldPatch(ctx, cs, chaosDetails, gvr, namespace, name, target, fieldPath, jsonPointerPath, newValue)
}

// MergeField is MergeWorkloadMapField for an explicitly-given GVR -- used for target
// kinds that aren't a Deployment/StatefulSet (e.g. Service selectors).
func MergeField(ctx context.Context, cs clients.ClientSets, gvr schema.GroupVersionResource, chaosDetails *types.ChaosDetails, fieldPath []string, key string, value interface{}) error {
	target, err := ResolveTarget(ctx, cs, gvr, chaosDetails)
	if err != nil {
		return err
	}
	name, namespace := target.GetName(), target.GetNamespace()
	log.Infof("Target resolved: %s/%s (ns=%s)", gvr.Resource, name, namespace)
	jsonPointerPath := "/" + jsonPointerJoin(fieldPath)

	original, found := readNested(target.Object, fieldPath)
	merged := map[string]interface{}{}
	if found {
		if m, ok := original.(map[string]interface{}); ok {
			for k, v := range m {
				merged[k] = v
			}
		}
	}
	merged[key] = value

	op := "replace"
	if !found {
		op = "add"
	}
	injectPatch, err := json.Marshal([]jsonPatchOp{{Op: op, Path: jsonPointerPath, Value: merged}})
	if err != nil {
		return err
	}
	log.Infof("Injecting: merging %s=%v into %s", key, value, jsonPointerPath)
	if err := JSONPatch(ctx, cs, gvr, namespace, name, injectPatch); err != nil {
		return fmt.Errorf("patching %s: %w", jsonPointerPath, err)
	}

	Sleep(ctx, chaosDetails.ChaosDuration)

	revertOp, skip, err := buildRevertOp(ctx, cs, gvr, namespace, name, fieldPath, jsonPointerPath, original, found)
	if err != nil {
		return err
	}
	if skip {
		log.Infof("Reverting: %s already back to absent, nothing to do", jsonPointerPath)
		return nil
	}
	revertPatch, err := json.Marshal([]jsonPatchOp{revertOp})
	if err != nil {
		return err
	}
	log.Infof("Reverting: restoring %s", jsonPointerPath)
	if err := JSONPatch(ctx, cs, gvr, namespace, name, revertPatch); err != nil {
		return fmt.Errorf("reverting %s: %w", jsonPointerPath, err)
	}
	return nil
}

// MergeWorkloadMapField reads the current map at fieldPath on the target
// Deployment/StatefulSet (empty map if absent), sets key=value in a copy of it, and
// replaces/adds the whole map, holding then reverting to the exact original (or removing
// the path if it was absent). Preserves sibling keys already present in the map -- e.g.
// merging affinity.podAntiAffinity without clobbering an existing nodeAffinity.
func MergeWorkloadMapField(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails, fieldPath []string, key string, value interface{}) error {
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
	jsonPointerPath := "/" + jsonPointerJoin(fieldPath)

	original, found := readNested(target.Object, fieldPath)
	merged := map[string]interface{}{}
	if found {
		if m, ok := original.(map[string]interface{}); ok {
			for k, v := range m {
				merged[k] = v
			}
		}
	}
	merged[key] = value

	op := "replace"
	if !found {
		op = "add"
	}
	injectPatch, err := json.Marshal([]jsonPatchOp{{Op: op, Path: jsonPointerPath, Value: merged}})
	if err != nil {
		return err
	}
	log.Infof("Injecting: merging %s=%v into %s", key, value, jsonPointerPath)
	if err := JSONPatch(ctx, cs, gvr, namespace, name, injectPatch); err != nil {
		return fmt.Errorf("patching %s: %w", jsonPointerPath, err)
	}

	Sleep(ctx, chaosDetails.ChaosDuration)

	revertOp, skip, err := buildRevertOp(ctx, cs, gvr, namespace, name, fieldPath, jsonPointerPath, original, found)
	if err != nil {
		return err
	}
	if skip {
		log.Infof("Reverting: %s already back to absent, nothing to do", jsonPointerPath)
		return nil
	}
	revertPatch, err := json.Marshal([]jsonPatchOp{revertOp})
	if err != nil {
		return err
	}
	log.Infof("Reverting: restoring %s", jsonPointerPath)
	if err := JSONPatch(ctx, cs, gvr, namespace, name, revertPatch); err != nil {
		return fmt.Errorf("reverting %s: %w", jsonPointerPath, err)
	}
	return nil
}

// PatchWorkloadFields is PatchWorkloadField for multiple pod-template fields patched and
// reverted atomically (e.g. dnsPolicy+dnsConfig together).
func PatchWorkloadFields(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails, fields []FieldSpec) error {
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
	log.Infof("Target resolved: %s/%s (ns=%s)", gvr.Resource, name, namespace)
	return applyMultiFieldPatch(ctx, cs, chaosDetails, gvr, namespace, name, target, fields)
}
