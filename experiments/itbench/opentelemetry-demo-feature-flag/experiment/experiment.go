// Package experiment implements opentelemetry-demo-feature-flag: flips one flag's
// defaultVariant (FLAG_NAME, FLAG_STATE) inside the flagd-config ConfigMap's opaque
// demo.flagd.json data key, holds, restores the exact original defaultVariant, including absence. Uses encoding/json rather than the
// original script's awk-based text surgery -- structurally equivalent, much less fragile.
package experiment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/log"
	"github.com/litmuschaos/litmus-go/pkg/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

// flagd serves the flags. Its init container copies the ConfigMap into an
// emptyDir once at pod start and flagd reads only that copy, so a ConfigMap
// edit takes effect only when the pod is recreated. The names are shared with
// the verification lifecycle, which observes the same two objects.
const configMapName = itbench.FlagdConfigMap
const dataKey = itbench.FlagdDataKey
const flagdDeployment = itbench.FlagdDeployment

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

// appkind is CRD-validated to a fixed enum and rejects "configmap", so this fault always
// hardcodes the real target kind, trusting TARGETS only for namespace.
func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) (retErr error) {
	if len(chaosDetails.AppDetail) == 0 {
		return fmt.Errorf("no target resolved: TARGETS env var was empty/unset")
	}
	flagName := os.Getenv("FLAG_NAME")
	flagState := os.Getenv("FLAG_STATE")

	cmClient := cs.DynamicClient.Resource(itbench.GVRConfigMaps).Namespace(chaosDetails.AppDetail[0].Namespace)
	cm, err := cmClient.Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	current, found, err := unstructured.NestedString(cm.Object, "data", dataKey)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("ConfigMap %s has no %q data key", configMapName, dataKey)
	}

	origVariant, origFound, injected, err := setDefaultVariant(current, flagName, flagState)
	if err != nil {
		return err
	}
	if origFound && origVariant == flagState {
		return fmt.Errorf("flag %q already selects variant %q", flagName, flagState)
	}
	namespace := chaosDetails.AppDetail[0].Namespace
	deployment, err := cs.DynamicClient.Resource(itbench.GVRDeployments).Namespace(namespace).Get(ctx, flagdDeployment, metav1.GetOptions{})
	if err != nil {
		return err
	}
	originalAnnotations, annotationsFound, err := unstructured.NestedStringMap(deployment.Object, "spec", "template", "metadata", "annotations")
	if err != nil {
		return err
	}
	log.Infof("flag=%s originalDefaultVariant=%s", flagName, origVariant)

	log.Infof("Injecting: setting %s.defaultVariant=%s", flagName, flagState)
	if err := patchConfigMapData(ctx, cmClient, configMapName, injected); err != nil {
		return err
	}
	defer func() {
		revertCtx, cancel := itbench.RevertContext()
		defer cancel()
		latest, err := cmClient.Get(revertCtx, configMapName, metav1.GetOptions{})
		if err != nil {
			retErr = errors.Join(retErr, err)
			return
		}
		data, _, err := unstructured.NestedString(latest.Object, "data", dataKey)
		if err != nil {
			retErr = errors.Join(retErr, err)
			return
		}
		restored, err := restoreDefaultVariant(data, flagName, origVariant, origFound)
		if err != nil {
			retErr = errors.Join(retErr, err)
			return
		}
		if err := patchConfigMapData(revertCtx, cmClient, configMapName, restored); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("restoring feature flag: %w", err))
			return
		}
		rc := cs.DynamicClient.Resource(itbench.GVRDeployments).Namespace(namespace)
		latestDeployment, err := rc.Get(revertCtx, flagdDeployment, metav1.GetOptions{})
		if err == nil {
			if annotationsFound {
				err = unstructured.SetNestedStringMap(latestDeployment.Object, originalAnnotations, "spec", "template", "metadata", "annotations")
			} else {
				unstructured.RemoveNestedField(latestDeployment.Object, "spec", "template", "metadata", "annotations")
			}
			if err == nil {
				_, err = rc.Update(revertCtx, latestDeployment, metav1.UpdateOptions{})
			}
		}
		retErr = errors.Join(retErr, err)
	}()

	if err := restartFlagd(ctx, cs, namespace); err != nil {
		return fmt.Errorf("flag written but flagd was not restarted to load it: %w", err)
	}

	itbench.HoldChaos(ctx, chaosDetails)

	return nil
}

// restartFlagd recreates the flagd pod (as `kubectl rollout restart` does) so
// its init container copies the current ConfigMap.
func restartFlagd(ctx context.Context, cs clients.ClientSets, namespace string) error {
	rc := cs.DynamicClient.Resource(itbench.GVRDeployments).Namespace(namespace)
	obj, err := rc.Get(ctx, flagdDeployment, metav1.GetOptions{})
	if err != nil {
		return err
	}
	annotations, _, _ := unstructured.NestedStringMap(obj.Object, "spec", "template", "metadata", "annotations")
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339Nano)
	if err := unstructured.SetNestedStringMap(obj.Object, annotations, "spec", "template", "metadata", "annotations"); err != nil {
		return err
	}
	if _, err := rc.Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
		return err
	}
	log.Infof("Restarted deployment %s/%s to load the flag configuration", namespace, flagdDeployment)
	return nil
}

// setDefaultVariant parses flagdJSON, sets flags[flagName].defaultVariant=newVariant, and
// returns the previous variant, whether it was present at all, and the re-serialized JSON.
// Fails if flagName isn't present.
func setDefaultVariant(flagdJSON, flagName, newVariant string) (origVariant string, origFound bool, out string, err error) {
	var doc map[string]interface{}
	if err := json.Unmarshal([]byte(flagdJSON), &doc); err != nil {
		return "", false, "", fmt.Errorf("parsing demo.flagd.json: %w", err)
	}
	flags, ok := doc["flags"].(map[string]interface{})
	if !ok {
		return "", false, "", fmt.Errorf("demo.flagd.json has no top-level \"flags\" object")
	}
	flag, ok := flags[flagName].(map[string]interface{})
	if !ok {
		return "", false, "", fmt.Errorf("flag %q not found in demo.flagd.json flags map", flagName)
	}
	origVariant, origFound = flag["defaultVariant"].(string)
	variants, ok := flag["variants"].(map[string]interface{})
	if !ok {
		return "", false, "", fmt.Errorf("flag %q has no variants", flagName)
	}
	if _, exists := variants[newVariant]; !exists {
		return "", false, "", fmt.Errorf("variant %q does not exist for flag %q", newVariant, flagName)
	}
	flag["defaultVariant"] = newVariant

	b, err := json.Marshal(doc)
	if err != nil {
		return "", false, "", err
	}
	return origVariant, origFound, string(b), nil
}

func patchConfigMapData(ctx context.Context, cmClient dynamic.ResourceInterface, name, value string) error {
	cm, err := cmClient.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := unstructured.SetNestedField(cm.Object, value, "data", dataKey); err != nil {
		return err
	}
	_, err = cmClient.Update(ctx, cm, metav1.UpdateOptions{})
	return err
}

func restoreDefaultVariant(data, name, value string, existed bool) (string, error) {
	var doc map[string]interface{}
	if err := json.Unmarshal([]byte(data), &doc); err != nil {
		return "", err
	}
	flags, _ := doc["flags"].(map[string]interface{})
	flag, ok := flags[name].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("cannot restore missing flag %q", name)
	}
	if existed {
		flag["defaultVariant"] = value
	} else {
		delete(flag, "defaultVariant")
	}
	encoded, err := json.Marshal(doc)
	return string(encoded), err
}
