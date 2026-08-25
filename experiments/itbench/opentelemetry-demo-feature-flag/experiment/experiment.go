// Package experiment implements opentelemetry-demo-feature-flag: flips one flag's
// defaultVariant (FLAG_NAME, FLAG_STATE) inside the flagd-config ConfigMap's opaque
// demo.flagd.json data key, holds, restores the original variant (or FLAG_REVERT_STATE
// if the flag had no defaultVariant set originally). Uses encoding/json rather than the
// original script's awk-based text surgery -- structurally equivalent, much less fragile.
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

const configMapName = "flagd-config"
const dataKey = "demo.flagd.json"

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

// appkind is CRD-validated to a fixed enum and rejects "configmap", so this fault always
// hardcodes the real target kind, trusting TARGETS only for namespace.
func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	flagName := os.Getenv("FLAG_NAME")
	flagState := os.Getenv("FLAG_STATE")
	flagRevertState := os.Getenv("FLAG_REVERT_STATE")

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

	origVariant, injected, err := setDefaultVariant(current, flagName, flagState)
	if err != nil {
		return err
	}
	if origVariant == "" {
		origVariant = flagRevertState
	}
	log.Infof("flag=%s originalDefaultVariant=%s", flagName, origVariant)

	log.Infof("Injecting: setting %s.defaultVariant=%s", flagName, flagState)
	if err := patchConfigMapData(ctx, cmClient, configMapName, injected); err != nil {
		return err
	}

	itbench.Sleep(ctx, chaosDetails.ChaosDuration)

	_, reverted, err := setDefaultVariant(injected, flagName, origVariant)
	if err != nil {
		return err
	}
	log.Infof("Reverting: restoring %s.defaultVariant=%s", flagName, origVariant)
	return patchConfigMapData(ctx, cmClient, configMapName, reverted)
}

// setDefaultVariant parses flagdJSON, sets flags[flagName].defaultVariant=newVariant, and
// returns (previous variant, re-serialized JSON). Fails if flagName isn't present.
func setDefaultVariant(flagdJSON, flagName, newVariant string) (string, string, error) {
	var doc map[string]interface{}
	if err := json.Unmarshal([]byte(flagdJSON), &doc); err != nil {
		return "", "", fmt.Errorf("parsing demo.flagd.json: %w", err)
	}
	flags, ok := doc["flags"].(map[string]interface{})
	if !ok {
		return "", "", fmt.Errorf("demo.flagd.json has no top-level \"flags\" object")
	}
	flag, ok := flags[flagName].(map[string]interface{})
	if !ok {
		return "", "", fmt.Errorf("flag %q not found in demo.flagd.json flags map", flagName)
	}
	origVariant, _ := flag["defaultVariant"].(string)
	flag["defaultVariant"] = newVariant

	out, err := json.Marshal(doc)
	if err != nil {
		return "", "", err
	}
	return origVariant, string(out), nil
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
