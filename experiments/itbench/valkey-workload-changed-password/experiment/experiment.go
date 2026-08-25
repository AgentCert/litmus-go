// Package experiment implements valkey-workload-changed-password: sets the credentials
// Secret's password to an invalid value (creating the Secret if it doesn't exist), and
// forces the target container to require that password via
// `valkey-server --requirepass $(VALKEY_PASSWORD)` (command+args+env patched together),
// holds, then restores both the container fields and the Secret to their originals.
package experiment

import (
	"context"
	"fmt"
	"os"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/log"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/types"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	secretName := os.Getenv("VALKEY_SECRET_NAME")
	secretKey := os.Getenv("VALKEY_SECRET_KEY")
	faultPassword := os.Getenv("FAULT_PASSWORD")

	if len(chaosDetails.AppDetail) == 0 {
		return fmt.Errorf("no target resolved: TARGETS env var was empty/unset")
	}
	namespace := chaosDetails.AppDetail[0].Namespace

	// --- Secret: set (or create) the invalid password, remembering original state ---
	secretClient := cs.DynamicClient.Resource(itbench.GVRSecrets).Namespace(namespace)
	existingSecret, getErr := secretClient.Get(ctx, secretName, metav1.GetOptions{})
	secretExisted := getErr == nil
	if getErr != nil && !k8serrors.IsNotFound(getErr) {
		return getErr
	}
	var origSecretValue string
	if secretExisted {
		origSecretValue, _, _ = unstructured.NestedString(existingSecret.Object, "data", secretKey)
		log.Infof("Injecting: setting %s/%s to an invalid password", secretName, secretKey)
		if err := unstructured.SetNestedField(existingSecret.Object, faultPassword, "stringData", secretKey); err != nil {
			return err
		}
		if _, err := secretClient.Update(ctx, existingSecret, metav1.UpdateOptions{}); err != nil {
			return err
		}
	} else {
		log.Infof("Injecting: creating Secret %s/%s (did not exist)", secretName, secretKey)
		secret := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Secret",
			"metadata":   map[string]interface{}{"name": secretName, "namespace": namespace},
			"stringData": map[string]interface{}{secretKey: faultPassword},
		}}
		if _, err := secretClient.Create(ctx, secret, metav1.CreateOptions{}); err != nil {
			return err
		}
	}

	// --- Container: force valkey-server --requirepass $(VALKEY_PASSWORD) ---
	envVar := map[string]interface{}{
		"name": "VALKEY_PASSWORD",
		"valueFrom": map[string]interface{}{
			"secretKeyRef": map[string]interface{}{"name": secretName, "key": secretKey},
		},
	}
	err := itbench.PatchContainerFields(ctx, cs, chaosDetails, []itbench.ContainerFieldSpec{
		{Path: []string{"command"}, NewValue: []string{"valkey-server"}},
		{Path: []string{"args"}, NewValue: []string{"--requirepass", "$(VALKEY_PASSWORD)"}},
		{Path: []string{"env"}, NewValue: []interface{}{envVar}},
	})
	if err != nil {
		return err
	}

	// --- Revert Secret ---
	if secretExisted {
		log.Infof("Reverting: restoring original %s/%s value", secretName, secretKey)
		current, err := secretClient.Get(ctx, secretName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if err := unstructured.SetNestedField(current.Object, origSecretValue, "data", secretKey); err != nil {
			return err
		}
		if _, err := secretClient.Update(ctx, current, metav1.UpdateOptions{}); err != nil {
			return err
		}
	} else {
		log.Infof("Reverting: deleting Secret %s (did not exist before injection)", secretName)
		if err := secretClient.Delete(ctx, secretName, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
