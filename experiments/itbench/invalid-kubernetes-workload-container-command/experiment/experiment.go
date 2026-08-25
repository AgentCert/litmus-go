// Package experiment implements invalid-kubernetes-workload-container-command: overwrites
// the target container's command and args with an invalid combination (INVALID_COMMAND,
// INVALID_ARGS -- JSON array literals), holds, restores both fields atomically.
package experiment

import (
	"context"
	"encoding/json"
	"os"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/log"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/types"
)

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	var command, args []string
	if err := json.Unmarshal([]byte(os.Getenv("INVALID_COMMAND")), &command); err != nil {
		log.Errorf("invalid INVALID_COMMAND JSON: %v", err)
		return err
	}
	if err := json.Unmarshal([]byte(os.Getenv("INVALID_ARGS")), &args); err != nil {
		log.Errorf("invalid INVALID_ARGS JSON: %v", err)
		return err
	}
	return itbench.PatchContainerFields(ctx, cs, chaosDetails, []itbench.ContainerFieldSpec{
		{Path: []string{"command"}, NewValue: command},
		{Path: []string{"args"}, NewValue: args},
	})
}
