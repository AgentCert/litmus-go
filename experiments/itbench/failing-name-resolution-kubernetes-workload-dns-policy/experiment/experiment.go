// Package experiment implements failing-name-resolution-kubernetes-workload-dns-policy:
// sets the target pod template's dnsPolicy=None and dnsConfig.nameservers to an
// unreachable IP (BAD_DNS_SERVER), holds, restores both fields atomically.
package experiment

import (
	"context"
	"os"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	itbench "github.com/litmuschaos/litmus-go/pkg/itbench/common"
	"github.com/litmuschaos/litmus-go/pkg/types"
)

func Run(ctx context.Context, cs clients.ClientSets) {
	itbench.Run(ctx, cs, inject)
}

func inject(ctx context.Context, cs clients.ClientSets, chaosDetails *types.ChaosDetails) error {
	badDNSServer := os.Getenv("BAD_DNS_SERVER")
	return itbench.PatchWorkloadFields(ctx, cs, chaosDetails, []itbench.FieldSpec{
		{
			Path:            []string{"spec", "template", "spec", "dnsPolicy"},
			JSONPointerPath: "/spec/template/spec/dnsPolicy",
			NewValue:        "None",
		},
		{
			Path:            []string{"spec", "template", "spec", "dnsConfig"},
			JSONPointerPath: "/spec/template/spec/dnsConfig",
			NewValue:        map[string]interface{}{"nameservers": []string{badDNSServer}},
		},
	})
}
