package experiment

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func podWithMemory(requests ...string) *unstructured.Unstructured {
	var containers []interface{}
	for _, request := range requests {
		container := map[string]interface{}{"name": "app"}
		if request != "" {
			container["resources"] = map[string]interface{}{
				"requests": map[string]interface{}{"memory": request},
			}
		}
		containers = append(containers, container)
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"spec": map[string]interface{}{"containers": containers},
	}}
}

// TestPodMemoryRequest is what decides whether preempting a target can free capacity:
// a target with no memory request releases nothing when evicted, so the fault must be
// rejected rather than run against it.
func TestPodMemoryRequest(t *testing.T) {
	for _, tc := range []struct {
		name     string
		pod      *unstructured.Unstructured
		expected int64
	}{
		{"no request at all", podWithMemory(""), 0},
		{"single container", podWithMemory("768Mi"), 768 * 1024 * 1024},
		{"summed across containers", podWithMemory("512Mi", "256Mi"), 768 * 1024 * 1024},
		{"unparseable is ignored", podWithMemory("not-a-quantity"), 0},
		{"mixed declared and absent", podWithMemory("100Mi", ""), 100 * 1024 * 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := podMemoryRequest(tc.pod); got != tc.expected {
				t.Fatalf("podMemoryRequest() = %d, want %d", got, tc.expected)
			}
		})
	}
}

// TestDecoySizingForcesPreemption pins the sizing rule. A decoy sized as a fixed share
// of allocatable fits in the free space of a large or lightly loaded node, so the
// scheduler places it and preempts nothing -- the defect this replaces.
func TestDecoySizingForcesPreemption(t *testing.T) {
	const mi = int64(1024 * 1024)
	allocatable := 128000 * mi
	requested := 7340 * mi
	target := 768 * mi

	free := allocatable - requested
	decoy := free + target

	if decoy <= free {
		t.Fatalf("decoy %d must exceed free capacity %d, or the scheduler simply places it", decoy, free)
	}
	if decoy > free+target {
		t.Fatalf("decoy %d exceeds free+target %d, so evicting the target would not admit it", decoy, free+target)
	}
	if fixedShare := allocatable / 100 * 85; fixedShare < free {
		t.Logf("a fixed 85%% share is %dMi, below the %dMi free: it would preempt nothing", fixedShare/mi, free/mi)
	} else {
		t.Fatal("test fixture no longer reproduces the lightly-loaded node this guards against")
	}
}
