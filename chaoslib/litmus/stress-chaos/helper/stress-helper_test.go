package helper

import (
	"strings"
	"testing"

	experimentTypes "github.com/litmuschaos/litmus-go/pkg/generic/stress-chaos/types"
)

// stressorFor builds the stress-ng command exactly as prepareStressChaos does, so
// these tests assert on the string that is actually handed to the shell.
func stressorFor(d *experimentTypes.ExperimentDetails) string {
	return strings.Join(prepareStressor(d), " ")
}

// findMalformedArg reports the first option in a joined stress-ng command whose
// value is missing, is a bare unit suffix, or still contains a format verb — the
// generalised form of every argument bug this package has had. It returns "" when
// the command is well formed.
//
// Every option this package appends takes a value, so a flag followed by another
// flag or by end-of-string is unambiguously malformed.
//
// Deliberately written as token logic rather than a regexp: the obvious pattern
// (`--[a-z-]+\s+[%GM]\b`) silently fails to match "--hdd-bytes %", because \b
// needs a word character on one side and "%" at end-of-string has none. That
// would have made this guard pass on the exact output it exists to reject.
func findMalformedArg(cmd string) string {
	unitOnly := map[string]bool{"%": true, "G": true, "M": true, "g": true, "m": true, "k": true, "K": true}

	tokens := strings.Fields(cmd)
	for i, tok := range tokens {
		if !strings.HasPrefix(tok, "--") {
			continue
		}
		if strings.Contains(tok, "%v") {
			return tok + " (unexpanded format verb)"
		}
		if i+1 >= len(tokens) {
			return tok + " (no value)"
		}
		next := tokens[i+1]
		if strings.HasPrefix(next, "--") {
			return tok + " (immediately followed by " + next + ")"
		}
		if unitOnly[next] {
			return tok + " " + next + " (unit suffix with no number)"
		}
		if strings.Contains(next, "%v") {
			return tok + " " + next + " (unexpanded format verb)"
		}
	}
	return ""
}

// TestFindMalformedArg_CatchesHistoricalRegressions proves the guard above
// actually rejects each command this package has really produced. Without this,
// a guard that quietly matches nothing would make every other test here vacuous.
func TestFindMalformedArg_CatchesHistoricalRegressions(t *testing.T) {
	mustCatch := []string{
		// "" vs "0" sentinel mismatch on the filesystem tunables.
		"stress-ng --timeout 60s --io 4 --hdd 4 --hdd-bytes %",
		// Same bug in bytes mode.
		"stress-ng --timeout 60s --io 4 --hdd 4 --hdd-bytes G",
		// Empty MEMORY_CONSUMPTION.
		"stress-ng --timeout 60s --vm 4 --vm-bytes M",
		// append() misused as Sprintf.
		"stress-ng --timeout 60s --hdd-bytes 10% --cpu %v 2",
		// Empty NUMBER_OF_WORKERS leaves a flag with no value.
		"stress-ng --timeout 60s --io --hdd --hdd-bytes 10%",
		// Empty VOLUME_MOUNT_PATH.
		"stress-ng --timeout 60s --hdd-bytes 10% --temp-path",
	}
	for _, cmd := range mustCatch {
		if got := findMalformedArg(cmd); got == "" {
			t.Errorf("guard failed to reject a known-bad command: %s", cmd)
		}
	}

	mustAccept := []string{
		"stress-ng --timeout 60s --io 4 --hdd 4 --hdd-bytes 10%",
		"stress-ng --timeout 60s --io 4 --hdd 4 --hdd-bytes 2G",
		"stress-ng --timeout 60s --vm 4 --vm-bytes 500M",
		"stress-ng --timeout 60s --cpu 1 --cpu-load 100",
		"stress-ng --timeout 60s --hdd-bytes 10% --temp-path /tmp",
		"stress-ng --timeout 60s --hdd-bytes 10% --cpu 2",
	}
	for _, cmd := range mustAccept {
		if got := findMalformedArg(cmd); got != "" {
			t.Errorf("guard wrongly rejected a valid command %q: %s", cmd, got)
		}
	}
}

func TestPrepareStressor_NoMalformedArguments(t *testing.T) {
	// Every combination below has produced, or could produce, an argument with a
	// missing value. The empty-string cases matter most: the helper pod's own
	// getENV defaults every tunable to "", and common.ENVDetails.SetEnv drops
	// empty values entirely, so "" is what the helper actually sees in practice.
	cases := []struct {
		name string
		d    experimentTypes.ExperimentDetails
	}{
		{"io/all-empty", experimentTypes.ExperimentDetails{StressType: "pod-io-stress"}},
		{"io/zero-sentinels", experimentTypes.ExperimentDetails{
			StressType: "pod-io-stress", FilesystemUtilizationPercentage: "0", FilesystemUtilizationBytes: "0", NumberOfWorkers: "0"}},
		{"io/bytes-only", experimentTypes.ExperimentDetails{
			StressType: "pod-io-stress", FilesystemUtilizationBytes: "2"}},
		{"io/percentage-only", experimentTypes.ExperimentDetails{
			StressType: "pod-io-stress", FilesystemUtilizationPercentage: "25"}},
		{"io/both", experimentTypes.ExperimentDetails{
			StressType: "pod-io-stress", FilesystemUtilizationPercentage: "25", FilesystemUtilizationBytes: "2"}},
		{"io/with-cpu-cores", experimentTypes.ExperimentDetails{
			StressType: "pod-io-stress", FilesystemUtilizationPercentage: "10", CPUcores: "2"}},
		{"io/with-volume-mount", experimentTypes.ExperimentDetails{
			StressType: "pod-io-stress", FilesystemUtilizationPercentage: "10", VolumeMountPath: "/tmp"}},
		{"cpu/all-empty", experimentTypes.ExperimentDetails{StressType: "pod-cpu-stress"}},
		{"cpu/populated", experimentTypes.ExperimentDetails{
			StressType: "pod-cpu-stress", CPUcores: "2", CPULoad: "80"}},
		{"memory/all-empty", experimentTypes.ExperimentDetails{StressType: "pod-memory-stress"}},
		{"memory/populated", experimentTypes.ExperimentDetails{
			StressType: "pod-memory-stress", NumberOfWorkers: "2", MemoryConsumption: "256"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := stressorFor(&tc.d)

			if m := findMalformedArg(got); m != "" {
				t.Errorf("argument with a missing value %q in: %s", m, got)
			}
			if strings.Contains(got, "%v") {
				t.Errorf("unexpanded format verb (append() misused as Sprintf) in: %s", got)
			}
			if strings.Contains(got, "  ") {
				t.Errorf("double space (stray leading space on an argument) in: %s", got)
			}
			if !strings.HasPrefix(got, "stress-ng --timeout ") {
				t.Errorf("missing stress-ng invocation or timeout in: %s", got)
			}
		})
	}
}

func TestPrepareStressor_IOFilesystemTunables(t *testing.T) {
	// The charts document the empty string as the way to deselect one of these two
	// mutually exclusive tunables, while the branch logic compares against "0".
	// Both spellings must mean "not provided" and resolve identically.
	cases := []struct {
		name       string
		percentage string
		bytes      string
		want       string
	}{
		{"neither provided (empty)", "", "", "--hdd-bytes 10%"},
		{"neither provided (zero)", "0", "0", "--hdd-bytes 10%"},
		{"percentage only, bytes empty", "25", "", "--hdd-bytes 25%"},
		{"percentage only, bytes zero", "25", "0", "--hdd-bytes 25%"},
		// The documented bytes-mode path: chart comment says percentage "should be
		// empty". This produced "--hdd-bytes %" before the fix.
		{"bytes only, percentage empty", "", "2", "--hdd-bytes 2G"},
		{"bytes only, percentage zero", "0", "2", "--hdd-bytes 2G"},
		{"both provided, percentage wins", "25", "2", "--hdd-bytes 25%"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := stressorFor(&experimentTypes.ExperimentDetails{
				StressType:                      "pod-io-stress",
				FilesystemUtilizationPercentage: tc.percentage,
				FilesystemUtilizationBytes:      tc.bytes,
			})
			if !strings.Contains(got, tc.want) {
				t.Errorf("want %q in stressor, got: %s", tc.want, got)
			}
		})
	}
}

// TestPrepareStressor_CPUCoresIsOneArgument is the direct regression test for
// `append(stressArgs, "--cpu %v", cores)`, which pushed two separate slice
// elements and produced `stress-ng ... --cpu %v 2`.
func TestPrepareStressor_CPUCoresIsOneArgument(t *testing.T) {
	args := prepareStressor(&experimentTypes.ExperimentDetails{
		StressType:                      "pod-io-stress",
		FilesystemUtilizationPercentage: "10",
		CPUcores:                        "2",
	})

	var found bool
	for _, a := range args {
		if a == "--cpu 2" {
			found = true
		}
		if a == "--cpu %v" || a == "2" {
			t.Fatalf("--cpu was appended as two elements: %#v", args)
		}
	}
	if !found {
		t.Errorf("expected a single %q argument, got: %#v", "--cpu 2", args)
	}
}

// An unset or "0" CPU_CORES must add no --cpu flag at all. The helper's getENV
// defaults CPU_CORES to "", so the guard has to cover both spellings.
func TestPrepareStressor_CPUCoresOmittedWhenUnset(t *testing.T) {
	for _, cores := range []string{"", "0"} {
		got := stressorFor(&experimentTypes.ExperimentDetails{
			StressType:                      "pod-io-stress",
			FilesystemUtilizationPercentage: "10",
			CPUcores:                        cores,
		})
		if strings.Contains(got, "--cpu") {
			t.Errorf("CPUcores=%q should add no --cpu flag, got: %s", cores, got)
		}
	}
}

func TestPrepareStressor_VolumeMountPathAddsTempPath(t *testing.T) {
	base := experimentTypes.ExperimentDetails{
		StressType:                      "pod-io-stress",
		FilesystemUtilizationPercentage: "10",
	}

	if got := stressorFor(&base); strings.Contains(got, "--temp-path") {
		t.Errorf("no VolumeMountPath should mean no --temp-path, got: %s", got)
	}

	// A whitespace-only path must be treated as unset rather than producing
	// `--temp-path ` with an empty value.
	blank := base
	blank.VolumeMountPath = "   "
	if got := stressorFor(&blank); strings.Contains(got, "--temp-path") {
		t.Errorf("whitespace-only VolumeMountPath should be treated as unset, got: %s", got)
	}

	set := base
	set.VolumeMountPath = "/tmp"
	if got := stressorFor(&set); !strings.Contains(got, "--temp-path /tmp") {
		t.Errorf("want --temp-path /tmp, got: %s", got)
	}
}

func TestUnsetToZero(t *testing.T) {
	for in, want := range map[string]string{
		"": "0", "   ": "0", "0": "0", "10": "10", " 10 ": "10",
	} {
		if got := unsetToZero(in); got != want {
			t.Errorf("unsetToZero(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestOrDefault(t *testing.T) {
	for in, want := range map[string]string{
		"": "fallback", "  ": "fallback", "5": "5", " 5 ": "5",
		// "0" is a real, explicit value here and must not be replaced.
		"0": "0",
	} {
		if got := orDefault(in, "fallback"); got != want {
			t.Errorf("orDefault(%q, %q) = %q, want %q", in, "fallback", got, want)
		}
	}
}
