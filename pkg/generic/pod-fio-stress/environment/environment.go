package environment

import (
	"strconv"

	clientTypes "k8s.io/apimachinery/pkg/types"

	experimentTypes "github.com/litmuschaos/litmus-go/pkg/generic/pod-fio-stress/types"
	"github.com/litmuschaos/litmus-go/pkg/types"
)

// GetENV fetches all the env variables from the runner pod
func GetENV(experimentDetails *experimentTypes.ExperimentDetails) {
	experimentDetails.ExperimentName = types.Getenv("EXPERIMENT_NAME", "")
	experimentDetails.ChaosNamespace = types.Getenv("CHAOS_NAMESPACE", "litmus")
	experimentDetails.EngineName = types.Getenv("CHAOSENGINE", "")
	experimentDetails.ChaosDuration, _ = strconv.Atoi(types.Getenv("TOTAL_CHAOS_DURATION", "30"))
	experimentDetails.ChaosInterval, _ = strconv.Atoi(types.Getenv("CHAOS_INTERVAL", "10"))
	experimentDetails.RampTime, _ = strconv.Atoi(types.Getenv("RAMP_TIME", "0"))
	experimentDetails.ChaosUID = clientTypes.UID(types.Getenv("CHAOS_UID", ""))
	experimentDetails.InstanceID = types.Getenv("INSTANCE_ID", "")
	experimentDetails.ChaosPodName = types.Getenv("POD_NAME", "")
	experimentDetails.Delay, _ = strconv.Atoi(types.Getenv("STATUS_CHECK_DELAY", "2"))
	experimentDetails.Timeout, _ = strconv.Atoi(types.Getenv("STATUS_CHECK_TIMEOUT", "180"))
	experimentDetails.ChaosKillCmd = types.Getenv("CHAOS_KILL_COMMAND", "killall fio")
	experimentDetails.TargetContainer = types.Getenv("TARGET_CONTAINER", "")
	experimentDetails.TargetPods = types.Getenv("TARGET_PODS", "")
	experimentDetails.PodsAffectedPerc, _ = strconv.Atoi(types.Getenv("PODS_AFFECTED_PERC", "0"))
	// SEQUENCE must default to "parallel" as every other fault does: the consuming
	// switch in chaoslib/litmus/pod-fio-stress/lib accepts only "serial"/"parallel"
	// and returns "'' sequence is not supported" on anything else, and this fault has
	// no SetChaosTunables/GetRandomSequence call to backfill it — so an unset value
	// aborted the experiment before it injected anything.
	experimentDetails.Sequence = types.Getenv("SEQUENCE", "parallel")
	// Every value below is interpolated unvalidated into the fio command line, where
	// an empty string or a zero yields `fio --ioengine= --iodepth=0 --bs= --size=M
	// --numjobs=0`. NOTE: this fault has no chart under chaos-charts/faults/ and no
	// entry in the hub catalog, so there is no ChaosExperiment CR to source defaults
	// from — these mirror the only in-repo reference, the upstream test manifest at
	// experiments/generic/pod-fio-stress/test/test.yml.
	experimentDetails.IOEngine = types.Getenv("IO_ENGINE", "libaio")
	experimentDetails.IODepth, _ = strconv.Atoi(types.Getenv("IO_DEPTH", "1"))
	experimentDetails.ReadWrite = types.Getenv("READ_WRITE_MODE", "randwrite")
	experimentDetails.BlockSize = types.Getenv("BLOCK_SIZE", "4k")
	experimentDetails.Size = types.Getenv("SIZE", "5120")
	experimentDetails.NumJobs, _ = strconv.Atoi(types.Getenv("NUMBER_OF_JOBS", "2"))
	experimentDetails.GroupReporting, _ = strconv.ParseBool(types.Getenv("GROUP_REPORTING", "true"))
}
