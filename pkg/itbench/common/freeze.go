package common

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var containerIDPattern = regexp.MustCompile(`^[a-f0-9]{32,128}$`)

// RunFreeze runs in a node-pinned hostPID helper. Signals come from the ancestor
// PID namespace, and process membership is checked against the exact container
// cgroup. Neither PID 1 inside an ephemeral container nor a guessed PID is used.
func RunFreeze(ctx context.Context, containerID string, maximum time.Duration) (retErr error) {
	if !containerIDPattern.MatchString(containerID) || maximum <= 0 {
		return fmt.Errorf("freeze requires a full container ID and a positive safety deadline")
	}
	self, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return err
	}
	if strings.Contains(string(self), containerID) {
		return fmt.Errorf("refusing to freeze the helper's own container")
	}
	processes, err := os.ReadDir("/proc")
	if err != nil {
		return err
	}
	var stopped []int
	defer func() {
		for _, pid := range stopped {
			if !processBelongsToContainer(pid, containerID) {
				continue
			} // PID reuse or a replaced container.
			if err := syscall.Kill(pid, syscall.SIGCONT); err != nil && !errors.Is(err, syscall.ESRCH) {
				retErr = errors.Join(retErr, err)
			}
		}
		message := fmt.Sprintf(`{"thawed":%t}`, retErr == nil)
		fmt.Println(message)
		if err := os.WriteFile("/dev/termination-log", []byte(message), 0600); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}()
	for _, process := range processes {
		pid, err := strconv.Atoi(process.Name())
		if err != nil || pid <= 1 || !processBelongsToContainer(pid, containerID) {
			continue
		}
		if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
			if errors.Is(err, syscall.ESRCH) {
				continue
			}
			return fmt.Errorf("stopping container process: %w", err)
		}
		stopped = append(stopped, pid)
	}
	if len(stopped) == 0 {
		return fmt.Errorf("no processes matched the exact target container cgroup")
	}
	if err := WaitForCheck(ctx, 5*time.Second, "target process freeze", func(context.Context) error {
		for _, pid := range stopped {
			status, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "status"))
			if err != nil {
				return err
			}
			if !stoppedState(string(status)) {
				return fmt.Errorf("target process has not stopped")
			}
		}
		return nil
	}); err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	if err := encoder.Encode(map[string]interface{}{"frozen": true, "processes": len(stopped)}); err != nil {
		return err
	}
	// Independent timeout guarantees thaw if the experiment pod disappears.
	deadline := time.NewTimer(maximum)
	defer deadline.Stop()
	select {
	case <-ctx.Done():
	case <-deadline.C:
	}
	return nil
}

func processBelongsToContainer(pid int, containerID string) bool {
	value, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cgroup"))
	return err == nil && strings.Contains(string(value), containerID)
}

func stoppedState(status string) bool {
	for _, line := range strings.Split(status, "\n") {
		if strings.HasPrefix(line, "State:") {
			fields := strings.Fields(line)
			return len(fields) > 1 && (fields[1] == "T" || fields[1] == "t")
		}
	}
	return false
}
