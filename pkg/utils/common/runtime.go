package common

import (
	"context"
	"strings"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/litmuschaos/litmus-go/pkg/clients"
	"github.com/litmuschaos/litmus-go/pkg/log"
)

// Container-runtime socket paths. Every chaos-charts fault.yaml ships
// DefaultContainerdSocket as its SOCKET_PATH default, which is correct for KinD,
// kubeadm and most managed distributions — but wrong on k3s and k3d, where the
// kubelet serves pods from a namespaced socket of its own. Querying the generic
// path on such a node succeeds in connecting yet reports "container not found"
// for every container ID, so PID resolution fails and every helper-pod fault
// (stress, network, dns, http, disk-fill, container-kill) dies before injecting.
const (
	DefaultContainerdSocket = "/run/containerd/containerd.sock"
	K3sContainerdSocket     = "/run/k3s/containerd/containerd.sock"
	DefaultCRIOSocket       = "/run/crio/crio.sock"
	DefaultDockerSocket     = "/var/run/docker.sock"
)

// ResolveSocketPath decides which container-runtime socket a helper pod should
// bind-mount, given the configured SOCKET_PATH and the runtime string the node
// itself reports in status.nodeInfo.containerRuntimeVersion (for example
// "containerd://2.3.2-k3s2", "containerd://1.7.1", "cri-o://1.29.0").
//
// An explicitly configured path that differs from the shipped default is always
// honoured — an operator who set SOCKET_PATH deliberately must win. Correction
// only applies when the caller is still on the shipped default, which is the
// case that cannot have been a deliberate choice for this node.
//
// Kept as a pure function of two strings so it is unit-testable without a
// cluster; NodeRuntimeVersion supplies the second argument at runtime.
func ResolveSocketPath(configured, runtimeVersion string) string {
	configured = strings.TrimSpace(configured)

	// An explicit, non-default override wins outright.
	if configured != "" && configured != DefaultContainerdSocket {
		return configured
	}

	rv := strings.ToLower(strings.TrimSpace(runtimeVersion))
	switch {
	case rv == "":
		// Nothing to go on — keep whatever we had rather than guessing.
		if configured == "" {
			return DefaultContainerdSocket
		}
		return configured
	case strings.Contains(rv, "k3s"):
		return K3sContainerdSocket
	case strings.HasPrefix(rv, "cri-o"), strings.HasPrefix(rv, "crio"):
		return DefaultCRIOSocket
	case strings.HasPrefix(rv, "docker"):
		return DefaultDockerSocket
	default:
		return DefaultContainerdSocket
	}
}

// NodeRuntimeVersion returns the given node's reported
// status.nodeInfo.containerRuntimeVersion. An empty string with a nil error means
// the node exists but reported nothing usable.
func NodeRuntimeVersion(ctx context.Context, cs clients.ClientSets, nodeName string) (string, error) {
	if nodeName == "" {
		return "", nil
	}
	node, err := cs.KubeClient.CoreV1().Nodes().Get(ctx, nodeName, v1.GetOptions{})
	if err != nil {
		return "", err
	}
	return node.Status.NodeInfo.ContainerRuntimeVersion, nil
}

// ResolveSocketPathForNode is the convenience wrapper the fault libs call before
// building a helper pod. It never fails the experiment: if the node cannot be
// read it logs and returns the configured value unchanged, leaving behaviour
// exactly as it was before this detection existed.
func ResolveSocketPathForNode(ctx context.Context, cs clients.ClientSets, nodeName, configured string) string {
	runtimeVersion, err := NodeRuntimeVersion(ctx, cs, nodeName)
	if err != nil {
		log.Warnf("could not read container runtime of node %q, using SOCKET_PATH %q as configured: %v", nodeName, configured, err)
		if strings.TrimSpace(configured) == "" {
			return DefaultContainerdSocket
		}
		return configured
	}

	resolved := ResolveSocketPath(configured, runtimeVersion)
	if resolved != strings.TrimSpace(configured) {
		log.Infof("node %q reports runtime %q — using socket path %q instead of the configured default %q", nodeName, runtimeVersion, resolved, configured)
	}
	return resolved
}
