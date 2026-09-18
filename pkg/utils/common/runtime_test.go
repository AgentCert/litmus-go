package common

import "testing"

func TestResolveSocketPath(t *testing.T) {
	cases := []struct {
		name           string
		configured     string
		runtimeVersion string
		want           string
	}{
		// The case that broke every helper-pod fault on the k3s cluster: the chart
		// ships the generic containerd socket, but k3s serves pods from its own.
		{"k3s node on shipped default", DefaultContainerdSocket, "containerd://2.3.2-k3s2", K3sContainerdSocket},
		{"k3s node, unset config", "", "containerd://1.7.11-k3s2", K3sContainerdSocket},
		{"k3s casing is ignored", DefaultContainerdSocket, "containerd://1.7.11-K3S1", K3sContainerdSocket},

		// Plain containerd (KinD, kubeadm, most managed distros) must be untouched.
		{"kind/containerd stays default", DefaultContainerdSocket, "containerd://1.7.1", DefaultContainerdSocket},
		{"unset config on containerd", "", "containerd://1.7.1", DefaultContainerdSocket},

		{"cri-o node", DefaultContainerdSocket, "cri-o://1.29.0", DefaultCRIOSocket},
		{"docker node", DefaultContainerdSocket, "docker://24.0.7", DefaultDockerSocket},

		// An operator who set SOCKET_PATH deliberately must always win, even on k3s.
		{"explicit override wins on k3s", "/custom/containerd.sock", "containerd://1.7.11-k3s2", "/custom/containerd.sock"},
		{"explicit override wins on containerd", "/custom/containerd.sock", "containerd://1.7.1", "/custom/containerd.sock"},
		{"explicit crio override wins", DefaultCRIOSocket, "containerd://1.7.11-k3s2", DefaultCRIOSocket},

		// With nothing to go on, behaviour must not change.
		{"no runtime info, default config", DefaultContainerdSocket, "", DefaultContainerdSocket},
		{"no runtime info, empty config", "", "", DefaultContainerdSocket},
		{"no runtime info, explicit config", "/custom/x.sock", "", "/custom/x.sock"},

		// Whitespace must not defeat the default comparison.
		{"padded default is still the default", "  " + DefaultContainerdSocket + "  ", "containerd://1.7.11-k3s2", K3sContainerdSocket},

		{"unknown runtime falls back to containerd", DefaultContainerdSocket, "something-else://9", DefaultContainerdSocket},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveSocketPath(tc.configured, tc.runtimeVersion); got != tc.want {
				t.Errorf("ResolveSocketPath(%q, %q) = %q, want %q", tc.configured, tc.runtimeVersion, got, tc.want)
			}
		})
	}
}

// The resolver must never return an empty path: it is used directly as a hostPath
// volume source, and an empty hostPath makes the helper pod fail admission.
func TestResolveSocketPath_NeverEmpty(t *testing.T) {
	for _, configured := range []string{"", "   ", DefaultContainerdSocket, "/custom/x.sock"} {
		for _, rv := range []string{"", "containerd://1.7.1", "containerd://1.7.11-k3s2", "cri-o://1.29", "docker://24", "weird"} {
			if got := ResolveSocketPath(configured, rv); got == "" {
				t.Errorf("ResolveSocketPath(%q, %q) returned an empty path", configured, rv)
			}
		}
	}
}
