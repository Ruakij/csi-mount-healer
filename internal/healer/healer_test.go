package healer

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
)

func TestDeadReason(t *testing.T) {
	running := corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{
		{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
	}}
	creating := corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{
		{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}},
	}}
	crashing := corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{
		{RestartCount: 3, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CreateContainerError"}}},
	}}
	terminating := metav1.Now()

	tests := []struct {
		name     string
		status   corev1.PodStatus
		deleting bool
		mounted  bool
		err      error
		dead     bool
	}{
		{name: "healthy", status: running, mounted: true},
		{name: "transport endpoint not connected", status: running, err: syscall.ENOTCONN, dead: true},
		{name: "hung", status: running, err: errHung, dead: true},
		{name: "unmounted while running", status: running, dead: true},
		{name: "unmounted after restarts", status: crashing, dead: true},
		{name: "unmounted before containers start", status: creating},
		{name: "unmounted while terminating", status: running, deleting: true},
		{name: "unmounted after success", status: corev1.PodStatus{Phase: corev1.PodSucceeded}},
		{name: "broken while terminating", status: running, deleting: true, err: syscall.ESTALE, dead: true},
		{name: "missing", status: running, err: fs.ErrNotExist},
		{name: "missing, wrapped", status: running, err: &fs.PathError{Op: "statx", Err: syscall.ENOENT}},
		{name: "other error", status: creating, err: errors.New("boom"), dead: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{Status: tt.status}
			if tt.deleting {
				pod.DeletionTimestamp = &terminating
			}
			if got := deadReason(pod, tt.mounted, tt.err); (got != "") != tt.dead {
				t.Errorf("deadReason = %q, want dead %v", got, tt.dead)
			}
		})
	}
}

func TestSelected(t *testing.T) {
	dir := t.TempDir()
	data, _ := json.Marshal(volData{DriverName: "csi.moosefs.com"})
	if err := os.WriteFile(filepath.Join(dir, "vol_data.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	v := volume{dir: dir}
	pod := testPod()
	pod.Labels = map[string]string{"app": "db", "namespace": "shadowed"}

	tests := []struct {
		selector string
		want     bool
	}{
		{"", true},
		{"driver=csi.moosefs.com", true},
		{"driver in (seaweedfs-csi-driver)", false},
		{"namespace=ns", true},
		{"namespace notin (ns)", false},
		{"app=db,driver=csi.moosefs.com,namespace=ns", true},
		{"app=db,driver=other", false},
		{"!csi-mount-healer/skip", true},
	}
	for _, tt := range tests {
		sel, err := labels.Parse(tt.selector)
		if err != nil {
			t.Fatal(err)
		}
		h, err := New(Config{NodeName: "n", Strikes: 1, Guard: GuardOff, Selector: sel})
		if err != nil {
			t.Fatal(err)
		}
		if got := h.selected(pod, v); got != tt.want {
			t.Errorf("%q: selected = %v, want %v", tt.selector, got, tt.want)
		}
	}

	// Without vol_data.json the driver is unknown.
	sel, _ := labels.Parse("driver")
	h, _ := New(Config{NodeName: "n", Strikes: 1, Guard: GuardOff, Selector: sel})
	if h.selected(pod, volume{dir: t.TempDir()}) {
		t.Error("volume without vol_data.json matched a driver selector")
	}
}

// A failed remount heals again at the next check until its attempts are used
// up, counted per class: a driver that is not registered, and one reached that
// cannot remount, here for want of vol_data.json.
func TestHealAttempts(t *testing.T) {
	tests := []struct {
		name     string
		down     bool
		attempts int
		// heals is how many heals fail before the pod is deleted, 0 for never.
		heals int
	}{
		{name: "driver down, never escalated", down: true, attempts: 0},
		{name: "driver down, escalated at once", down: true, attempts: 1, heals: 1},
		{name: "driver down, escalated at the second heal", down: true, attempts: 2, heals: 2},
		{name: "remount failed, never escalated", attempts: 0},
		{name: "remount failed, escalated at the third heal", attempts: 3, heals: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			pod := testPod()
			pod.Spec.RestartPolicy = corev1.RestartPolicyAlways
			pod.OwnerReferences = []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "app"}}
			dir := filepath.Join(root, "pods", string(pod.UID), "volumes", "kubernetes.io~csi", "pv-1")
			if err := os.MkdirAll(dir, 0o750); err != nil {
				t.Fatal(err)
			}
			if tt.down {
				data, _ := json.Marshal(volData{SpecVolID: "pv-1", VolumeHandle: "h", DriverName: "d"})
				if err := os.WriteFile(filepath.Join(dir, "vol_data.json"), data, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			client := fake.NewClientset(pod)
			events := record.NewFakeRecorder(20)
			cfg := Config{KubeletRoot: root, KubeClient: client, RemountAttempts: tt.attempts, DriverDownAttempts: tt.attempts}
			h := &Healer{cfg: cfg, events: events, failed: map[types.UID]int{}}
			if err := h.cfg.Tiers.Set("live,restart,delete"); err != nil {
				t.Fatal(err)
			}

			retry, want := "RemountFailed", []string{}
			if tt.down {
				retry = "DriverDown"
			}
			for i := 1; i <= 4; i++ {
				h.down = map[string]error{}
				healed := h.heal(context.Background(), pod, []volume{{podUID: pod.UID, dir: dir, target: filepath.Join(dir, "mount")}})
				if escalated := i == tt.heals; healed != escalated {
					t.Fatalf("heal %d: healed = %v, want %v", i, healed, escalated)
				}
				if i == tt.heals {
					want = append(want, "DeletingPod")
					break
				}
				want = append(want, retry)
			}
			close(events.Events)
			var got []string
			for e := range events.Events {
				got = append(got, strings.Fields(e)[1])
			}
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("events %v, want %v", got, want)
			}
			_, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
			if deleted := err != nil; deleted != (tt.heals > 0) {
				t.Errorf("pod deleted = %v, want %v", deleted, tt.heals > 0)
			}
		})
	}
}
