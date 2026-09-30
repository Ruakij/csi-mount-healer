package healer

import (
	"errors"
	"io/fs"
	"syscall"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
