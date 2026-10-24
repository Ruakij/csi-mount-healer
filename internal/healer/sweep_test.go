package healer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// fakeGuards stands in for the immutable flags and the mount table.
type fakeGuards struct {
	guarded map[string]bool
	mounted map[string]bool
	readErr error
}

func (f *fakeGuards) change(path string, on bool) (bool, error) {
	changed := f.guarded[path] != on
	f.guarded[path] = on
	return changed, nil
}

func (f *fakeGuards) mountPoints() (map[string]bool, error) {
	return f.mounted, f.readErr
}

func runningPod() *corev1.Pod {
	pod := testPod()
	pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{
		{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
	}}
	return pod
}

func TestSweepUnmountedTargets(t *testing.T) {
	terminating := metav1.NewTime(time.Now())
	creating := runningPod()
	creating.Status = corev1.PodStatus{Phase: corev1.PodPending}
	stopping := runningPod()
	stopping.DeletionTimestamp = &terminating

	tests := []struct {
		name  string
		guard GuardMode
		pod   *corev1.Pod
		// mounted says whether the target is a mount point.
		mounted bool
		// lost marks a target a remount failed on.
		lost    bool
		readErr error
		// stopped says kubelet stopped the sandbox of the pod, sandboxErr that it
		// cannot be looked up.
		stopped    bool
		sandboxErr error
		// wasGuarded starts the target guarded.
		wasGuarded bool
		// missing leaves the target directory out, created says it is made.
		missing, created bool
		// guarded is whether the target ends up guarded, found how many unguarded.
		guarded bool
		found   uint64
	}{
		{name: "mounted, always", guard: GuardAlways, pod: runningPod(), mounted: true, guarded: true},
		{name: "not mounted, always", guard: GuardAlways, pod: runningPod(), guarded: true, found: 1},
		{name: "not mounted, remount mode", guard: GuardRemount, pod: runningPod(), guarded: true, found: 1},
		{name: "mounted, remount mode", guard: GuardRemount, pod: runningPod(), mounted: true},
		{name: "not mounted, guard off", guard: GuardOff, pod: runningPod()},
		{name: "not mounted before containers start", guard: GuardAlways, pod: creating},
		{name: "not mounted while terminating", guard: GuardAlways, pod: stopping},
		{name: "failed remount, always", guard: GuardAlways, pod: runningPod(), lost: true, guarded: true, found: 1},
		{name: "failed remount, remount mode", guard: GuardRemount, pod: runningPod(), lost: true, guarded: true, found: 1},
		{name: "failed remount, mounted since, remount mode", guard: GuardRemount, pod: runningPod(), lost: true, mounted: true},
		{name: "failed remount, mounted since, always", guard: GuardAlways, pod: runningPod(), lost: true, mounted: true, guarded: true},
		{name: "failed remount, guard off", guard: GuardOff, pod: runningPod(), lost: true},
		{name: "failed remount, pod stopping", guard: GuardAlways, pod: stopping, lost: true},
		{name: "not mounted, sandbox stopped, remount mode", guard: GuardRemount, pod: runningPod(), stopped: true},
		{name: "not mounted, sandbox stopped, always", guard: GuardAlways, pod: runningPod(), stopped: true, guarded: true},
		{name: "not mounted, sandbox unknown, remount mode", guard: GuardRemount, pod: runningPod(), sandboxErr: errors.New("boom"), guarded: true, found: 1},
		{name: "guarded, sandbox stopped, remount mode", guard: GuardRemount, pod: runningPod(), wasGuarded: true, stopped: true, guarded: true},
		{name: "guarded, sandbox stopped, always", guard: GuardAlways, pod: runningPod(), wasGuarded: true, stopped: true, guarded: true},
		{name: "guarded, sandbox unknown, remount mode", guard: GuardRemount, pod: runningPod(), wasGuarded: true, sandboxErr: errors.New("boom"), guarded: true},
		{name: "failed remount, sandbox stopped", guard: GuardRemount, pod: runningPod(), lost: true, stopped: true, guarded: true, found: 1},
		{name: "missing, remount mode", guard: GuardRemount, pod: runningPod(), missing: true, created: true, guarded: true, found: 1},
		{name: "missing, always", guard: GuardAlways, pod: runningPod(), missing: true, created: true, guarded: true, found: 1},
		{name: "missing, sandbox stopped", guard: GuardRemount, pod: runningPod(), missing: true, stopped: true},
		{name: "missing, sandbox unknown", guard: GuardRemount, pod: runningPod(), missing: true, sandboxErr: errors.New("boom"), guarded: true, found: 1},
		{name: "missing, guard off", guard: GuardOff, pod: runningPod(), missing: true},
		{name: "missing, failed remount", guard: GuardRemount, pod: runningPod(), missing: true, lost: true, created: true, guarded: true, found: 1},
		{name: "missing, mount table unreadable", guard: GuardRemount, pod: runningPod(), missing: true, lost: true, readErr: errors.New("boom"), guarded: true, found: 1},
		{name: "mount table unreadable, always", guard: GuardAlways, pod: runningPod(), readErr: errors.New("boom"), guarded: true},
		{name: "mount table unreadable, failed remount", guard: GuardRemount, pod: runningPod(), lost: true, readErr: errors.New("boom"), guarded: true, found: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			h, err := New(Config{NodeName: "n", KubeletRoot: root, Strikes: 1, Guard: tt.guard})
			if err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(root, "pods", string(tt.pod.UID), "volumes", "kubernetes.io~csi", "pv-1", "mount")
			made := target
			if tt.missing {
				made = filepath.Dir(target)
			}
			if err := os.MkdirAll(made, 0o750); err != nil {
				t.Fatal(err)
			}
			f := &fakeGuards{guarded: map[string]bool{}, mounted: map[string]bool{}, readErr: tt.readErr}
			if tt.mounted {
				f.mounted[target] = true
			}
			f.guarded[target] = tt.wasGuarded
			h.changeGuard, h.mountPoints = f.change, f.mountPoints
			h.readySandboxes = func(context.Context) (map[types.UID]bool, error) {
				if tt.stopped {
					return map[types.UID]bool{}, tt.sandboxErr
				}
				return map[types.UID]bool{tt.pod.UID: true}, tt.sandboxErr
			}
			h.pods = cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{uidIndex: indexByUID})
			if err := h.pods.Add(tt.pod); err != nil {
				t.Fatal(err)
			}
			if tt.lost {
				h.unmounted[target] = true
			}

			// A second sweep finds the guard in place and counts nothing more.
			for range 2 {
				h.sweep(context.Background(), "*")
			}
			if f.guarded[target] != tt.guarded {
				t.Errorf("guarded = %v, want %v", f.guarded[target], tt.guarded)
			}
			if got := h.metrics.unguarded.Load(); got != tt.found {
				t.Errorf("unguarded targets found = %d, want %d", got, tt.found)
			}
			if _, err := os.Lstat(target); (err == nil) != (!tt.missing || tt.created) {
				t.Errorf("target exists = %v, want %v", err == nil, !tt.missing || tt.created)
			}
			if h.unmounted[target] != (tt.lost && !tt.mounted && tt.pod.DeletionTimestamp == nil) {
				t.Errorf("target still marked unmounted = %v", h.unmounted[target])
			}
		})
	}
}

// A sweep that cannot read the mount table leaves a running pod guard as it is,
// and counts a target it guarded once.
func TestSweepKeepsGuardWithoutMountTable(t *testing.T) {
	root := t.TempDir()
	h, err := New(Config{NodeName: "n", KubeletRoot: root, Strikes: 1, Guard: GuardRemount})
	if err != nil {
		t.Fatal(err)
	}
	pod := runningPod()
	target := filepath.Join(root, "pods", string(pod.UID), "volumes", "kubernetes.io~csi", "pv-1", "mount")
	if err := os.MkdirAll(target, 0o750); err != nil {
		t.Fatal(err)
	}
	f := &fakeGuards{guarded: map[string]bool{}, mounted: map[string]bool{}}
	h.changeGuard, h.mountPoints = f.change, f.mountPoints
	h.readySandboxes = func(context.Context) (map[types.UID]bool, error) { return map[types.UID]bool{pod.UID: true}, nil }
	h.pods = cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{uidIndex: indexByUID})
	if err := h.pods.Add(pod); err != nil {
		t.Fatal(err)
	}
	for i, readErr := range []error{nil, errors.New("boom"), nil} {
		f.readErr = readErr
		h.sweep(context.Background(), "*")
		if !f.guarded[target] {
			t.Errorf("sweep %d: guard released", i)
		}
	}
	if got := h.metrics.unguarded.Load(); got != 1 {
		t.Errorf("unguarded targets found = %d, want 1", got)
	}
	// Once something is mounted, the sweep releases it again.
	f.mounted[target] = true
	h.sweep(context.Background(), "*")
	if f.guarded[target] {
		t.Error("guard kept on a mounted target")
	}
}

func TestListReadySandboxes(t *testing.T) {
	f := &fakeCRI{sandboxes: []*runtimeapi.PodSandbox{
		{Id: "a", State: runtimeapi.PodSandboxState_SANDBOX_READY, Labels: map[string]string{podUIDLabel: "ready"}},
		{Id: "b", State: runtimeapi.PodSandboxState_SANDBOX_NOTREADY, Labels: map[string]string{podUIDLabel: "stopped"}},
		{Id: "c", State: runtimeapi.PodSandboxState_SANDBOX_NOTREADY, Labels: map[string]string{podUIDLabel: "replaced"}},
		{Id: "d", State: runtimeapi.PodSandboxState_SANDBOX_READY, Labels: map[string]string{podUIDLabel: "replaced"}},
	}}
	h, err := New(Config{NodeName: "n", Strikes: 1, Guard: GuardOff, CRIEndpoint: serveCRI(t, f)})
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.listReadySandboxes(context.Background())
	if want := (map[types.UID]bool{"ready": true, "replaced": true}); err != nil || !maps.Equal(got, want) {
		t.Errorf("listReadySandboxes = %v, %v, want %v", got, err, want)
	}
	f.listErr = errors.New("boom")
	if _, err := h.listReadySandboxes(context.Background()); err == nil {
		t.Error("listReadySandboxes: no error from a failing runtime")
	}
}

// A runtime that does not answer holds the sweep up for one timeout, however
// many targets need the answer, and the targets are guarded meanwhile.
func TestSweepListsSandboxesOnce(t *testing.T) {
	root := t.TempDir()
	h, err := New(Config{NodeName: "n", KubeletRoot: root, Strikes: 1, Guard: GuardRemount})
	if err != nil {
		t.Fatal(err)
	}
	pod := runningPod()
	f := &fakeGuards{guarded: map[string]bool{}, mounted: map[string]bool{}}
	var targets []string
	for i := range 5 {
		target := filepath.Join(root, "pods", string(pod.UID), "volumes", "kubernetes.io~csi", fmt.Sprintf("pv-%d", i), "mount")
		if err := os.MkdirAll(target, 0o750); err != nil {
			t.Fatal(err)
		}
		targets = append(targets, target)
	}
	calls := 0
	h.changeGuard, h.mountPoints = f.change, f.mountPoints
	h.readySandboxes = func(ctx context.Context) (map[types.UID]bool, error) {
		calls++
		<-ctx.Done()
		return nil, ctx.Err()
	}
	h.pods = cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{uidIndex: indexByUID})
	if err := h.pods.Add(pod); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	h.sweep(context.Background(), "*")
	if calls != 1 {
		t.Errorf("sandbox listings = %d, want 1", calls)
	}
	if d := time.Since(start); d > sandboxTimeout+time.Second {
		t.Errorf("sweep took %v, want about %v", d, sandboxTimeout)
	}
	for _, target := range targets {
		if !f.guarded[target] {
			t.Errorf("%s not guarded", target)
		}
	}
}

// A mark for a volume directory that is gone does not outlive it.
func TestSweepForgetsGoneTargets(t *testing.T) {
	h, err := New(Config{NodeName: "n", KubeletRoot: t.TempDir(), Strikes: 1, Guard: GuardAlways})
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeGuards{guarded: map[string]bool{}, mounted: map[string]bool{}}
	h.changeGuard, h.mountPoints = f.change, f.mountPoints
	h.pods = cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{uidIndex: indexByUID})
	h.unmounted["/gone/mount"] = true
	h.sweep(context.Background(), "*")
	if len(h.unmounted) != 0 {
		t.Errorf("unmounted = %v, want none", h.unmounted)
	}
}

func TestMetricsWrite(t *testing.T) {
	var m metrics
	m.unguarded.Add(2)
	m.remountFailed.Add(1)
	rec := httptest.NewRecorder()
	m.write(rec)
	want := "# HELP csi_mount_healer_unguarded_unmounted_targets_total Targets of running pods found with nothing mounted on them and no guard.\n" +
		"# TYPE csi_mount_healer_unguarded_unmounted_targets_total counter\n" +
		"csi_mount_healer_unguarded_unmounted_targets_total 2\n" +
		"# HELP csi_mount_healer_remount_failures_total Remounts through a CSI driver that failed.\n" +
		"# TYPE csi_mount_healer_remount_failures_total counter\n" +
		"csi_mount_healer_remount_failures_total 1\n"
	if got := rec.Body.String(); got != want {
		t.Errorf("metrics:\n%s\nwant:\n%s", got, want)
	}
}

func TestMetricsServe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	var m metrics
	m.remountFailed.Add(3)
	if err := m.serve(ctx, addr); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "csi_mount_healer_remount_failures_total 3\n") {
		t.Errorf("metrics:\n%s", body)
	}
}
