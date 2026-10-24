package healer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// fakeCRI is a container runtime with running containers, whose main processes
// have the pids given.
type fakeCRI struct {
	runtimeapi.UnimplementedRuntimeServiceServer
	mu         sync.Mutex
	containers []*runtimeapi.Container
	sandboxes  []*runtimeapi.PodSandbox
	pids       map[string]int
	listErr    error
	statusErr  map[string]error
	lists      int
	statuses   []string
	stopped    []string
}

func (f *fakeCRI) ListContainers(_ context.Context, req *runtimeapi.ListContainersRequest) (*runtimeapi.ListContainersResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists++
	if f.listErr != nil {
		return nil, f.listErr
	}
	resp := &runtimeapi.ListContainersResponse{}
	for _, c := range f.containers {
		match := true
		for k, v := range req.GetFilter().GetLabelSelector() {
			match = match && c.GetLabels()[k] == v
		}
		if match {
			resp.Containers = append(resp.Containers, c)
		}
	}
	return resp, nil
}

func (f *fakeCRI) ListPodSandbox(_ context.Context, req *runtimeapi.ListPodSandboxRequest) (*runtimeapi.ListPodSandboxResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	resp := &runtimeapi.ListPodSandboxResponse{}
	for _, sb := range f.sandboxes {
		match := true
		if st := req.GetFilter().GetState(); st != nil {
			match = sb.GetState() == st.GetState()
		}
		for k, v := range req.GetFilter().GetLabelSelector() {
			match = match && sb.GetLabels()[k] == v
		}
		if match {
			resp.Items = append(resp.Items, sb)
		}
	}
	return resp, nil
}

func (f *fakeCRI) ContainerStatus(_ context.Context, req *runtimeapi.ContainerStatusRequest) (*runtimeapi.ContainerStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses = append(f.statuses, req.GetContainerId())
	if err := f.statusErr[req.GetContainerId()]; err != nil {
		return nil, err
	}
	return &runtimeapi.ContainerStatusResponse{
		Status: &runtimeapi.ContainerStatus{Id: req.GetContainerId()},
		Info:   map[string]string{"info": fmt.Sprintf(`{"pid":%d}`, f.pids[req.GetContainerId()])},
	}, nil
}

func (f *fakeCRI) StopContainer(_ context.Context, req *runtimeapi.StopContainerRequest) (*runtimeapi.StopContainerResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = append(f.stopped, req.GetContainerId())
	return &runtimeapi.StopContainerResponse{}, nil
}

func (f *fakeCRI) container(id, podUID, name string) *runtimeapi.Container {
	return &runtimeapi.Container{Id: id, Labels: map[string]string{podUIDLabel: podUID, containerNameLabel: name}}
}

// serveCRI serves f on a unix socket and returns its path. The path is short, as
// the limit on a socket path is small.
func serveCRI(t *testing.T, f *fakeCRI) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "cri")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	runtimeapi.RegisterRuntimeServiceServer(s, f)
	go func() { _ = s.Serve(l) }()
	t.Cleanup(s.Stop)
	return sock
}

// copiesTest is a node with a pod whose volumes "data" (a claim) and "inline"
// are healthy on the node, and two containers that mount them:
//
//	app (app-1, pid 100) mounts data at /data and /srv//data/
//	sidecar (side-1, pid 200) mounts data at /mnt/data and inline at /inline
type copiesTest struct {
	h    *Healer
	pod  *corev1.Pod
	cri  *fakeCRI
	data volume
	// inline is the volume of the pod volume of that name.
	inline volume
	// broken has the error of a probe by "<pid> <path>"; others succeed.
	broken map[string]error
	// probed lists the probes in containers, as "<pid> <path>".
	probed []string
	mu     sync.Mutex
}

func newCopiesTest(t *testing.T) *copiesTest {
	t.Helper()
	root := t.TempDir()
	pod := testPod()
	pod.Spec.RestartPolicy = corev1.RestartPolicyAlways
	pod.OwnerReferences = []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "app"}}
	pod.Spec.Containers = []corev1.Container{
		{Name: "app", VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}, {Name: "data", MountPath: "/srv//data/"}}},
		{Name: "sidecar", VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/mnt/data"}, {Name: "inline", MountPath: "/inline"}}},
	}
	pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{
		{Name: "app", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
	}}

	vol := func(name string, data volData) volume {
		dir := filepath.Join(root, "pods", string(pod.UID), "volumes", "kubernetes.io~csi", name)
		if err := os.MkdirAll(filepath.Join(dir, "mount"), 0o750); err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(data)
		if err := os.WriteFile(filepath.Join(dir, "vol_data.json"), b, 0o644); err != nil {
			t.Fatal(err)
		}
		return volume{podUID: pod.UID, dir: dir, target: filepath.Join(dir, "mount")}
	}
	c := &copiesTest{
		pod:    pod,
		data:   vol("pv-1", volData{SpecVolID: "pv-1", VolumeHandle: "h", DriverName: "d", LifecycleMode: "Persistent"}),
		inline: vol("inline", volData{SpecVolID: "inline", VolumeHandle: "h2", DriverName: "d", LifecycleMode: "Ephemeral"}),
		broken: map[string]error{},
		cri: &fakeCRI{
			pids:      map[string]int{"app-1": 100, "side-1": 200, "other-1": 300},
			statusErr: map[string]error{},
		},
	}
	c.cri.containers = []*runtimeapi.Container{
		c.cri.container("app-1", string(pod.UID), "app"),
		c.cri.container("side-1", string(pod.UID), "sidecar"),
		c.cri.container("other-1", "uid-2", "app"),
	}

	pods := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{uidIndex: indexByUID})
	if err := pods.Add(pod); err != nil {
		t.Fatal(err)
	}
	h, err := New(Config{
		NodeName: "n", KubeletRoot: root, Strikes: 1, Interval: time.Minute, StatTimeout: time.Second, Guard: GuardOff,
		CheckContainers: true, CRIEndpoint: serveCRI(t, c.cri), KubeClient: fake.NewClientset(pod, testPV("data-app-0")),
	})
	if err != nil {
		t.Fatal(err)
	}
	h.pods, h.events = pods, record.NewFakeRecorder(20)
	h.prober.stat = func(string) (bool, error) { return true, nil }
	h.prober.statIn = func(pid int, path string) (bool, error) {
		key := fmt.Sprintf("%d %s", pid, path)
		c.probed = append(c.probed, key)
		return c.broken[key] == nil, c.broken[key]
	}
	c.h = h
	return c
}

func (c *copiesTest) healthy(vols ...volume) map[*corev1.Pod][]volume {
	return map[*corev1.Pod][]volume{c.pod: vols}
}

// copyStates says for each copy found, as "<container>/<pod volume>", whether it
// is "dead", "ok" or "unknown".
func copyStates(checks []copyCheck) map[string]string {
	got := map[string]string{}
	for _, c := range checks {
		state := "ok"
		switch {
		case c.unknown:
			state = "unknown"
		case c.reason != "":
			state = "dead"
		}
		got[c.container+"/"+c.podVolume] = state
	}
	return got
}

func TestContainerCopies(t *testing.T) {
	ctx := context.Background()

	t.Run("dead in one container", func(t *testing.T) {
		c := newCopiesTest(t)
		c.broken["200 /mnt/data"] = syscall.ENOTCONN
		checks := c.h.containerCopies(ctx, c.healthy(c.data, c.inline))
		want := map[string]string{"app/data": "ok", "sidecar/data": "dead", "sidecar/inline": "ok"}
		if got := copyStates(checks); !maps.Equal(got, want) {
			t.Errorf("dead copies %v, want %v", got, want)
		}
		for _, cc := range checks {
			if cc.container == "sidecar" && cc.id != "side-1" || cc.container == "app" && cc.id != "app-1" {
				t.Errorf("container %s has ID %s", cc.container, cc.id)
			}
		}
		// The paths are cleaned and the container of another pod is left alone.
		wantProbes := []string{"100 /data", "100 /srv/data", "200 /inline", "200 /mnt/data"}
		slices.Sort(c.probed)
		if !slices.Equal(c.probed, wantProbes) {
			t.Errorf("probed %v, want %v", c.probed, wantProbes)
		}
	})

	t.Run("one dead mount of a volume is enough", func(t *testing.T) {
		c := newCopiesTest(t)
		c.broken["100 /srv/data"] = syscall.ENOTCONN
		if got, want := copyStates(c.h.containerCopies(ctx, c.healthy(c.data))), map[string]string{"app/data": "dead", "sidecar/data": "ok"}; !maps.Equal(got, want) {
			t.Errorf("dead copies %v, want %v", got, want)
		}
	})

	t.Run("only the volumes given are probed", func(t *testing.T) {
		c := newCopiesTest(t)
		checks := c.h.containerCopies(ctx, c.healthy(c.inline))
		if got, want := copyStates(checks), map[string]string{"sidecar/inline": "ok"}; !maps.Equal(got, want) {
			t.Errorf("copies %v, want %v", got, want)
		}
		if want := []string{"200 /inline"}; !slices.Equal(c.probed, want) {
			t.Errorf("probed %v, want %v", c.probed, want)
		}
	})

	t.Run("a container without a pid is unknown", func(t *testing.T) {
		c := newCopiesTest(t)
		c.cri.statusErr["side-1"] = errors.New("gone")
		c.broken["200 /mnt/data"] = syscall.ENOTCONN
		want := map[string]string{"app/data": "ok", "sidecar/data": "unknown", "sidecar/inline": "unknown"}
		if got := copyStates(c.h.containerCopies(ctx, c.healthy(c.data, c.inline))); !maps.Equal(got, want) {
			t.Errorf("copies %v, want %v", got, want)
		}
	})

	t.Run("the runtime fails", func(t *testing.T) {
		c := newCopiesTest(t)
		c.cri.listErr = errors.New("down")
		c.broken["100 /data"] = syscall.ENOTCONN
		checks := c.h.containerCopies(ctx, c.healthy(c.data, c.inline))
		if len(checks) != 2 || len(c.probed) != 0 || len(c.cri.statuses) != 0 {
			t.Errorf("checks %v, probes %v, status calls %v after a failed listing", checks, c.probed, c.cri.statuses)
		}
		for _, cc := range checks {
			if !cc.unknown || cc.id != "" {
				t.Errorf("check %+v, want one unknown in every container", cc)
			}
		}
	})

	t.Run("the root of a container cannot be opened", func(t *testing.T) {
		c := newCopiesTest(t)
		c.broken["100 /data"] = fmt.Errorf("%w: permission denied", errNoRoot)
		if got, want := copyStates(c.h.containerCopies(ctx, c.healthy(c.data))), map[string]string{"app/data": "unknown", "sidecar/data": "ok"}; !maps.Equal(got, want) {
			t.Errorf("copies %v, want %v", got, want)
		}
	})

	t.Run("nothing is probed below a dead copy", func(t *testing.T) {
		c := newCopiesTest(t)
		c.pod.Spec.Containers[1].VolumeMounts[1].MountPath = "/mnt/data/sub"
		c.broken["200 /mnt/data"] = syscall.ENOTCONN
		got := copyStates(c.h.containerCopies(ctx, c.healthy(c.data, c.inline)))
		if want := map[string]string{"app/data": "ok", "sidecar/data": "dead"}; !maps.Equal(got, want) {
			t.Errorf("copies %v, want %v", got, want)
		}
		if slices.Contains(c.probed, "200 /mnt/data/sub") {
			t.Errorf("probed %v, the mount below the dead one included", c.probed)
		}
	})

	t.Run("nothing is probed below a volume with a dead publish path", func(t *testing.T) {
		c := newCopiesTest(t)
		c.pod.Spec.Containers[1].VolumeMounts[1].MountPath = "/mnt/data/sub"
		checks := c.h.containerCopies(ctx, c.healthy(c.inline))
		if len(checks) != 0 || len(c.probed) != 0 {
			t.Errorf("checks %v, probed %v, want none", checks, c.probed)
		}
	})

	t.Run("nothing is probed below a hung copy", func(t *testing.T) {
		c := newCopiesTest(t)
		c.pod.Spec.Containers[1].VolumeMounts[1].MountPath = "/mnt/data/sub"
		c.h.prober.timeout = 10 * time.Millisecond
		c.h.prober.statIn = func(pid int, path string) (bool, error) {
			c.mu.Lock()
			c.probed = append(c.probed, fmt.Sprintf("%d %s", pid, path))
			c.mu.Unlock()
			if pid == 200 && path == "/mnt/data" {
				select {}
			}
			return true, nil
		}
		got := copyStates(c.h.containerCopies(ctx, c.healthy(c.data, c.inline)))
		if want := map[string]string{"app/data": "ok", "sidecar/data": "dead"}; !maps.Equal(got, want) {
			t.Errorf("copies %v, want %v", got, want)
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if slices.Contains(c.probed, "200 /mnt/data/sub") {
			t.Errorf("probed %v, the mount below the hung one included", c.probed)
		}
	})

	t.Run("a hung copy is dead", func(t *testing.T) {
		c := newCopiesTest(t)
		c.h.prober.timeout = 10 * time.Millisecond
		c.h.prober.statIn = func(pid int, _ string) (bool, error) {
			if pid == 200 {
				select {}
			}
			return true, nil
		}
		if got, want := copyStates(c.h.containerCopies(ctx, c.healthy(c.data))), map[string]string{"app/data": "ok", "sidecar/data": "dead"}; !maps.Equal(got, want) {
			t.Errorf("copies %v, want %v", got, want)
		}
	})

	t.Run("pids are cached until the container is gone", func(t *testing.T) {
		c := newCopiesTest(t)
		for range 2 {
			c.h.containerCopies(ctx, c.healthy(c.data))
		}
		slices.Sort(c.cri.statuses)
		if want := []string{"app-1", "side-1"}; !slices.Equal(c.cri.statuses, want) {
			t.Errorf("status calls %v, want %v", c.cri.statuses, want)
		}

		// A restart gives the container a new ID, and the sidecar is gone.
		c.cri.containers = []*runtimeapi.Container{c.cri.container("app-2", string(c.pod.UID), "app"), c.cri.containers[2]}
		c.cri.pids["app-2"] = 101
		c.cri.statuses = nil
		checks := c.h.containerCopies(ctx, c.healthy(c.data))
		if len(checks) != 1 || checks[0].id != "app-2" {
			t.Errorf("checks %+v, want one in app-2", checks)
		}
		if !slices.Equal(c.cri.statuses, []string{"app-2"}) {
			t.Errorf("status calls %v, want app-2 only", c.cri.statuses)
		}
		if want := map[string]int{"app-2": 101}; !maps.Equal(c.h.pids, want) {
			t.Errorf("cached pids %v, want %v", c.h.pids, want)
		}
	})
}

func TestScanContainers(t *testing.T) {
	// A strike that has waited for settle, so the next failed check is the first.
	first := strike{first: time.Now().Add(-time.Minute)}

	t.Run("heals only the container whose copy is dead", func(t *testing.T) {
		c := newCopiesTest(t)
		h := c.h
		if err := h.cfg.Tiers.Set("restart,delete"); err != nil {
			t.Fatal(err)
		}
		c.broken["200 /mnt/data"] = syscall.ENOTCONN
		key := c.data.target + " side-1"
		h.strikes[key] = first
		// What a container that was replaced left behind.
		h.strikes[c.data.target+" gone-1"] = strike{n: 1}
		h.strikes[c.inline.target+" app-1"] = strike{n: 1}

		h.scan(context.Background())

		if want := []string{"side-1"}; !slices.Equal(c.cri.stopped, want) {
			t.Errorf("stopped containers %v, want %v", c.cri.stopped, want)
		}
		if len(h.strikes) != 0 || len(h.failed) != 0 {
			t.Errorf("strikes %v, failed %v, want none once healed", h.strikes, h.failed)
		}
		close(h.events.(*record.FakeRecorder).Events)
		var events []string
		for e := range h.events.(*record.FakeRecorder).Events {
			events = append(events, e)
		}
		// No remount happened, as the driver is not involved.
		if len(events) != 1 || !strings.HasPrefix(events[0], "Warning DeadMount Volume pv-1 in container sidecar: ") {
			t.Errorf("events %q, want a DeadMount for the volume in the sidecar", events)
		}
	})

	t.Run("strikes build up and clear when the copy recovers", func(t *testing.T) {
		c := newCopiesTest(t)
		c.h.cfg.Strikes = 2
		c.broken["200 /mnt/data"] = syscall.ENOTCONN
		key := c.data.target + " side-1"
		c.h.strikes[key] = first

		c.h.scan(context.Background())
		if len(c.cri.stopped) != 0 || c.h.strikes[key].n != 1 {
			t.Errorf("stopped %v, strikes %+v, want a strike and nothing stopped", c.cri.stopped, c.h.strikes[key])
		}
		// The copy is healthy again.
		delete(c.broken, "200 /mnt/data")
		c.h.scan(context.Background())
		if len(c.h.strikes) != 0 {
			t.Errorf("strikes %v after the copy recovered", c.h.strikes)
		}
	})

	t.Run("strikes stay when a copy cannot be checked", func(t *testing.T) {
		tests := []struct {
			name   string
			break_ func(*copiesTest)
			// keepsGone is set if the strikes of a container that is gone stay as well,
			// as a listing that failed says nothing about which containers are gone.
			keepsGone bool
		}{
			{"the runtime cannot be listed", func(c *copiesTest) { c.cri.listErr = errors.New("down") }, true},
			{"the status of the container fails", func(c *copiesTest) { c.cri.statusErr["side-1"] = errors.New("gone") }, false},
			{"the root of the container cannot be opened", func(c *copiesTest) {
				c.broken["200 /mnt/data"] = fmt.Errorf("%w: permission denied", errNoRoot)
			}, false},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				c := newCopiesTest(t)
				c.h.cfg.Strikes = 3
				tt.break_(c)
				key := c.data.target + " side-1"
				c.h.strikes[key] = strike{n: 1, first: time.Now().Add(-time.Hour), at: time.Now().Add(-time.Hour)}
				// What a container that was replaced left behind.
				c.h.strikes[c.data.target+" gone-1"] = strike{n: 1}

				c.h.scan(context.Background())
				if got := c.h.strikes[key]; got.n != 1 {
					t.Errorf("strikes of the copy %+v, want them kept as they were", got)
				}
				if _, ok := c.h.strikes[c.data.target+" gone-1"]; ok != tt.keepsGone {
					t.Errorf("strikes of a container that is gone kept = %v, want %v", ok, tt.keepsGone)
				}
				if len(c.cri.stopped) != 0 {
					t.Errorf("stopped %v, want nothing", c.cri.stopped)
				}
			})
		}
	})

	t.Run("off by default", func(t *testing.T) {
		c := newCopiesTest(t)
		c.h.cfg.CheckContainers = false
		c.broken["200 /mnt/data"] = syscall.ENOTCONN

		c.h.scan(context.Background())
		if c.cri.lists != 0 || len(c.probed) != 0 || len(c.h.strikes) != 0 {
			t.Errorf("%d listings, probed %v, strikes %v, want none", c.cri.lists, c.probed, c.h.strikes)
		}
	})

	// A volume that is dead only in the sidecar needs no remount, so it is healed
	// whatever becomes of the remount of the volume that is dead on the node.
	for _, tt := range []struct {
		name     string
		attempts int
		tiers    string
		healed   bool
		stopped  []string
		deleted  bool
		events   []string
	}{
		{name: "remount tried again", attempts: 3, tiers: "restart,delete", stopped: []string{"side-1"}, events: []string{"DriverDown"}},
		{name: "remount failed, pod deleted", attempts: 1, tiers: "restart,delete", healed: true, deleted: true, events: []string{"TierFailed", "DeletingPod"}},
		{name: "remount failed, no delete", attempts: 1, tiers: "restart", healed: true, stopped: []string{"side-1"}, events: []string{"TierFailed", "NotHealed"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newCopiesTest(t)
			h := c.h
			h.cfg.RemountAttempts, h.cfg.DriverDownAttempts = tt.attempts, tt.attempts
			if err := h.cfg.Tiers.Set(tt.tiers); err != nil {
				t.Fatal(err)
			}
			h.down = map[string]error{}
			d := &podDue{dead: []volume{c.data}, containers: containerVolumes{"sidecar": {"inline": c.inline.target}}}

			if healed := h.heal(context.Background(), c.pod, d); healed != tt.healed {
				t.Errorf("healed = %v, want %v", healed, tt.healed)
			}
			if !slices.Equal(c.cri.stopped, tt.stopped) {
				t.Errorf("stopped %v, want %v", c.cri.stopped, tt.stopped)
			}
			_, err := h.cfg.KubeClient.CoreV1().Pods(c.pod.Namespace).Get(context.Background(), c.pod.Name, metav1.GetOptions{})
			if deleted := err != nil; deleted != tt.deleted {
				t.Errorf("pod deleted = %v, want %v", deleted, tt.deleted)
			}
			rec := h.events.(*record.FakeRecorder)
			close(rec.Events)
			var events []string
			for e := range rec.Events {
				events = append(events, strings.Fields(e)[1])
			}
			if !slices.Equal(events, tt.events) {
				t.Errorf("events %v, want %v", events, tt.events)
			}
		})
	}
}

func TestMountedBy(t *testing.T) {
	always := corev1.ContainerRestartPolicyAlways
	mount := func(names ...string) []corev1.VolumeMount {
		var m []corev1.VolumeMount
		for _, n := range names {
			m = append(m, corev1.VolumeMount{Name: n, MountPath: "/" + n})
		}
		return m
	}
	pod := &corev1.Pod{Spec: corev1.PodSpec{
		InitContainers: []corev1.Container{
			{Name: "setup", VolumeMounts: mount("data")},
			{Name: "proxy", RestartPolicy: &always, VolumeMounts: mount("data", "logs")},
		},
		Containers: []corev1.Container{
			{Name: "app", VolumeMounts: mount("data", "scratch")},
			{Name: "idle", VolumeMounts: mount("scratch")},
		},
	}}
	got := mountedBy(pod, map[string]string{"data": "/t/data", "logs": "/t/logs"})
	want := containerVolumes{"app": {"data": "/t/data"}, "proxy": {"data": "/t/data", "logs": "/t/logs"}}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for name, vols := range want {
		if !maps.Equal(got[name], vols) {
			t.Errorf("container %s: %v, want %v", name, got[name], vols)
		}
	}
}

func TestStatusPid(t *testing.T) {
	tests := []struct {
		info    string
		want    int
		wantErr bool
	}{
		{info: `{"pid":42,"sandboxID":"x"}`, want: 42},
		{info: `{"pid":0}`, wantErr: true},
		{info: `{}`, wantErr: true},
		{info: ``, wantErr: true},
		{info: `{"pid":`, wantErr: true},
	}
	for _, tt := range tests {
		pid, err := statusPid(&runtimeapi.ContainerStatusResponse{Info: map[string]string{"info": tt.info}})
		if pid != tt.want || (err != nil) != tt.wantErr {
			t.Errorf("%q: pid %d, err %v; want pid %d, error %v", tt.info, pid, err, tt.want, tt.wantErr)
		}
	}
}
