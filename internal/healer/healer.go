// Package healer finds dead CSI mounts of the pods on this node and heals them
// with the least disruptive of the enabled tiers that works.
package healer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

// GuardMode says when the directory underneath a CSI mount is made immutable, so
// a container started while the mount is missing cannot write to the node disk.
type GuardMode string

const (
	// GuardAlways guards the mounts of every running pod, so a mount that dies
	// between two checks never exposes a writable directory.
	GuardAlways GuardMode = "always"
	// GuardRemount guards a mount only while it is being remounted.
	GuardRemount GuardMode = "remount"
	GuardOff     GuardMode = "off"
)

// Config is the startup configuration, built from CLI flags.
type Config struct {
	NodeName    string
	KubeletRoot string
	// Interval between two checks of every mount.
	Interval time.Duration
	// Strikes is how many checks in a row a mount has to fail before it is healed.
	Strikes     int
	StatTimeout time.Duration
	Tiers       Tiers
	// LiveTimeout is how long a container swapped by the live tier may hold on to
	// the dead mount before it is escalated to the next tier; 0 disables that.
	LiveTimeout time.Duration
	// RemountAttempts is how many heals in a row may fail to remount a volume
	// through its reachable driver before it is escalated, the attempts before
	// waiting for the next check; 0 never escalates.
	RemountAttempts int
	// DriverDownAttempts is the same for a driver that cannot be reached.
	DriverDownAttempts int
	// ForceDelete lets the delete tier force delete a pod stuck terminating on a
	// dead mount, whose processes may then outlive their replacement.
	ForceDelete bool
	Guard       GuardMode
	// GuardStage guards the staging directory of every volume a running pod
	// uses, so a pod published while the staging mount is gone cannot write
	// to the node disk.
	GuardStage  bool
	CRIEndpoint string
	// CheckContainers also probes the copy of every healthy volume in each
	// container that mounts it, which costs a CRI listing and a probe per volume
	// mount of every container on each scan.
	CheckContainers bool
	// Selector picks the volumes to check, heal and guard. It matches the labels
	// of the pod plus namespace and driver, which override pod labels of the same
	// name. Nil or empty picks every volume.
	Selector   labels.Selector
	KubeClient kubernetes.Interface
	// MetricsAddr is the address to serve the counters on, in the Prometheus text
	// format; empty serves nothing.
	MetricsAddr string
}

const uidIndex = "uid"

type Healer struct {
	cfg     Config
	prober  *prober
	pods    cache.Indexer
	events  record.EventRecorder
	strikes map[string]strike
	// failed counts the heals in a row of a pod that failed to remount.
	failed map[types.UID]int
	// down are the drivers found down during the current scan, by name.
	down map[string]error
	// pids are the pids of the main processes of running containers, by container ID.
	pids map[string]int
	// volumeNames are the pod volume names of the volume directories, by target.
	volumeNames map[string]string
	stale       func(*swap) (int, []string, error)
	// changeGuard sets or clears the guard on a directory and reports whether the
	// flag changed; mountPoints lists the mount points of the node.
	changeGuard func(path string, on bool) (changed bool, err error)
	mountPoints func() (map[string]bool, error)
	// readySandboxes lists the UIDs of the pods the runtime has a ready sandbox for.
	readySandboxes func(ctx context.Context) (map[types.UID]bool, error)
	metrics        metrics
	// mu serialises changes to mounts and guard flags, so a sweep never guards or
	// releases a volume halfway through a remount.
	mu sync.Mutex
	// unmounted are the publish targets a remount tore down and could not mount
	// again, which stay guarded until a remount succeeds or the pod stops. Guarded
	// by mu.
	unmounted map[string]bool
}

func New(cfg Config) (*Healer, error) {
	switch cfg.Guard {
	case GuardAlways, GuardRemount, GuardOff:
	default:
		return nil, fmt.Errorf("invalid guard mode %q, must be always, remount or off", cfg.Guard)
	}
	if cfg.NodeName == "" {
		return nil, errors.New("node name is required")
	}
	if cfg.Strikes < 1 {
		return nil, errors.New("strikes must be at least 1")
	}
	if cfg.LiveTimeout < 0 {
		return nil, errors.New("live timeout must not be negative")
	}
	if cfg.RemountAttempts < 0 || cfg.DriverDownAttempts < 0 {
		return nil, errors.New("attempts must not be negative")
	}
	if cfg.Selector == nil {
		cfg.Selector = labels.Everything()
	}
	h := &Healer{
		cfg: cfg, prober: newProber(cfg.StatTimeout), strikes: map[string]strike{}, failed: map[types.UID]int{},
		pids: map[string]int{}, volumeNames: map[string]string{}, unmounted: map[string]bool{},
		changeGuard: changeImmutable, mountPoints: mountPoints,
	}
	h.readySandboxes = h.listReadySandboxes
	h.stale = func(s *swap) (int, []string, error) { return staleHandles(s.pid, s.ns, s.dead, cfg.StatTimeout) }
	return h, nil
}

// Run checks the mounts until ctx is done, then releases every guard so nothing
// stays immutable while the healer is not running to clear it.
func (h *Healer) Run(ctx context.Context) error {
	factory := informers.NewSharedInformerFactoryWithOptions(h.cfg.KubeClient, 0,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", h.cfg.NodeName).String()
		}))
	informer := factory.Core().V1().Pods().Informer()
	if err := informer.AddIndexers(cache.Indexers{uidIndex: indexByUID}); err != nil {
		return err
	}
	// A pod is guarded as soon as its first container starts.
	// kubelet cannot remove an immutable mount directory, and the pod object
	// stays until it has, so the guard is released as soon as a pod stops.
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, obj any) {
			if pod := obj.(*corev1.Pod); !active(pod) || started(pod) != started(old.(*corev1.Pod)) {
				h.sweep(ctx, string(pod.UID))
			}
		},
		DeleteFunc: func(obj any) {
			if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tomb.Obj
			}
			if pod, ok := obj.(*corev1.Pod); ok {
				h.sweep(ctx, string(pod.UID))
			}
		},
	}); err != nil {
		return err
	}
	h.pods = informer.GetIndexer()
	// Events on the pod, so what happened to it shows in kubectl describe and in
	// whatever watches the cluster's events.
	broadcaster := record.NewBroadcaster(record.WithContext(ctx))
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: h.cfg.KubeClient.CoreV1().Events("")})
	defer broadcaster.Shutdown()
	h.events = broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "csi-mount-healer", Host: h.cfg.NodeName})
	if h.cfg.MetricsAddr != "" {
		if err := h.metrics.serve(ctx, h.cfg.MetricsAddr); err != nil {
			return err
		}
	}
	factory.Start(ctx.Done())
	defer factory.Shutdown()
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return ctx.Err()
	}
	klog.Infof("watching CSI mounts on %s: interval %v, strikes %d, tiers %q, live timeout %v, remount attempts %d, driver down attempts %d, guard %s, guard stage %v, check containers %v, selector %q",
		h.cfg.NodeName, h.cfg.Interval, h.cfg.Strikes, h.cfg.Tiers, h.cfg.LiveTimeout, h.cfg.RemountAttempts, h.cfg.DriverDownAttempts, h.cfg.Guard, h.cfg.GuardStage, h.cfg.CheckContainers, h.cfg.Selector)

	// Also clears what a previous run in another mode left behind.
	h.sweep(ctx, "*")

	// A mount that appears or disappears is checked at once rather than at the
	// next tick.
	every(ctx, h.cfg.Interval, settle, watchMounts(ctx), func() bool {
		// Retries guards that failed to set on a pod update, and guards the
		// targets of running pods that lost their mount.
		if h.cfg.Guard != GuardOff || h.cfg.GuardStage {
			h.sweep(ctx, "*")
		}
		return h.scan(ctx)
	})

	h.mu.Lock()
	defer h.mu.Unlock()
	for _, v := range h.volumes("*") {
		if err := h.setGuard(v.target, false); err != nil {
			klog.Warningf("releasing guard on %s: %v", v.target, err)
		}
	}
	h.guardStages(false)
	return nil
}

func (h *Healer) setGuard(path string, on bool) error {
	_, err := h.changeGuard(path, on)
	return err
}

func indexByUID(obj any) ([]string, error) {
	return []string{string(obj.(*corev1.Pod).UID)}, nil
}

// settle is how long a mount has to stay dead before its first strike. Kubelet
// unmounts a finished pod before its status reaches the API, so a mount gone
// during that window would otherwise look dead.
const settle = 5 * time.Second

// every runs fn on wake and each interval, which a wake restarts, and again
// after settle when fn asks for a recheck.
func every(ctx context.Context, interval, settle time.Duration, wake <-chan struct{}, fn func() (recheck bool)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	var recheck <-chan time.Time
	run := func() {
		if fn() && recheck == nil {
			recheck = time.After(settle)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			run()
		case <-wake:
			run()
			t.Reset(interval)
		case <-recheck:
			recheck = nil
			run()
		}
	}
}

// strike is how many checks in a row found a mount dead, when the first of them
// did, and when the last strike counted.
type strike struct {
	n         int
	first, at time.Time
}

// strike records a failed check of key, a volume target or a target and a
// container, and returns the strikes so far. The first strike needs the mount
// dead for settle, each one after it half an interval more, so the strikes stand
// for time however often checks run.
func (h *Healer) strike(key string, now time.Time) int {
	s, ok := h.strikes[key]
	switch {
	case !ok:
		s.first = now
	case s.n == 0 && now.Sub(s.first) >= settle, s.n > 0 && now.Sub(s.at) >= h.cfg.Interval/2:
		s.n++
		s.at = now
	}
	h.strikes[key] = s
	return s.n
}

// fail records a failed check of key and reports whether its strikes call for
// a heal now, and whether the first strike is still waiting for settle. where
// names the volume, and the container if the check was in one.
func (h *Healer) fail(pod *corev1.Pod, key, where, reason string) (heal, recheck bool) {
	prev := h.strikes[key].n
	n := h.strike(key, time.Now())
	if n == prev {
		return false, n == 0
	}
	klog.Warningf("pod %s/%s volume %s: %s (strike %d/%d)", pod.Namespace, pod.Name, where, reason, n, h.cfg.Strikes)
	if n == h.cfg.Strikes {
		h.events.Eventf(pod, corev1.EventTypeWarning, "DeadMount", "Volume %s: %s, %d checks in a row", where, reason, h.cfg.Strikes)
	}
	return n >= h.cfg.Strikes, false
}

// volume is one CSI mount of a pod, as kubelet laid it out on disk.
type volume struct {
	podUID types.UID
	dir    string
	target string
}

func (v volume) data() (volData, error) {
	var d volData
	b, err := os.ReadFile(filepath.Join(v.dir, "vol_data.json"))
	if err != nil {
		return d, err
	}
	return d, json.Unmarshal(b, &d)
}

// volumes lists the CSI volumes of the pod with the given UID, or of every pod for
// "*". Glob only reads the directories above the mounts, so a hung mount cannot
// block it.
func (h *Healer) volumes(podUID string) []volume {
	dirs, _ := filepath.Glob(filepath.Join(h.cfg.KubeletRoot, "pods", podUID, "volumes", "kubernetes.io~csi", "*"))
	vols := make([]volume, 0, len(dirs))
	for _, dir := range dirs {
		uid := filepath.Base(filepath.Dir(filepath.Dir(filepath.Dir(dir))))
		vols = append(vols, volume{podUID: types.UID(uid), dir: dir, target: filepath.Join(dir, "mount")})
	}
	return vols
}

func (h *Healer) pod(uid types.UID) *corev1.Pod {
	objs, _ := h.pods.ByIndex(uidIndex, string(uid))
	if len(objs) == 0 {
		return nil
	}
	return objs[0].(*corev1.Pod)
}

// selected reports whether the selector picks the volume. A volume whose driver
// is unknown has no driver label.
func (h *Healer) selected(pod *corev1.Pod, v volume) bool {
	if h.cfg.Selector.Empty() {
		return true
	}
	set := labels.Set{}
	maps.Copy(set, pod.Labels)
	set["namespace"] = pod.Namespace
	if d, err := v.data(); err == nil {
		set["driver"] = d.DriverName
	}
	return h.cfg.Selector.Matches(set)
}

// sweep guards the mounts of running pods and releases every other one, for the
// pod with the given UID or every pod for "*". Each step is idempotent, so it
// simply converges on what the pods look like right now.
//
// The target of a running pod that is not a mount point, or that a remount tore
// down and could not mount again, is guarded in every mode but off: a container
// started on it would write to the node disk. A target that is missing is made
// first. Finding one unguarded is counted.
//
// kubelet unmounts the volumes of a finished pod before its status reaches the
// informer, so a target without a mount is only taken for lost while the runtime
// has a ready sandbox for the pod: kubelet stops the sandbox before it tears the
// volumes down, and a pod whose containers crash keeps it. Without one, the
// guard of the target stays as it is, as the sandbox may only be recreated.
// Sandboxes that cannot be listed count as ready, as a guard on a target with
// nothing mounted on it is worth more than the chance of holding up a teardown.
// They are listed once per sweep, and only when a target needs it, so the lock
// is held for the timeout at most once.
func (h *Healer) sweep(ctx context.Context, podUID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var points map[string]bool
	if h.cfg.Guard != GuardOff {
		var err error
		if points, err = h.mountPoints(); err != nil {
			klog.Warningf("reading mounts, not looking for unmounted targets: %v", err)
			points = nil
		}
	}
	var ready map[types.UID]bool
	listedSandboxes := false
	sandbox := func(uid types.UID) (isReady, known bool) {
		if !listedSandboxes {
			listedSandboxes = true
			sctx, cancel := context.WithTimeout(ctx, sandboxTimeout)
			defer cancel()
			var err error
			if ready, err = h.readySandboxes(sctx); err != nil {
				klog.Warningf("listing the pod sandboxes: %v", err)
				ready = nil
			}
		}
		if ready == nil {
			return true, false
		}
		return ready[uid], true
	}
	listed := map[string]bool{}
	for _, v := range h.volumes(podUID) {
		listed[v.target] = true
		pod := h.pod(v.podUID)
		running := pod != nil && h.selected(pod, v) && active(pod) && started(pod)
		mounted := points != nil && points[v.target]
		// A mark is stale once the pod stopped, or the target got mounted after
		// the remount gave up, as it can when a publish timed out.
		if !running || mounted {
			delete(h.unmounted, v.target)
		}
		guard := h.cfg.Guard == GuardAlways && running
		lost := running && h.cfg.Guard != GuardOff && h.unmounted[v.target]
		missing := lost && points != nil
		stopped := false
		if running && h.cfg.Guard != GuardOff && points != nil && !mounted && !lost {
			r, known := sandbox(v.podUID)
			lost = r
			missing = r && known
			stopped = !r
		}
		if missing {
			if err := makeTarget(v.target); err != nil {
				klog.Warningf("making %s: %v", v.target, err)
			}
		}
		if h.cfg.Guard != GuardOff && running && !guard && !lost && (points == nil || stopped) {
			// Nothing is known about the mounts, or the pod may be stopping, so a
			// guard stays as it is.
			continue
		}
		changed, err := h.changeGuard(v.target, guard || lost)
		if err != nil {
			klog.Warningf("setting guard on %s to %v: %v", v.target, guard || lost, err)
		} else if lost && changed && !mounted {
			h.metrics.unguarded.Add(1)
			klog.Warningf("pod %s: %s has nothing mounted on it, a container started on it writes to the node disk; guarded it now", v.podUID, v.target)
		}
	}
	if podUID == "*" {
		for target := range h.unmounted {
			if !listed[target] {
				delete(h.unmounted, target)
			}
		}
	}
	if h.cfg.GuardStage || podUID == "*" {
		h.guardStages(h.cfg.GuardStage)
	}
}

// sandboxTimeout bounds the listing of the sandboxes in a sweep, which holds the
// lock of the mounts.
const sandboxTimeout = 2 * time.Second

// makeTarget creates a publish target that is missing, as remount does. A path
// is only looked at that has no mount on it, so a hung mount cannot block it.
func makeTarget(path string) error {
	if _, err := os.Lstat(path); !errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return os.MkdirAll(path, 0o750)
}

// guardStages guards the staging directories of the volumes running pods use
// and releases every other one. Pods share the staging of a volume, so it is
// always worked out over every pod. Inline ephemeral volumes are not staged.
func (h *Healer) guardStages(enabled bool) {
	want := map[string]bool{}
	for _, v := range h.volumes("*") {
		if !enabled {
			break
		}
		pod := h.pod(v.podUID)
		if pod == nil || !active(pod) || !started(pod) || !h.selected(pod, v) {
			continue
		}
		if d, err := v.data(); err == nil && !d.ephemeral() {
			want[stagingPath(h.cfg.KubeletRoot, d.DriverName, d.VolumeHandle)] = true
		}
	}
	stages, _ := filepath.Glob(filepath.Join(h.cfg.KubeletRoot, "plugins/kubernetes.io/csi/*/*/globalmount"))
	for _, s := range stages {
		if err := h.setGuard(s, want[s]); err != nil {
			klog.Warningf("setting guard on %s to %v: %v", s, want[s], err)
		}
	}
}

// podDue is what a pod has to heal.
type podDue struct {
	// dead are the volumes with a dead publish path, to remount.
	dead []volume
	// containers are the volumes that are dead in some containers only, whose
	// publish path is healthy.
	containers containerVolumes
	// keys are the strikes of the dead volumes, to clear once they are healed.
	keys []string
	// copyKeys are the strikes of the containers, cleared once the tiers ran for
	// them, whatever became of the remount.
	copyKeys []string
}

type dueMap map[types.UID]*podDue

func (m dueMap) of(pod *corev1.Pod) *podDue {
	if m[pod.UID] == nil {
		m[pod.UID] = &podDue{containers: containerVolumes{}}
	}
	return m[pod.UID]
}

// scan reports whether a mount waits for its first strike.
func (h *Healer) scan(ctx context.Context) (recheck bool) {
	h.down = map[string]error{}
	seen := map[string]bool{}
	due := dueMap{}
	// healthy are the volumes of started pods whose publish path is mounted.
	healthy := map[*corev1.Pod][]volume{}
	binds, err := stagingBinds()
	if err != nil {
		klog.Warningf("reading mounts: %v", err)
	}
	for _, v := range h.volumes("*") {
		pod := h.pod(v.podUID)
		if pod == nil || !h.selected(pod, v) {
			// A pod that is gone leaves this for kubelet to clean up.
			continue
		}
		seen[v.target] = true
		mounted, err := h.prober.probe(v.target)
		reason := deadReason(pod, mounted, err)
		if reason == "" && mounted && binds[v.target] && active(pod) {
			reason = "bound to its bare staging directory on the node disk, the staging mount is gone"
		}
		if reason == "" {
			delete(h.strikes, v.target)
			if h.cfg.CheckContainers && mounted && active(pod) && started(pod) {
				healthy[pod] = append(healthy[pod], v)
			}
			continue
		}
		heal, again := h.fail(pod, v.target, filepath.Base(v.dir), reason)
		recheck = recheck || again
		if heal {
			d := due.of(pod)
			d.dead = append(d.dead, v)
			d.keys = append(d.keys, v.target)
		}
	}
	if h.cfg.CheckContainers {
		// A dead publish path leaves the copies in the containers dead as well, which
		// the heal of the path takes care of.
		for _, c := range h.containerCopies(ctx, healthy) {
			key := copyKey(c.volume.target, c.id)
			if c.unknown {
				// The strikes of a copy that cannot be checked stay as they are.
				for k := range h.strikes {
					if strings.HasPrefix(k, key) {
						seen[k] = true
					}
				}
				continue
			}
			seen[key] = true
			if c.reason == "" {
				delete(h.strikes, key)
				continue
			}
			heal, again := h.fail(c.pod, key, filepath.Base(c.volume.dir)+" in container "+c.container, c.reason)
			recheck = recheck || again
			if heal {
				d := due.of(c.pod)
				d.containers.add(c.container, c.podVolume, c.volume.target)
				d.copyKeys = append(d.copyKeys, key)
			}
		}
		maps.DeleteFunc(h.volumeNames, func(target, _ string) bool { return !seen[target] })
	}
	for key := range h.strikes {
		if !seen[key] {
			delete(h.strikes, key)
		}
	}
	for uid := range h.failed {
		if d := due[uid]; d == nil || len(d.dead) == 0 {
			delete(h.failed, uid)
		}
	}
	for uid, d := range due {
		healed := true
		if pod := h.pod(uid); pod != nil {
			healed = h.heal(ctx, pod, d)
		}
		// The tiers ran for the containers even if the remount is tried again.
		for _, key := range d.copyKeys {
			delete(h.strikes, key)
		}
		if !healed {
			// The strikes stay, so the next check heals again.
			continue
		}
		delete(h.failed, uid)
		for _, key := range d.keys {
			delete(h.strikes, key)
		}
	}
	return recheck
}

// deadReason says why a mount counts as dead, or "" when it does not.
func deadReason(pod *corev1.Pod, mounted bool, err error) string {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		// Not published yet, or already torn down.
		return ""
	case err != nil:
		return err.Error()
	case !mounted && active(pod) && started(pod):
		return "not mounted"
	}
	return ""
}

// heal reports false when the remount failed with attempts left, for the next
// check to heal again. Only the dead volumes are remounted through the driver;
// the tiers then act on each container for the volumes that are dead in it. The
// volumes dead in some containers only need no remount, so the tiers act for them
// whatever becomes of the remount.
func (h *Healer) heal(ctx context.Context, pod *corev1.Pod, d *podDue) bool {
	healed, retry := true, false
	from, why := Tier(0), ""
	if pod.DeletionTimestamp != nil {
		from, why = TierDelete, "it is stuck terminating on a dead mount"
	}
	// The volumes that are dead for each container, once remounted, until the
	// tiers acted for them.
	dead := containerVolumes{}
	for name, vols := range d.containers {
		for podVolume, target := range vols {
			dead.add(name, podVolume, target)
		}
	}
	remounted := false
	var remountErr error
	h.escalate(pod, from, why, func(t Tier, why string) error {
		if t == TierDelete {
			return h.deletePod(ctx, pod, why)
		}
		if !remounted {
			remounted = true
			var podVolumes map[string]string
			podVolumes, remountErr = h.remountAll(ctx, pod, d.dead)
			if remountErr != nil {
				retry, remountErr = h.remountFailed(pod, remountErr)
			} else {
				for name, vols := range mountedBy(pod, podVolumes) {
					for podVolume, target := range vols {
						dead.add(name, podVolume, target)
					}
				}
			}
		}
		if remountErr != nil {
			// Every tier before delete needs the remount for its volumes; it is not
			// tried twice. A pod that is deleted anyway takes the containers along.
			if to, _ := h.next(pod, TierDelete); len(dead) == 0 || to == TierDelete {
				return remountErr
			}
		}
		if len(dead) > 0 {
			if err := h.actOn(ctx, pod, t, dead, d.containers); err != nil {
				return err
			}
			dead = nil
		}
		if retry {
			healed = false
		}
		return remountErr
	})
	return healed
}

// actOn runs tier t for the volumes that are dead in each container. The ones in
// copies are dead in the container alone, so kubelet still holds the subPath
// binds it made of them, which a restart would reuse.
func (h *Healer) actOn(ctx context.Context, pod *corev1.Pod, t Tier, dead, copies containerVolumes) error {
	for name, vols := range copies {
		for _, target := range vols {
			if err := h.detachSubpaths(pod, filepath.Base(filepath.Dir(target)), name); err != nil {
				return err
			}
		}
	}
	if t == TierLive {
		return h.live(ctx, pod, dead)
	}
	var names []string
	for _, c := range longRunning(pod) {
		if len(dead[c.Name]) > 0 {
			names = append(names, c.Name)
		}
	}
	if err := h.restartContainers(ctx, pod, names); err != nil {
		return fmt.Errorf("restarting its containers: %w", err)
	}
	return nil
}

// remountFailed counts a failed remount of pod and reports whether attempts
// are left, in which case the next check heals again.
func (h *Healer) remountFailed(pod *corev1.Pod, err error) (bool, error) {
	down := errors.Is(err, errDriverDown)
	limit := h.cfg.RemountAttempts
	if down {
		limit = h.cfg.DriverDownAttempts
	}
	h.failed[pod.UID]++
	n := h.failed[pod.UID]
	if limit != 0 && n >= limit {
		if n > 1 {
			err = fmt.Errorf("%w, %d heals in a row", err, n)
		}
		return false, err
	}
	attempt := ""
	if limit != 0 {
		attempt = fmt.Sprintf(" (attempt %d of %d)", n, limit)
	}
	klog.Warningf("pod %s/%s: %v, healing again at the next check%s", pod.Namespace, pod.Name, err, attempt)
	if down {
		h.events.Eventf(pod, corev1.EventTypeWarning, "DriverDown", "The CSI driver cannot be reached, healing again at the next check%s", attempt)
	} else {
		h.events.Eventf(pod, corev1.EventTypeWarning, "RemountFailed", "%v, healing again at the next check%s", err, attempt)
	}
	return true, nil
}

func (h *Healer) remountAll(ctx context.Context, pod *corev1.Pod, vols []volume) (map[string]string, error) {
	podVolumes := map[string]string{}
	for _, v := range vols {
		name, err := h.remount(ctx, pod, v)
		if err != nil {
			return nil, fmt.Errorf("remounting %s: %w", filepath.Base(v.dir), err)
		}
		klog.Infof("pod %s/%s: remounted volume %s", pod.Namespace, pod.Name, name)
		h.events.Eventf(pod, corev1.EventTypeNormal, "Remounted", "Remounted volume %s through its CSI driver", name)
		podVolumes[name] = v.target
	}
	return podVolumes, nil
}

func (h *Healer) deletePod(ctx context.Context, pod *corev1.Pod, reason string) error {
	opts := metav1.DeleteOptions{Preconditions: metav1.NewUIDPreconditions(string(pod.UID))}
	if pod.DeletionTimestamp != nil {
		zero := int64(0)
		opts.GracePeriodSeconds = &zero
	}
	klog.Warningf("deleting pod %s/%s: %s", pod.Namespace, pod.Name, reason)
	h.events.Eventf(pod, corev1.EventTypeWarning, "DeletingPod", "Deleting the pod to heal its dead mount: %s", reason)
	return h.cfg.KubeClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, opts)
}

func active(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp == nil &&
		pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed
}

// started reports whether kubelet got as far as starting a container, which it
// only does once every volume is mounted. Before that a missing mount is normal.
func started(pod *corev1.Pod) bool {
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
		for _, s := range statuses {
			if s.State.Running != nil || s.State.Terminated != nil || s.RestartCount > 0 {
				return true
			}
		}
	}
	return false
}
