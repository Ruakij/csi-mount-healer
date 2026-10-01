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
	// Selector picks the volumes to check, heal and guard. It matches the labels
	// of the pod plus namespace and driver, which override pod labels of the same
	// name. Nil or empty picks every volume.
	Selector   labels.Selector
	KubeClient kubernetes.Interface
}

const uidIndex = "uid"

type Healer struct {
	cfg     Config
	prober  *prober
	pods    cache.Indexer
	events  record.EventRecorder
	strikes map[string]int
	// failed counts the heals in a row of a pod that failed to remount.
	failed map[types.UID]int
	// down are the drivers found down during the current scan, by name.
	down  map[string]error
	stale func(*swap) (int, []string, error)
	// mu serialises changes to mounts and guard flags, so a sweep never guards or
	// releases a volume halfway through a remount.
	mu sync.Mutex
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
	h := &Healer{cfg: cfg, prober: newProber(cfg.StatTimeout), strikes: map[string]int{}, failed: map[types.UID]int{}}
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
				h.sweep(string(pod.UID))
			}
		},
		DeleteFunc: func(obj any) {
			if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tomb.Obj
			}
			if pod, ok := obj.(*corev1.Pod); ok {
				h.sweep(string(pod.UID))
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
	factory.Start(ctx.Done())
	defer factory.Shutdown()
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return ctx.Err()
	}
	klog.Infof("watching CSI mounts on %s: interval %v, strikes %d, tiers %q, live timeout %v, remount attempts %d, driver down attempts %d, guard %s, guard stage %v, selector %q",
		h.cfg.NodeName, h.cfg.Interval, h.cfg.Strikes, h.cfg.Tiers, h.cfg.LiveTimeout, h.cfg.RemountAttempts, h.cfg.DriverDownAttempts, h.cfg.Guard, h.cfg.GuardStage, h.cfg.Selector)

	// Also clears what a previous run in another mode left behind.
	h.sweep("*")

	// A mount that appears or disappears is checked at once rather than at the
	// next tick; the strikes after it still come an interval apart.
	every(ctx, h.cfg.Interval, watchMounts(ctx), func() {
		// Retries guards that failed to set on a pod update.
		if h.cfg.Guard == GuardAlways || h.cfg.GuardStage {
			h.sweep("*")
		}
		h.scan(ctx)
	})

	h.mu.Lock()
	defer h.mu.Unlock()
	for _, v := range h.volumes("*") {
		if err := setImmutable(v.target, false); err != nil {
			klog.Warningf("releasing guard on %s: %v", v.target, err)
		}
	}
	h.guardStages(false)
	return nil
}

func indexByUID(obj any) ([]string, error) {
	return []string{string(obj.(*corev1.Pod).UID)}, nil
}

// every runs fn each interval, and at once on wake, which restarts the interval.
func every(ctx context.Context, interval time.Duration, wake <-chan struct{}, fn func()) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn()
		case <-wake:
			fn()
			t.Reset(interval)
		}
	}
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
func (h *Healer) sweep(podUID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, v := range h.volumes(podUID) {
		pod := h.pod(v.podUID)
		guard := h.cfg.Guard == GuardAlways && pod != nil && h.selected(pod, v) && active(pod) && started(pod)
		if err := setImmutable(v.target, guard); err != nil {
			klog.Warningf("setting guard on %s to %v: %v", v.target, guard, err)
		}
	}
	if h.cfg.GuardStage || podUID == "*" {
		h.guardStages(h.cfg.GuardStage)
	}
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
		if err := setImmutable(s, want[s]); err != nil {
			klog.Warningf("setting guard on %s to %v: %v", s, want[s], err)
		}
	}
}

func (h *Healer) scan(ctx context.Context) {
	h.down = map[string]error{}
	seen := map[string]bool{}
	due := map[types.UID][]volume{}
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
			continue
		}
		h.strikes[v.target]++
		klog.Warningf("pod %s/%s volume %s: %s (strike %d/%d)",
			pod.Namespace, pod.Name, filepath.Base(v.dir), reason, h.strikes[v.target], h.cfg.Strikes)
		if h.strikes[v.target] == h.cfg.Strikes {
			h.events.Eventf(pod, corev1.EventTypeWarning, "DeadMount", "Volume %s: %s, %d checks in a row",
				filepath.Base(v.dir), reason, h.cfg.Strikes)
		}
		if h.strikes[v.target] >= h.cfg.Strikes {
			due[pod.UID] = append(due[pod.UID], v)
		}
	}
	for target := range h.strikes {
		if !seen[target] {
			delete(h.strikes, target)
		}
	}
	for uid := range h.failed {
		if due[uid] == nil {
			delete(h.failed, uid)
		}
	}
	for uid, vols := range due {
		if pod := h.pod(uid); pod != nil && !h.heal(ctx, pod, vols) {
			// The strikes stay, so the next check heals again.
			continue
		}
		delete(h.failed, uid)
		for _, v := range vols {
			delete(h.strikes, v.target)
		}
	}
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
// check to heal again.
func (h *Healer) heal(ctx context.Context, pod *corev1.Pod, vols []volume) bool {
	healed, retry := true, false
	from, why := Tier(0), ""
	if pod.DeletionTimestamp != nil {
		from, why = TierDelete, "it is stuck terminating on a dead mount"
	}
	// Pod volume names to their targets, once remounted.
	var podVolumes map[string]string
	var remountErr error
	h.escalate(pod, from, why, func(t Tier, why string) error {
		if t == TierDelete {
			return h.deletePod(ctx, pod, why)
		}
		if podVolumes == nil && remountErr == nil {
			podVolumes, remountErr = h.remountAll(ctx, pod, vols)
			if remountErr != nil {
				retry, remountErr = h.remountFailed(pod, remountErr)
			}
		}
		if retry {
			healed = false
			return nil
		}
		if remountErr != nil {
			// Every tier before delete needs the remount; it is not tried twice.
			return remountErr
		}
		if t == TierLive {
			return h.live(ctx, pod, podVolumes)
		}
		var names []string
		for _, c := range containersUsing(pod, podVolumes) {
			names = append(names, c.Name)
		}
		if err := h.restartContainers(ctx, pod, names); err != nil {
			return fmt.Errorf("restarting its containers: %w", err)
		}
		return nil
	})
	return healed
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
