// Package healer finds dead CSI mounts of the pods on this node and remounts them
// through the CSI driver, or deletes the pod when that is not possible.
package healer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
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
	// Remount asks the CSI driver to mount a dead volume again. Without it, or when
	// that fails, the pod is deleted.
	Remount       bool
	Guard         GuardMode
	GuardInterval time.Duration
	CRIEndpoint   string
	KubeClient    kubernetes.Interface
}

const uidIndex = "uid"

type Healer struct {
	cfg     Config
	prober  *prober
	pods    cache.Indexer
	strikes map[string]int
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
	return &Healer{cfg: cfg, prober: newProber(cfg.StatTimeout), strikes: map[string]int{}}, nil
}

// Run checks the mounts until ctx is done, then releases every guard so nothing
// stays immutable while the healer is not running to clear it.
func (h *Healer) Run(ctx context.Context) error {
	factory := informers.NewSharedInformerFactoryWithOptions(h.cfg.KubeClient, 0,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", h.cfg.NodeName).String()
		}))
	informer := factory.Core().V1().Pods().Informer()
	if err := informer.AddIndexers(cache.Indexers{uidIndex: func(obj any) ([]string, error) {
		return []string{string(obj.(*corev1.Pod).UID)}, nil
	}}); err != nil {
		return err
	}
	// kubelet cannot remove an immutable mount directory, and the pod object stays
	// until it has, so the guard is released as soon as a pod stops.
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(_, obj any) {
			if pod := obj.(*corev1.Pod); !active(pod) {
				h.release(pod.UID)
			}
		},
		DeleteFunc: func(obj any) {
			if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tomb.Obj
			}
			if pod, ok := obj.(*corev1.Pod); ok {
				h.release(pod.UID)
			}
		},
	}); err != nil {
		return err
	}
	h.pods = informer.GetIndexer()
	factory.Start(ctx.Done())
	defer factory.Shutdown()
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return ctx.Err()
	}
	klog.Infof("watching CSI mounts on %s: interval %v, strikes %d, remount %v, guard %s",
		h.cfg.NodeName, h.cfg.Interval, h.cfg.Strikes, h.cfg.Remount, h.cfg.Guard)

	// Also clears what a previous run in another mode left behind.
	h.sweep()

	var wg sync.WaitGroup
	if h.cfg.Guard == GuardAlways {
		wg.Go(func() { every(ctx, h.cfg.GuardInterval, h.sweep) })
	}
	every(ctx, h.cfg.Interval, func() { h.scan(ctx) })
	wg.Wait()

	h.mu.Lock()
	defer h.mu.Unlock()
	for _, v := range h.volumes("*") {
		if err := setImmutable(v.target, false); err != nil {
			klog.Warningf("releasing guard on %s: %v", v.target, err)
		}
	}
	return nil
}

func every(ctx context.Context, interval time.Duration, fn func()) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn()
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

// sweep guards the mounts of running pods and releases every other one. Each step
// is idempotent, so it simply converges on what the pods look like right now.
func (h *Healer) sweep() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, v := range h.volumes("*") {
		pod := h.pod(v.podUID)
		guard := h.cfg.Guard == GuardAlways && pod != nil && active(pod) && started(pod)
		if err := setImmutable(v.target, guard); err != nil {
			klog.Warningf("setting guard on %s to %v: %v", v.target, guard, err)
		}
	}
}

func (h *Healer) release(uid types.UID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, v := range h.volumes(string(uid)) {
		if err := setImmutable(v.target, false); err != nil {
			klog.Warningf("releasing guard on %s: %v", v.target, err)
		}
	}
}

func (h *Healer) scan(ctx context.Context) {
	seen := map[string]bool{}
	due := map[types.UID][]volume{}
	for _, v := range h.volumes("*") {
		pod := h.pod(v.podUID)
		if pod == nil {
			// Left over from a pod that is gone; kubelet cleans it up.
			continue
		}
		seen[v.target] = true
		mounted, err := h.prober.probe(v.target)
		reason := deadReason(pod, mounted, err)
		if reason == "" {
			delete(h.strikes, v.target)
			continue
		}
		h.strikes[v.target]++
		klog.Warningf("pod %s/%s volume %s: %s (strike %d/%d)",
			pod.Namespace, pod.Name, filepath.Base(v.dir), reason, h.strikes[v.target], h.cfg.Strikes)
		if h.strikes[v.target] >= h.cfg.Strikes {
			delete(h.strikes, v.target)
			due[pod.UID] = append(due[pod.UID], v)
		}
	}
	for target := range h.strikes {
		if !seen[target] {
			delete(h.strikes, target)
		}
	}
	for uid, vols := range due {
		if pod := h.pod(uid); pod != nil {
			h.heal(ctx, pod, vols)
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

func (h *Healer) heal(ctx context.Context, pod *corev1.Pod, vols []volume) {
	switch {
	case pod.DeletionTimestamp != nil:
		h.deletePod(ctx, pod, "it is stuck terminating on a dead mount")
		return
	case !h.cfg.Remount:
		h.deletePod(ctx, pod, "remounting is disabled")
		return
	case pod.Spec.RestartPolicy != corev1.RestartPolicyAlways:
		// Its containers would not come back after being stopped for the remount.
		h.deletePod(ctx, pod, "restartPolicy is "+string(pod.Spec.RestartPolicy))
		return
	}
	podVolumes := map[string]bool{}
	for _, v := range vols {
		name, err := h.remount(ctx, pod, v)
		if err != nil {
			h.deletePod(ctx, pod, fmt.Sprintf("remounting %s failed: %v", filepath.Base(v.dir), err))
			return
		}
		klog.Infof("pod %s/%s: remounted volume %s", pod.Namespace, pod.Name, name)
		podVolumes[name] = true
	}
	if err := h.restartContainers(ctx, pod, podVolumes); err != nil {
		h.deletePod(ctx, pod, fmt.Sprintf("restarting its containers failed: %v", err))
	}
}

func (h *Healer) deletePod(ctx context.Context, pod *corev1.Pod, reason string) {
	opts := metav1.DeleteOptions{Preconditions: metav1.NewUIDPreconditions(string(pod.UID))}
	if pod.DeletionTimestamp != nil {
		zero := int64(0)
		opts.GracePeriodSeconds = &zero
	}
	klog.Warningf("deleting pod %s/%s: %s", pod.Namespace, pod.Name, reason)
	if err := h.cfg.KubeClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, opts); err != nil {
		klog.Errorf("deleting pod %s/%s: %v", pod.Namespace, pod.Name, err)
	}
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
