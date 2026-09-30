package healer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
)

var errGone = errors.New("the container is gone")

// liveMount is one volumeMount of a container, to replace with a clone of the
// new mount on the node.
type liveMount struct {
	target, subPath string
	// path is the mountPath in the container.
	path        string
	readOnly    bool
	propagation *corev1.MountPropagationMode
}

// swap is a container the live tier swapped new mounts into, whose processes
// may still hold handles on the dead ones.
type swap struct {
	pod       types.UID
	container string
	volumes   []string
	pid       int
	// ns is the inode of the mount namespace of the container.
	ns    uint64
	dead  map[uint64]bool
	since time.Time
	// handles is the count last reported in an event.
	handles int
}

// live swaps the remounted volumes into the running containers that mount
// them. A container it cannot swap goes on to the next tier on its own.
func (h *Healer) live(ctx context.Context, pod *corev1.Pod, podVolumes map[string]string) error {
	containers := containersUsing(pod, podVolumes)
	if len(containers) == 0 {
		return nil
	}
	conn, err := dial(h.cfg.CRIEndpoint)
	if err != nil {
		return err
	}
	defer conn.Close()
	rt := runtimeapi.NewRuntimeServiceClient(conn)
	running, err := runningContainers(ctx, rt, pod)
	if err != nil {
		return err
	}
	for _, c := range containers {
		ctr := running[c.Name]
		if ctr == nil {
			// kubelet starts it on the new mount.
			continue
		}
		s, err := h.swapContainer(ctx, rt, ctr.GetId(), c, podVolumes)
		if err != nil {
			h.escalateContainer(ctx, pod, c.Name, fmt.Sprintf("swapping the new mount into it failed: %v", err))
			continue
		}
		s.pod, s.container, s.since = pod.UID, c.Name, time.Now()
		klog.Infof("pod %s/%s: swapped volumes %s into container %s", pod.Namespace, pod.Name, strings.Join(s.volumes, ", "), c.Name)
		key := string(pod.UID) + "/" + c.Name
		h.swaps[key] = s
		h.checkSwap(ctx, key, s)
	}
	return nil
}

func (h *Healer) swapContainer(ctx context.Context, rt runtimeapi.RuntimeServiceClient, id string, c corev1.Container, podVolumes map[string]string) (*swap, error) {
	s := &swap{}
	var mounts []liveMount
	for _, m := range c.VolumeMounts {
		target := podVolumes[m.Name]
		if target == "" {
			continue
		}
		if m.SubPathExpr != "" {
			return nil, fmt.Errorf("the subPathExpr of %s is only expanded by kubelet", m.MountPath)
		}
		mounts = append(mounts, liveMount{target: target, subPath: m.SubPath, path: path.Clean(m.MountPath), readOnly: m.ReadOnly, propagation: m.MountPropagation})
		s.volumes = append(s.volumes, m.Name)
	}

	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := rt.ContainerStatus(sctx, &runtimeapi.ContainerStatusRequest{ContainerId: id, Verbose: true})
	if err != nil {
		return nil, fmt.Errorf("container status: %w", err)
	}
	var info struct {
		Pid int `json:"pid"`
	}
	if err := json.Unmarshal([]byte(resp.GetInfo()["info"]), &info); err != nil {
		return nil, fmt.Errorf("verbose container status: %w", err)
	}
	if info.Pid == 0 {
		return nil, errors.New("no pid in the verbose container status")
	}
	s.pid = info.Pid
	s.ns, s.dead, err = swapMounts(s.pid, mounts)
	return s, err
}

// escalateContainer sends one container the live tier did not heal on to the
// next tier.
func (h *Healer) escalateContainer(ctx context.Context, pod *corev1.Pod, name, why string) {
	if to, _ := h.next(pod, TierRestart); to < tierCount {
		klog.Warningf("pod %s/%s: container %s goes from the live tier to the %s tier: %s", pod.Namespace, pod.Name, name, to, why)
		h.events.Eventf(pod, corev1.EventTypeWarning, "Escalating", "Container %s goes from the live tier to the %s tier: %s", name, to, why)
	}
	h.escalate(pod, TierRestart, "container "+name+": "+why, func(t Tier, why string) error {
		if t == TierDelete {
			return h.deletePod(ctx, pod, why)
		}
		return h.restartContainers(ctx, pod, []string{name})
	})
}

func (h *Healer) checkSwaps(ctx context.Context) {
	for key, s := range h.swaps {
		h.checkSwap(ctx, key, s)
	}
}

// checkSwap counts the handles a swapped container still holds on its dead
// mounts, and sends it on to the next tier when they outlive LiveTimeout.
func (h *Healer) checkSwap(ctx context.Context, key string, s *swap) {
	pod := h.pod(s.pod)
	if pod == nil || !active(pod) {
		delete(h.swaps, key)
		return
	}
	n, procs, err := h.stale(s)
	switch {
	case errors.Is(err, errGone):
		delete(h.swaps, key)
	case err != nil:
		klog.Warningf("pod %s/%s: counting handles on the dead mounts of container %s: %v", pod.Namespace, pod.Name, s.container, err)
	case n == 0:
		delete(h.swaps, key)
		klog.Infof("pod %s/%s: container %s holds nothing on its dead mounts", pod.Namespace, pod.Name, s.container)
		h.events.Eventf(pod, corev1.EventTypeNormal, "Remounted", "Swapped volumes %s into running container %s, nothing holds the dead mounts",
			strings.Join(s.volumes, ", "), s.container)
	default:
		klog.Warningf("pod %s/%s: container %s holds %d handles on its dead mounts: %s", pod.Namespace, pod.Name, s.container, n, strings.Join(procs, ", "))
		// Checks run once per interval, so the timeout ends at the check nearest to
		// it rather than one interval late.
		if time.Until(s.since.Add(h.cfg.LiveTimeout)) < h.cfg.Interval/2 {
			delete(h.swaps, key)
			h.escalateContainer(ctx, pod, s.container, fmt.Sprintf("%d handles still on the dead mounts after %v: %s", n, h.cfg.LiveTimeout, strings.Join(procs, ", ")))
			return
		}
		if n != s.handles {
			s.handles = n
			h.events.Eventf(pod, corev1.EventTypeWarning, "StaleHandles", "Container %s holds %d handles on the dead mounts of volumes %s: %s",
				s.container, n, strings.Join(s.volumes, ", "), strings.Join(procs, ", "))
		}
	}
}
