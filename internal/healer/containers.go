package healer

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
)

// Labels the container runtime puts on the containers of a pod.
const (
	podUIDLabel        = "io.kubernetes.pod.uid"
	containerNameLabel = "io.kubernetes.container.name"
)

// errNoRoot means the root of a container could not be opened, which says
// nothing about the mounts in it.
var errNoRoot = errors.New("cannot open the root of the container")

// containerRef names a container of a pod.
type containerRef struct {
	pod  types.UID
	name string
}

// copyCheck is the outcome of probing the copy of a volume in a container, with
// the reason it is dead or "" if it is not.
type copyCheck struct {
	pod       *corev1.Pod
	volume    volume
	podVolume string
	container string
	// id is the ID of the container, which a restart changes. It is empty when the
	// check concerns every container of the volume.
	id     string
	reason string
	// unknown is set when the copy could not be checked, which leaves its strikes
	// as they are.
	unknown bool
}

// copyKey is the key of the strikes of the copy of a volume in a container.
func copyKey(target, id string) string { return target + " " + id }

// containerCopies probes the copy of each healthy volume in every running
// container that mounts it. The runtime makes that copy when the container
// starts, so it can be dead while the publish path is healthy, as when a driver
// mounts that path anew under running pods. A lookup that fails says nothing
// about the mounts it concerns, so their checks are unknown.
func (h *Healer) containerCopies(ctx context.Context, healthy map[*corev1.Pod][]volume) []copyCheck {
	if len(healthy) == 0 {
		return nil
	}
	conn, err := dial(h.cfg.CRIEndpoint)
	if err != nil {
		klog.Warningf("skipping the container checks: %v", err)
		return unknownCopies(healthy)
	}
	defer conn.Close()
	rt := runtimeapi.NewRuntimeServiceClient(conn)
	ids, err := h.runningIDs(ctx, rt)
	if err != nil {
		klog.Warningf("skipping the container checks: %v", err)
		return unknownCopies(healthy)
	}
	var checks []copyCheck
	for pod, vols := range healthy {
		named, failed := h.namedVolumes(ctx, pod, vols)
		for _, v := range failed {
			checks = append(checks, copyCheck{pod: pod, volume: v, unknown: true})
		}
		// A copy below the copy of one of the other volumes cannot be probed if that
		// one is dead.
		var others []volume
		for _, v := range h.volumes(string(pod.UID)) {
			if !slices.Contains(vols, v) {
				others = append(others, v)
			}
		}
		down, _ := h.namedVolumes(ctx, pod, others)
		targets := map[string]string{}
		for name, v := range named {
			targets[name] = v.target
		}
		for _, c := range containersUsing(pod, targets) {
			id, ok := ids[containerRef{pod.UID, c.Name}]
			if !ok {
				// kubelet mounts the current volume when it starts the container.
				continue
			}
			pid, err := h.containerPid(ctx, rt, id)
			if err != nil {
				klog.Warningf("pod %s/%s: not checking the volumes in container %s: %v", pod.Namespace, pod.Name, c.Name, err)
				checks = append(checks, unknownIn(pod, c, id, named)...)
				continue
			}
			checks = append(checks, h.containerChecks(pod, c, id, pid, named, down)...)
		}
	}
	return checks
}

// unknownCopies makes every copy of the volumes in any container unknown.
func unknownCopies(healthy map[*corev1.Pod][]volume) []copyCheck {
	var checks []copyCheck
	for pod, vols := range healthy {
		for _, v := range vols {
			checks = append(checks, copyCheck{pod: pod, volume: v, unknown: true})
		}
	}
	return checks
}

// unknownIn makes the copies in container c of the volumes in named unknown.
func unknownIn(pod *corev1.Pod, c corev1.Container, id string, named map[string]volume) []copyCheck {
	var checks []copyCheck
	for name, v := range named {
		if slices.ContainsFunc(c.VolumeMounts, func(m corev1.VolumeMount) bool { return m.Name == name }) {
			checks = append(checks, copyCheck{pod: pod, volume: v, podVolume: name, container: c.Name, id: id, unknown: true})
		}
	}
	return checks
}

// containerChecks probes the copies in container c, whose main process is pid, of
// the volumes in named. The first dead one of the mounts of a volume tells. A
// lookup below a dead FUSE mount makes the kernel detach the mounts on that
// dentry, so the mounts go in path order and none below a dead or hung one is
// probed, nor any below one of the volumes in down, whose publish path is not
// healthy. The heal of that mount takes the ones below along.
func (h *Healer) containerChecks(pod *corev1.Pod, c corev1.Container, id string, pid int, named, down map[string]volume) []copyCheck {
	mounts := slices.Clone(c.VolumeMounts)
	slices.SortStableFunc(mounts, func(a, b corev1.VolumeMount) int {
		return strings.Compare(path.Clean(a.MountPath), path.Clean(b.MountPath))
	})
	reasons := map[string]string{}
	var dead []string
	for _, m := range mounts {
		p := path.Clean(m.MountPath)
		if slices.ContainsFunc(dead, func(d string) bool { return strings.HasPrefix(p, d+"/") }) {
			continue
		}
		if _, ok := down[m.Name]; ok {
			dead = append(dead, p)
			continue
		}
		if _, ok := named[m.Name]; !ok {
			continue
		}
		mounted, err := h.prober.probeIn(pid, p)
		if errors.Is(err, errNoRoot) {
			klog.Warningf("pod %s/%s: not checking the volumes in container %s: %v", pod.Namespace, pod.Name, c.Name, err)
			return unknownIn(pod, c, id, named)
		}
		reason := deadReason(pod, mounted, err)
		if reasons[m.Name] == "" {
			reasons[m.Name] = reason
		}
		if reason != "" {
			dead = append(dead, p)
		}
	}
	var checks []copyCheck
	for name, reason := range reasons {
		checks = append(checks, copyCheck{pod: pod, volume: named[name], podVolume: name, container: c.Name, id: id, reason: reason})
	}
	return checks
}

// runningIDs maps the running containers of the node to their IDs with one
// listing, and forgets the pids of the containers that are gone.
func (h *Healer) runningIDs(ctx context.Context, rt runtimeapi.RuntimeServiceClient) (map[containerRef]string, error) {
	listed, err := listRunning(ctx, rt, nil)
	if err != nil {
		return nil, err
	}
	ids, running := map[containerRef]string{}, map[string]bool{}
	for _, c := range listed {
		ids[containerRef{types.UID(c.GetLabels()[podUIDLabel]), c.GetLabels()[containerNameLabel]}] = c.GetId()
		running[c.GetId()] = true
	}
	maps.DeleteFunc(h.pids, func(id string, _ int) bool { return !running[id] })
	return ids, nil
}

// containerPid returns the pid of the main process of a container. It stays the
// same for the life of the container, so the runtime is asked once.
func (h *Healer) containerPid(ctx context.Context, rt runtimeapi.RuntimeServiceClient, id string) (int, error) {
	if pid, ok := h.pids[id]; ok {
		return pid, nil
	}
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := rt.ContainerStatus(sctx, &runtimeapi.ContainerStatusRequest{ContainerId: id, Verbose: true})
	if err != nil {
		return 0, fmt.Errorf("container status: %w", err)
	}
	pid, err := statusPid(resp)
	if err != nil {
		return 0, err
	}
	h.pids[id] = pid
	return pid, nil
}

// namedVolumes keys the volumes by the name of their pod volume, which is what
// the containers mount them by. The volumes whose pod volume cannot be told are
// returned apart.
func (h *Healer) namedVolumes(ctx context.Context, pod *corev1.Pod, vols []volume) (named map[string]volume, failed []volume) {
	named = map[string]volume{}
	for _, v := range vols {
		name, err := h.podVolumeName(ctx, pod, v)
		if err != nil {
			klog.Warningf("pod %s/%s: not telling the pod volume of %s: %v", pod.Namespace, pod.Name, filepath.Base(v.dir), err)
			failed = append(failed, v)
			continue
		}
		named[name] = v
	}
	return named, failed
}

// podVolumeName returns the name of the pod volume that the volume directory of
// kubelet belongs to. kubelet names the directory after the volume spec: the pod
// volume for an inline ephemeral volume, the PV for a claim. The name is fixed
// for the pod, so it is looked up once.
func (h *Healer) podVolumeName(ctx context.Context, pod *corev1.Pod, v volume) (string, error) {
	if name, ok := h.volumeNames[v.target]; ok {
		return name, nil
	}
	data, err := v.data()
	if err != nil {
		return "", err
	}
	name := data.SpecVolID
	if !data.ephemeral() {
		pv, err := h.cfg.KubeClient.CoreV1().PersistentVolumes().Get(ctx, data.SpecVolID, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		var ok bool
		if name, _, ok = claimVolume(pod, pv); !ok {
			return "", fmt.Errorf("pod has no volume for persistent volume %q", pv.Name)
		}
	}
	h.volumeNames[v.target] = name
	return name, nil
}
