package healer

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
)

// csiTimeout bounds a single call to a CSI driver, as the one in kubelet
// csiTimeout does.
const csiTimeout = 2 * time.Minute

// remount mounts a dead volume again through its CSI driver, the way kubelet
// mounted it, and returns the name of the pod volume.
func (h *Healer) remount(ctx context.Context, pod *corev1.Pod, v volume) (string, error) {
	data, err := v.data()
	if err != nil {
		return "", err
	}
	conn, caps, err := h.reachDriver(ctx, data.DriverName)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	node := csi.NewNodeClient(conn)

	in, err := h.inputs(ctx, pod, v, data)
	if err != nil {
		return "", err
	}
	in.caps = caps
	reqs, err := buildRequests(in)
	if err != nil {
		return "", err
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if reqs.stage != nil {
		if err := h.restage(ctx, node, reqs.stage); err != nil {
			return "", fmt.Errorf("staging: %w", err)
		}
	}

	// Drivers remove the target directory on unpublish, which the guard forbids.
	if err := setImmutable(v.target, false); err != nil {
		return "", err
	}
	cctx, cancel := context.WithTimeout(ctx, csiTimeout)
	defer cancel()
	if _, err := node.NodeUnpublishVolume(cctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId: data.VolumeHandle, TargetPath: v.target,
	}); err != nil {
		klog.Warningf("NodeUnpublishVolume %s: %v, detaching it instead", v.target, err)
	}
	if err := detach(v.target); err != nil {
		return "", err
	}
	if err := os.MkdirAll(v.target, 0o750); err != nil {
		return "", err
	}
	if h.cfg.Guard != GuardOff {
		if err := setImmutable(v.target, true); err != nil {
			return "", err
		}
	}
	pctx, pcancel := context.WithTimeout(ctx, csiTimeout)
	defer pcancel()
	if _, err := node.NodePublishVolume(pctx, reqs.publish); err != nil {
		return "", fmt.Errorf("NodePublishVolume: %w", err)
	}
	if err := h.verify(v.target); err != nil {
		return "", err
	}

	// kubelet reuses a subPath bind that still exists, and this one points into
	// the dead mount. Without it, kubelet binds the new mount on container start.
	// kubelet names the directory after the inner volume spec name, the PV name
	// for a PVC, as it does the volume directory.
	subpaths, _ := filepath.Glob(filepath.Join(h.cfg.KubeletRoot, "pods", string(pod.UID), "volume-subpaths", filepath.Base(v.dir), "*", "*"))
	for _, p := range subpaths {
		if err := detach(p); err != nil {
			return "", fmt.Errorf("detaching subPath %s: %w", p, err)
		}
	}

	if h.cfg.Guard == GuardRemount {
		if err := setImmutable(v.target, false); err != nil {
			return "", err
		}
	}
	return reqs.podVolume, nil
}

// restage stages the volume again when its staging mount is dead. A live one
// is left alone: other pods on this node may be publishing from it.
func (h *Healer) restage(ctx context.Context, node csi.NodeClient, req *csi.NodeStageVolumeRequest) error {
	path := req.StagingTargetPath
	if mounted, err := h.prober.probe(path); err == nil && mounted {
		return nil
	}
	klog.Infof("restaging %s", path)
	cctx, cancel := context.WithTimeout(ctx, csiTimeout)
	defer cancel()
	if _, err := node.NodeUnstageVolume(cctx, &csi.NodeUnstageVolumeRequest{
		VolumeId: req.VolumeId, StagingTargetPath: path,
	}); err != nil {
		klog.Warningf("NodeUnstageVolume %s: %v, detaching it instead", path, err)
	}
	if err := detach(path); err != nil {
		return err
	}
	if err := os.MkdirAll(path, 0o750); err != nil {
		return err
	}
	if _, err := node.NodeStageVolume(cctx, req); err != nil {
		return fmt.Errorf("NodeStageVolume: %w", err)
	}
	return h.verify(path)
}

// verify checks that path is a live mount. A stat still hung on the old mount
// says nothing about the new one, so it is forgotten first.
func (h *Healer) verify(path string) error {
	h.prober.forget(path)
	mounted, err := h.prober.probe(path)
	if err != nil {
		return fmt.Errorf("new mount on %s: %w", path, err)
	}
	if !mounted {
		return fmt.Errorf("driver reported success but %s is not mounted", path)
	}
	return nil
}

// inputs looks up what kubelet looks up before it calls the driver.
func (h *Healer) inputs(ctx context.Context, pod *corev1.Pod, v volume, data volData) (requestInputs, error) {
	in := requestInputs{pod: pod, vol: data, target: v.target, kubeletDir: h.cfg.KubeletRoot}
	var err error
	api := h.cfg.KubeClient
	if in.driver, err = api.StorageV1().CSIDrivers().Get(ctx, data.DriverName, metav1.GetOptions{}); err != nil {
		return in, err
	}

	if data.ephemeral() {
		if pv := podVolume(pod, data.SpecVolID); pv != nil && pv.CSI != nil && pv.CSI.NodePublishSecretRef != nil {
			in.publishSecrets, err = h.secret(ctx, pod.Namespace, pv.CSI.NodePublishSecretRef.Name)
		}
		return in, err
	}

	if in.pv, err = api.CoreV1().PersistentVolumes().Get(ctx, data.SpecVolID, metav1.GetOptions{}); err != nil {
		return in, err
	}
	if src := in.pv.Spec.CSI; src != nil {
		if ref := src.NodePublishSecretRef; ref != nil {
			if in.publishSecrets, err = h.secret(ctx, ref.Namespace, ref.Name); err != nil {
				return in, err
			}
		}
		if ref := src.NodeStageSecretRef; ref != nil && in.caps.stage {
			if in.stageSecrets, err = h.secret(ctx, ref.Namespace, ref.Name); err != nil {
				return in, err
			}
		}
	}
	if in.driver.Spec.AttachRequired == nil || *in.driver.Spec.AttachRequired {
		name := data.AttachmentID
		if name == "" {
			// getAttachmentName in kubelet
			name = fmt.Sprintf("csi-%x", sha256.Sum256([]byte(data.VolumeHandle+data.DriverName+data.NodeName)))
		}
		va, err := api.StorageV1().VolumeAttachments().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return in, err
		}
		in.publishContext = va.Status.AttachmentMetadata
	}
	return in, nil
}

func (h *Healer) secret(ctx context.Context, namespace, name string) (map[string]string, error) {
	s, err := h.cfg.KubeClient.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	m := make(map[string]string, len(s.Data))
	for k, v := range s.Data {
		m[k] = string(v)
	}
	return m, nil
}

func nodeCapabilities(ctx context.Context, node csi.NodeClient) (nodeCaps, error) {
	var caps nodeCaps
	cctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	resp, err := node.NodeGetCapabilities(cctx, &csi.NodeGetCapabilitiesRequest{})
	if err != nil {
		return caps, fmt.Errorf("NodeGetCapabilities: %w", err)
	}
	for _, c := range resp.GetCapabilities() {
		switch c.GetRpc().GetType() {
		case csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME:
			caps.stage = true
		case csi.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER:
			caps.singleNodeMulti = true
		case csi.NodeServiceCapability_RPC_VOLUME_MOUNT_GROUP:
			caps.volumeMountGroup = true
		}
	}
	return caps, nil
}

// dialDriver finds the socket of a CSI driver the way kubelet does: by asking
// each registration socket in plugins_registry who it is.
func (h *Healer) dialDriver(ctx context.Context, name string) (*grpc.ClientConn, error) {
	socks, _ := filepath.Glob(filepath.Join(h.cfg.KubeletRoot, "plugins_registry", "*.sock"))
	for _, sock := range socks {
		info, err := pluginInfo(ctx, sock)
		if err != nil {
			klog.V(2).Infof("plugin registration %s: %v", sock, err)
			continue
		}
		if info.Type == registerapi.CSIPlugin && info.Name == name {
			if info.Endpoint == "" {
				return nil, fmt.Errorf("driver %s registered without an endpoint", name)
			}
			return dial(info.Endpoint)
		}
	}
	return nil, fmt.Errorf("driver %s %w", name, errNotRegistered)
}

var (
	errNotRegistered = errors.New("is not registered with kubelet on this node")
	errDriverDown    = errors.New("the CSI driver cannot be reached")
)

// probeTimeout bounds the calls that only ask a socket who it is, which any
// running driver answers at once.
const probeTimeout = 10 * time.Second

// Attempts to reach a driver before it counts as down, so a driver in the
// middle of a restart does not.
var driverAttempts, driverRetryDelay = 3, 10 * time.Second

// reachDriver connects to a CSI driver and asks for its node capabilities. A
// driver that is not registered, refuses the connection or does not answer is
// tried again, and after the last attempt counts as down, wrapping
// errDriverDown, for the rest of the scan.
func (h *Healer) reachDriver(ctx context.Context, name string) (*grpc.ClientConn, nodeCaps, error) {
	if err := h.down[name]; err != nil {
		return nil, nodeCaps{}, err
	}
	var err error
	for attempt := range driverAttempts {
		if attempt > 0 {
			klog.V(2).Infof("driver %s: %v, trying again in %v", name, err, driverRetryDelay)
			select {
			case <-ctx.Done():
				return nil, nodeCaps{}, ctx.Err()
			case <-time.After(driverRetryDelay):
			}
		}
		var conn *grpc.ClientConn
		if conn, err = h.dialDriver(ctx, name); err == nil {
			var caps nodeCaps
			if caps, err = nodeCapabilities(ctx, csi.NewNodeClient(conn)); err == nil {
				return conn, caps, nil
			}
			conn.Close()
		}
		if code := status.Code(err); !errors.Is(err, errNotRegistered) && code != codes.Unavailable && code != codes.DeadlineExceeded {
			return nil, nodeCaps{}, err
		}
	}
	err = fmt.Errorf("%w: %w", errDriverDown, err)
	if h.down != nil {
		h.down[name] = err
	}
	return nil, nodeCaps{}, err
}

func pluginInfo(ctx context.Context, sock string) (*registerapi.PluginInfo, error) {
	conn, err := dial(sock)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	cctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	return registerapi.NewRegistrationClient(conn).GetInfo(cctx, &registerapi.InfoRequest{})
}

// dial connects to a unix socket, given as a path or a unix:// URL.
func dial(endpoint string) (*grpc.ClientConn, error) {
	return grpc.NewClient("unix://"+strings.TrimPrefix(endpoint, "unix://"),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// containersUsing lists the containers of the pod that mount one of the pod
// volumes. Only sidecars among the init containers keep running; the others are
// done by now.
func containersUsing(pod *corev1.Pod, podVolumes map[string]string) []corev1.Container {
	var using []corev1.Container
	containers := pod.Spec.Containers
	for _, c := range pod.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			containers = append(containers, c)
		}
	}
	for _, c := range containers {
		if slices.ContainsFunc(c.VolumeMounts, func(m corev1.VolumeMount) bool { return podVolumes[m.Name] != "" }) {
			using = append(using, c)
		}
	}
	return using
}

// runningContainers maps the names of the running containers of the pod to them.
func runningContainers(ctx context.Context, rt runtimeapi.RuntimeServiceClient, pod *corev1.Pod) (map[string]*runtimeapi.Container, error) {
	lctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := rt.ListContainers(lctx, &runtimeapi.ListContainersRequest{Filter: &runtimeapi.ContainerFilter{
		State:         &runtimeapi.ContainerStateValue{State: runtimeapi.ContainerState_CONTAINER_RUNNING},
		LabelSelector: map[string]string{"io.kubernetes.pod.uid": string(pod.UID)},
	}})
	if err != nil {
		return nil, fmt.Errorf("listing containers: %w", err)
	}
	running := map[string]*runtimeapi.Container{}
	for _, c := range resp.GetContainers() {
		running[c.GetLabels()["io.kubernetes.container.name"]] = c
	}
	return running, nil
}

// restartContainers stops the named containers that are running, each of which
// still sees the dead mount. kubelet starts them again on the new one.
func (h *Healer) restartContainers(ctx context.Context, pod *corev1.Pod, names []string) error {
	if len(names) == 0 {
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

	grace := int64(corev1.DefaultTerminationGracePeriodSeconds)
	if pod.Spec.TerminationGracePeriodSeconds != nil {
		grace = *pod.Spec.TerminationGracePeriodSeconds
	}
	var errs []error
	for _, name := range names {
		c := running[name]
		if c == nil {
			continue
		}
		klog.Infof("pod %s/%s: stopping container %s", pod.Namespace, pod.Name, name)
		sctx, cancel := context.WithTimeout(ctx, time.Duration(grace)*time.Second+30*time.Second)
		_, err := rt.StopContainer(sctx, &runtimeapi.StopContainerRequest{ContainerId: c.GetId(), Timeout: grace})
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("stopping %s: %w", name, err))
		}
	}
	return errors.Join(errs...)
}
