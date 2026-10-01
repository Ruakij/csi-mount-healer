//go:build linux && mounttest

package healer

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
)

func mountTmpfs(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount("tmpfs", path, "tmpfs", 0, ""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = detach(path) })
}

func mustStat(t *testing.T, path string) bool {
	t.Helper()
	mounted, err := stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return mounted
}

func TestStat(t *testing.T) {
	root := t.TempDir()
	plain := filepath.Join(root, "plain")
	if err := os.Mkdir(plain, 0o755); err != nil {
		t.Fatal(err)
	}
	if mustStat(t, plain) {
		t.Error("plain directory reported as mounted")
	}

	fsRoot := filepath.Join(root, "tmpfs")
	mountTmpfs(t, fsRoot)
	if !mustStat(t, fsRoot) {
		t.Error("tmpfs not reported as mounted")
	}

	// A bind from the same filesystem shares st_dev with its parent.
	bind := filepath.Join(fsRoot, "bind")
	src := filepath.Join(fsRoot, "src")
	for _, d := range []string{bind, src} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := unix.Mount(src, bind, "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	if !mustStat(t, bind) {
		t.Error("same-filesystem bind not reported as mounted")
	}

	// Stacked mounts all go.
	if err := unix.Mount("tmpfs", bind, "tmpfs", 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := detach(bind); err != nil {
		t.Fatal(err)
	}
	if mustStat(t, bind) {
		t.Error("still mounted after detach")
	}
	if err := detach(filepath.Join(root, "missing")); err != nil {
		t.Errorf("detach of a missing path: %v", err)
	}
}

func TestGuard(t *testing.T) {
	target := filepath.Join(t.TempDir(), "mount")
	// No access for the healer without DAC capabilities, as a directory a container
	// chowned to its own user.
	if err := os.Mkdir(target, 0); err != nil {
		t.Fatal(err)
	}
	mountTmpfs(t, target)

	if err := setImmutable(target, true); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = setImmutable(target, false) })
	if err := os.WriteFile(filepath.Join(target, "on-mount"), nil, 0o644); err != nil {
		t.Fatalf("the mount itself must stay writable: %v", err)
	}

	if err := detach(target); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "on-disk"), nil, 0o644); !errors.Is(err, unix.EPERM) {
		t.Fatalf("write underneath a guarded mount: got %v, want EPERM", err)
	}
	if err := os.Remove(target); !errors.Is(err, unix.EPERM) {
		t.Fatalf("removing a guarded directory: got %v, want EPERM", err)
	}

	if err := setImmutable(target, false); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(target, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "on-disk"), nil, 0o644); err != nil {
		t.Fatalf("write after release: %v", err)
	}
	if err := setImmutable(filepath.Join(target, "missing", "mount"), true); err != nil {
		t.Errorf("guarding a missing path: %v", err)
	}
}

// fakeDriver stages a tmpfs and publishes it by bind mount, removing the target
// on unpublish as real drivers do.
type fakeDriver struct {
	csi.UnimplementedNodeServer
	registerapi.UnimplementedRegistrationServer
	name, endpoint string
	published      *csi.NodePublishVolumeRequest
}

func (d *fakeDriver) GetInfo(context.Context, *registerapi.InfoRequest) (*registerapi.PluginInfo, error) {
	return &registerapi.PluginInfo{Type: registerapi.CSIPlugin, Name: d.name, Endpoint: d.endpoint}, nil
}

func (d *fakeDriver) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{Capabilities: []*csi.NodeServiceCapability{{
		Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME}},
	}}}, nil
}

func (d *fakeDriver) NodeStageVolume(_ context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return &csi.NodeStageVolumeResponse{}, unix.Mount("tmpfs", req.StagingTargetPath, "tmpfs", 0, "")
}

func (d *fakeDriver) NodeUnstageVolume(_ context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return &csi.NodeUnstageVolumeResponse{}, unix.Unmount(req.StagingTargetPath, 0)
}

func (d *fakeDriver) NodePublishVolume(_ context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	d.published = req
	if err := os.MkdirAll(req.TargetPath, 0o750); err != nil {
		return nil, err
	}
	return &csi.NodePublishVolumeResponse{}, unix.Mount(req.StagingTargetPath, req.TargetPath, "", unix.MS_BIND, "")
}

func (d *fakeDriver) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	_ = unix.Unmount(req.TargetPath, 0)
	return &csi.NodeUnpublishVolumeResponse{}, os.Remove(req.TargetPath)
}

func serve(t *testing.T, sock string, register func(*grpc.Server)) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(sock), 0o755); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	register(s)
	go func() { _ = s.Serve(l) }()
	t.Cleanup(s.Stop)
}

func TestRemount(t *testing.T) {
	root := t.TempDir()
	pod := testPod()
	pod.Spec.RestartPolicy = corev1.RestartPolicyAlways
	pv := testPV("data-app-0", corev1.ReadWriteOnce)
	dir := filepath.Join(root, "pods", string(pod.UID), "volumes", "kubernetes.io~csi", pv.Name)
	v := volume{podUID: pod.UID, dir: dir, target: filepath.Join(dir, "mount")}

	// A publish whose mount is gone, and a subPath bind into it.
	if err := os.MkdirAll(v.target, 0o750); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(volData{SpecVolID: pv.Name, VolumeHandle: "h", DriverName: "d", NodeName: "n", LifecycleMode: "Persistent"})
	if err := os.WriteFile(filepath.Join(dir, "vol_data.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	subpath := filepath.Join(root, "pods", string(pod.UID), "volume-subpaths", pv.Name, "app", "0")
	mountTmpfs(t, subpath)
	staging := stagingPath(root, "d", "h")
	if err := os.MkdirAll(staging, 0o750); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = setImmutable(v.target, false); _ = detach(v.target); _ = detach(staging) })

	d := &fakeDriver{name: "d", endpoint: filepath.Join(root, "plugins", "d", "csi.sock")}
	serve(t, d.endpoint, func(s *grpc.Server) { csi.RegisterNodeServer(s, d) })
	serve(t, filepath.Join(root, "plugins_registry", "d-reg.sock"), func(s *grpc.Server) { registerapi.RegisterRegistrationServer(s, d) })

	client := fake.NewClientset(pv, &storagev1.CSIDriver{
		ObjectMeta: metav1.ObjectMeta{Name: "d"},
		Spec:       storagev1.CSIDriverSpec{AttachRequired: ptr(false)},
	})
	h, err := New(Config{NodeName: "n", KubeletRoot: root, Strikes: 1, StatTimeout: 5 * time.Second, Guard: GuardAlways, KubeClient: client})
	if err != nil {
		t.Fatal(err)
	}

	name, err := h.remount(context.Background(), pod, v)
	if err != nil {
		t.Fatal(err)
	}
	if name != "data" {
		t.Errorf("pod volume = %q, want data", name)
	}
	if !mustStat(t, staging) || !mustStat(t, v.target) {
		t.Fatal("staging or target not mounted after remount")
	}
	if d.published.StagingTargetPath != staging || !d.published.Readonly {
		t.Errorf("publish request = %+v", d.published)
	}
	if mustStat(t, subpath) {
		t.Error("stale subPath bind still mounted")
	}

	// The guard is in place underneath the new mount.
	if err := detach(v.target); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(v.target, "x"), nil, 0o644); !errors.Is(err, unix.EPERM) {
		t.Errorf("write underneath the remounted volume: got %v, want EPERM", err)
	}
}

func TestStagingBinds(t *testing.T) {
	root := t.TempDir()
	staging := stagingPath(root, "d", "h")
	bare, live := filepath.Join(root, "bare"), filepath.Join(root, "live")
	for _, d := range []string{staging, bare, live} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	// What a driver publishes while the staging mount is gone.
	if err := unix.Mount(staging, bare, "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = detach(bare) })
	mountTmpfs(t, staging)
	if err := unix.Mount(staging, live, "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = detach(live) })

	binds, err := stagingBinds()
	if err != nil {
		t.Fatal(err)
	}
	if !binds[bare] {
		t.Error("bind of the bare staging directory not found")
	}
	if binds[live] || binds[staging] {
		t.Error("bind of the live staging mount reported as bare")
	}
}
