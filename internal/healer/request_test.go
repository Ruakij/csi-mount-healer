package healer

import (
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func ptr[T any](v T) *T { return &v }

func testPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-0", Namespace: "ns", UID: "uid-1"},
		Spec: corev1.PodSpec{
			ServiceAccountName: "sa",
			SecurityContext:    &corev1.PodSecurityContext{FSGroup: ptr(int64(1000))},
			Volumes: []corev1.Volume{
				{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-app-0", ReadOnly: true}}},
				{Name: "scratch", VolumeSource: corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{}}},
				{Name: "inline", VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{
					Driver: "d", FSType: ptr("ext4"), ReadOnly: ptr(true), VolumeAttributes: map[string]string{"k": "v"},
				}}},
			},
		},
	}
}

func testPV(claim string, modes ...corev1.PersistentVolumeAccessMode) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-1"},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes:  modes,
			MountOptions: []string{"noatime"},
			ClaimRef:     &corev1.ObjectReference{Namespace: "ns", Name: claim},
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
				Driver: "d", VolumeHandle: "h", FSType: "xfs", VolumeAttributes: map[string]string{"path": "/p"},
			}},
		},
	}
}

func TestBuildRequests(t *testing.T) {
	pvVol := volData{SpecVolID: "pv-1", VolumeHandle: "h", DriverName: "d", LifecycleMode: "Persistent"}
	tests := []struct {
		name    string
		in      requestInputs
		check   func(t *testing.T, r volumeRequests)
		wantErr string
	}{
		{
			name: "pvc with staging, pod info and fsGroup",
			in: requestInputs{
				vol: pvVol, pv: testPV("data-app-0", corev1.ReadWriteMany),
				driver:         &storagev1.CSIDriver{Spec: storagev1.CSIDriverSpec{PodInfoOnMount: ptr(true)}},
				caps:           nodeCaps{stage: true, volumeMountGroup: true},
				publishContext: map[string]string{"dev": "/dev/x"},
			},
			check: func(t *testing.T, r volumeRequests) {
				if r.podVolume != "data" {
					t.Errorf("podVolume = %q, want data", r.podVolume)
				}
				want := stagingPath("/var/lib/kubelet", "d", "h")
				if r.stage == nil || r.stage.StagingTargetPath != want || r.publish.StagingTargetPath != want {
					t.Fatalf("staging path not set to %s: %+v", want, r.stage)
				}
				if r.stage.VolumeContext["csi.storage.k8s.io/pod.name"] != "" {
					t.Error("stage request carries pod info")
				}
				ctx := r.publish.VolumeContext
				if ctx["path"] != "/p" || ctx["csi.storage.k8s.io/pod.name"] != "app-0" || ctx["csi.storage.k8s.io/ephemeral"] != "false" {
					t.Errorf("publish volume context = %v", ctx)
				}
				if !r.publish.Readonly {
					t.Error("readOnly from the pod claim was dropped")
				}
				m := r.publish.VolumeCapability.GetMount()
				if m.FsType != "xfs" || m.VolumeMountGroup != "1000" || len(m.MountFlags) != 1 {
					t.Errorf("mount capability = %+v", m)
				}
				if r.publish.VolumeCapability.AccessMode.Mode != csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
					t.Errorf("access mode = %v", r.publish.VolumeCapability.AccessMode.Mode)
				}
				if r.publish.PublishContext["dev"] != "/dev/x" || r.publish.Secrets == nil {
					t.Errorf("publish context %v, secrets %v", r.publish.PublishContext, r.publish.Secrets)
				}
			},
		},
		{
			name: "generic ephemeral volume, default access mode",
			in:   requestInputs{vol: pvVol, pv: testPV("app-0-scratch"), driver: &storagev1.CSIDriver{}, caps: nodeCaps{singleNodeMulti: true}},
			check: func(t *testing.T, r volumeRequests) {
				if r.podVolume != "scratch" || r.stage != nil || r.publish.StagingTargetPath != "" {
					t.Errorf("got podVolume %q, stage %v", r.podVolume, r.stage)
				}
				if r.publish.VolumeCapability.AccessMode.Mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER {
					t.Errorf("access mode = %v", r.publish.VolumeCapability.AccessMode.Mode)
				}
				if r.publish.VolumeCapability.GetMount().VolumeMountGroup != "" {
					t.Error("VolumeMountGroup set without the capability")
				}
			},
		},
		{
			name: "inline ephemeral volume",
			in: requestInputs{
				vol:    volData{SpecVolID: "inline", VolumeHandle: "csi-abc", DriverName: "d", LifecycleMode: "Ephemeral"},
				driver: &storagev1.CSIDriver{Spec: storagev1.CSIDriverSpec{PodInfoOnMount: ptr(true)}},
				caps:   nodeCaps{stage: true},
			},
			check: func(t *testing.T, r volumeRequests) {
				if r.podVolume != "inline" || r.stage != nil || !r.publish.Readonly {
					t.Errorf("got podVolume %q, stage %v, readonly %v", r.podVolume, r.stage, r.publish.Readonly)
				}
				if r.publish.VolumeContext["k"] != "v" || r.publish.VolumeContext["csi.storage.k8s.io/ephemeral"] != "true" {
					t.Errorf("volume context = %v", r.publish.VolumeContext)
				}
				if r.publish.VolumeCapability.GetMount().FsType != "ext4" {
					t.Error("fsType not taken from the inline volume")
				}
			},
		},
		{
			name:    "claim not used by the pod",
			in:      requestInputs{vol: pvVol, pv: testPV("other"), driver: &storagev1.CSIDriver{}},
			wantErr: "has no volume",
		},
		{
			name: "block volume",
			in: requestInputs{vol: pvVol, driver: &storagev1.CSIDriver{}, pv: func() *corev1.PersistentVolume {
				pv := testPV("data-app-0")
				pv.Spec.VolumeMode = ptr(corev1.PersistentVolumeBlock)
				return pv
			}()},
			wantErr: "block",
		},
		{
			name: "token requests",
			in: requestInputs{vol: pvVol, pv: testPV("data-app-0"), driver: &storagev1.CSIDriver{Spec: storagev1.CSIDriverSpec{
				TokenRequests: []storagev1.TokenRequest{{Audience: "a"}},
			}}},
			wantErr: "token",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.in.pod = testPod()
			tt.in.kubeletDir = "/var/lib/kubelet"
			tt.in.target = "/var/lib/kubelet/pods/uid-1/volumes/kubernetes.io~csi/pv-1/mount"
			r, err := buildRequests(tt.in)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("got error %v, want one containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if r.publish.TargetPath != tt.in.target || r.publish.VolumeId != tt.in.vol.VolumeHandle {
				t.Errorf("publish target %q volume %q", r.publish.TargetPath, r.publish.VolumeId)
			}
			tt.check(t, r)
		})
	}
}

func TestStagingPath(t *testing.T) {
	// sha256("h"), as kubelet names the directory.
	want := "/var/lib/kubelet/plugins/kubernetes.io/csi/d/aaa9402664f1a41f40ebbc52c9993eb66aeb366602958fdfaa283b71e64db123/globalmount"
	if got := stagingPath("/var/lib/kubelet", "d", "h"); got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}
