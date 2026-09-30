package healer

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
)

// volData is the vol_data.json kubelet writes next to every CSI mount of a pod.
type volData struct {
	SpecVolID     string `json:"specVolID"`
	VolumeHandle  string `json:"volumeHandle"`
	DriverName    string `json:"driverName"`
	NodeName      string `json:"nodeName"`
	AttachmentID  string `json:"attachmentID"`
	LifecycleMode string `json:"volumeLifecycleMode"`
}

func (d volData) ephemeral() bool {
	return d.LifecycleMode == string(storagev1.VolumeLifecycleEphemeral)
}

type nodeCaps struct {
	stage            bool
	singleNodeMulti  bool
	volumeMountGroup bool
}

// requestInputs is everything kubelet looks up before it calls the driver.
type requestInputs struct {
	pod        *corev1.Pod
	vol        volData
	target     string
	kubeletDir string
	// pv is nil for inline ephemeral volumes.
	pv             *corev1.PersistentVolume
	driver         *storagev1.CSIDriver
	caps           nodeCaps
	publishContext map[string]string
	publishSecrets map[string]string
	stageSecrets   map[string]string
}

type volumeRequests struct {
	// stage is nil when the driver does not stage volumes.
	stage     *csi.NodeStageVolumeRequest
	publish   *csi.NodePublishVolumeRequest
	podVolume string
}

// buildRequests rebuilds the NodeStageVolume and NodePublishVolume requests kubelet sent
// for a volume, following pkg/volume/csi in kubelet (csi_mounter.go SetUpAt,
// csi_attacher.go MountDevice). The driver has to see the same request to mount the
// same thing.
func buildRequests(in requestInputs) (volumeRequests, error) {
	if len(in.driver.Spec.TokenRequests) > 0 {
		return volumeRequests{}, errors.New("driver requests service account tokens, which are not rebuilt")
	}
	var (
		reqs         volumeRequests
		fsType       string
		attrs        map[string]string
		mountOptions []string
		readOnly     bool
		accessMode   = corev1.ReadWriteOnce
	)

	if in.vol.ephemeral() {
		reqs.podVolume = in.vol.SpecVolID
		v := podVolume(in.pod, reqs.podVolume)
		if v == nil || v.CSI == nil {
			return reqs, fmt.Errorf("pod has no inline CSI volume %q", reqs.podVolume)
		}
		if v.CSI.FSType != nil {
			fsType = *v.CSI.FSType
		}
		if v.CSI.ReadOnly != nil {
			readOnly = *v.CSI.ReadOnly
		}
		attrs = v.CSI.VolumeAttributes
	} else {
		if in.pv == nil || in.pv.Spec.CSI == nil {
			return reqs, fmt.Errorf("persistent volume %q is not a CSI volume", in.vol.SpecVolID)
		}
		if in.pv.Spec.VolumeMode != nil && *in.pv.Spec.VolumeMode == corev1.PersistentVolumeBlock {
			return reqs, errors.New("block volumes are not supported")
		}
		name, ro, ok := claimVolume(in.pod, in.pv)
		if !ok {
			return reqs, fmt.Errorf("pod has no volume for persistent volume %q", in.pv.Name)
		}
		reqs.podVolume, readOnly = name, ro
		src := in.pv.Spec.CSI
		fsType = src.FSType
		attrs = src.VolumeAttributes
		mountOptions = in.pv.Spec.MountOptions
		if len(in.pv.Spec.AccessModes) > 0 {
			accessMode = in.pv.Spec.AccessModes[0]
		}
	}

	capability := &csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csiAccessMode(accessMode, in.caps.singleNodeMulti)},
	}
	mount := &csi.VolumeCapability_MountVolume{FsType: fsType, MountFlags: mountOptions}
	if in.caps.volumeMountGroup && in.pod.Spec.SecurityContext != nil && in.pod.Spec.SecurityContext.FSGroup != nil {
		mount.VolumeMountGroup = strconv.FormatInt(*in.pod.Spec.SecurityContext.FSGroup, 10)
	}
	capability.AccessType = &csi.VolumeCapability_Mount{Mount: mount}

	var staging string
	if in.caps.stage && !in.vol.ephemeral() {
		staging = stagingPath(in.kubeletDir, in.vol.DriverName, in.vol.VolumeHandle)
		reqs.stage = &csi.NodeStageVolumeRequest{
			VolumeId:          in.vol.VolumeHandle,
			PublishContext:    in.publishContext,
			StagingTargetPath: staging,
			VolumeCapability:  capability,
			Secrets:           orEmpty(in.stageSecrets),
			VolumeContext:     attrs,
		}
	}

	if in.driver.Spec.PodInfoOnMount != nil && *in.driver.Spec.PodInfoOnMount {
		merged := map[string]string{}
		for k, v := range attrs {
			merged[k] = v
		}
		merged["csi.storage.k8s.io/pod.name"] = in.pod.Name
		merged["csi.storage.k8s.io/pod.namespace"] = in.pod.Namespace
		merged["csi.storage.k8s.io/pod.uid"] = string(in.pod.UID)
		merged["csi.storage.k8s.io/serviceAccount.name"] = in.pod.Spec.ServiceAccountName
		merged["csi.storage.k8s.io/ephemeral"] = strconv.FormatBool(in.vol.ephemeral())
		attrs = merged
	}

	reqs.publish = &csi.NodePublishVolumeRequest{
		VolumeId:          in.vol.VolumeHandle,
		PublishContext:    in.publishContext,
		StagingTargetPath: staging,
		TargetPath:        in.target,
		VolumeCapability:  capability,
		Readonly:          readOnly,
		Secrets:           orEmpty(in.publishSecrets),
		VolumeContext:     attrs,
	}
	return reqs, nil
}

// stagingPath is makeDeviceMountPath in kubelet.
func stagingPath(kubeletDir, driver, handle string) string {
	sum := sha256.Sum256([]byte(handle))
	return filepath.Join(kubeletDir, "plugins/kubernetes.io/csi", driver, fmt.Sprintf("%x", sum), "globalmount")
}

// csiAccessMode is asCSIAccessModeV1 in kubelet, or asSingleNodeMultiWriterCapableCSIAccessModeV1
// for drivers with the SINGLE_NODE_MULTI_WRITER capability.
func csiAccessMode(am corev1.PersistentVolumeAccessMode, singleNodeMulti bool) csi.VolumeCapability_AccessMode_Mode {
	switch am {
	case corev1.ReadWriteOnce:
		if singleNodeMulti {
			return csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER
		}
		return csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
	case corev1.ReadOnlyMany:
		return csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY
	case corev1.ReadWriteMany:
		return csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER
	case corev1.ReadWriteOncePod:
		if singleNodeMulti {
			return csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER
		}
		return csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
	}
	return csi.VolumeCapability_AccessMode_UNKNOWN
}

func podVolume(pod *corev1.Pod, name string) *corev1.Volume {
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].Name == name {
			return &pod.Spec.Volumes[i]
		}
	}
	return nil
}

// claimVolume finds the pod volume bound to pv, through a PVC or a generic ephemeral volume.
func claimVolume(pod *corev1.Pod, pv *corev1.PersistentVolume) (name string, readOnly, ok bool) {
	ref := pv.Spec.ClaimRef
	if ref == nil || ref.Namespace != pod.Namespace {
		return "", false, false
	}
	for _, v := range pod.Spec.Volumes {
		switch {
		case v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == ref.Name:
			return v.Name, v.PersistentVolumeClaim.ReadOnly, true
		case v.Ephemeral != nil && pod.Name+"-"+v.Name == ref.Name:
			return v.Name, false, true
		}
	}
	return "", false, false
}

func orEmpty(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}
