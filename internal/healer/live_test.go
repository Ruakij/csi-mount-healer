package healer

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestSwapTimeout(t *testing.T) {
	errCount := errors.New("no /proc")
	tests := []struct {
		name string
		// expired runs the check at the end of the live timeout instead of right
		// after the swap.
		expired bool
		handles int
		err     error
		podGone bool
		// noTimeout sets LiveTimeout to 0.
		noTimeout bool
		// events are the types and reasons recorded, in order.
		events  []string
		deleted bool
	}{
		{name: "nothing held", events: []string{"Normal Remounted"}},
		{name: "held", handles: 2, events: []string{"Warning Remounted"}},
		{name: "count failed", err: errCount, events: []string{"Warning Remounted"}},
		{name: "container gone", err: errGone},
		{name: "held without a timeout", handles: 2, noTimeout: true, events: []string{"Warning Remounted"}},
		{name: "released in time", expired: true, events: []string{"Normal Remounted"}},
		{name: "held too long", expired: true, handles: 2,
			events: []string{"Warning Escalating", "Warning DeletingPod"}, deleted: true},
		{name: "count failed at the timeout", expired: true, err: errCount,
			events: []string{"Warning Escalating", "Warning DeletingPod"}, deleted: true},
		{name: "container gone at the timeout", expired: true, err: errGone},
		{name: "pod gone at the timeout", expired: true, handles: 2, podGone: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := testPod()
			pod.Spec.RestartPolicy = corev1.RestartPolicyAlways
			pod.OwnerReferences = []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "app"}}
			pods := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{uidIndex: indexByUID})
			if !tt.podGone {
				_ = pods.Add(pod)
			}
			client := fake.NewClientset(pod)
			events := record.NewFakeRecorder(10)
			h := &Healer{
				// Long enough that no timer fires during the test.
				cfg:    Config{LiveTimeout: time.Hour, KubeClient: client},
				pods:   pods,
				events: events,
				stale: func(*swap) (int, []string, error) {
					return tt.handles, []string{"app[42]"}, tt.err
				},
			}
			if tt.noTimeout {
				h.cfg.LiveTimeout = 0
			}
			// The restart tier is disabled, so the escalation needs no CRI.
			if err := h.cfg.Tiers.Set("live,delete"); err != nil {
				t.Fatal(err)
			}
			s := &swap{pod: pod.UID, container: "app", volumes: []string{"data"}}

			if tt.expired {
				h.expireSwap(context.Background(), s)
			} else {
				h.checkSwap(context.Background(), pod, s)
			}

			close(events.Events)
			var got []string
			for e := range events.Events {
				got = append(got, strings.Join(strings.Fields(e)[:2], " "))
			}
			if strings.Join(got, ",") != strings.Join(tt.events, ",") {
				t.Errorf("events %v, want %v", got, tt.events)
			}
			_, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
			if deleted := err != nil; deleted != tt.deleted {
				t.Errorf("pod deleted = %v, want %v", deleted, tt.deleted)
			}
		})
	}
}

func TestMountSources(t *testing.T) {
	const pod = "/var/lib/kubelet/pods/0b6c5a8e-2f1d-4c3a-9e7b-5d4f3a2b1c0d"
	csi, emptyDir := pod+"/volumes/kubernetes.io~csi/pvc-4f1e/mount", pod+"/volumes/kubernetes.io~empty-dir/logs"
	subPath := pod + "/volume-subpaths/crowdsec-profiles-volume/crowdsec-lapi/3"
	type src struct {
		target, path string
		readOnly     bool
		propagation  corev1.MountPropagationMode
	}
	tests := []struct {
		name   string
		mounts []*runtimeapi.Mount
		want   map[string]src
	}{
		{name: "nil"},
		{name: "empty", mounts: []*runtimeapi.Mount{}},
		{
			name: "emptyDir below a volume",
			mounts: []*runtimeapi.Mount{
				{ContainerPath: "/config", HostPath: csi},
				{ContainerPath: "/config/logs", HostPath: emptyDir},
			},
			want: map[string]src{
				"/config":      {csi, "/config", false, corev1.MountPropagationNone},
				"/config/logs": {emptyDir, "/config/logs", false, corev1.MountPropagationNone},
			},
		},
		{
			name:   "read-only subPath file",
			mounts: []*runtimeapi.Mount{{ContainerPath: "/etc/crowdsec_data/profiles.yaml", HostPath: subPath, Readonly: true}},
			want: map[string]src{
				"/etc/crowdsec_data/profiles.yaml": {subPath, "/etc/crowdsec_data/profiles.yaml", true, corev1.MountPropagationNone},
			},
		},
		{
			name: "propagation",
			mounts: []*runtimeapi.Mount{
				{ContainerPath: "/private", HostPath: csi, Propagation: runtimeapi.MountPropagation_PROPAGATION_PRIVATE},
				{ContainerPath: "/slave", HostPath: csi, Propagation: runtimeapi.MountPropagation_PROPAGATION_HOST_TO_CONTAINER},
				{ContainerPath: "/shared", HostPath: csi, Propagation: runtimeapi.MountPropagation_PROPAGATION_BIDIRECTIONAL},
			},
			want: map[string]src{
				"/private": {csi, "/private", false, corev1.MountPropagationNone},
				"/slave":   {csi, "/slave", false, corev1.MountPropagationHostToContainer},
				"/shared":  {csi, "/shared", false, corev1.MountPropagationBidirectional},
			},
		},
		{
			name:   "unclean container path",
			mounts: []*runtimeapi.Mount{{ContainerPath: "/config//logs/", HostPath: emptyDir}},
			want:   map[string]src{"/config/logs": {emptyDir, "/config/logs", false, corev1.MountPropagationNone}},
		},
		{
			name: "stacked mounts, the last on top",
			mounts: []*runtimeapi.Mount{
				{ContainerPath: "/config/logs", HostPath: emptyDir},
				{ContainerPath: "/config/logs/", HostPath: subPath, Readonly: true},
			},
			want: map[string]src{"/config/logs": {subPath, "/config/logs", true, corev1.MountPropagationNone}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := map[string]src{}
			for k, m := range mountSources(tt.mounts) {
				if m.propagation == nil || m.subPath != "" {
					t.Errorf("%s: propagation %v, subPath %q", k, m.propagation, m.subPath)
					continue
				}
				got[k] = src{m.target, m.path, m.readOnly, *m.propagation}
			}
			if !maps.Equal(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}
