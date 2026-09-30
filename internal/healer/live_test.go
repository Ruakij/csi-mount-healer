package healer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
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
		// events are the types and reasons recorded, in order.
		events  []string
		deleted bool
	}{
		{name: "nothing held", events: []string{"Normal Remounted"}},
		{name: "held", handles: 2, events: []string{"Warning Remounted"}},
		{name: "count failed", err: errCount, events: []string{"Warning Remounted"}},
		{name: "container gone", err: errGone},
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
