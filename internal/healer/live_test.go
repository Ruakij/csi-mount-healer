package healer

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

func TestCheckSwap(t *testing.T) {
	tests := []struct {
		name    string
		handles int
		err     error
		age     time.Duration
		podGone bool
		// events are the reasons recorded, in order.
		events  []string
		kept    bool
		deleted bool
	}{
		{name: "nothing held", events: []string{"Remounted"}},
		{name: "held", handles: 2, age: time.Minute, events: []string{"StaleHandles"}, kept: true},
		{name: "held too long", handles: 2, age: 5 * time.Minute,
			events: []string{"StaleHandles", "Escalating", "DeletingPod"}, deleted: true},
		{name: "container gone", err: errGone},
		{name: "pod gone", handles: 2, age: time.Hour, podGone: true},
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
				cfg:    Config{LiveTimeout: 5 * time.Minute, KubeClient: client},
				pods:   pods,
				events: events,
				swaps:  map[string]*swap{},
				stale: func(*swap) (int, []string, error) {
					return tt.handles, []string{"app[42]"}, tt.err
				},
			}
			// The restart tier is disabled, so the escalation needs no CRI.
			if err := h.cfg.Tiers.Set("live,delete"); err != nil {
				t.Fatal(err)
			}
			h.swaps["k"] = &swap{pod: pod.UID, container: "app", volumes: []string{"data"}, since: time.Now().Add(-tt.age)}

			h.checkSwaps(context.Background())

			if _, kept := h.swaps["k"]; kept != tt.kept {
				t.Errorf("swap kept = %v, want %v", kept, tt.kept)
			}
			close(events.Events)
			var got []string
			for e := range events.Events {
				got = append(got, strings.Fields(e)[1])
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
