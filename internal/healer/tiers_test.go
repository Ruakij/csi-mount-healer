package healer

import (
	"errors"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
)

func TestTiersSet(t *testing.T) {
	tests := []struct {
		list    string
		want    string
		wantErr bool
	}{
		{list: "", want: ""},
		{list: "live,restart,delete", want: "live,restart,delete"},
		{list: "delete, restart,live", want: "live,restart,delete"},
		{list: "delete,delete", want: "delete"},
		{list: "restart,,", want: "restart"},
		{list: "restart,reboot", wantErr: true},
		{list: "Restart", wantErr: true},
	}
	for _, tt := range tests {
		var s Tiers
		err := s.Set(tt.list)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q: error %v, wantErr %v", tt.list, err, tt.wantErr)
			continue
		}
		if !tt.wantErr && s.String() != tt.want {
			t.Errorf("%q: tiers %q, want %q", tt.list, s, tt.want)
		}
	}
}

func TestEscalate(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name   string
		tiers  string
		policy corev1.RestartPolicy
		// bare pods have no controller.
		bare bool
		// noForce turns ForceDelete off.
		noForce bool
		from    Tier
		fail    []Tier
		// tried lists the tiers run, and why the ones that ran were chosen, or
		// the reason of the NotHealed event when no tier was left.
		tried []Tier
		why   []string
	}{
		{name: "live", tiers: "live,restart,delete", tried: []Tier{TierLive}},
		{name: "live without restarts", tiers: "live,restart,delete", policy: corev1.RestartPolicyNever, tried: []Tier{TierLive}},
		{name: "live fails", tiers: "live,restart,delete", fail: []Tier{TierLive},
			tried: []Tier{TierLive, TierRestart}, why: []string{"", "the live tier failed: boom"}},
		{name: "live fails, restartPolicy", tiers: "live,restart,delete", policy: corev1.RestartPolicyOnFailure, fail: []Tier{TierLive},
			tried: []Tier{TierLive, TierDelete}, why: []string{"", "the live tier failed: boom; the restart tier needs restartPolicy Always, the pod has OnFailure"}},
		{name: "restart", tiers: "restart,delete", tried: []Tier{TierRestart}, why: []string{"the live tier is disabled"}},
		{name: "restart fails", tiers: "restart,delete", fail: []Tier{TierRestart},
			tried: []Tier{TierRestart, TierDelete}, why: []string{"", "the restart tier failed: boom"}},
		{name: "restartPolicy", tiers: "restart,delete", policy: corev1.RestartPolicyOnFailure,
			tried: []Tier{TierDelete}, why: []string{"the restart tier needs restartPolicy Always, the pod has OnFailure"}},
		{name: "restart disabled", tiers: "delete",
			tried: []Tier{TierDelete}, why: []string{"the restart tier is disabled"}},
		{name: "terminating", tiers: "restart,delete", from: TierDelete,
			tried: []Tier{TierDelete}, why: []string{"stuck terminating"}},
		{name: "nothing left", tiers: "restart", fail: []Tier{TierRestart},
			tried: []Tier{TierRestart}, why: []string{"", "the restart tier failed: boom; the delete tier is disabled"}},
		{name: "bare pod", tiers: "live,restart,delete", policy: corev1.RestartPolicyNever, bare: true, fail: []Tier{TierLive},
			tried: []Tier{TierLive}, why: []string{"", "the live tier failed: boom; the restart tier needs restartPolicy Always, the pod has Never; the delete tier needs a controller"}},
		{name: "bare pod terminating", tiers: "restart,delete", bare: true, from: TierDelete,
			tried: []Tier{TierDelete}, why: []string{"stuck terminating"}},
		{name: "terminating without force", tiers: "restart,delete", from: TierDelete, noForce: true,
			why: []string{"stuck terminating on a dead mount; the delete tier only force deletes"}},
		{name: "report only", tiers: "",
			why: []string{"the restart tier is disabled; the delete tier is disabled"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Healer{cfg: Config{ForceDelete: !tt.noForce}, events: record.NewFakeRecorder(10)}
			if err := h.cfg.Tiers.Set(tt.tiers); err != nil {
				t.Fatal(err)
			}
			pod := testPod()
			pod.Spec.RestartPolicy = corev1.RestartPolicyAlways
			if tt.policy != "" {
				pod.Spec.RestartPolicy = tt.policy
			}
			if !tt.bare {
				pod.OwnerReferences = []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "app"}}
			}
			why := ""
			if tt.from == TierDelete {
				pod.DeletionTimestamp = &metav1.Time{}
				why = "it is stuck terminating on a dead mount"
			}
			var tried []Tier
			var whys []string
			h.escalate(pod, tt.from, why, func(t Tier, why string) error {
				tried, whys = append(tried, t), append(whys, why)
				if slices.Contains(tt.fail, t) {
					return boom
				}
				return nil
			})
			if !slices.Equal(tried, tt.tried) {
				t.Errorf("tried %v, want %v", tried, tt.tried)
			}
			var events []string
			for len(h.events.(*record.FakeRecorder).Events) > 0 {
				events = append(events, <-h.events.(*record.FakeRecorder).Events)
			}
			// Every failed tier is reported at once.
			var failed []string
			for _, e := range events {
				if strings.Contains(e, "TierFailed") {
					failed = append(failed, e)
				}
			}
			if want := len(slices.DeleteFunc(slices.Clone(tried), func(t Tier) bool { return !slices.Contains(tt.fail, t) })); len(failed) != want {
				t.Errorf("TierFailed events %q, want %d", failed, want)
			}
			// The NotHealed event carries the reason when every tier was passed.
			if len(tt.why) > len(tried) {
				if len(events) == 0 || !strings.Contains(events[len(events)-1], "NotHealed") {
					t.Errorf("no NotHealed event in %q", events)
				} else {
					whys = append(whys, events[len(events)-1])
				}
			}
			for i, want := range tt.why {
				if i >= len(whys) || !strings.Contains(whys[i], want) {
					t.Errorf("reason %d = %q, want it to contain %q", i, whys, want)
				}
			}
		})
	}
}
