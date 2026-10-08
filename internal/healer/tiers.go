package healer

import (
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// Tier is one way to heal a dead mount. Tiers are ordered from least to most
// disruptive, and a dead mount goes to the first enabled one that applies.
type Tier int

const (
	TierLive Tier = iota
	TierRestart
	TierDelete
	tierCount
)

var tierNames = [tierCount]string{"live", "restart", "delete"}

func (t Tier) String() string { return tierNames[t] }

// Tiers is the set of enabled tiers. As a flag it is a comma-separated list of
// tier names, in any order; an empty list only reports dead mounts.
type Tiers uint

func (s Tiers) has(t Tier) bool { return s&(1<<t) != 0 }

func (s Tiers) String() string {
	var names []string
	for t := range tierCount {
		if s.has(t) {
			names = append(names, t.String())
		}
	}
	return strings.Join(names, ",")
}

func (s *Tiers) Set(list string) error {
	var set Tiers
	for name := range strings.SplitSeq(list, ",") {
		if name = strings.TrimSpace(name); name == "" {
			continue
		}
		t := slices.Index(tierNames[:], name)
		if t < 0 {
			return fmt.Errorf("unknown heal tier %q, must be one of %s", name, strings.Join(tierNames[:], ", "))
		}
		set |= 1 << t
	}
	*s = set
	return nil
}

// next returns the first enabled tier from `from` on that applies to the pod,
// or tierCount if none does, and why each tier before it was passed over.
func (h *Healer) next(pod *corev1.Pod, from Tier) (Tier, []string) {
	var skipped []string
	for t := from; t < tierCount; t++ {
		switch {
		case !h.cfg.Tiers.has(t):
			skipped = append(skipped, "the "+t.String()+" tier is disabled")
		case t == TierRestart && pod.Spec.RestartPolicy != corev1.RestartPolicyAlways:
			skipped = append(skipped, "the restart tier needs restartPolicy Always, the pod has "+string(pod.Spec.RestartPolicy))
		case t == TierDelete && pod.DeletionTimestamp != nil && !h.cfg.ForceDelete:
			skipped = append(skipped, "the delete tier only force deletes a pod that is already terminating, and -force-delete is off")
		case t == TierDelete && len(pod.OwnerReferences) == 0 && pod.DeletionTimestamp == nil:
			// A pod that is already terminating is on its way out regardless.
			skipped = append(skipped, "the delete tier needs a controller to recreate the pod, the pod has none")
		default:
			return t, skipped
		}
	}
	return tierCount, skipped
}

// escalate tries the tiers from `from` on until one heals the pod, each failure
// passing it on to the next. why says why the tiers before `from` did not.
func (h *Healer) escalate(pod *corev1.Pod, from Tier, why string, try func(t Tier, why string) error) {
	for {
		t, skipped := h.next(pod, from)
		if why != "" {
			skipped = append([]string{why}, skipped...)
		}
		why = strings.Join(skipped, "; ")
		if t == tierCount {
			klog.Warningf("pod %s/%s: dead mount not healed: %s", pod.Namespace, pod.Name, why)
			h.events.Eventf(pod, corev1.EventTypeWarning, "NotHealed", "No heal tier left for the dead mount: %s", why)
			return
		}
		err := try(t, why)
		if err == nil {
			return
		}
		klog.Warningf("pod %s/%s: the %s tier failed: %v", pod.Namespace, pod.Name, t, err)
		h.events.Eventf(pod, corev1.EventTypeWarning, "TierFailed", "The %s tier failed to heal the dead mount: %v", t, err)
		why, from = fmt.Sprintf("the %s tier failed: %v", t, err), t+1
	}
}
