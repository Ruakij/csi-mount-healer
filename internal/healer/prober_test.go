package healer

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestProberHung(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	p := newProber(10 * time.Millisecond)
	p.stat = func(string) (bool, error) {
		calls.Add(1)
		<-release
		return true, nil
	}

	if _, err := p.probe("/x"); !errors.Is(err, errHung) {
		t.Fatalf("first probe: got %v, want errHung", err)
	}
	if _, err := p.probe("/x"); !errors.Is(err, errHung) {
		t.Fatalf("second probe: got %v, want errHung", err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("stat called %d times while hung, want 1", n)
	}

	close(release)
	deadline := time.Now().Add(time.Second)
	for {
		mounted, err := p.probe("/x")
		if err == nil {
			if !mounted {
				t.Fatal("got not mounted, want mounted")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("path still hung after stat returned: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A copy in a container is hung on its own, per container and path.
func TestProberHungIn(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	var calls atomic.Int32
	p := newProber(10 * time.Millisecond)
	p.statIn = func(pid int, _ string) (bool, error) {
		calls.Add(1)
		if pid == 1 {
			<-release
		}
		return true, nil
	}

	for range 2 {
		if _, err := p.probeIn(1, "/x"); !errors.Is(err, errHung) {
			t.Fatalf("probe in 1: got %v, want errHung", err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("statIn called %d times while hung, want 1", n)
	}
	if mounted, err := p.probeIn(2, "/x"); err != nil || !mounted {
		t.Errorf("probe in 2: %v, %v; want mounted", mounted, err)
	}

	// What is mounted there changed, so the stat stuck on the old mount is left behind.
	p.forgetIn(1, "/x")
	if _, err := p.probeIn(1, "/x"); !errors.Is(err, errHung) {
		t.Fatalf("probe in 1 after forgetting: got %v, want errHung", err)
	}
	if n := calls.Load(); n != 3 {
		t.Errorf("statIn called %d times, want a new call after forgetting", n)
	}
}
