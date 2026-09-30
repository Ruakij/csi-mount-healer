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
