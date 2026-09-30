package healer

import (
	"errors"
	"sync"
	"time"
)

var errHung = errors.New("stat did not return in time")

// prober runs stat with a timeout. A stat stuck in the kernel cannot be
// cancelled, so a path stays hung, and is not probed again, until it returns.
type prober struct {
	timeout time.Duration
	stat    func(string) (bool, error)

	mu   sync.Mutex
	hung map[string]bool
}

func newProber(timeout time.Duration) *prober {
	return &prober{timeout: timeout, stat: stat, hung: map[string]bool{}}
}

// probe reports whether path is a mount root.
func (p *prober) probe(path string) (bool, error) {
	p.mu.Lock()
	hung := p.hung[path]
	p.mu.Unlock()
	if hung {
		return false, errHung
	}

	type result struct {
		mounted bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		mounted, err := p.stat(path)
		done <- result{mounted, err}
	}()
	select {
	case r := <-done:
		return r.mounted, r.err
	case <-time.After(p.timeout):
	}

	p.mu.Lock()
	p.hung[path] = true
	p.mu.Unlock()
	go func() {
		<-done
		p.mu.Lock()
		delete(p.hung, path)
		p.mu.Unlock()
	}()
	return false, errHung
}

// forget drops the hung state of path, for when what is mounted there changed.
// The stat still stuck on the old mount leaks until it returns.
func (p *prober) forget(path string) {
	p.mu.Lock()
	delete(p.hung, path)
	p.mu.Unlock()
}
