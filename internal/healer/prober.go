package healer

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var errHung = errors.New("stat did not return in time")

// prober runs stat with a timeout. A stat stuck in the kernel cannot be
// cancelled, so a path stays hung, and is not probed again, until it returns.
type prober struct {
	timeout time.Duration
	stat    func(string) (bool, error)
	// statIn is stat for a path as the process with the given pid sees it.
	statIn func(int, string) (bool, error)

	mu   sync.Mutex
	hung map[string]bool
}

func newProber(timeout time.Duration) *prober {
	return &prober{timeout: timeout, stat: stat, statIn: statInRoot, hung: map[string]bool{}}
}

// probe reports whether path is a mount root.
func (p *prober) probe(path string) (bool, error) {
	return p.run(path, func() (bool, error) { return p.stat(path) })
}

// probeIn is probe for path as the process with the given pid sees it.
func (p *prober) probeIn(pid int, path string) (bool, error) {
	return p.run(rootKey(pid, path), func() (bool, error) { return p.statIn(pid, path) })
}

func rootKey(pid int, path string) string { return fmt.Sprintf("/proc/%d/root%s", pid, path) }

// run runs a check of what key names, which stays hung until it returns.
func (p *prober) run(key string, check func() (bool, error)) (bool, error) {
	p.mu.Lock()
	hung := p.hung[key]
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
		mounted, err := check()
		done <- result{mounted, err}
	}()
	select {
	case r := <-done:
		return r.mounted, r.err
	case <-time.After(p.timeout):
	}

	p.mu.Lock()
	p.hung[key] = true
	p.mu.Unlock()
	go func() {
		<-done
		p.mu.Lock()
		delete(p.hung, key)
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

// forgetIn is forget for path as the process with the given pid sees it.
func (p *prober) forgetIn(pid int, path string) { p.forget(rootKey(pid, path)) }
