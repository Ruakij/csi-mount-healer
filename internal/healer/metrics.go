package healer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"
)

// metrics are the counters of the healer, served in the Prometheus text format.
type metrics struct {
	// unguarded counts the targets of running pods found with nothing mounted on
	// them and no guard, which a container could start on and write to the node disk.
	unguarded atomic.Uint64
	// remountFailed counts the remounts that failed, whatever the reason.
	remountFailed atomic.Uint64
}

// write renders the counters.
func (m *metrics) write(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	for _, c := range []struct {
		name, help string
		n          *atomic.Uint64
	}{
		{"csi_mount_healer_unguarded_unmounted_targets_total", "Targets of running pods found with nothing mounted on them and no guard.", &m.unguarded},
		{"csi_mount_healer_remount_failures_total", "Remounts through a CSI driver that failed.", &m.remountFailed},
	} {
		fmt.Fprintf(w, "# HELP %[1]s %[2]s\n# TYPE %[1]s counter\n%[1]s %[3]d\n", c.name, c.help, c.n.Load())
	}
}

// serve serves the counters on addr at /metrics until ctx is done.
func (m *metrics) serve(ctx context.Context, addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("serving metrics: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) { m.write(w) })
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.Serve(l); !errors.Is(err, http.ErrServerClosed) {
			klog.Errorf("serving metrics: %v", err)
		}
	}()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	klog.Infof("serving metrics on %s", l.Addr())
	return nil
}
