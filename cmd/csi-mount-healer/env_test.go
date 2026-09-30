package main

import (
	"flag"
	"testing"
	"time"
)

func TestApplyEnv(t *testing.T) {
	tests := []struct {
		name         string
		env          map[string]string
		args         []string
		wantRoot     string
		wantInterval time.Duration
		wantErr      bool
	}{
		{name: "defaults", wantRoot: "/var/lib/kubelet", wantInterval: 5 * time.Minute},
		{name: "env", env: map[string]string{"KUBELET_ROOT": "/k", "INTERVAL": "1m"}, wantRoot: "/k", wantInterval: time.Minute},
		{name: "flag wins over env", env: map[string]string{"INTERVAL": "1m"}, args: []string{"-interval=2m"}, wantRoot: "/var/lib/kubelet", wantInterval: 2 * time.Minute},
		{name: "invalid env", env: map[string]string{"INTERVAL": "soon"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			root := fs.String("kubelet-root", "/var/lib/kubelet", "")
			interval := fs.Duration("interval", 5*time.Minute, "")
			err := applyEnv(fs, func(k string) (string, bool) { v, ok := tt.env[k]; return v, ok })
			if (err != nil) != tt.wantErr {
				t.Fatalf("applyEnv error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if err := fs.Parse(tt.args); err != nil {
				t.Fatal(err)
			}
			if *root != tt.wantRoot || *interval != tt.wantInterval {
				t.Errorf("got %s %v, want %s %v", *root, *interval, tt.wantRoot, tt.wantInterval)
			}
		})
	}
}
