//go:build linux && mounttest

package healer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMain drops the capabilities first, so the mount tests prove keptCaps are
// enough.
func TestMain(m *testing.M) {
	if err := DropPrivileges(); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestDropPrivileges(t *testing.T) {
	want := "0000000000200204" // CAP_SYS_ADMIN (21), CAP_LINUX_IMMUTABLE (9), CAP_DAC_READ_SEARCH (2)
	tasks, err := filepath.Glob("/proc/self/task/*/status")
	if err != nil || len(tasks) == 0 {
		t.Fatalf("no tasks: %v", err)
	}
	for _, task := range tasks {
		b, err := os.ReadFile(task)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(b), "\n") {
			k, v, _ := strings.Cut(line, ":\t")
			switch k {
			case "CapInh", "CapAmb":
				if v != "0000000000000000" {
					t.Errorf("%s %s = %s, want none", task, k, v)
				}
			case "CapPrm", "CapEff", "CapBnd":
				if v != want {
					t.Errorf("%s %s = %s, want %s", task, k, v, want)
				}
			case "NoNewPrivs":
				if v != "1" {
					t.Errorf("%s NoNewPrivs = %s", task, v)
				}
			}
		}
	}
}
