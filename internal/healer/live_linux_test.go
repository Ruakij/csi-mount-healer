//go:build linux && mounttest

package healer

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const childEnv = "HEALER_TEST_CHILD"

// init runs the child of TestLiveSwap: the test binary again, in a mount
// namespace of its own like a container. The runtime keeps init on the main
// thread, whose namespace /proc/<pid>/ns/mnt shows, so all of the child runs
// there.
func init() {
	path := os.Getenv(childEnv)
	if path == "" {
		return
	}
	if err := runChild(path); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

// runChild mounts a tmpfs at path, holds a file on it open, and answers "ls"
// with what it sees at path, "touch" by creating a file there and "close" by
// closing the file it holds.
func runChild(path string) error {
	if err := unix.Unshare(unix.CLONE_NEWNS); err != nil {
		return err
	}
	if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
		return err
	}
	if err := unix.Mount("tmpfs", path, "tmpfs", 0, ""); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(path, "old"), nil, 0o644); err != nil {
		return err
	}
	f, err := os.Open(filepath.Join(path, "old"))
	if err != nil {
		return err
	}
	fmt.Println("ready")
	in := bufio.NewScanner(os.Stdin)
	for in.Scan() {
		switch in.Text() {
		case "ls":
			entries, err := os.ReadDir(path)
			names := []string{fmt.Sprint(err)}
			for _, e := range entries {
				names = append(names, e.Name())
			}
			fmt.Println(strings.Join(names, " "))
		case "touch":
			fmt.Println(os.WriteFile(filepath.Join(path, "touched"), nil, 0o644))
		case "close":
			fmt.Println(f.Close())
		}
	}
	return in.Err()
}

func TestLiveSwap(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "container", "data")
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	node := filepath.Join(root, "node")
	mountTmpfs(t, node)
	if err := os.WriteFile(filepath.Join(node, "new"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), childEnv+"="+path)
	cmd.Stderr = os.Stderr
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	out := bufio.NewReader(stdout)
	ask := func(line string) string {
		t.Helper()
		if line != "" {
			if _, err := io.WriteString(stdin, line+"\n"); err != nil {
				t.Fatal(err)
			}
		}
		reply, err := out.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(reply)
	}
	if got := ask(""); got != "ready" {
		t.Fatalf("child: %q", got)
	}

	ns, dead, err := swapMounts(cmd.Process.Pid, []liveMount{{target: node, path: path, readOnly: true}})
	if err != nil {
		t.Fatal(err)
	}
	if got := ask("ls"); got != "<nil> new" {
		t.Errorf("child sees %q at the mount path, want the new mount", got)
	}
	if got := ask("touch"); !strings.Contains(got, "read-only") {
		t.Errorf("write to the readOnly mount: %s", got)
	}

	handles, procs, err := staleHandles(cmd.Process.Pid, ns, dead, 5*time.Second)
	if err != nil || handles != 1 || len(procs) != 1 {
		t.Errorf("with the file open: %d handles by %v, err %v; want 1", handles, procs, err)
	}
	if got := ask("close"); got != "<nil>" {
		t.Fatalf("close: %s", got)
	}
	handles, procs, err = staleHandles(cmd.Process.Pid, ns, dead, 5*time.Second)
	if err != nil || handles != 0 {
		t.Errorf("with the file closed: %d handles by %v, err %v; want 0", handles, procs, err)
	}

	// A container with a mount below the path is not swapped.
	if _, _, err := swapMounts(cmd.Process.Pid, []liveMount{{target: node, path: filepath.Dir(path)}}); err == nil {
		t.Error("swapped a mount with a mount below it")
	}

	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	if _, _, err := staleHandles(cmd.Process.Pid, ns, dead, time.Second); !errors.Is(err, errGone) {
		t.Errorf("after the child exited: %v, want errGone", err)
	}
}
