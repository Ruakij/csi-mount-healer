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
// closing the file it holds. "tmpfs <dir>" mounts a tmpfs at path/dir as an app
// would, "bind <name> <src>" binds src to path/name as the runtime would,
// "umount <name>" unmounts path/name and "cat <file>" reads path/file.
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
		args := strings.Fields(in.Text())
		if len(args) == 0 {
			continue
		}
		switch args[0] {
		case "tmpfs":
			dir := filepath.Join(path, args[1])
			fmt.Println(errors.Join(os.Mkdir(dir, 0o755), unix.Mount("tmpfs", dir, "tmpfs", 0, "")))
		case "bind":
			at := filepath.Join(path, args[1])
			var err error
			if st, _ := os.Stat(args[2]); st != nil && st.IsDir() {
				err = os.Mkdir(at, 0o755)
			} else {
				err = os.WriteFile(at, nil, 0o644)
			}
			fmt.Println(errors.Join(err, unix.Mount(args[2], at, "", unix.MS_BIND|unix.MS_REC, "")))
		case "umount":
			fmt.Println(unix.Unmount(filepath.Join(path, args[1]), 0))
		case "cat":
			b, err := os.ReadFile(filepath.Join(path, args[1]))
			fmt.Println(string(b), err)
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

	cmd, ask := startChild(t, path)

	ns, dead, err := swapMounts(cmd.Process.Pid, []liveMount{{target: node, path: path, readOnly: true}}, nil, "")
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

	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	if _, _, err := staleHandles(cmd.Process.Pid, ns, dead, time.Second); !errors.Is(err, errGone) {
		t.Errorf("after the child exited: %v, want errGone", err)
	}
}

// startChild starts runChild for path and returns a function that sends it a
// line, or nothing for "", and returns its reply.
func startChild(t *testing.T, path string) (*exec.Cmd, func(string) string) {
	t.Helper()
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
	return cmd, ask
}

func TestLiveSwapNested(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "container", "data")
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	// Sources as the runtime bound them: an emptyDir in memory, a ConfigMap
	// subPath file, a plain emptyDir, and a directory with a tmpfs on it only in
	// the host namespace.
	kubelet := filepath.Join(root, "kubelet")
	logsSrc, profilesSrc, cacheSrc := filepath.Join(kubelet, "logs"), filepath.Join(kubelet, "profiles.yaml"), filepath.Join(kubelet, "cache")
	hostSrc := filepath.Join(root, "host")
	mountTmpfs(t, logsSrc)
	node := filepath.Join(root, "node")
	mountTmpfs(t, node)
	for _, err := range []error{
		os.WriteFile(filepath.Join(logsSrc, "kept"), []byte("kept"), 0o644),
		os.WriteFile(profilesSrc, []byte("profile"), 0o644),
		os.Mkdir(cacheSrc, 0o755),
		os.WriteFile(filepath.Join(cacheSrc, "kept"), []byte("cache"), 0o644),
		os.WriteFile(filepath.Join(node, "new"), nil, 0o644),
		os.Mkdir(filepath.Join(node, "logs"), 0o755),
		os.WriteFile(filepath.Join(node, "profiles.yaml"), nil, 0o644),
		os.Mkdir(filepath.Join(node, "host"), 0o755),
		os.Mkdir(hostSrc, 0o755),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	sources := map[string]liveMount{}
	for _, src := range []string{logsSrc, profilesSrc, cacheSrc, hostSrc} {
		p := filepath.Join(path, filepath.Base(src))
		sources[p] = liveMount{target: src, path: p}
	}

	// The host namespace is a child of its own, which shares every mount above
	// and has a tmpfs with the file "old" at hostSrc.
	hostCmd, _ := startChild(t, hostSrc)
	hostNS := fmt.Sprintf("/proc/%d/ns/mnt", hostCmd.Process.Pid)

	cmd, ask := startChild(t, path)
	for _, line := range []string{"tmpfs own", "bind logs " + logsSrc, "bind profiles.yaml " + profilesSrc, "bind cache " + cacheSrc, "bind host " + hostSrc} {
		if got := ask(line); got != "<nil>" {
			t.Fatalf("%s: %s", line, got)
		}
	}
	if got := ask("cat host/old"); !strings.Contains(got, "no such file") {
		t.Fatalf("host/old is visible outside the host namespace: %q", got)
	}
	mountinfo := func() string {
		t.Helper()
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/mountinfo", cmd.Process.Pid))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	failSwap := func(want ...string) {
		t.Helper()
		before := mountinfo()
		_, _, err := swapMounts(cmd.Process.Pid, []liveMount{{target: node, path: path}}, sources, hostNS)
		for _, w := range want {
			if err == nil || !strings.Contains(err.Error(), w) {
				t.Errorf("swap: %v, want an error with %q", err, w)
			}
		}
		if after := mountinfo(); after != before {
			t.Errorf("the failed swap changed the mounts from\n%s\nto\n%s", before, after)
		}
		if got := ask("cat logs/kept"); got != "kept <nil>" {
			t.Errorf("logs after the failed swap: %q", got)
		}
	}

	failSwap(filepath.Join(path, "own"), "not by the container runtime")
	if got := ask("umount own"); got != "<nil>" {
		t.Fatalf("umount own: %s", got)
	}
	failSwap(filepath.Join(path, "cache"), "missing in the new mount")

	if err := os.Mkdir(filepath.Join(node, "cache"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := swapMounts(cmd.Process.Pid, []liveMount{{target: node, path: path}}, sources, hostNS); err != nil {
		t.Fatal(err)
	}
	if got := ask("ls"); got != "<nil> cache host logs new profiles.yaml" {
		t.Errorf("child sees %q at the mount path, want the new mount", got)
	}
	for file, want := range map[string]string{"logs/kept": "kept <nil>", "cache/kept": "cache <nil>", "profiles.yaml": "profile <nil>", "host/old": "<nil>"} {
		if got := ask("cat " + file); got != want {
			t.Errorf("%s on the new mount: %q, want %q", file, got, want)
		}
	}

	// A swapped mount below another swapped one is swapped on its own, not carried.
	if err := os.WriteFile(filepath.Join(node, "logs", "newlogs"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	logs := filepath.Join(path, "logs")
	if _, _, err := swapMounts(cmd.Process.Pid, []liveMount{{target: filepath.Join(node, "logs"), path: logs}, {target: node, path: path}}, sources, hostNS); err != nil {
		t.Fatal(err)
	}
	for file, want := range map[string]string{"logs/newlogs": "<nil>", "cache/kept": "cache <nil>"} {
		if got := ask("cat " + file); got != want {
			t.Errorf("%s after swapping logs too: %q, want %q", file, got, want)
		}
	}
}
