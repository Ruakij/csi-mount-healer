// Command csi-mount-healer watches the CSI mounts of the pods on its node and
// heals a dead one: mounting it again through its CSI driver and swapping the new
// mount into the running containers or restarting them, or deleting the pod. It can also make the directory underneath each mount immutable, so a pod
// never writes to the node disk while its mount is missing.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"github.com/Ruakij/csi-mount-healer/internal/healer"
)

var (
	// Set by the build process
	version = "dev"
)

func main() {
	var cfg healer.Config
	var guard, selector string

	flag.StringVar(&cfg.NodeName, "node-name", "", "name of this node")
	flag.StringVar(&cfg.KubeletRoot, "kubelet-root", "/var/lib/kubelet", "path to the kubelet directory, mounted at the same path as on the node")
	flag.DurationVar(&cfg.Interval, "interval", time.Minute, "time between two checks of every mount")
	flag.IntVar(&cfg.Strikes, "strikes", 3, "checks in a row a mount has to fail before it is healed")
	flag.DurationVar(&cfg.StatTimeout, "stat-timeout", 30*time.Second, "how long a stat may take before the mount counts as hung")
	_ = cfg.Tiers.Set("live,restart,delete")
	flag.Var(&cfg.Tiers, "tiers", "comma-separated heal tiers to use, least disruptive first whatever the order: live, restart, delete; empty only reports")
	flag.DurationVar(&cfg.LiveTimeout, "live-timeout", time.Minute, "how long a container swapped by the live tier may hold handles on the dead mount before it is escalated to the next tier; 0 disables the escalation")
	flag.BoolVar(&cfg.DeleteOnDriverDown, "delete-on-driver-down", false, "escalate a dead volume whose CSI driver cannot be reached, which ends at the delete tier, instead of healing it again at the next check")
	flag.StringVar(&guard, "guard", string(healer.GuardAlways), "make the directory underneath a mount immutable: always | remount | off")
	flag.StringVar(&selector, "selector", "", "label selector over the pod labels plus namespace and driver, picking the volumes to check, heal and guard; empty picks all")
	flag.StringVar(&cfg.CRIEndpoint, "cri-endpoint", "unix:///run/containerd/containerd.sock", "container runtime socket, used to restart containers after a remount")

	if err := applyEnv(flag.CommandLine, os.LookupEnv); err != nil {
		klog.Fatalf("%v", err)
	}
	showVersion := flag.Bool("version", false, "show version")

	klog.InitFlags(nil)
	flag.Parse()

	if *showVersion {
		fmt.Println(path.Base(os.Args[0]), version)
		return
	}
	if err := healer.DropPrivileges(); err != nil {
		klog.Fatalf("dropping privileges: %v", err)
	}
	cfg.Guard = healer.GuardMode(guard)
	var err error
	if cfg.Selector, err = labels.Parse(selector); err != nil {
		klog.Fatalf("invalid selector: %v", err)
	}

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("failed to build in-cluster kubeconfig: %v", err)
	}
	if cfg.KubeClient, err = kubernetes.NewForConfig(restConfig); err != nil {
		klog.Fatalf("failed to build kubernetes client: %v", err)
	}

	h, err := healer.New(cfg)
	if err != nil {
		klog.Fatalf("invalid configuration: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT)
	defer stop()
	if err := h.Run(ctx); err != nil {
		klog.Fatalf("healer exited: %v", err)
	}
}
