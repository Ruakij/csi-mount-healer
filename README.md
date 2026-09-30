# csi-mount-healer

[![Linux](https://img.shields.io/badge/Linux-5.8%2B-FCC624?logo=linux&logoColor=black)](#requirements)

**Remounts dead CSI volumes of running pods through their CSI driver, and keeps
pods from writing to the node disk while a mount is missing.**

A DaemonSet that checks every CSI mount kubelet made on its node. A dead one is
mounted again by calling the driver the way kubelet did, and the containers using
it are restarted onto the new mount. When that is not possible, the pod is
deleted.

## Why

When a FUSE-based CSI driver (MooseFS, SeaweedFS, rclone, ...) restarts or its
daemon crashes, every pod on that node keeps a dead mount: `transport endpoint is
not connected`. kubelet never mounts a volume again for a running pod, so the pod
stays broken until someone deletes it.

Deleting it is the usual fix, but a restarted pod loses what it held in memory.
Remounting under the running pod keeps it: the containers are restarted, the pod,
its IP and its place on the node stay.

Worse than a dead mount is a missing one. Once a dead mount is unmounted, the
empty directory underneath is a plain directory on the node disk, and the next
container started on it initialises fresh, empty state there: a new database, new
keys, while the real data sits untouched on the storage. The guard makes that
directory immutable, so such a container gets `EPERM` on its first write instead.

## Features

- Detects mounts that fail `stat` (`ENOTCONN`, `ESTALE`, ...), hang, or are not
  mounted at all under a running pod; heals after several failed checks in a row.
- Remounts through the node service of the driver with the request kubelet built:
  access mode, fs type, mount options, volume attributes, pod info, secrets,
  publish context, `fsGroup`. Restages first when the staging mount is dead too.
- Restarts only the containers that mount the volume, sidecars included.
- Falls back to deleting the pod: when the remount fails, for pods that would not
  restart their containers (`restartPolicy` other than `Always`), and force
  deleting pods stuck terminating on a dead mount.
- Guards the directory underneath each mount with the immutable flag
  (`chattr +i`), which stops even root in the pod: always, only while
  remounting, or not at all.

## Requirements

- Linux 5.8 or newer (`statx` mount root, `open_tree`).
- A filesystem under `/var/lib/kubelet` that supports the immutable flag (ext4,
  xfs, btrfs, zfs, ...), for the guard.
- CSI drivers registered with kubelet through `node-driver-registrar`, with their
  sockets under the kubelet directory.
- A CRI runtime socket, to restart containers after a remount.

Not rebuilt, so healed by deleting the pod: raw block volumes and drivers that
request service account tokens (`CSIDriver.spec.tokenRequests`).

## Install

```sh
kubectl apply -f https://raw.githubusercontent.com/Ruakij/csi-mount-healer/main/deploy/csi-mount-healer.yaml
```

On k3s, point `CRI_ENDPOINT` and the `cri` hostPath at
`/run/k3s/containerd/containerd.sock`.

The container is privileged, as Bidirectional mount propagation requires, but
the binary drops every capability except `CAP_SYS_ADMIN` and
`CAP_LINUX_IMMUTABLE` at startup and sets `no_new_privs`.

The ClusterRole can read every secret: kubelet passes node stage and publish
secrets to the driver, and the healer has to pass the same ones.

## Configuration

| Flag | Environment | Default | |
|---|---|---|---|
| `-node-name` | `NODE_NAME` | | node to watch |
| `-kubelet-root` | `KUBELET_ROOT` | `/var/lib/kubelet` | kubelet directory, mounted at the same path as on the node |
| `-interval` | `INTERVAL` | `5m` | time between two checks of every mount |
| `-strikes` | `STRIKES` | `3` | failed checks in a row before a mount is healed |
| `-stat-timeout` | `STAT_TIMEOUT` | `30s` | how long a `stat` may take before the mount counts as hung |
| `-remount` | `REMOUNT` | `true` | remount before falling back to deleting the pod |
| `-guard` | `GUARD` | `always` | `always`: every mount of a started pod; `remount`: only while remounting; `off` |
| `-guard-interval` | `GUARD_INTERVAL` | `30s` | how soon new pods get guarded (`-guard=always`) |
| `-cri-endpoint` | `CRI_ENDPOINT` | `unix:///run/containerd/containerd.sock` | container runtime socket |

A command-line flag wins over its environment variable. Plus the `klog` flags,
e.g. `-v=2`, which have no environment variables.

## How it works

1. Every CSI volume of a pod lives at
   `<kubelet>/pods/<uid>/volumes/kubernetes.io~csi/<name>/mount`, next to the
   `vol_data.json` kubelet wrote for it. Listing these only reads the directories
   above the mounts, so a hung mount cannot block the scan.
2. Each mount is checked with `statx` in a goroutine with a timeout. A stat that
   hangs is not repeated until it returns. `STATX_ATTR_MOUNT_ROOT` tells whether
   the path is still mounted, bind mounts from the same filesystem included.
3. To remount, the driver is found the way kubelet finds it: by asking each
   socket in `<kubelet>/plugins_registry` for its name and endpoint. The
   requests are rebuilt from the pod, the PersistentVolume, the CSIDriver, the
   VolumeAttachment and the referenced secrets, following the CSI volume plugin
   of kubelet. Then `NodeUnstageVolume` and `NodeStageVolume` if the staging mount is
   dead, `NodeUnpublishVolume` and a lazy unmount of whatever is left on the
   target, and `NodePublishVolume`.
4. subPath binds of the volume still point into the dead mount, and kubelet
   reuses a bind that exists, so they are unmounted and kubelet binds them again.
5. Containers see volumes through their own bind of the target, made when they
   started, so the running ones are stopped through the CRI. kubelet starts them
   again on the new mount.
6. The guard sets `FS_IMMUTABLE_FL` on the directory underneath the mount,
   reached through a non-recursive `open_tree` clone of the parent, which shows
   the directory without what is mounted on it and never touches a hung mount.
   kubelet cannot remove an immutable directory when the pod stops, so the flag
   is cleared as soon as a pod watch sees the pod terminate, and for every mount
   when the healer shuts down.

## Development

```sh
git config core.hooksPath .githooks  # gofmt, vet, tests and lint before each commit
make build       # bin/csi-mount-healer
make test-mount  # real mounts, guards and a remount through a fake driver, needs Docker
```
