# csi-mount-healer

[![Kubernetes](https://img.shields.io/badge/Kubernetes-1.26%2B-326CE5?logo=kubernetes&logoColor=white)](#requirements)
[![Linux](https://img.shields.io/badge/Linux-5.8%2B-FCC624?logo=linux&logoColor=black)](#requirements)

**Remounts dead CSI volumes of running pods through their CSI driver, and keeps
pods from writing to the node disk while a mount is missing.**

A DaemonSet that checks every CSI mount kubelet made on its node. A dead one is
mounted again by calling the driver the way kubelet did, and the new mount is
swapped into the running containers that use it, or those are restarted onto
it. When that is not possible, the pod is deleted. Which of these it may do is configurable as [heal tiers](#heal-tiers).

## Why

When a FUSE-based CSI driver (MooseFS, SeaweedFS, rclone, ...) restarts or its
daemon crashes, every pod on that node keeps a dead mount: `transport endpoint is
not connected`. kubelet never mounts a volume again for a running pod, so the pod
stays broken until someone deletes it.

Deleting it is the usual fix, but a restarted pod loses what it held in memory.
Remounting under the running pod keeps it: the new mount is swapped into the
running containers, or the containers are restarted, and the pod, its IP and its
place on the node stay.

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
- Swaps the new mount into the running containers that mount the volume,
  sidecars included, and watches until no process holds the dead mount; or
  restarts only those containers.
- Deletes the pod when the restart tier fails or does not apply (`restartPolicy`
  other than `Always`) and a controller recreates it, and force deletes pods
  stuck terminating on a dead mount.
- Heal tiers can be enabled one by one, down to only reporting dead mounts.
- Guards the directory underneath each mount with the immutable flag
  (`chattr +i`), which stops even root in the pod: always, only while
  remounting, or not at all.

## Requirements

- Kubernetes 1.26 or newer, for the fsGroup kubelet hands to CSI drivers with
  `VOLUME_MOUNT_GROUP`. Tested on 1.37.
- Linux 5.8 or newer (`statx` mount root, `open_tree`); 5.12 or newer for the
  live tier (`mount_setattr`).
- A filesystem under `/var/lib/kubelet` that supports the immutable flag (ext4,
  xfs, btrfs, zfs, ...), for the guard.
- CSI drivers registered with kubelet through `node-driver-registrar`, with their
  sockets under the kubelet directory.
- A CRI runtime socket, to restart containers after a remount. The live tier
  reads the pid of a container from the verbose container status, which
  containerd reports.
- `hostPID`, for the live tier to reach container processes through `/proc`.

Not rebuilt, so healed by deleting the pod: raw block volumes and drivers that
request service account tokens (`CSIDriver.spec.tokenRequests`).

## Install

```sh
kubectl apply -f https://raw.githubusercontent.com/Ruakij/csi-mount-healer/main/deploy/csi-mount-healer.yaml
```

On k3s, point `CRI_ENDPOINT` and the `cri` hostPath at
`/run/k3s/containerd/containerd.sock`.

The container is privileged, as Bidirectional mount propagation requires, but
the binary drops every capability except `CAP_SYS_ADMIN`,
`CAP_LINUX_IMMUTABLE`, `CAP_DAC_READ_SEARCH`, `CAP_SYS_PTRACE` and
`CAP_SYS_CHROOT` at startup and sets `no_new_privs`. The last two are for the
live tier: `CAP_SYS_PTRACE` opens the `/proc` entries of container processes
running as other users, and `setns` into a mount namespace needs
`CAP_SYS_CHROOT`. The DaemonSet runs with `hostPID`, so the pids the runtime
reports are visible.

The ClusterRole can read every secret: kubelet passes node stage and publish
secrets to the driver, and the healer has to pass the same ones.

## Events

Every step shows as an event on the pod, from the `csi-mount-healer` component
on its node:

| Reason | Type | |
|---|---|---|
| `DeadMount` | Warning | a mount failed `-strikes` checks in a row, with the error; healing starts |
| `Remounted` | Normal | a volume was mounted again through its driver, or swapped into a running container that holds nothing on the dead mount, or released it before `-live-timeout` |
| `Remounted` | Warning | a volume was swapped into a running container that still holds handles on the dead mount, with their count, the processes and the time of its escalation |
| `Escalating` | Warning | a container is escalated from the live tier to the next, with the reason |
| `DeletingPod` | Warning | the delete tier, with the reason the tiers before it did not heal it |
| `NotHealed` | Warning | no enabled tier was left, with the reasons |

## Heal tiers

A dead volume goes to the first enabled tier that applies to its pod, least
disruptive first, whatever the order in `-tiers`. When a tier fails, the volume is
escalated to the next enabled one. When none is left, the healer logs it and
records a `NotHealed` event, and does nothing more.

The live tier works per container: a container it cannot swap is escalated to the
next tier right away, and so is one whose processes still hold handles on the
dead mount `-live-timeout` after the swap, with an `Escalating` event. Other
containers of the pod keep their swap. A pod or container gone before then is
left alone. With `-live-timeout=0`, a swapped container is never escalated,
whatever it holds on the dead mount.

| Tier | What happens | What the pod keeps | Skipped when |
|---|---|---|---|
| `live` | the volume is mounted again through its driver, and the new mount replaces the dead one inside each running container that uses it | everything, including the memory of every process | per container: a `subPathExpr`, another mount below the mount path, or a failed swap |
| `restart` | the volume is mounted again through its driver, and the containers that use it are stopped for kubelet to start them again | the pod, its IP, its place on the node and every container that does not use the volume | `restartPolicy` is not `Always`, or the pod is terminating |
| `delete` | the pod is deleted, and force deleted when it is already terminating | nothing | the pod has no controller to recreate it (no `ownerReferences`) and is not terminating |

With `-tiers=` empty, dead mounts are only reported, as `DeadMount` events and
in the log.

## Configuration

| Flag | Environment | Default | |
|---|---|---|---|
| `-node-name` | `NODE_NAME` | | node to watch |
| `-kubelet-root` | `KUBELET_ROOT` | `/var/lib/kubelet` | kubelet directory, mounted at the same path as on the node |
| `-interval` | `INTERVAL` | `5m` | time between two checks of every mount |
| `-strikes` | `STRIKES` | `3` | failed checks in a row before a mount is healed |
| `-stat-timeout` | `STAT_TIMEOUT` | `30s` | how long a `stat` may take before the mount counts as hung |
| `-tiers` | `TIERS` | `live,restart,delete` | [heal tiers](#heal-tiers) to use; empty only reports |
| `-live-timeout` | `LIVE_TIMEOUT` | `5m` | how long a container swapped by the live tier may hold handles on the dead mount before it is escalated to the next tier; `0` disables the escalation |
| `-guard` | `GUARD` | `always` | `always`: every mount of a started pod; `remount`: only while remounting; `off` |
| `-selector` | `SELECTOR` | | label selector picking the volumes to check, heal and guard; empty picks all |
| `-cri-endpoint` | `CRI_ENDPOINT` | `unix:///run/containerd/containerd.sock` | container runtime socket |

The selector has the syntax of `kubectl -l`, its terms all have to match, and it
sees the labels of the pod plus two of the volume: `namespace` and `driver`, the
CSI driver name. These two override pod labels of the same name. A volume the
selector leaves out is not checked, healed or guarded, e.g.:

```sh
-selector='driver in (csi.moosefs.com,seaweedfs-csi-driver),namespace notin (kube-system),!csi-mount-healer/skip'
```

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
3. A mount that fails `-strikes` checks in a row goes to the first
   [heal tier](#heal-tiers) that applies.
4. To remount, the driver is found the way kubelet finds it: by asking each
   socket in `<kubelet>/plugins_registry` for its name and endpoint. The
   requests are rebuilt from the pod, the PersistentVolume, the CSIDriver, the
   VolumeAttachment and the referenced secrets, following the CSI volume plugin
   of kubelet. Then `NodeUnstageVolume` and `NodeStageVolume` if the staging mount is
   dead, `NodeUnpublishVolume` and a lazy unmount of whatever is left on the
   target, and `NodePublishVolume`.
5. subPath binds of the volume still point into the dead mount, and kubelet
   reuses a bind that exists, so they are unmounted and kubelet binds them again.
6. Containers see volumes through their own bind of the target, made when they
   started. The live tier replaces that bind: it finds the main process of each
   running container through the verbose CRI container status, clones the new
   mount (or its subPath) with `open_tree`, read-only if the volumeMount is,
   and enters the mount namespace of the container with `setns` on a thread of
   its own, which is discarded afterwards. There it lazily unmounts the dead
   mount at the mount path and moves the clone there with `move_mount`.
7. The processes of a swapped container may still hold the dead mount: open
   files, working or root directories, mapped files. They are counted
   across every process in the mount namespace of the container, through
   `mnt_id` in `/proc/<pid>/fdinfo`, which never touches the file, and `statx`
   with `STATX_MNT_ID` and `AT_STATX_DONT_SYNC` on `/proc/<pid>/cwd`, `root`
   and `map_files`, under the stat timeout. `ENOTCONN`, `ESTALE` or a hung
   `statx` count as a handle on the dead mount. The count runs right after the
   swap and, if anything is held, once more `-live-timeout` later, when the
   container is escalated to the next tier unless it released them.
8. The restart tier stops the running containers through the CRI instead.
   kubelet starts them again on the new mount.
9. The delete tier deletes the pod with a UID precondition, so a pod recreated
   under the same name is left alone.
10. The guard sets `FS_IMMUTABLE_FL` on the directory underneath the mount,
    reached through a non-recursive `open_tree` clone of the parent, which shows
    the directory without what is mounted on it and never touches a hung mount. A
    pod is guarded as soon as a pod watch sees its first container start.
    kubelet cannot remove an immutable directory when the pod stops, so the flag
    is cleared as soon as a pod watch sees the pod terminate, and for every mount
    when the healer shuts down.

## Development

```sh
git config core.hooksPath .githooks  # gofmt, vet, tests and lint before each commit
make build       # bin/csi-mount-healer
make test-mount  # real mounts, guards, a remount through a fake driver and a live swap, needs Docker
```
