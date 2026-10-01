# csi-mount-healer

[![Helm](https://img.shields.io/badge/dynamic/yaml?url=https%3A%2F%2Fruakij.github.io%2Fcsi-mount-healer%2Findex.yaml&query=%24.entries%5B%27csi-mount-healer%27%5D%5B0%5D.version&label=Helm&logo=helm&color=0F1689&prefix=v)](#install)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-1.26%2B-326CE5?logo=kubernetes&logoColor=white)](#requirements)
[![Linux](https://img.shields.io/badge/Linux-5.8%2B-FCC624?logo=linux&logoColor=black)](#requirements)

**Remounts dead CSI volumes of running pods through their CSI driver, and keeps
pods from writing to the node disk while a mount is missing.**

A DaemonSet that checks every CSI mount kubelet made on its node. A dead one is
mounted again by calling the driver the way kubelet did, and the new mount is
swapped into the running containers that use it, or those are restarted onto
it. When that is not possible, the pod is deleted. Which of these it may do is
configurable as [heal tiers](#heal-tiers).

## Why

When the daemon behind a FUSE-based CSI mount dies, for example in a driver
restart or upgrade, the pods on that node can keep a dead mount: `transport endpoint is not connected`. kubelet never mounts a
volume again for a running pod, so unless the driver repairs its mounts itself,
the pod stays broken until someone deletes it, losing what it held in memory.

Worse than a dead mount is a missing one. The empty directory underneath is a
plain directory on the node disk, and a container started on it initialises
fresh, empty state there: a new database, new keys, while the real data sits
untouched on the storage. The guard makes that directory immutable, so such a
container gets `EPERM` on its first write instead.

## Features

- Detects mounts that fail `stat` (`ENOTCONN`, `ESTALE`, ...), hang, are not
  mounted at all under a running pod, or were published from the bare staging
  directory while the staging mount was gone; heals after several failed checks
  in a row.
- Remounts through the node service of the driver with the request kubelet built:
  access mode, fs type, mount options, volume attributes, pod info, secrets,
  publish context, `fsGroup`. Restages first when the staging mount is dead too.
- Swaps the new mount into the running containers that mount the volume,
  sidecars included, and watches until no process holds the dead mount; or
  restarts only those containers.
- Deletes the pod when the restart tier fails or does not apply and a controller
  recreates it, and force deletes pods stuck terminating on a dead mount unless
  `-force-delete=false`.
- Heal tiers can be enabled one by one, down to only reporting dead mounts.
- Guards the directory underneath each mount with the immutable flag
  (`chattr +i`), which stops even root in the pod: always, only while
  remounting, or not at all; optionally the staging directories too.

## Requirements

- Kubernetes 1.26 or newer, for the fsGroup kubelet hands to CSI drivers with
  `VOLUME_MOUNT_GROUP`. Tested on 1.37.
- Linux 5.8 or newer (`statx` mount root, `open_tree`); 5.12 or newer for the
  live tier (`mount_setattr`).
- A filesystem under `/var/lib/kubelet` that supports the immutable flag (ext4,
  xfs, btrfs, zfs, ...), for the guard.
- CSI drivers registered with kubelet through `node-driver-registrar`, with their
  sockets under the kubelet directory.
- A CRI runtime socket, to restart containers. The live tier reads container
  pids from the verbose container status, which containerd reports, and needs
  `hostPID` to reach them through `/proc`.

Raw block volumes and drivers that request service account tokens
(`CSIDriver.spec.tokenRequests`) are not remounted, so only the delete tier
heals them.

## Install

From the Helm repository:

```sh
helm repo add csi-mount-healer https://ruakij.github.io/csi-mount-healer
helm repo update
helm install csi-mount-healer csi-mount-healer/csi-mount-healer -n kube-system
```

Or straight from the OCI registry, which is also where prereleases go:

```sh
helm install csi-mount-healer oci://ghcr.io/ruakij/charts/csi-mount-healer -n kube-system
```

On k3s and RKE2, add `--set criSocket=/run/k3s/containerd/containerd.sock`.
Flags go under `config` by name, e.g. `--set config.interval=2m`; see
[values.yaml](charts/csi-mount-healer/values.yaml). Without Helm,
`helm template` renders plain manifests to apply.

The container is privileged, as Bidirectional mount propagation requires, but
the binary drops every capability except `CAP_SYS_ADMIN`,
`CAP_LINUX_IMMUTABLE`, `CAP_DAC_READ_SEARCH`, `CAP_SYS_PTRACE` and
`CAP_SYS_CHROOT` at startup and sets `no_new_privs`. The last two are for the
live tier: reading the `/proc` entries of processes running as other users, and
`setns` into their mount namespace.

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
| `DriverDown` | Warning | the driver of a dead volume cannot be reached, healing again at the next check, with the attempt when `-driver-down-attempts` limits them; counted on one event otherwise |
| `RemountFailed` | Warning | the driver was reached but the remount failed, with the error and the attempt, healing again at the next check |
| `NotHealed` | Warning | no enabled tier was left, with the reasons |

## Heal tiers

A dead volume goes to the first enabled tier that applies to its pod, least
disruptive first, whatever the order in `-tiers`. A tier that fails passes the
volume on to the next enabled one. With none left, the pod gets a `NotHealed`
event. With `-tiers=` empty, dead mounts are only reported.

| Tier | What happens | Advantages | Disadvantages | Skipped when |
|---|---|---|---|---|
| `live` | the volume is remounted through its driver, and the new mount replaces the dead one inside each running container that uses it | the pod keeps everything, including the memory of every process | open files, working directories and mapped files on the dead mount stay broken; needs `hostPID` and Linux 5.12 | per container: a `subPathExpr`, another mount below the mount path, or a failed swap |
| `restart` | the volume is remounted, and the containers that use it are stopped for kubelet to start them again | the pod keeps its IP, its place on the node and every container that does not use the volume | the restarted containers lose their memory and are down until they start again | `restartPolicy` is not `Always`, or the pod is terminating |
| `delete` | the pod is deleted, and force deleted when it is already terminating | works whatever state the mount or driver is in | everything is lost; the new pod may land on another node with a new IP; a force deleted pod's processes may outlive it next to the replacement | the pod has no controller to recreate it (no `ownerReferences`) and is not terminating |

The live tier works per container: a container it cannot swap goes to the next
tier right away, and so does one whose processes still hold the dead mount
`-live-timeout` after the swap, with an `Escalating` event. Other containers keep
their swap. With `-live-timeout=0` a swapped container is never escalated.

Every tier but `delete` needs the remount through the driver. A driver that is
not registered, refuses the connection or does not answer within 10 seconds
counts as down. A failed remount is healed again at the next check, one
`-interval` later, until the pod fails
`-driver-down-attempts` heals in a row on an unreachable driver, or
`-remount-attempts` on a reachable one that cannot remount; then it is
escalated, which ends at the delete tier. Both count the same heals in a row,
each against its own limit, and `0` never escalates.

- An unreachable driver is waited for by default: the pod keeps running, and
  once the driver is back the live tier keeps its processes. With
  `-driver-down-attempts` the pod is deleted instead, and the new pod waits in
  `ContainerCreating` until the driver is back, so the app is stopped rather
  than running on a dead mount. `1` deletes it at the first heal, even when the
  driver is only in the middle of a restart; `2` waits out a restart.
- A reachable driver that cannot remount gets 2 more checks by default. This
  covers drivers whose own mount on the node is dead too and recovers by itself,
  for example when a liveness probe restarts it.

### Outcomes

With the default settings, a mount that dies is healed at its third failed
check in a row, 2 to 3 minutes later. Until then, a guarded mount that is
missing fails every write with `EPERM`.

Some drivers repair their own mounts: they keep the FUSE daemon alive outside
the driver pod (a systemd scope on the node, a separate mount pod), or remount
on startup or from a periodic health check. A mount they repair in time passes
the next check and is left alone, so the healer only steps in when they do not. The default leaves them at least 2 minutes; for a
driver that takes longer, raise `-strikes` or `-interval`.

| When | What happens | Events |
|---|---|---|
| A check fails fewer than `-strikes` times in a row | nothing, a passing check resets the count | none, only log lines |
| No process of a container holds the dead mount | `live`: swapped in, the container keeps running | `DeadMount`, `Remounted` (driver), `Remounted` |
| Processes hold the dead mount and release it within `-live-timeout` | `live`: swapped, the container keeps running | `DeadMount`, `Remounted` (driver), `Remounted` (Warning), `Remounted` once released |
| Processes still hold the dead mount after `-live-timeout` | the container is restarted with `restartPolicy: Always`, else the pod is deleted if a controller recreates it | as above, then `Escalating`, and `DeletingPod` or `NotHealed` |
| The same, with `-live-timeout=0` | the container keeps running; what it holds on the dead mount stays broken | as above, without the escalation |
| A container cannot be swapped | restarted, or the pod deleted, as above | `DeadMount`, `Remounted` (driver), `Escalating`, then as above |
| The driver cannot be reached | healed again at every following check | `DeadMount`, `DriverDown` |
| The same, with `-driver-down-attempts` | the pod is deleted if a controller recreates it, at that heal in a row | `DeadMount`, `DriverDown`, then `DeletingPod` or `NotHealed` |
| The driver is reached but the remount fails | healed again at the next check; the pod is deleted if a controller recreates it at the `-remount-attempts` heal in a row | `DeadMount`, `RemountFailed`, then `DeletingPod` or `NotHealed` |
| The same, with `-remount-attempts=0` | healed again at every following check | `DeadMount`, `RemountFailed` |
| The pod has no controller and no other tier heals it | nothing | `DeadMount`, `NotHealed` |
| The pod is stuck terminating on a dead mount | force deleted, unless `-force-delete=false` | `DeadMount`, `DeletingPod` or `NotHealed` |
| `-tiers=` is empty | nothing; repeats every `-strikes` checks while the mount stays dead | `DeadMount`, `NotHealed` |

## Guards

| Flag | Directory made immutable | Advantages | Disadvantages |
|---|---|---|---|
| `-guard=always` (default) | underneath the publish target of every started pod | a container started on a missing mount fails on its first write | none known; released as soon as the pod stops, so kubelet can remove the directory |
| `-guard=remount` | underneath the publish target, while it is remounted | nothing is left immutable between heals | a mount that dies between checks exposes a writable directory |
| `-guard-stage` (off by default) | underneath the staging mount of every volume a started pod uses | a pod published while the staging mount is gone cannot write to the node disk | breaks drivers that remove the staging directory while pods use the volume, e.g. to recover it |

Every guard is released when the healer shuts down.

## Configuration

| Flag | Environment | Default | |
|---|---|---|---|
| `-node-name` | `NODE_NAME` | | node to watch |
| `-kubelet-root` | `KUBELET_ROOT` | `/var/lib/kubelet` | kubelet directory, mounted at the same path as on the node |
| `-interval` | `INTERVAL` | `1m` | time between two checks of every mount |
| `-strikes` | `STRIKES` | `3` | failed checks in a row before a mount is healed |
| `-stat-timeout` | `STAT_TIMEOUT` | `30s` | how long a `stat` may take before the mount counts as hung |
| `-tiers` | `TIERS` | `live,restart,delete` | [heal tiers](#heal-tiers) to use; empty only reports |
| `-live-timeout` | `LIVE_TIMEOUT` | `1m` | how long a swapped container may hold the dead mount before it is escalated; `0` disables that |
| `-remount-attempts` | `REMOUNT_ATTEMPTS` | `3` | heals in a row that may fail on a reachable driver before the volume is [escalated](#heal-tiers); `0` never escalates |
| `-driver-down-attempts` | `DRIVER_DOWN_ATTEMPTS` | `0` | heals in a row that may find the driver unreachable before the volume is [escalated](#heal-tiers); `0` never escalates |
| `-force-delete` | `FORCE_DELETE` | `true` | force delete a pod stuck terminating on a dead mount |
| `-guard` | `GUARD` | `always` | [guard](#guards) mode: `always`, `remount` or `off` |
| `-guard-stage` | `GUARD_STAGE` | `false` | also [guard](#guards) staging directories |
| `-selector` | `SELECTOR` | | label selector picking the volumes to check, heal and guard; empty picks all |
| `-cri-endpoint` | `CRI_ENDPOINT` | `unix:///run/containerd/`<br>`containerd.sock` | container runtime socket |

The selector has the syntax of `kubectl -l` and sees the labels of the pod plus
`namespace` and `driver`, the CSI driver name, which override pod labels of the
same name, e.g.:

```sh
-selector='driver in (fuse.csi.example.com),namespace notin (kube-system),!csi-mount-healer/skip'
```

A flag wins over its environment variable. Plus the `klog` flags, e.g. `-v=2`,
which have no environment variables.

## How it works

1. Every CSI volume of a pod lives at
   `<kubelet>/pods/<uid>/volumes/kubernetes.io~csi/<name>/mount`, next to the
   `vol_data.json` kubelet wrote for it. Listing these only reads the directories
   above the mounts, so a hung mount cannot block the scan.
2. Each mount is checked with `statx` in a goroutine with a timeout; a stat that
   hangs is not repeated until it returns. `STATX_ATTR_MOUNT_ROOT` tells whether
   the path is still mounted, bind mounts from the same filesystem included.
   `/proc/self/mountinfo` tells where a mount was bound from, which shows a
   publish of the bare `.../globalmount` staging directory.
3. A mount that fails `-strikes` checks in a row goes to the first
   [heal tier](#heal-tiers) that applies.
4. The driver is found the way kubelet finds it: by asking each socket in
   `<kubelet>/plugins_registry` for its name and endpoint. The requests are
   rebuilt from the pod, the PersistentVolume, the CSIDriver, the
   VolumeAttachment and the referenced secrets, following the CSI volume plugin
   of kubelet. Then `NodeUnstageVolume` and `NodeStageVolume` if the staging
   mount is dead, `NodeUnpublishVolume`, a lazy unmount of whatever is left on
   the target, and `NodePublishVolume`.
5. subPath binds still point into the dead mount, and kubelet reuses a bind that
   exists, so they are unmounted for kubelet to bind them again.
6. Containers see volumes through their own bind of the target, made when they
   started. The live tier finds the main process of each running container
   through the verbose CRI container status, clones the new mount (or its
   subPath) with `open_tree`, read-only if the volumeMount is, and enters the
   mount namespace of the container with `setns` on a thread of its own, which
   is discarded afterwards. There it lazily unmounts the dead mount and moves
   the clone in with `move_mount`.
7. Handles on the dead mount (open files, working or root directories, mapped
   files) are counted across every process in the mount namespace of the
   container: `mnt_id` in `/proc/<pid>/fdinfo`, which never touches the file,
   and `statx` with `STATX_MNT_ID` and `AT_STATX_DONT_SYNC` on
   `/proc/<pid>/cwd`, `root` and `map_files`, under the stat timeout. The count
   runs right after the swap and, if anything is held, again `-live-timeout`
   later.
8. The restart tier stops the containers through the CRI; kubelet starts them
   again on the new mount.
9. The delete tier deletes the pod with a UID precondition, so a pod recreated
   under the same name is left alone.
10. The guard sets `FS_IMMUTABLE_FL` on the directory underneath the mount,
    reached through a non-recursive `open_tree` clone of the parent, which shows
    the directory without what is mounted on it and never touches a hung mount.
    A pod watch sets it when the first container of a pod starts and clears it
    when the pod terminates, since kubelet cannot remove an immutable directory.

## Development

```sh
git config core.hooksPath .githooks  # gofmt, vet, tests and lint before each commit
make build       # bin/csi-mount-healer
make test-mount  # real mounts, guards, a remount through a fake driver and a live swap, needs Docker
```
