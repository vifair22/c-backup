#!/bin/bash
#
# Unit backup against real engines, inside the real-engines sibling.
#
# The real-engines CI job (.gitlab-ci.yml) starts one privileged container
# from ci/real-engines/Dockerfile, copies the source tree to /src and c-utils
# to /c-utils, and this script is that container's entrypoint: it is the only
# thing the sibling ever runs. Inside it:
#
#   - a btrfs pool on a loop device, mounted at /mnt/cache the way an Unraid
#     cache pool is, as the helper's snapshot domain;
#   - a private Docker engine (dind) on this container's own socket, never
#     the runner's, with a local registry so a test image has a digest that
#     a define can pull back;
#   - a private libvirt daemon (libvirtd; Debian 13 has no virtqemud) with
#     cbk-uat-* domains of one vCPU and 64 MiB;
#   - LXC with a busybox container on the pool;
#   - c-backup-helper as root and c-backupd dropping itself to uid 990, as
#     deployed (docs/helper.md).
#
# Then real unit backups and restores, each restore compared byte for byte
# with what was there at backup time, and after every step a check that
# nothing was left behind: no capture snapshot, no hold, no paused or frozen
# member, no qemu-nbd, no libvirt backup job, no restore staging.
#
# ZFS is not exercised here: the runner has no zfs module. It stays a
# scripted UAT step on FS1 (docs/helper.md, "UAT: real ZFS").
#
# Every path out (success, a failed check, a signal, the lifetime running
# out) goes through cleanup(), which tears down in reverse and detaches the
# pool. The pool is mounted with -o loop, so the kernel's autoclear frees the
# loop device even when this container is killed outright and cleanup()
# never runs.

set -u -o pipefail

# ---- the hard lifetime ---------------------------------------------------
# Re-exec under timeout(1) before anything else, so an orphaned sibling
# (its job killed, nobody left to remove it) dies on its own. The job sets
# CBK_LIFETIME_S; the ceiling here holds whatever it says.
LIFETIME_MAX=3600
LIFETIME="${CBK_LIFETIME_S:-2400}"
case "$LIFETIME" in ''|*[!0-9]*) LIFETIME=2400 ;; esac
[ "$LIFETIME" -gt "$LIFETIME_MAX" ] && LIFETIME=$LIFETIME_MAX
if [ -z "${CBK_UNDER_TIMEOUT:-}" ]; then
    export CBK_UNDER_TIMEOUT=1
    exec timeout --kill-after=30 "$LIFETIME" "$0" "$@"
fi

# Every daemon started here, this script's own and the e2e suite's, aborts
# on a database write made outside the write lock (B157), so a violation
# fails the run at the statement instead of surfacing later as a symptom.
export C_BACKUP_TXN_GUARD=abort

SRC=/src
CUTILS=/c-utils
WORK=/work
POOL_IMG="$WORK/pool.img"
POOL_MB="${CBK_POOL_MB:-1024}"
VM_MEM_MB="${CBK_VM_MEM_MB:-64}"
CACHE=/mnt/cache
APPS="$CACHE/appdata"
SNAPDIR="$CACHE/system/.cbackup-snapshots"
LXCP="$CACHE/lxc"
DOMS="$CACHE/domains"
HRUN=/run/c-backup-helper
HSOCK="$HRUN/helper.sock"
HSTATE="$CACHE/system/c-backup-helper"
DSTATE="$WORK/daemon"
DSOCK="$DSTATE/run/api.sock"
REPO="$WORK/repo"
OUT="$WORK/out"
REG=127.0.0.1:5000
IMG="$REG/cbk-uat/bb:ci"
VIRT_URI=qemu:///system
DUID=990

PASS=0
FAIL=0
T0=$(date +%s)

log()  { printf '[%4ds] %s\n' "$(( $(date +%s) - T0 ))" "$*"; }
die()  { log "FATAL: $*"; exit 1; }
ok()   { PASS=$((PASS+1)); printf '  ok   %s\n' "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; }
check()    { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "$2" "$3"; fi; }
contains() { case "$3" in *"$2"*) ok "$1" ;; *) bad "$1" "contains '$2'" "$(printf '%.600s' "$3")" ;; esac; }
lacks()    { case "$3" in *"$2"*) bad "$1" "no '$2'" "$(printf '%.600s' "$3")" ;; *) ok "$1" ;; esac; }
# A check expected to fail because of a recorded bug: XFAIL while it fails,
# and a failure (XPASS) once it passes, so the marker cannot outlive the fix.
# The check runs in this shell (a redirected call is not a subshell), so
# its effect on the counters is what decides.
XFAIL=0
known_bug() {
    local why="$1" p0=$PASS f0=$FAIL
    shift
    "$@" >/dev/null
    if [ "$FAIL" -gt "$f0" ]; then
        FAIL=$f0; XFAIL=$((XFAIL+1))
        printf '  XFAIL %s\n       known bug: %s\n' "$2" "$why"
    else
        PASS=$p0
        bad "XPASS $2" "fails (known bug: $why)" "passes: the bug is fixed, remove the known_bug marker"
    fi
}
cb()       { "$SRC/build/bins/c-backup" --socket "$DSOCK" "$@"; }

# ---- teardown, on every path --------------------------------------------

DOCKERD_PID=
REGISTRY_PID=
VIRTQEMUD_PID=
HELPER_PID=
DAEMON_PID=

stop_pid() {
    local pid="$1" i
    [ -n "$pid" ] || return 0
    kill "$pid" 2>/dev/null || return 0
    for i in $(seq 1 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    kill -9 "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
}

cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    log "cleanup (exit $rc)"
    if [ "$rc" -ne 0 ]; then
        for f in "$WORK/daemon.log" "$WORK/helper.log" "$WORK/dockerd.log" "$WORK/libvirtd.log"; do
            [ -s "$f" ] || continue
            echo "---- tail of $f"
            tail -n 60 "$f"
        done
    fi
    stop_pid "$DAEMON_PID"
    stop_pid "$HELPER_PID"
    if [ -n "$VIRTQEMUD_PID" ]; then
        for d in $(virsh -q -c "$VIRT_URI" list --all --name 2>/dev/null | grep '^cbk-uat-'); do
            virsh -q -c "$VIRT_URI" destroy "$d" >/dev/null 2>&1
            virsh -q -c "$VIRT_URI" undefine --nvram "$d" >/dev/null 2>&1 ||
                virsh -q -c "$VIRT_URI" undefine "$d" >/dev/null 2>&1
        done
        stop_pid "$VIRTQEMUD_PID"
    fi
    pkill -9 -x qemu-nbd 2>/dev/null
    pkill -9 -f '^/usr/bin/qemu-system' 2>/dev/null
    if command -v lxc-ls >/dev/null && [ -d "$LXCP" ]; then
        for c in $(lxc-ls -P "$LXCP" 2>/dev/null); do
            lxc-stop -P "$LXCP" -n "$c" -k >/dev/null 2>&1
            lxc-destroy -P "$LXCP" -n "$c" >/dev/null 2>&1
        done
    fi
    if [ -n "$DOCKERD_PID" ]; then
        docker ps -aq 2>/dev/null | xargs -r docker rm -f -v >/dev/null 2>&1
        stop_pid "$DOCKERD_PID"
    fi
    stop_pid "$REGISTRY_PID"
    # Anything still mounted under the pool or the work tree (dind leaves
    # its own mounts if it died hard), deepest first, then the pool. The
    # loop device goes with the pool's unmount (autoclear).
    if mountpoint -q "$CACHE" 2>/dev/null; then
        for s in $(btrfs subvolume list -o "$SNAPDIR" 2>/dev/null | awk '{print $NF}'); do
            btrfs subvolume delete "$CACHE/$s" >/dev/null 2>&1
        done
    fi
    awk '{print $2}' /proc/self/mounts | grep -E "^($CACHE|$WORK|/run/docker|/var/lib/docker)(/|$)" |
        sort -r | while read -r m; do umount -l "$m" 2>/dev/null; done
    local loops
    loops=$(losetup -j "$POOL_IMG" 2>/dev/null)
    if [ -n "$loops" ]; then
        log "loop device still attached after unmount, detaching: $loops"
        losetup -j "$POOL_IMG" | cut -d: -f1 | xargs -r -n1 losetup -d
    fi
    rm -rf "$WORK" "$HRUN" /var/run/docker.sock /var/run/docker.pid
    log "cleanup done"
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

# ---- preconditions -------------------------------------------------------

[ "$(id -u)" = 0 ] || die "must run as root in the privileged sibling"
[ -d "$SRC" ] && [ -f "$SRC/Makefile" ] || die "no source tree at $SRC"
[ -d "$CUTILS" ] || die "no c-utils tree at $CUTILS"
# The whole point of the private engine is that the runner's never sees a
# test container. A socket here means someone bound the host's in.
[ -e /var/run/docker.sock ] && die "/var/run/docker.sock already exists: the runner's engine must never be bound into the sibling"
grep -q 'CapEff:.*ffffffff' /proc/self/status || die "not privileged (needs loop, mount, dind, KVM)"

log "real-engines: lifetime ${LIFETIME}s, pool ${POOL_MB} MiB, VM ${VM_MEM_MB} MiB, cpu.max $(cat /sys/fs/cgroup/cpu.max 2>/dev/null), memory.max $(cat /sys/fs/cgroup/memory.max 2>/dev/null)"
mkdir -p "$WORK" "$OUT"

# cgroup v2: move ourselves out of the root so controllers can be delegated
# to dockerd, LXC and libvirt (what the upstream dind entrypoint does).
if [ -f /sys/fs/cgroup/cgroup.controllers ]; then
    mkdir -p /sys/fs/cgroup/init
    # The list is read whole and the move repeated: iterating cgroup.procs
    # while moving its entries out skips some, and one left behind in the
    # root makes every controller refuse delegation.
    for _ in 1 2 3 4 5; do
        procs=$(cat /sys/fs/cgroup/cgroup.procs)
        [ -n "$procs" ] || break
        for p in $procs; do echo "$p" > /sys/fs/cgroup/init/cgroup.procs 2>/dev/null; done
    done
    for c in $(cat /sys/fs/cgroup/cgroup.controllers); do
        echo "+$c" > /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null || log "cgroup: cannot delegate $c"
    done
fi

# Parallelism from the CPU quota, not nproc: a capped container still sees
# every host CPU, and -j32 under a 2.5 GiB limit is an OOM kill.
JOBS=1
if read -r q per < /sys/fs/cgroup/cpu.max 2>/dev/null && [ "$q" != max ]; then
    JOBS=$(( (q + per - 1) / per ))
else
    JOBS=$(nproc)
fi
[ "$JOBS" -gt 4 ] && JOBS=4
[ "$JOBS" -lt 1 ] && JOBS=1

# ---- build ---------------------------------------------------------------

log "build: c-utils"
make -s -C "$CUTILS" -j"$JOBS" >"$WORK/build-cutils.log" 2>&1 || { tail -40 "$WORK/build-cutils.log"; die "c-utils build failed"; }
# CBK_SANITIZE=1 (the real-engines-asan job, B192): the ASan+UBSan build
# of the three programs, put where the release ones would be, so every step
# below runs them unchanged. UBSan does not recover (-fno-sanitize-recover)
# and ASan halts on its first error, so a finding kills the program at the
# fault and the run fails there; each report also lands in $SAN_DIR, which
# the end of the run checks is empty. Leaks are left to the unit suites'
# ASan job: the programs here are stopped by signal at teardown, after the
# summary.
SAN_DIR="$WORK/sanitizer"
if [ "${CBK_SANITIZE:-0}" = 1 ]; then
    log "build: c-backupd, c-backup, c-backup-helper with ASan and UBSan"
    make -s -C "$SRC" CUTILS_DIR="$CUTILS" MARCH=x86-64-v2 MTUNE=generic -j"$JOBS" BUILD_TYPE=asan _build \
        >"$WORK/build.log" 2>&1 || { tail -60 "$WORK/build.log"; die "sanitizer build failed"; }
    for b in c-backupd c-backup c-backup-helper; do
        [ -x "$SRC/build/bins/$b-asan" ] || die "build produced no $b-asan"
        cp -f "$SRC/build/bins/$b-asan" "$SRC/build/bins/$b"
    done
    # 1777: the daemon writes its reports after it drops to uid 990, and a
    # report it cannot open goes nowhere (ASan says only "Can't open file").
    mkdir -p "$SAN_DIR" && chmod 1777 "$SAN_DIR"
    export ASAN_OPTIONS="halt_on_error=1:detect_leaks=0:detect_stack_use_after_return=1:log_path=$SAN_DIR/asan"
    export UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1:log_path=$SAN_DIR/ubsan"
else
    log "build: c-backupd, c-backup, c-backup-helper"
    make -s -C "$SRC" CUTILS_DIR="$CUTILS" MARCH=x86-64-v2 MTUNE=generic -j"$JOBS" all \
        >"$WORK/build.log" 2>&1 || { tail -60 "$WORK/build.log"; die "build failed"; }
fi
for b in c-backupd c-backup c-backup-helper; do
    [ -x "$SRC/build/bins/$b" ] || die "build produced no $b"
done
ldd "$SRC/build/bins/c-backup-helper" | grep -q libvirt || die "the helper was built without libvirt"
log "build: done"

# ---- the btrfs pool ------------------------------------------------------

log "pool: ${POOL_MB} MiB btrfs on a loop device at $CACHE"
modprobe btrfs 2>/dev/null || true
truncate -s "${POOL_MB}M" "$POOL_IMG"
mkfs.btrfs -q -f "$POOL_IMG" >/dev/null || die "mkfs.btrfs failed"
# A privileged container sees the host's /dev as it was at start: a loop
# device the kernel creates since has no node here, and mount -o loop then
# fails to open the device loop-control just handed it. Nodes for the first
# 256 minors, in this container's /dev only, cover whichever it hands out.
for i in $(seq 0 255); do [ -e "/dev/loop$i" ] || mknod "/dev/loop$i" b 7 "$i" 2>/dev/null; done
mkdir -p "$CACHE"
mount -o loop "$POOL_IMG" "$CACHE" || die "cannot mount the pool (loop devices: $(ls /dev/loop* 2>/dev/null | tr '\n' ' '))"
LOOPDEV=$(losetup -j "$POOL_IMG" | cut -d: -f1)
check "the pool's loop device is autoclear" 1 "$(cat "/sys/block/${LOOPDEV#/dev/}/loop/autoclear" 2>/dev/null)"
mkdir -p "$APPS" "$CACHE/system" "$LXCP" "$DOMS" "$HSTATE" "$HRUN"
chmod 0750 "$HRUN"; chgrp "$DUID" "$HRUN"

# ---- the private Docker engine and a local registry ----------------------

log "dind: starting a private dockerd"
mkdir -p "$WORK/docker-exec" /etc/docker
# vfs: overlay2 cannot nest on this container's own overlay, and the test
# images are a few MiB. The data root is on the pool the way Unraid's is,
# /var/lib/docker a bind of /mnt/cache/system/docker, so a volume is in the
# pool's snapshot domain (the helper reads it through the pool, B170) and
# can be captured and restored in place; the pool goes with the container,
# so nothing of this engine outlives it. A data root that is not already a
# mount point dockerd bind-mounts onto itself, which would put a mount
# boundary between the volumes and the rest of the pool that Unraid does
# not have. No bridge, no iptables: the sibling has no network and every
# test container runs with network none.
DOCKER_ROOT=/var/lib/docker
mkdir -p "$CACHE/system/docker" "$DOCKER_ROOT"
mount --bind "$CACHE/system/docker" "$DOCKER_ROOT" || die "cannot bind the docker data root"
dockerd --data-root "$DOCKER_ROOT" --exec-root "$WORK/docker-exec" \
    --storage-driver vfs --bridge none --iptables=false --ip6tables=false \
    -H unix:///var/run/docker.sock >"$WORK/dockerd.log" 2>&1 &
DOCKERD_PID=$!
for _ in $(seq 1 150); do docker version >/dev/null 2>&1 && break; sleep 0.2; done
docker version >/dev/null 2>&1 || die "dockerd did not start"
log "dind: $(docker version -f 'Engine {{.Server.Version}}, API {{.Server.APIVersion}}')"

cat > "$WORK/registry.yml" <<EOF
version: 0.1
storage:
  filesystem:
    rootdirectory: $WORK/registry
http:
  addr: $REG
log:
  level: warn
EOF
docker-registry serve "$WORK/registry.yml" >"$WORK/registry.log" 2>&1 &
REGISTRY_PID=$!
for _ in $(seq 1 50); do curl -fs "http://$REG/v2/" >/dev/null && break; sleep 0.2; done
curl -fs "http://$REG/v2/" >/dev/null || die "the local registry did not start"

# The test image: busybox and nothing else, made here rather than pulled,
# then pushed so it carries a RepoDigest (127.0.0.0/8 registries are
# insecure by default, so no TLS).
rm -rf "$WORK/bbroot"; mkdir -p "$WORK/bbroot/bin" "$WORK/bbroot/data" "$WORK/bbroot/tmp"
cp /bin/busybox "$WORK/bbroot/bin/busybox"
for a in sh cat sleep echo ls cp mkdir rm sync date sha256sum head dd tar tee true; do ln -s busybox "$WORK/bbroot/bin/$a"; done
tar -C "$WORK/bbroot" -cf - . | docker import -c 'ENV PATH=/bin' -c 'CMD ["/bin/sh"]' - "$IMG" >/dev/null || die "docker import failed"
docker push -q "$IMG" >/dev/null || die "push to the local registry failed"
DIGEST=$(docker image inspect -f '{{index .RepoDigests 0}}' "$IMG")
contains "the test image has a digest from the local registry" "$REG/cbk-uat/bb@sha256:" "$DIGEST"

# ---- fixtures: data with every shape a restore has to get right ----------

# Content, mode, size, owner and link target of every entry, and a hash of
# every regular file. mtimes are left out: a restore puts them back, but the
# pre hook rewrites a marker and the comparison is about bytes.
# A directory's size is the filesystem's own bookkeeping (btrfs counts name
# bytes, ext4 and overlay a block), not something a restore carries, and the
# restore lands on a different filesystem from the btrfs source. It is
# blanked; every other entry's size is compared.
manifest() {
    local d="$1"
    ( cd "$d" && find . -mindepth 1 -printf '%P\t%y\t%m\t%U:%G\t%s\t%l\n' |
          awk -F'\t' -v OFS='\t' '$2 == "d" { $5 = "-" } 1' | sort &&
      find . -type f -print0 | sort -z | xargs -0 -r sha256sum ) 2>/dev/null
}

fill() {
    local d="$1" tag="$2"
    mkdir -p "$d/sub/deeper" "$d/empty"
    head -c 1048576 /dev/urandom > "$d/random.bin"
    head -c 300000 /dev/urandom > "$d/sub/deeper/odd-size.bin"
    printf '%s\n' "$tag" > "$d/name.txt"
    # A sparse file: 8 MiB with data at the start and near the end.
    truncate -s 8M "$d/sparse.img"
    head -c 65536 /dev/urandom | dd of="$d/sparse.img" conv=notrunc status=none
    head -c 65536 /dev/urandom | dd of="$d/sparse.img" bs=1M seek=7 conv=notrunc status=none
    ln -s sub/deeper/odd-size.bin "$d/link"
    ln "$d/random.bin" "$d/hardlink.bin"
    chmod 0640 "$d/name.txt"; chmod 0750 "$d/sub"
    chown 1234:2345 "$d/sub/deeper/odd-size.bin"
    : > "$d/zero-length"
}

log "fixtures: a standalone container and a two-service compose project"
SOLO="$APPS/cbk-uat-solo"
PROJ="$APPS/cbk-uat-proj"
mkdir -p "$SOLO" "$PROJ/app" "$PROJ/db"
fill "$SOLO" solo
fill "$PROJ/app" app
fill "$PROJ/db" db
head -c 2097152 /dev/urandom > "$SOLO/dump-source.bin"
printf 'id,value\n1,alpha\n2,beta\n' > "$PROJ/db/table.csv"
docker run -d --name cbk-uat-solo --network none -v "$SOLO:/data" "$IMG" \
    sh -c 'trap "exit 0" TERM; while :; do sleep 1; done' >/dev/null || die "cannot start cbk-uat-solo"
# A container whose data is in volumes, one named and one anonymous (the
# image's VOLUME, as most images declare): a restore has to reach both.
VOLS_NAMED=cbk-uat-vols-data
docker volume create "$VOLS_NAMED" >/dev/null || die "cannot create $VOLS_NAMED"
docker run -d --name cbk-uat-vols --network none -v "$VOLS_NAMED:/named" -v /anon "$IMG" \
    sh -c 'echo named-v1 > /named/f; head -c 262144 /dev/urandom > /named/r.bin; mkdir -p /named/d;
           echo deep > /named/d/x; echo anon-v1 > /anon/f; head -c 131072 /dev/urandom > /anon/r.bin; sync;
           trap "exit 0" TERM; while :; do sleep 1; done' >/dev/null || die "cannot start cbk-uat-vols"
VOLS_ANON=$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/anon"}}{{.Name}}{{end}}{{end}}' cbk-uat-vols)
[ -n "$VOLS_ANON" ] || die "cbk-uat-vols has no anonymous volume"
for _ in $(seq 1 50); do docker exec cbk-uat-vols cat /anon/f >/dev/null 2>&1 && break; sleep 0.2; done
cat > "$PROJ/compose.yaml" <<EOF
services:
  app:
    image: $IMG
    network_mode: none
    command: ["sh", "-c", "trap 'exit 0' TERM; while :; do sleep 1; done"]
    volumes:
      - ./app:/data
  db:
    image: $IMG
    network_mode: none
    command: ["sh", "-c", "trap 'exit 0' TERM; while :; do sleep 1; done"]
    volumes:
      - ./db:/data
      - dbvol:/vol
      - /anon
volumes:
  dbvol:
EOF
printf 'CBK_UAT=1\n' > "$PROJ/.env"
docker compose -p cbk-uat-proj --project-directory "$PROJ" -f "$PROJ/compose.yaml" up -d --no-build >"$WORK/compose-up.log" 2>&1 ||
    { cat "$WORK/compose-up.log"; die "compose up failed"; }
PROJ_VOL=cbk-uat-proj_dbvol
docker exec cbk-uat-proj-db-1 sh -c 'echo vol-v1 > /vol/f; head -c 65536 /dev/urandom > /vol/r.bin; echo anon-v1 > /anon/f; sync' ||
    die "cannot fill $PROJ_VOL"

# ---- helper and daemon ---------------------------------------------------

log "helper: policy and start"
cat > "$WORK/helper.ini" <<EOF
[helper]
daemon_uid = $DUID
daemon_id = 0c1ea11e
max_pause_s = 30
capture_max_s = 3600
allow_restore = 1
allow_define = 1
state_dir = $HSTATE
socket = $HSOCK

[units]
allow = docker:cbk-uat-*, docker:compose:cbk-uat-*, libvirt:cbk-uat-*, lxc:cbk-uat-*

[snapshot]
btrfs_domains = $CACHE
btrfs_snapshot_dir = $SNAPDIR

[host]
nsenter = 0
host_root =
; Debian's paths; the defaults are Unraid's (/sbin/btrfs).
btrfs = $(command -v btrfs)
docker = $(command -v docker)
realpath = $(command -v realpath)
lxc_dir = $(dirname "$(command -v lxc-ls)")

[docker]
enabled = 1
include_roots = $APPS

[lxc]
enabled = 1
path = $LXCP

[libvirt]
enabled = 1
uri = $VIRT_URI
nbd_dir = $HRUN/nbd
scratch_dir = $CACHE/system/cbackup-scratch
staging_root = $WORK/vm-staging

[hook solo-pre]
unit = docker:cbk-uat-solo
kind = run
phase = pre
container = cbk-uat-solo
argv.0 = sh
argv.1 = -c
argv.2 = echo pre-hook-ran | tee /data/pre-hook.marker
on_fail = fail

[hook solo-dump]
unit = docker:cbk-uat-solo
kind = dump
phase = pre
container = cbk-uat-solo
argv.0 = cat
argv.1 = /data/dump-source.bin
timeout_s = 120
on_fail = continue

[hook proj-dump]
unit = docker:compose:cbk-uat-proj
kind = dump
phase = pre
container = cbk-uat-proj-db-1
argv.0 = cat
argv.1 = /data/table.csv
on_fail = continue

[hook proj-post]
unit = docker:compose:cbk-uat-proj
kind = run
phase = post
container = cbk-uat-proj-app-1
argv.0 = sh
argv.1 = -c
argv.2 = echo post-hook-ran
on_fail = continue
EOF
chmod 0600 "$WORK/helper.ini"
mkdir -p "$HRUN/nbd" "$CACHE/system/cbackup-scratch" "$WORK/vm-staging"
chown "$DUID:$DUID" "$WORK/vm-staging"
"$SRC/build/bins/c-backup-helper" --policy "$WORK/helper.ini" --check >/dev/null || die "the helper refuses the policy"

# The libvirt daemon first: the helper's libvirt provider connects at start.
log "libvirt: a private libvirt daemon"
# Root, in this container only: no systemd, so no machined and no cgroup
# placement from libvirt, no security driver, no ownership changes, logs to
# files rather than virtlogd.
mkdir -p /etc/libvirt
cat > /etc/libvirt/qemu.conf <<EOF
user = "root"
group = "root"
dynamic_ownership = 0
security_driver = "none"
cgroup_controllers = [ ]
stdio_handler = "file"
EOF
# Debian 13 packages only the monolithic libvirtd (no virtqemud); either is
# a daemon private to this container with only the QEMU driver in use.
LIBVIRT_DAEMON=$(command -v virtqemud || command -v libvirtd) || die "neither virtqemud nor libvirtd is installed"
mkdir -p /run/libvirt
"$LIBVIRT_DAEMON" --verbose >"$WORK/libvirtd.log" 2>&1 &
VIRTQEMUD_PID=$!
for _ in $(seq 1 100); do virsh -q -c "$VIRT_URI" uri >/dev/null 2>&1 && break; sleep 0.1; done
virsh -q -c "$VIRT_URI" uri >/dev/null 2>&1 || die "$LIBVIRT_DAEMON did not answer"

"$SRC/build/bins/c-backup-helper" --policy "$WORK/helper.ini" >"$WORK/helper.log" 2>&1 &
HELPER_PID=$!
for _ in $(seq 1 100); do [ -S "$HSOCK" ] && break; sleep 0.1; done
[ -S "$HSOCK" ] || die "the helper did not start"
chgrp "$DUID" "$HSOCK"

log "daemon: start as root, drop to $DUID"
mkdir -p "$DSTATE/run" "$DSTATE/keys" "$DSTATE/exports" "$REPO"
chmod 0700 "$DSTATE" "$DSTATE/keys" "$DSTATE/exports"
cat > "$DSTATE/config.yaml" <<EOF
db:
  path: $DSTATE/state.db
http:
  socket: $DSOCK
  port: 18765
  bind: 127.0.0.1
log:
  level: info
security:
  key_dir: $DSTATE/keys
  run_as_uid: $DUID
units:
  helper_socket: $HSOCK
vm:
  export_dir: $DSTATE/exports
EOF
chmod 0600 "$DSTATE/config.yaml"
chown -R "$DUID:$DUID" "$DSTATE" "$REPO" "$OUT"
# The capabilities the deployed container keeps (docs/helper.md): the drop
# takes everything else out of the bounding set itself.
C_BACKUP_CONFIG_PATH="$DSTATE/config.yaml" "$SRC/build/bins/c-backupd" >"$WORK/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 100); do [ -S "$DSOCK" ] && break; sleep 0.1; done
[ -S "$DSOCK" ] || die "the daemon did not start"
check "the daemon runs as uid $DUID" "$DUID" "$(awk '/^Uid:/ {print $2}' "/proc/$DAEMON_PID/status")"
for _ in $(seq 1 300); do cb unit providers 2>/dev/null | grep -q '"state":"connected"' && break; sleep 0.1; done
PROV=$(cb unit providers)
contains "the daemon is connected to the helper" '"state":"connected"' "$PROV"
for p in docker lxc libvirt; do
    contains "provider $p is enabled through the helper" "\"name\":\"$p\"" "$PROV"
done
contains "repo add" '"id":1' "$(cb repo add "$REPO" main)"

# ---- leftovers -----------------------------------------------------------

snapshots_left() { btrfs subvolume list -o "$SNAPDIR" 2>/dev/null | wc -l; }
leftovers() {
    local what="$1"
    check "$what: no capture snapshot left" 0 "$(snapshots_left)"
    check "$what: no hold left in the helper's journal" "[]" "$(tr -d ' \n' < "$HSTATE/holds.json" 2>/dev/null)"
    check "$what: no paused container" "" "$(docker ps -q --filter status=paused)"
    check "$what: no restore staging left" "" "$(find "$CACHE" -xdev -name .cbackup-restore -mindepth 1 -maxdepth 4 2>/dev/null | while read -r s; do ls -A "$s"; done)"
    check "$what: no qemu-nbd left" "" "$(pgrep -x qemu-nbd)"
}

# The snapshot a unit backup made, from the job's result; and a guard so a
# failed backup is one failure, not every restore after it failing too.
snap_of() { jq -r '[.. | .snapshot_id? // empty][0] // empty' <<<"$1" 2>/dev/null; }
have_snap() {
    [ -n "$2" ] && return 0
    bad "$1: a snapshot to restore from" "a snapshot id" "none: the backup failed, its restores are not run"
    return 1
}

# The CLI waits for the job and prints its final JSON.
job_ok() {
    local what="$1" out="$2"
    contains "$what completes" '"state":"completed"' "$out"
}

# ---- Docker: a standalone container --------------------------------------

test_solo() {
log "docker: standalone container cbk-uat-solo"
# The pre hook writes the marker inside the container before the snapshot,
# so the manifest is taken with it in place.
echo pre-hook-ran > "$SOLO/pre-hook.marker"
MAN_SOLO=$(manifest "$SOLO")
rm -f "$SOLO/pre-hook.marker"
OUT_B=$(cb unit backup 1 --provider docker cbk-uat-solo 2>&1)
job_ok "unit backup of a standalone container" "$OUT_B"
# A policy pre hook ran and succeeded, which is what raises a snapshot
# from crash to application consistency.
contains "with a pre hook it is application-consistent" '"consistency":"application"' "$OUT_B"
SNAP_SOLO=$(snap_of "$OUT_B")
leftovers "after the solo backup"
have_snap "the solo backup" "$SNAP_SOLO" || return 0
check "the pre hook ran inside the container before the snapshot" pre-hook-ran "$(cat "$SOLO/pre-hook.marker" 2>/dev/null)"

# Change the live data so a restore that read it, not the snapshot, shows.
echo changed >> "$SOLO/name.txt"
rm -f "$SOLO/random.bin"
head -c 4096 /dev/urandom > "$SOLO/new-after-backup"

R="$OUT/solo"
OUT_R=$(cb unit restore 1 "$SNAP_SOLO" "$R" 2>&1)
job_ok "files restore of the solo snapshot" "$OUT_R"
check "the solo tree restores byte for byte" "$MAN_SOLO" "$(manifest "$R$SOLO")"
check "the dump hook's stream is stored exactly" "$(sha256sum < "$SOLO/dump-source.bin")" "$(sha256sum < "$R/.cbackup-unit/hooks/solo-dump.dump" 2>/dev/null)"
contains "the pre hook's log holds its output" "pre-hook-ran" "$(cat "$R/.cbackup-unit/hooks/solo-pre.log" 2>/dev/null)"
# Through the helper the dump hook's log travels in a BLOB memfd; it was
# once stored empty because the memfd was sealed before the dump's stream
# closed (B141), so a failed dump no longer said why.
contains "the dump hook's log records its exit status" "exit status 0" "$(cat "$R/.cbackup-unit/hooks/solo-dump.log" 2>/dev/null)"
contains "the container's inspect is stored" '"Name"' "$(head -c 100000 "$R/.cbackup-unit/config/containers/cbk-uat-solo/inspect.json" 2>/dev/null)"
contains "mounts.json lists the bind as snapshotted" "snapshot" "$(cat "$R/.cbackup-unit/config/mounts.json" 2>/dev/null)"

log "docker: in-place define over the live container"
OUT_D=$(cb unit restore 1 "$SNAP_SOLO" --define --replace --start 2>&1)
job_ok "in-place restore with define" "$OUT_D"
check "the live tree is the snapshot's again" "$MAN_SOLO" "$(manifest "$SOLO")"
OLD=$(ls -d "$SOLO".cbackup-old-* 2>/dev/null | head -1)
check "the replaced data was moved aside, not deleted" changed "$(tail -n1 "$OLD/name.txt" 2>/dev/null)"
check "the container is running again" running "$(docker inspect -f '{{.State.Status}}' cbk-uat-solo 2>/dev/null)"
check "re-created from the recorded digest" "$(docker image inspect -f '{{.Id}}' "$IMG")" "$(docker inspect -f '{{.Image}}' cbk-uat-solo 2>/dev/null)"
leftovers "after the solo define"
rm -rf "$SOLO".cbackup-old-*

log "docker: define under a new name"
TR="$APPS/restored"
OUT_N=$(cb unit restore 1 "$SNAP_SOLO" --define --name cbk-uat-solo2 --target-root "$TR" --start 2>&1)
job_ok "new-name restore with define" "$OUT_N"
check "the copy's tree is the snapshot's" "$MAN_SOLO" "$(manifest "$TR$SOLO")"
check "the copy binds the restored path" "$TR$SOLO" "$(docker inspect -f '{{range .Mounts}}{{.Source}}{{end}}' cbk-uat-solo2 2>/dev/null)"
check "the original still binds its own" "$SOLO" "$(docker inspect -f '{{range .Mounts}}{{.Source}}{{end}}' cbk-uat-solo 2>/dev/null)"
docker rm -f cbk-uat-solo2 >/dev/null 2>&1
leftovers "after the solo new-name define"
}
test_solo

# ---- Docker: data in volumes (B188) ---------------------------------------

# Every file's path and hash under a volume's mount point, as one sum.
vol_sum() {
    local mp
    mp=$(docker volume inspect -f '{{.Mountpoint}}' "$1" 2>/dev/null) || { echo "no volume $1"; return; }
    (cd "$mp" && find . -printf '%p %y %m\n' | sort && find . -type f -exec sha256sum {} + | sort) | sha256sum
}
# The containers, running or not, that mount a volume.
vol_users() { docker ps -a --filter "volume=$1" --format '{{.Names}}' | sort | tr '\n' ' ' | sed 's/ $//'; }

test_vols() {
log "docker: cbk-uat-vols, a named and an anonymous volume"
OUT_B=$(cb unit backup 1 --provider docker cbk-uat-vols 2>&1)
job_ok "unit backup of a container with volumes" "$OUT_B"
contains "the volumes are in the pool's domain: crash consistency" '"consistency":"crash"' "$OUT_B"
SNAP_VOLS=$(snap_of "$OUT_B")
leftovers "after the volumes backup"
have_snap "the volumes backup" "$SNAP_VOLS" || return 0

log "docker: volumes, in place: the restored data is what the container mounts (B188)"
docker exec cbk-uat-vols sh -c 'echo changed > /named/f; echo changed > /anon/f; sync' >/dev/null 2>&1
docker stop -t 2 cbk-uat-vols >/dev/null 2>&1
OUT_D=$(cb unit restore 1 "$SNAP_VOLS" --define --replace --start 2>&1)
job_ok "in-place restore of a container with volumes" "$OUT_D"
check "the container runs again" running "$(docker inspect -f '{{.State.Status}}' cbk-uat-vols 2>/dev/null)"
check "it mounts the same named volume" "$VOLS_NAMED" "$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/named"}}{{.Name}}{{end}}{{end}}' cbk-uat-vols 2>/dev/null)"
check "it mounts the same anonymous volume, not a new one" "$VOLS_ANON" "$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/anon"}}{{.Name}}{{end}}{{end}}' cbk-uat-vols 2>/dev/null)"
check "the named volume holds the restored data" named-v1 "$(docker exec cbk-uat-vols cat /named/f 2>/dev/null)"
check "the anonymous volume holds the restored data" anon-v1 "$(docker exec cbk-uat-vols cat /anon/f 2>/dev/null)"
docker ps -aq --filter name=cbk-uat-vols.cbackup-old- | xargs -r docker rm >/dev/null 2>&1
rm -rf "$DOCKER_ROOT"/volumes/*/_data.cbackup-old-* "$CACHE"/system/docker/volumes/*/_data.cbackup-old-*
leftovers "after the volumes in-place define"

log "docker: volumes, a restore under a new name never touches the original's (B188)"
SUM_N=$(vol_sum "$VOLS_NAMED")
SUM_A=$(vol_sum "$VOLS_ANON")
TRV="$APPS/restored-vols"
OUT_N=$(cb unit restore 1 "$SNAP_VOLS" --define --name cbk-uat-vols2 --target-root "$TRV" --start 2>&1)
job_ok "new-name restore of a container with volumes" "$OUT_N"
check "the copy runs" running "$(docker inspect -f '{{.State.Status}}' cbk-uat-vols2 2>/dev/null)"
MOUNTS=$(docker inspect -f '{{range .Mounts}}{{.Type}}:{{.Name}}:{{.Source}}:{{.Destination}} {{end}}' cbk-uat-vols2 2>/dev/null)
lacks "the copy mounts no volume at all" "volume:" "$MOUNTS"
lacks "the copy does not mount the original's named volume" ":$VOLS_NAMED:" "$MOUNTS"
lacks "the copy does not mount the original's anonymous volume" ":$VOLS_ANON:" "$MOUNTS"
contains "the copy binds the named volume's restored copy" "bind::$TRV$DOCKER_ROOT/volumes/$VOLS_NAMED/_data:/named" "$MOUNTS"
contains "the copy binds the anonymous volume's restored copy" "bind::$TRV$DOCKER_ROOT/volumes/$VOLS_ANON/_data:/anon" "$MOUNTS"
check "the copy has the named volume's data" named-v1 "$(docker exec cbk-uat-vols2 cat /named/f 2>/dev/null)"
check "the copy has the anonymous volume's data" anon-v1 "$(docker exec cbk-uat-vols2 cat /anon/f 2>/dev/null)"
check "only the original mounts the named volume" cbk-uat-vols "$(vol_users "$VOLS_NAMED")"
check "only the original mounts the anonymous volume" cbk-uat-vols "$(vol_users "$VOLS_ANON")"
# The copy writes; the original's bytes do not move.
docker exec cbk-uat-vols2 sh -c 'echo copy-wrote > /named/f; echo copy-wrote > /anon/f; rm -f /named/r.bin; sync' >/dev/null 2>&1
check "the original's named volume is byte-identical" "$SUM_N" "$(vol_sum "$VOLS_NAMED")"
check "the original's anonymous volume is byte-identical" "$SUM_A" "$(vol_sum "$VOLS_ANON")"
check "the original still reads its own data" named-v1 "$(docker exec cbk-uat-vols cat /named/f 2>/dev/null)"
docker rm -f cbk-uat-vols2 >/dev/null 2>&1
leftovers "after the volumes new-name define"
}
test_vols

# ---- Docker: a host block device bound in (B188) --------------------------

test_blk() {
log "docker: cbk-uat-blk, the pool's loop device bound in"
mkdir -p "$APPS/cbk-uat-blk"
echo blk-data > "$APPS/cbk-uat-blk/f"
docker run -d --name cbk-uat-blk --network none -v "$APPS/cbk-uat-blk:/data" -v "$LOOPDEV:/disk" \
    -v /etc/localtime:/etc/localtime:ro "$IMG" \
    sh -c 'trap "exit 0" TERM; while :; do sleep 1; done' >/dev/null || { bad "cbk-uat-blk starts" started failed; return 0; }
OUT_B=$(cb unit backup 1 --provider docker cbk-uat-blk 2>&1)
job_ok "unit backup of a container with a block device bound" "$OUT_B"
SNAP_BLK=$(snap_of "$OUT_B")
have_snap "the block-device backup" "$SNAP_BLK" || { docker rm -f cbk-uat-blk >/dev/null 2>&1; return 0; }
TRB="$APPS/restored-blk"
OUT_N=$(cb unit restore 1 "$SNAP_BLK" --define --name cbk-uat-blk2 --target-root "$TRB" --start 2>&1)
contains "a copy with a block device bound is refused" '"state":"failed"' "$OUT_N"
contains "the refusal names the bind and why" "$LOOPDEV -> /disk: a block device" "$OUT_N"
check "no copy was created" "" "$(docker ps -aq --filter name=cbk-uat-blk2)"
check "nothing was written under the target root" no "$([ -e "$TRB" ] && echo yes || echo no)"
docker rm -f cbk-uat-blk >/dev/null 2>&1
leftovers "after the block-device refusal"

# B210: the device node is inside an include root, so it is data the backup
# captures and the copy's bind is rewritten under the target root. The
# define check runs before the restore writes anything, when that path does
# not exist yet; it must judge the original, or the restore writes the data
# and the node there and only the real define refuses.
log "docker: cbk-uat-blkin, a block device node inside an include root"
# Two binds that do not nest: a backup refuses items that overlap.
mkdir -p "$APPS/cbk-uat-blkin/data" "$APPS/cbk-uat-blkin/dev"
echo blkin-data > "$APPS/cbk-uat-blkin/data/f"
LOOPMM=$(stat -L -c '%t %T' "$LOOPDEV")
mknod "$APPS/cbk-uat-blkin/dev/disk" b $((16#${LOOPMM% *})) $((16#${LOOPMM#* }))
docker run -d --name cbk-uat-blkin --network none -v "$APPS/cbk-uat-blkin/data:/data" \
    -v "$APPS/cbk-uat-blkin/dev/disk:/disk" "$IMG" \
    sh -c 'trap "exit 0" TERM; while :; do sleep 1; done' >/dev/null || { bad "cbk-uat-blkin starts" started failed; return 0; }
OUT_B=$(cb unit backup 1 --provider docker cbk-uat-blkin 2>&1)
job_ok "unit backup of a container with a block device node in an include root" "$OUT_B"
SNAP_BLKIN=$(snap_of "$OUT_B")
if have_snap "the in-root block-device backup" "$SNAP_BLKIN"; then
    TRBI="$APPS/restored-blkin"
    OUT_N=$(cb unit restore 1 "$SNAP_BLKIN" --define --name cbk-uat-blkin2 --target-root "$TRBI" 2>&1)
    contains "a copy binding an in-root block device is refused" '"state":"failed"' "$OUT_N"
    contains "the refusal is the check's, before anything is written" "refused, nothing written" "$OUT_N"
    contains "the refusal names the bind and why" "$TRBI$APPS/cbk-uat-blkin/dev/disk -> /disk: a block device" "$OUT_N"
    check "no in-root copy was created" "" "$(docker ps -aq --filter name=cbk-uat-blkin2)"
    check "nothing was written under the in-root target root" no "$([ -e "$TRBI" ] && echo yes || echo no)"
    # The original gone from the host after the backup: the host cannot
    # say, and the snapshot's recorded type of the node decides.
    rm -f "$APPS/cbk-uat-blkin/dev/disk"
    TRBI2="$APPS/restored-blkin2"
    OUT_N=$(cb unit restore 1 "$SNAP_BLKIN" --define --name cbk-uat-blkin3 --target-root "$TRBI2" 2>&1)
    contains "with the original gone, the copy is still refused" '"state":"failed"' "$OUT_N"
    contains "by the check, from the snapshot's record" "refused, nothing written" "$OUT_N"
    contains "the refusal says the snapshot records a block device" "/disk: a block device, as the snapshot records it" "$OUT_N"
    check "no copy was created with the original gone" "" "$(docker ps -aq --filter name=cbk-uat-blkin3)"
    check "nothing was written under that target root" no "$([ -e "$TRBI2" ] && echo yes || echo no)"
fi
docker rm -f cbk-uat-blkin >/dev/null 2>&1
rm -f "$APPS/cbk-uat-blkin/dev/disk"
leftovers "after the in-root block-device refusal"
}
test_blk

# ---- Docker: a compose project -------------------------------------------

test_project() {
log "docker: compose project cbk-uat-proj"
MAN_APP=$(manifest "$PROJ/app")
MAN_DB=$(manifest "$PROJ/db")
COMPOSE_SUM=$(sha256sum < "$PROJ/compose.yaml")
OUT_B=$(cb unit backup 1 --provider docker compose:cbk-uat-proj 2>&1)
job_ok "unit backup of a compose project" "$OUT_B"
contains "one btrfs domain is one instant: crash consistency" '"consistency":"crash"' "$OUT_B"
SNAP_PROJ=$(snap_of "$OUT_B")
leftovers "after the project backup"
have_snap "the project backup" "$SNAP_PROJ" || return 0
check "the project's members are running, not paused" "running running" "$(docker inspect -f '{{.State.Status}}' cbk-uat-proj-app-1 cbk-uat-proj-db-1 | tr '\n' ' ' | sed 's/ $//')"

echo changed >> "$PROJ/db/table.csv"
rm -rf "$PROJ/app/sub"

R="$OUT/proj"
OUT_R=$(cb unit restore 1 "$SNAP_PROJ" "$R" 2>&1)
job_ok "files restore of the project snapshot" "$OUT_R"
check "the app service's tree restores byte for byte" "$MAN_APP" "$(manifest "$R$PROJ/app")"
check "the db service's tree restores byte for byte" "$MAN_DB" "$(manifest "$R$PROJ/db")"
check "the project's dump is the table as it was" "$(printf 'id,value\n1,alpha\n2,beta\n' | sha256sum)" "$(sha256sum < "$R/.cbackup-unit/hooks/proj-dump.dump" 2>/dev/null)"
contains "the post hook's log is stored" "post-hook-ran" "$(cat "$R/.cbackup-unit/hooks/proj-post.log" 2>/dev/null)"
check "the compose file is stored verbatim" "$COMPOSE_SUM" "$(sha256sum < "$R/.cbackup-unit/config/compose/compose.yaml" 2>/dev/null)"

log "docker: compose project in place"
docker exec cbk-uat-proj-db-1 sh -c 'echo changed > /anon/f; sync' >/dev/null 2>&1
docker compose -p cbk-uat-proj --project-directory "$PROJ" -f "$PROJ/compose.yaml" stop >/dev/null 2>&1
OUT_D=$(cb unit restore 1 "$SNAP_PROJ" --define --replace --start 2>&1)
job_ok "in-place restore of the project" "$OUT_D"
check "app is the snapshot's again" "$MAN_APP" "$(manifest "$PROJ/app")"
check "db is the snapshot's again" "$MAN_DB" "$(manifest "$PROJ/db")"
check "db's anonymous volume holds the restored data (B188)" anon-v1 "$(docker exec cbk-uat-proj-db-1 cat /anon/f 2>/dev/null)"
check "the project's services run again" "running running" "$(docker inspect -f '{{.State.Status}}' cbk-uat-proj-app-1 cbk-uat-proj-db-1 2>/dev/null | tr '\n' ' ' | sed 's/ $//')"
leftovers "after the project define"

log "docker: compose project under a new name never touches the original's (B188)"
SUM_P=$(vol_sum "$PROJ_VOL")
TRP="$APPS/restored-proj"
OUT_N=$(cb unit restore 1 "$SNAP_PROJ" --define --name cbk-uat-projcopy --target-root "$TRP" --start 2>&1)
job_ok "new-name restore of the project" "$OUT_N"
check "the copy's services run" "running running" "$(docker inspect -f '{{.State.Status}}' cbk-uat-projcopy-app cbk-uat-projcopy-db 2>/dev/null | tr '\n' ' ' | sed 's/ $//')"
OUTSIDE=$(docker inspect -f '{{range .Mounts}}{{.Source}}{{"\n"}}{{end}}' cbk-uat-projcopy-app cbk-uat-projcopy-db 2>/dev/null | grep -v '^$' | grep -cv "^$TRP/")
check "every mount of the copy is under its target root" 0 "$OUTSIDE"
check "the copy has the volume's data" vol-v1 "$(docker exec cbk-uat-projcopy-db cat /vol/f 2>/dev/null)"
check "only the original mounts the project's volume" cbk-uat-proj-db-1 "$(vol_users "$PROJ_VOL")"
docker exec cbk-uat-projcopy-db sh -c 'echo copy-wrote > /vol/f; echo copy-wrote > /data/table.csv; sync' >/dev/null 2>&1
check "the original's volume is byte-identical" "$SUM_P" "$(vol_sum "$PROJ_VOL")"
check "the original's bind is untouched" "$MAN_DB" "$(manifest "$PROJ/db")"
docker rm -f cbk-uat-projcopy-app cbk-uat-projcopy-db >/dev/null 2>&1
leftovers "after the project new-name define"
}
test_project

# ---- LXC -----------------------------------------------------------------

LXC_RUNNING=0
test_lxc() {
log "lxc: busybox container on the pool"
cat > "$WORK/lxc.conf" <<EOF
lxc.net.0.type = empty
lxc.apparmor.profile = unconfined
EOF
if lxc-create -P "$LXCP" -n cbk-uat-lxc -f "$WORK/lxc.conf" -t busybox >"$WORK/lxc-create.log" 2>&1; then
    head -c 524288 /dev/urandom > "$LXCP/cbk-uat-lxc/rootfs/payload.bin"
    MAN_LXC=$(manifest "$LXCP/cbk-uat-lxc/rootfs")
    OUT_B=$(cb unit backup 1 --provider lxc cbk-uat-lxc 2>&1)
    job_ok "unit backup of a stopped LXC container" "$OUT_B"
    contains "a stopped container is offline-consistent" '"consistency":"offline"' "$OUT_B"
    SNAP_LXC=$(snap_of "$OUT_B")
    leftovers "after the stopped LXC backup"
    if have_snap "the stopped LXC backup" "$SNAP_LXC"; then
        R="$OUT/lxc"
        job_ok "files restore of the LXC snapshot" "$(cb unit restore 1 "$SNAP_LXC" "$R" 2>&1)"
        check "the rootfs restores byte for byte" "$MAN_LXC" "$(manifest "$R$LXCP/cbk-uat-lxc/rootfs")"
        contains "the LXC config is stored" "lxc.rootfs.path" "$(cat "$R/.cbackup-unit/config/lxc/config" 2>/dev/null)"

        # B191: only Docker located a mount's snapshot domain, so an LXC
        # restore in place was always refused.
        log "lxc: in place, stopped"
        echo changed >> "$LXCP/cbk-uat-lxc/rootfs/payload.bin"
        OUT_I=$(cb unit restore 1 "$SNAP_LXC" --define --replace 2>&1)
        job_ok "in-place restore of the stopped LXC container" "$OUT_I"
        check "the rootfs is the snapshot's again" "$MAN_LXC" "$(manifest "$LXCP/cbk-uat-lxc/rootfs")"
        check "the replaced rootfs was moved aside" 1 "$(ls -d "$LXCP"/cbk-uat-lxc/rootfs.cbackup-old-* 2>/dev/null | wc -l)"
        check "it is left stopped, as it was" STOPPED "$(lxc-info -P "$LXCP" -n cbk-uat-lxc -s -H 2>/dev/null)"
        rm -rf "$LXCP"/cbk-uat-lxc/rootfs.cbackup-old-*
        leftovers "after the LXC in-place restore"
    fi

    if lxc-start -P "$LXCP" -n cbk-uat-lxc >"$WORK/lxc-start.log" 2>&1 &&
       lxc-wait -P "$LXCP" -n cbk-uat-lxc -s RUNNING -t 20 >/dev/null 2>&1; then
        LXC_RUNNING=1
        OUT_B=$(cb unit backup 1 --provider lxc cbk-uat-lxc 2>&1)
        job_ok "unit backup of a running LXC container" "$OUT_B"
        contains "frozen around the snapshot: crash consistency" '"consistency":"crash"' "$OUT_B"
        check "and thawed after" RUNNING "$(lxc-info -P "$LXCP" -n cbk-uat-lxc -s -H 2>/dev/null)"
        leftovers "after the running LXC backup"
        SNAP_LXC2=$(snap_of "$OUT_B")
        PAY_SUM=$(sha256sum < "$LXCP/cbk-uat-lxc/rootfs/payload.bin")

        # B189: a staging the helper refuses (here the base replaced by a
        # symlink) is refused before the unit is stopped.
        log "lxc: in place, refused before the stop"
        SB="$CACHE/system/.cbackup-restore"
        mkdir -p "$SB"
        mv "$SB" "$SB.held"
        ln -s "$WORK" "$SB"
        OUT_X=$(cb unit restore 1 "$SNAP_LXC2" --define --replace --start 2>&1)
        contains "a refused staging fails the restore" '"state":"failed"' "$OUT_X"
        contains "and says nothing was written" "nothing written" "$OUT_X"
        check "the container was never stopped" RUNNING "$(lxc-info -P "$LXCP" -n cbk-uat-lxc -s -H 2>/dev/null)"
        rm -f "$SB"
        mv "$SB.held" "$SB"

        log "lxc: in place, running, started again"
        echo changed >> "$LXCP/cbk-uat-lxc/rootfs/payload.bin"
        OUT_I=$(cb unit restore 1 "$SNAP_LXC2" --define --replace --start 2>&1)
        job_ok "in-place restore of the running LXC container" "$OUT_I"
        check "the payload is the snapshot's again" "$PAY_SUM" "$(sha256sum < "$LXCP/cbk-uat-lxc/rootfs/payload.bin")"
        lxc-wait -P "$LXCP" -n cbk-uat-lxc -s RUNNING -t 20 >/dev/null 2>&1
        check "and it runs again" RUNNING "$(lxc-info -P "$LXCP" -n cbk-uat-lxc -s -H 2>/dev/null)"
        rm -rf "$LXCP"/cbk-uat-lxc/rootfs.cbackup-old-*
        leftovers "after the running LXC in-place restore"
        lxc-stop -P "$LXCP" -n cbk-uat-lxc -k >/dev/null 2>&1
    else
        log "lxc: the container would not start here; running backup not exercised"
        sed 's/^/    /' "$WORK/lxc-start.log" | tail -20
    fi
    lxc-destroy -P "$LXCP" -n cbk-uat-lxc >/dev/null 2>&1
else
    bad "lxc-create with the busybox template" "created" "$(tail -5 "$WORK/lxc-create.log")"
fi
}
test_lxc

# ---- libvirt: one VM, offline then online --------------------------------

VMTYPE=qemu
test_vm() {
log "libvirt: cbk-uat-vm, 1 vCPU, ${VM_MEM_MB} MiB"
VMD="$DOMS/cbk-uat-vm"
mkdir -p "$VMD"
truncate -s 16M "$VMD/vda.img"
head -c 1048576 /dev/urandom | dd of="$VMD/vda.img" conv=notrunc status=none
head -c 65536 /dev/urandom | dd of="$VMD/vda.img" bs=1M seek=12 conv=notrunc status=none
qemu-img convert -q -f raw -O qcow2 "$VMD/vda.img" "$VMD/vdb.qcow2"
VDA_SUM=$(sha256sum < "$VMD/vda.img")
[ -c /dev/kvm ] && [ -w /dev/kvm ] && VMTYPE=kvm
cat > "$WORK/vm.xml" <<EOF
<domain type='$VMTYPE'>
  <name>cbk-uat-vm</name>
  <memory unit='MiB'>$VM_MEM_MB</memory>
  <vcpu>1</vcpu>
  <os><type arch='x86_64' machine='pc'>hvm</type></os>
  <devices>
    <emulator>/usr/bin/qemu-system-x86_64</emulator>
    <disk type='file' device='disk'><driver name='qemu' type='raw'/><source file='$VMD/vda.img'/><target dev='vda' bus='virtio'/></disk>
    <disk type='file' device='disk'><driver name='qemu' type='qcow2'/><source file='$VMD/vdb.qcow2'/><target dev='vdb' bus='virtio'/></disk>
    <graphics type='vnc' autoport='yes' listen='127.0.0.1'/>
  </devices>
</domain>
EOF
virsh -q -c "$VIRT_URI" define "$WORK/vm.xml" >/dev/null || die "cannot define cbk-uat-vm"

OUT_B=$(cb unit backup 1 --provider libvirt cbk-uat-vm 2>&1)
job_ok "offline VM backup (raw read in place, qcow2 through qemu-nbd)" "$OUT_B"
contains "a shut-off VM is offline-consistent" '"consistency":"offline"' "$OUT_B"
SNAP_VM=$(snap_of "$OUT_B")
leftovers "after the offline VM backup"
have_snap "the offline VM backup" "$SNAP_VM" || return 0
R="$OUT/vm"
job_ok "files restore of the VM snapshot" "$(cb unit restore 1 "$SNAP_VM" "$R" 2>&1)"
check "the raw disk restores byte for byte" "$VDA_SUM" "$(sha256sum < "$R/.cbackup-unit/disks/vda" 2>/dev/null)"
check "the qcow2 disk restores as its contents" "$VDA_SUM" "$(sha256sum < "$R/.cbackup-unit/disks/vdb" 2>/dev/null)"
contains "the domain XML is stored" "<name>cbk-uat-vm</name>" "$(cat "$R/.cbackup-unit/config/domain.xml" 2>/dev/null)"
BLK=$(stat -c %b "$R/.cbackup-unit/disks/vda" 2>/dev/null || echo 999999)
if [ "$((BLK * 512))" -lt $((4 * 1048576)) ]; then ok "the restored raw disk is sparse"; else bad "the restored raw disk is sparse" "< 4 MiB allocated" "$((BLK * 512))"; fi

log "libvirt: define under a new name"
OUT_N=$(cb unit restore 1 "$SNAP_VM" --define --name cbk-uat-vm2 --target-root "$DOMS/restored" 2>&1)
job_ok "VM restore with define under a new name" "$OUT_N"
X=$(virsh -q -c "$VIRT_URI" dumpxml cbk-uat-vm2 2>/dev/null)
contains "the new domain points at the restored image" "$DOMS/restored$VMD/vda.img" "$X"
check "its raw image is the original's bytes" "$VDA_SUM" "$(sha256sum < "$DOMS/restored$VMD/vda.img" 2>/dev/null)"
check "its qcow2 disk came back raw with the same contents" "$VDA_SUM" "$(sha256sum < "$DOMS/restored$VMD/vdb.qcow2" 2>/dev/null)"
virsh -q -c "$VIRT_URI" undefine cbk-uat-vm2 >/dev/null 2>&1

log "libvirt: online (pull-mode backup of the running VM)"
if virsh -q -c "$VIRT_URI" start cbk-uat-vm >"$WORK/vm-start.log" 2>&1; then
    check "the VM is running" running "$(virsh -q -c "$VIRT_URI" domstate cbk-uat-vm 2>/dev/null)"
    OUT_B=$(cb unit backup 1 --provider libvirt cbk-uat-vm 2>&1)
    job_ok "online VM backup through a pull-mode export" "$OUT_B"
    contains "no guest agent: crash consistency" '"consistency":"crash"' "$OUT_B"
    SNAP_VM2=$(snap_of "$OUT_B")
    check "the libvirt backup job is over" "" "$(virsh -q -c "$VIRT_URI" domjobinfo cbk-uat-vm 2>/dev/null | grep -i backup)"
    check "the VM still runs" running "$(virsh -q -c "$VIRT_URI" domstate cbk-uat-vm 2>/dev/null)"
    leftovers "after the online VM backup"
    check "no scratch file left" "" "$(ls -A "$CACHE/system/cbackup-scratch" 2>/dev/null)"
    virsh -q -c "$VIRT_URI" destroy cbk-uat-vm >/dev/null 2>&1
    have_snap "the online VM backup" "$SNAP_VM2" || return 0
    R="$OUT/vm2"
    job_ok "files restore of the online snapshot" "$(cb unit restore 1 "$SNAP_VM2" "$R" 2>&1)"
    check "the raw disk read through the export is byte for byte" "$VDA_SUM" "$(sha256sum < "$R/.cbackup-unit/disks/vda" 2>/dev/null)"
    check "the qcow2 disk read through the export is its contents" "$VDA_SUM" "$(sha256sum < "$R/.cbackup-unit/disks/vdb" 2>/dev/null)"
else
    bad "the test VM starts ($VMTYPE)" "running" "$(tail -5 "$WORK/vm-start.log")"
fi
virsh -q -c "$VIRT_URI" undefine cbk-uat-vm >/dev/null 2>&1
}
test_vm

# ---- B231: a btrfs subvolume below a container's mount --------------------
# A snapshot of the pool shows a nested subvolume as an empty directory. A
# mount with one below it must not be read from the snapshot (its contents
# would be silently missing): the helper flags it, the daemon reads it
# through the fallback, the job warns naming it, and the restore holds the
# subvolume's files.

test_b231_nested_subvolume() {
    local n="$APPS/cbk-uat-nest" out snap r="$OUT/b231" man
    log "B231: a container mount with a btrfs subvolume below it"
    mkdir -p "$n"
    echo top > "$n/top.txt"
    btrfs subvolume create "$n/inner" >/dev/null || { bad "B231: a nested subvolume is created" "created" "btrfs failed"; return 0; }
    head -c 65536 /dev/urandom > "$n/inner/data.bin"
    echo inner > "$n/inner/note.txt"
    man=$(manifest "$n")
    docker run -d --name cbk-uat-nest --network none -v "$n:/data" "$IMG" \
        sh -c 'trap "exit 0" TERM; while :; do sleep 1; done' >/dev/null || { bad "B231: the container starts" "started" "docker failed"; return 0; }
    out=$(cb unit backup 1 --provider docker cbk-uat-nest 2>&1)
    job_ok "B231: the unit backup" "$out"
    contains "B231: the job warns naming the mount and what is below it" "$n/inner" "$out"
    contains "B231: and says it was read live" "read live" "$out"
    snap=$(snap_of "$out")
    # The fixture's own subvolume goes first: snapshots_left counts every
    # subvolume below the pool's top level, it included.
    docker rm -f cbk-uat-nest >/dev/null 2>&1
    btrfs subvolume delete "$n/inner" >/dev/null 2>&1
    leftovers "after the B231 backup"
    if have_snap "B231" "$snap"; then
        cb unit restore 1 "$snap" "$r" >/dev/null 2>&1
        check "B231: the subvolume's files are in the snapshot, not an empty directory" "$man" "$(manifest "$r$n")"
    fi
    rm -r -- "$n"
}
test_b231_nested_subvolume

# ---- B186: the helper's container layout does not multiply host mounts ---
# A bind under an rshared bind propagates back into the host's peer group,
# so with the old documented layout (state_dir bound separately beside
# -v /mnt/cache:/mnt/cache:rshared) every helper start doubled the host's
# stacked mounts of the state dir: 16383 on FS1 after 14 starts. Here the
# pool is made shared, as Unraid's is, and a container with the documented
# layout is started and stopped repeatedly: the mount table must not move.
# Then the old layout on a scratch directory shows the doubling is real
# under this Docker, the helper refuses to start on the result, and the
# documented cleanup removes every copy.
# CBK_B186_LAYOUT=old runs the main loop on the old layout instead, which
# must fail it (run locally to prove the test catches the bug).

test_b186_layout() {
    local starts=5 i lay="$CACHE/system/cbk-b186-state" olds="$CACHE/system/cbk-b186-old"
    local layout="${CBK_B186_LAYOUT:-documented}" args t0 s0 out rc keep
    mi_at() { awk -v p="$1" '$5==p' /proc/self/mountinfo | wc -l; }
    mkdir -p "$lay" "$olds"
    mount --make-rshared "$CACHE"
    if [ "$layout" = old ]; then
        args="-v $lay:$lay -v $CACHE:$CACHE:rshared"
    else
        args="-v $CACHE:$CACHE:rslave"
    fi
    log "B186: $starts starts of a helper-layout container ($layout layout)"
    t0=$(wc -l </proc/self/mountinfo)
    s0=$(mi_at "$lay")
    for i in $(seq 1 $starts); do
        # shellcheck disable=SC2086
        docker run --rm --network none $args -v "$HRUN:$HRUN" "$IMG" true >/dev/null 2>&1 ||
            bad "B186 start $i runs" "exit 0" "docker run failed"
        check "B186 start $i ($layout): the state dir's host mounts unchanged" "$s0" "$(mi_at "$lay")"
        check "B186 start $i ($layout): the host mount table unchanged" "$t0" "$(wc -l </proc/self/mountinfo)"
    done

    log "B186: the old layout on a scratch state dir, the refusal and the cleanup"
    for i in 1 2 3; do
        docker run --rm --network none -v "$olds:$olds" -v "$CACHE:$CACHE:rshared" "$IMG" true >/dev/null 2>&1
    done
    check "B186: the old layout doubles at each start (2^3-1 after 3)" 7 "$(mi_at "$olds")"
    sed -e "s|^state_dir = .*|state_dir = $olds|" "$WORK/helper.ini" > "$WORK/helper-b186.ini"
    chmod 0600 "$WORK/helper-b186.ini"
    out=$("$SRC/build/bins/c-backup-helper" --policy "$WORK/helper-b186.ini" 2>&1); rc=$?
    check "B186: the helper refuses to start on stacked mounts" 1 "$rc"
    contains "B186: and says how many and what to do" "$olds is mounted 7 times" "$out"
    check "B186: and adds none" 7 "$(mi_at "$olds")"
    # The cleanup as docs/helper.md gives it (the scratch dir is no dataset).
    keep=0
    while [ "$(mi_at "$olds")" -gt "$keep" ]; do umount -l "$olds" || break; done
    check "B186: the documented cleanup removes every copy" 0 "$(mi_at "$olds")"
    mount --make-rprivate "$CACHE"
    rmdir "$lay" "$olds" 2>/dev/null
}
test_b186_layout

# ---- B229: what a btrfs snapshot keeps of a file ---------------------------
# A snapshot read is recorded with the record root's live device in the
# hard-link identity and the file cache key, which only gives the trees and
# cache rows of a live read if the snapshot keeps every inode number, size,
# mtime, ctime and link count, under a device number of its own. Checked
# here on a real read-only snapshot of the pool's top level, the kind the
# helper takes; test_snapshot_read proves the rest over a stand-in. ZFS
# keeps them too (object numbers are inode numbers), checked in UAT.

test_b229_snapshot_identity() {
    local d="$CACHE/system/cbk-b229" sv="$CACHE/system/cbk-b229-snap" live snap
    log "B229: a read-only btrfs snapshot keeps inode numbers and times"
    mkdir -p "$d/sub"
    head -c 65536 /dev/urandom > "$d/a.bin"
    echo text > "$d/sub/b.txt"
    ln "$d/a.bin" "$d/sub/a-link"
    sleep 1   # so a ctime the snapshot rewrote could not equal the original
    btrfs subvolume snapshot -r "$CACHE" "$sv" >/dev/null || { bad "B229: a snapshot is taken" "taken" "btrfs failed"; return 0; }
    live=$(cd "$d" && find . -printf '%P %i %s %T@ %C@ %n\n' | sort)
    snap=$(cd "$sv/system/cbk-b229" && find . -printf '%P %i %s %T@ %C@ %n\n' | sort)
    check "B229: every inode number, size, mtime, ctime and link count is kept" "$live" "$snap"
    [ "$(stat -c %d "$d/a.bin")" != "$(stat -c %d "$sv/system/cbk-b229/a.bin")" ] &&
        ok "B229: under a device number of its own" ||
        bad "B229: under a device number of its own" "different devices" "the same device"
    btrfs subvolume delete "$sv" >/dev/null
    rm -r -- "$d"
}
test_b229_snapshot_identity

# ---- the privilege drop and the helper suites, as root -------------------
# The CI job containers are root with Docker's default capabilities, which
# hold no CAP_DAC_READ_SEARCH, so there test_root_drop can only check that
# privs_drop refuses and changes nothing. This sibling is privileged: the
# real drop to uid 990, and the helper suites' own root paths (hf_unroot,
# the e2e's uid-990 daemon and socket group), run here for real.

suite_root() {   # <label> <logfile> <command...>
    local label=$1 logf=$2 rc
    shift 2
    (cd "$SRC" && C_BACKUP_TEST_DIR="$TSCR" "$@") >"$logf" 2>&1
    rc=$?
    check "$label passes as root" 0 "$rc"
    [ "$rc" -eq 0 ] || tail -30 "$logf"
}

log "suites as root: test_privs, test_helper, test_helper_remote, test_snapshot_read, test_helper_e2e.sh"
TSCR="$WORK/test-scratch"
mkdir -p "$TSCR"
if make -s -C "$SRC" CUTILS_DIR="$CUTILS" MARCH=x86-64-v2 MTUNE=generic C_BACKUP_TEST_DIR="$TSCR" \
        -j"$JOBS" build/bins/test_privs build/bins/test_helper build/bins/test_helper_remote \
        build/bins/test_snapshot_read \
        >"$WORK/build-suites.log" 2>&1; then
    suite_root test_privs "$WORK/test_privs.log" build/bins/test_privs
    contains "test_root_drop ran the real drop" "[       OK ] test_root_drop" "$(cat "$WORK/test_privs.log")"
    check "and not only the refusal" 0 "$(grep -c 'the drop is not' "$WORK/test_privs.log")"
    suite_root test_helper "$WORK/test_helper.log" build/bins/test_helper
    suite_root test_helper_remote "$WORK/test_helper_remote.log" build/bins/test_helper_remote
    suite_root test_snapshot_read "$WORK/test_snapshot_read.log" build/bins/test_snapshot_read
    check "test_snapshot_read ran its overlay cases" 0 "$(grep -c 'the overlay cases skip' "$WORK/test_snapshot_read.log")"
    suite_root test_helper_e2e.sh "$WORK/test_helper_e2e.log" \
        tests/test_helper_e2e.sh build/bins/c-backupd build/bins/c-backup build/bins/c-backup-helper
else
    tail -40 "$WORK/build-suites.log"
    bad "the root suites build" "built" "make failed"
fi

# ---- the end -------------------------------------------------------------

if [ "${CBK_SANITIZE:-0}" = 1 ]; then
    log "sanitizer reports"
    reports=$(ls -A "$SAN_DIR" 2>/dev/null)
    check "no ASan or UBSan report from the daemon, the CLI or the helper" "" "$reports"
    for f in $reports; do echo "---- $f"; head -60 "$SAN_DIR/$f"; done
    # A report that could not be written, or one a program printed itself,
    # is in its log instead.
    check "no sanitizer finding in the daemon's or the helper's log" 0 \
        "$(cat "$WORK/daemon.log" "$WORK/helper.log" 2>/dev/null |
           grep -cE 'AddressSanitizer|UndefinedBehaviorSanitizer|runtime error:|Can.t open file: .*/sanitizer/')"
fi

log "final leftovers"
check "the daemon is still running" 0 "$(kill -0 "$DAEMON_PID" 2>/dev/null; echo $?)"
check "the daemon logged no write outside the write lock" 0 "$(grep -c 'db_txn guard:' "$WORK/daemon.log")"
leftovers "at the end"
check "no cbk-uat domain left" "" "$(virsh -q -c "$VIRT_URI" list --all --name 2>/dev/null | grep '^cbk-uat-')"
check "the daemon reports no hold open" 0 "$(cb unit providers | jq '[.. | .holds_open? // empty] | add // 0')"

log "summary: $PASS passed, $FAIL failed, $XFAIL known bug(s), LXC running=$LXC_RUNNING, VM type $VMTYPE, $(( $(date +%s) - T0 ))s"
[ "$FAIL" -eq 0 ]
