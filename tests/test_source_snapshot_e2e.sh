#!/bin/bash
#
# Source snapshot mode end to end (docs/db-detection.md 9.4): c-backupd and
# the real c-backup-helper over the stand-in zfs and btrfs (tests/fake_host),
# file sources opted in with `sources set --snapshot` / `--snapshot-warn`.
#
#   - strict sources are read from the helper's snapshots and recorded at
#     their own paths: what is restored is the snapshot's instant, excludes
#     name the live paths, and nothing of .zfs or the snapshot directory is
#     recorded; the capture is gone afterwards
#   - a strict source that cannot be snapshotted fails the backup before
#     anything is read, naming the path and the reason
#   - a warn source is read live, and the job, its result and the journal
#     say so, naming the path and the reason
#   - a cancelled backup and a helper that is away: nothing left held, and
#     strict still fails rather than reading live
#
# The stand-ins copy rather than snapshot, so inode numbers change between
# captures; that the trees and file cache match a live read is proven over a
# real snapshot (test_snapshot_read, and real-engines on btrfs).

set -u

DAEMON="${1:-build/bins/c-backupd}"
CLI="${2:-build/bins/c-backup}"
HELPER="${3:-build/bins/c-backup-helper}"
export C_BACKUP_TXN_GUARD=abort
TEST_ROOT="${C_BACKUP_TEST_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/c-backup-test}"
mkdir -p "$TEST_ROOT"
if [ "$(id -u)" = 0 ]; then
    d="$TEST_ROOT"
    while [ -n "$d" ] && [ "$d" != / ]; do
        [ -d "$d" ] && chmod o+x "$d"
        d="${d%/*}"
    done
fi
WORK="$(mktemp -d "$TEST_ROOT/c_backup_srcsnap_e2e.XXXXXX")"
PORTBASE=$((20000 + ($$ % 15000) * 3))
port_busy() { ss -Htan "( sport = :$1 )" 2>/dev/null | grep -q .; }
while port_busy "$PORTBASE"; do
    PORTBASE=$((PORTBASE + 3)); [ "$PORTBASE" -gt 64990 ] && PORTBASE=20000
done
SOCK="$WORK/c.sock"
HSOCK="$WORK/h.sock"
PASS=0
FAIL=0

cleanup() {
    [ -n "${DAEMON_PID:-}" ] && kill "$DAEMON_PID" 2>/dev/null
    wait "${DAEMON_PID:-}" 2>/dev/null
    [ -n "${HELPER_PID:-}" ] && kill "$HELPER_PID" 2>/dev/null
    wait "${HELPER_PID:-}" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

ok()  { PASS=$((PASS+1)); printf '  ok   %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; }
check()    { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "$2" "$3"; fi; }
contains() { case "$3" in *"$2"*) ok "$1" ;; *) bad "$1" "contains '$2'" "$3" ;; esac; }
lacks()    { case "$3" in *"$2"*) bad "$1" "no '$2'" "$3" ;; *) ok "$1" ;; esac; }
cb() { "$CLI" --socket "$SOCK" "$@"; }
snap_of() { printf '%s' "$1" | sed -n 's/.*"snapshot_id":\([0-9]*\).*/\1/p' | head -1; }
job_of() { printf '%s' "$1" | grep -oE '"(job_id|id)":[0-9]+' | head -1 | grep -oE '[0-9]+$'; }

FH="$PWD/tests/fake_host"
FSTATE="$WORK/fstate"
DS="$WORK/zfs/data"            # dataset tank/data
POOL="$WORK/pool"              # a btrfs pool root
SNAPDIR="$POOL/system/.cbackup-snapshots"
S1="$DS/app"                   # on ZFS
S2="$POOL/share"               # on btrfs
S3="$WORK/plain"               # on no snapshot domain
mkdir -p "$FSTATE" "$S1/cache" "$S1/sub" "$S2" "$S3" "$POOL/system"
echo v1 > "$S1/db.sqlite"
echo conf > "$S1/conf"
echo junk > "$S1/cache/junk"
echo linked > "$S1/linked"
ln "$S1/linked" "$S1/sub/linked2"
echo doc > "$S2/doc"
echo plain > "$S3/x"
printf 'tank/data\t%s\n' "$DS" > "$FSTATE/datasets"
echo "20 1 0:20 / / rw,relatime - ext4 /dev/root rw" > "$WORK/mountinfo"

if [ "$(id -u)" = 0 ]; then
    DUID=990; DGID=990
    AS_DAEMON=(setpriv --reuid="$DUID" --regid="$DUID" --clear-groups --)
else
    DUID=$(id -u); DGID=$(id -g)
    AS_DAEMON=()
fi
cat > "$WORK/policy.ini" <<EOF
[helper]
daemon_uid = $DUID
daemon_gid = $DGID
daemon_id = 0badf00d
capture_max_s = 3600
state_dir = $WORK/hstate
socket = $HSOCK

[snapshot]
zfs_domains = tank/data
btrfs_domains = $POOL
btrfs_snapshot_dir = $SNAPDIR

[host]
nsenter = 0
host_root =
zfs = $FH/zfs
btrfs = $FH/btrfs
realpath = /usr/bin/realpath
mountinfo = $WORK/mountinfo
EOF
chmod 0600 "$WORK/policy.ini"
cat > "$WORK/config.yaml" <<EOF
db:
  path: $WORK/state.db
http:
  socket: $SOCK
  port: $PORTBASE
  bind: 127.0.0.1
log:
  level: info
security:
  key_dir: $WORK/keys
units:
  helper_socket: $HSOCK
EOF
[ "$(id -u)" = 0 ] && chown -R "$DUID" "$WORK" && chown 0 "$WORK/policy.ini"

start_helper() {
    FAKE_HOST_STATE="$FSTATE" "$HELPER" --policy "$WORK/policy.ini" >> "$WORK/helper.log" 2>&1 &
    HELPER_PID=$!
    for _ in $(seq 1 50); do [ -S "$HSOCK" ] && break; sleep 0.1; done
    for _ in $(seq 1 400); do
        cb unit providers 2>/dev/null | grep -q '"path_snapshots":true' && return 0
        sleep 0.1
    done
    echo "the helper did not connect"; tail -20 "$WORK/helper.log"; exit 1
}
leftovers() {
    check "$1: no ZFS snapshot left" "" "$(cat "$FSTATE/snaps" 2>/dev/null)"
    check "$1: no btrfs snapshot left" "" "$(ls -A "$SNAPDIR" 2>/dev/null)"
    check "$1: nothing journalled in the helper" "[]" "$(tr -d ' \n' < "$WORK/hstate/holds.json" 2>/dev/null)"
}

C_BACKUP_CONFIG_PATH="$WORK/config.yaml" "${AS_DAEMON[@]}" "$DAEMON" > "$WORK/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
[ -S "$SOCK" ] || { echo "daemon did not start"; cat "$WORK/daemon.log"; exit 1; }
start_helper
contains "repo add" '"id":1' "$(cb repo add "$WORK/repo" main)"

echo "configuration"
OUT=$(cb sources set 1 "$S1" "$S2" --exclude "$S1/cache" --snapshot "$S1" --snapshot "$S2")
contains "sources set takes two sources in snapshot mode" '"snapshot":2' "$OUT"
OUT=$(cb sources list 1)
contains "sources list shows the ZFS one strict" "{\"path\":\"$S1\",\"mode\":\"strict\"}" "$OUT"
contains "and the btrfs one" "{\"path\":\"$S2\",\"mode\":\"strict\"}" "$OUT"
OUT=$(cb sources set 1 "$S1" --snapshot "$S3" 2>&1); RC=$?
check    "snapshot mode for a path that is not a source is refused" 1 "$RC"
contains "and says why" "is not one" "$OUT"
contains "and the configuration is unchanged" "{\"path\":\"$S2\",\"mode\":\"strict\"}" "$(cb sources list 1)"
OUT=$(cb sources set 1 "$S1" "$S2" --exclude "$S1/cache")
contains "a set naming no mode keeps the modes of the sources that stay" '"snapshot":2' "$OUT"
# The route's own checks, as a client other than the CLI meets them.
put_src() { curl -s -X PUT --unix-socket "$SOCK" -H 'Content-Type: application/json' --data "$1" "http://localhost/repos/1/sources"; }
contains "an unknown mode is refused" "a snapshot mode is" \
    "$(put_src "{\"sources\":[\"$S1\"],\"snapshot\":[{\"path\":\"$S1\",\"mode\":\"on\"}]}")"
contains "a source listed twice is refused" "listed twice" \
    "$(put_src "{\"sources\":[\"$S1\"],\"snapshot\":[{\"path\":\"$S1\"},{\"path\":\"$S1\"}]}")"
contains "snapshot must be a list" "snapshot must be a list" \
    "$(put_src "{\"sources\":[\"$S1\"],\"snapshot\":\"$S1\"}")"
contains "a refused request changed nothing" "{\"path\":\"$S2\",\"mode\":\"strict\"}" "$(cb sources list 1)"
OUT=$(put_src "{\"sources\":[\"$S1\",\"$S2\"],\"snapshot\":[{\"path\":\"$S1\"}]}")
contains "a mode left out is strict, and a source left out is off" "{\"path\":\"$S1\",\"mode\":\"strict\"}]" "$(cb sources list 1)"
lacks    "so the other is no longer in snapshot mode" "{\"path\":\"$S2\"" "$(cb sources list 1)"
cb sources set 1 "$S1" "$S2" --exclude "$S1/cache" --snapshot "$S1" --snapshot "$S2" > /dev/null

echo "strict: read from the snapshots, recorded at the sources' paths"
echo 3 > "$FSTATE/snapshot.sleep"
cb backup 1 > "$WORK/b1.out" 2>&1 &
BP=$!
for _ in $(seq 1 100); do [ -f "$FSTATE/snapshot.taken" ] && break; sleep 0.1; done
echo v2 > "$S1/db.sqlite"         # after the instant, before the read
wait "$BP"; RC=$?
rm -f "$FSTATE/snapshot.sleep" "$FSTATE/snapshot.taken"
OUT=$(cat "$WORK/b1.out")
check    "the backup succeeds" 0 "$RC"
contains "the job result names the ZFS snapshot read" "\"$S1\":\"zfs\"" "$OUT"
contains "and the btrfs one" "\"$S2\":\"btrfs\"" "$OUT"
contains "and no source read live" '"live_sources":[]' "$OUT"
SNAP=$(snap_of "$OUT")
leftovers "after a strict backup"
cb restore 1 "$SNAP" "$WORK/out1" > /dev/null
check "the file holds what it held at the snapshot's instant" v1 "$(cat "$WORK/out1$S1/db.sqlite" 2>/dev/null)"
check "the btrfs source is recorded at its own path" doc "$(cat "$WORK/out1$S2/doc" 2>/dev/null)"
check "an exclude written for the live path still excludes" no "$([ -e "$WORK/out1$S1/cache" ] && echo yes || echo no)"
check "nothing under .zfs is recorded" no "$([ -e "$WORK/out1$DS/.zfs" ] && echo yes || echo no)"
check "nor the btrfs snapshot directory" no "$([ -e "$SNAPDIR" ] && [ -e "$WORK/out1$POOL/system" ] && echo yes || echo no)"
check "hard links stay one file" "$(stat -c %i "$WORK/out1$S1/linked")" "$(stat -c %i "$WORK/out1$S1/sub/linked2" 2>/dev/null)"
lacks "a clean snapshot read journals no warning" "snapshot mode" "$(cb journal 1 --result warning)"

echo "strict: a source that cannot be snapshotted fails the backup"
cb sources set 1 "$S1" "$S3" --snapshot "$S1" --snapshot "$S3" > /dev/null
NSNAP=$(cb snapshots 1 | grep -o '"id":' | wc -l)
OUT=$(cb backup 1 2>&1); RC=$?
check    "the backup fails" 1 "$RC"
contains "naming the path" "$S3: " "$OUT"
contains "and the reason" "no ZFS dataset or btrfs pool" "$OUT"
contains "and that nothing was backed up" "nothing was backed up" "$OUT"
check    "no snapshot was written" "$NSNAP" "$(cb snapshots 1 | grep -o '"id":' | wc -l)"
leftovers "after a strict failure"
contains "the journal records the failure with the path" "$S3" "$(cb journal 1 --result failed)"

echo "warn: read live, and said"
cb sources set 1 "$S1" "$S3" --snapshot "$S1" --snapshot-warn "$S3" > /dev/null
contains "sources list shows warn mode" "{\"path\":\"$S3\",\"mode\":\"warn\"}" "$(cb sources list 1)"
OUT=$(cb backup 1 2>&1); RC=$?
check    "the backup completes" 0 "$RC"
contains "the result lists the source read live" "\"live_sources\":[{\"path\":\"$S3\",\"reason\":\"" "$OUT"
contains "with the reason" "no ZFS dataset or btrfs pool" "$OUT"
contains "and the snapshot-read one" "\"$S1\":\"zfs\"" "$OUT"
contains "the job carries a warning naming the path" "$S3: " "$(cb jobs "$(job_of "$OUT")")"
contains "the journal records it as a warning" "$S3" "$(cb journal 1 --result warning)"
SNAP=$(snap_of "$OUT")
cb restore 1 "$SNAP" "$WORK/out2" > /dev/null
check "the live source is in the snapshot" plain "$(cat "$WORK/out2$S3/x" 2>/dev/null)"
leftovers "after a warn backup"

echo "a cancelled backup releases its capture"
cb sources set 1 "$S1" --snapshot "$S1" > /dev/null
echo 3 > "$FSTATE/snapshot.sleep"
OUT=$(cb backup 1 --no-wait)
J=$(job_of "$OUT")
for _ in $(seq 1 100); do [ -f "$FSTATE/snapshot.taken" ] && break; sleep 0.1; done
cb jobs cancel "$J" > /dev/null
for _ in $(seq 1 150); do
    cb jobs "$J" | grep -Eq '"state":"(cancelled|completed|failed)"' && break
    sleep 0.1
done
rm -f "$FSTATE/snapshot.sleep" "$FSTATE/snapshot.taken"
contains "the job ends cancelled" '"state":"cancelled"' "$(cb jobs "$J")"
leftovers "after a cancel"

echo "the helper away"
kill "$HELPER_PID"; wait "$HELPER_PID" 2>/dev/null; HELPER_PID=
OUT=$(cb backup 1 2>&1); RC=$?
check    "a strict source fails without its helper" 1 "$RC"
contains "naming the path and the helper" "$S1: the helper took no snapshot" "$OUT"
cb sources set 1 "$S1" --snapshot-warn "$S1" > /dev/null
OUT=$(cb backup 1 2>&1); RC=$?
check    "a warn source is read live without its helper" 0 "$RC"
contains "and the result says why" "the helper took no snapshot" "$OUT"
check    "and holds what is live now" v2 "$(cb restore 1 "$(snap_of "$OUT")" "$WORK/out3" >/dev/null; cat "$WORK/out3$S1/db.sqlite" 2>/dev/null)"

echo "the helper back: its start-up sweep finds nothing to do"
start_helper
leftovers "after the helper restarts"

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
