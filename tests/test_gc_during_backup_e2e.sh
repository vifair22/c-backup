#!/bin/bash
#
# gc cannot run while a backup of the same repository is storing, file
# workers included: the job manager holds the repository's write lock for a
# backup's whole run, gc asks for the same lock, and a job that cannot get
# its lock is deferred, never run beside the holder (B123). So a power cut
# "during gc during a backup" is not a state the daemon can reach, and
# test_powercut_fault.c has no scenario for it. This is the proof: a real
# daemon, a backup slowed (bench/slow_dev.c paces source opens) so it is
# still storing on several file workers when gc is asked for, and gc must
# stay queued until the backup has ended, then run.
#
#   tests/test_gc_during_backup_e2e.sh [c-backupd] [c-backup] [slow_dev.so]
set -u
DAEMON="$(realpath "${1:-build/bins/c-backupd}")"
CLI="$(realpath "${2:-build/bins/c-backup}")"
SHIM="$(realpath "${3:-build/bins/slow_dev.so}")"
export C_BACKUP_TXN_GUARD=abort
TEST_ROOT="${C_BACKUP_TEST_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/c-backup-test}"
mkdir -p "$TEST_ROOT"
WORK="$(mktemp -d "$TEST_ROOT/c_backup_gc_during_backup.XXXXXX")"
SOCK="$WORK/c.sock"
PASS=0
FAIL=0

cleanup() {
    [ -n "${DAEMON_PID:-}" ] && kill "$DAEMON_PID" 2>/dev/null
    wait "${DAEMON_PID:-}" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

ok()  { PASS=$((PASS+1)); printf '  ok   %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; }
check()    { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "$2" "$3"; fi; }
contains() { case "$3" in *"$2"*) ok "$1" ;; *) bad "$1" "contains '$2'" "$3" ;; esac; }
cb() { "$CLI" --socket "$SOCK" "$@"; }
field() { printf '%s' "$1" | sed -n "s/.*\"$2\":\\([0-9]*\\).*/\\1/p" | head -1; }
state() { cb jobs "$1" 2>/dev/null | sed -n 's/.*"state":"\([a-z]*\)".*/\1/p' | head -1; }

# Four 12 MiB files for the file workers (48 MiB: three of them) and 60
# small files, every open paced at 120 ms: the store phase lasts seconds.
mkdir -p "$WORK/src/big" "$WORK/src/small" "$WORK/state" "$WORK/repo"
for i in 1 2 3 4; do head -c $((12 * 1024 * 1024)) /dev/urandom > "$WORK/src/big/f$i"; done
for i in $(seq 1 60); do echo "small $i $RANDOM" > "$WORK/src/small/s$i"; done
cat > "$WORK/c.yaml" <<EOF
db:
  path: $WORK/state/state.db
http:
  socket: $SOCK
  port: 0
log:
  level: info
security:
  key_dir: $WORK/keys
EOF
LD_PRELOAD="$SHIM" SLOWDEV_DIRS="$WORK/nothing" SLOWDEV_SRC_DIR="$WORK/src" \
SLOWDEV_SRC_OPEN_MS=120 C_BACKUP_CONFIG_PATH="$WORK/c.yaml" "$DAEMON" > "$WORK/d.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 100); do [ -S "$SOCK" ] && break; sleep 0.1; done
[ -S "$SOCK" ] || { echo "daemon did not start"; cat "$WORK/d.log"; exit 1; }

echo "gc asked for while a backup stores on file workers"
contains "repo add" '"id":1' "$(cb repo add "$WORK/repo" r)"
B=$(field "$(cb backup 1 "$WORK/src" --no-wait)" job_id)
for _ in $(seq 200); do [ "$(state "$B")" = running ] && break; sleep 0.05; done
check "the backup is running" "running" "$(state "$B")"
G=$(field "$(cb gc 1 --no-wait)" job_id)
[ -n "$G" ] || G=0
check "gc is accepted" "yes" "$([ "$G" -gt 0 ] && echo yes || echo no)"

# While the backup runs, gc never starts.
overlap=0 samples=0
while [ "$(state "$B")" = running ]; do
    s=$(state "$G")
    [ "$s" = queued ] || { [ "$(state "$B")" = running ] && overlap=1; }
    samples=$((samples+1))
    sleep 0.05
done
check "gc stayed queued the whole time the backup ran" "0" "$overlap"
check "and that was long enough to look" "yes" "$([ "$samples" -ge 10 ] && echo yes || echo "no: $samples")"
contains "the backup ran file workers" "large files stored at once" \
         "$(grep -E 'up to [2-9] large files stored at once' "$WORK/d.log")"
# Held back behind the running backup, never dispatched and refused.
check "gc was never refused as busy" "0" "$(grep -c 'is busy' "$WORK/d.log")"
check "the backup completed" "completed" "$(state "$B")"

for _ in $(seq 400); do case "$(state "$G")" in completed|failed) break ;; esac; sleep 0.05; done
check "gc then ran and completed" "completed" "$(state "$G")"
BE=$(field "$(cb jobs "$B")" ended)
GS=$(field "$(cb jobs "$G")" started)
check "gc started after the backup ended" "yes" "$([ -n "$GS" ] && [ -n "$BE" ] && [ "$GS" -ge "$BE" ] && echo yes || echo "no: gc started $GS, backup ended $BE")"
V=$(cb verify 1 --level deep 2>&1)
contains "the repository verifies" '"completed":true' "$V"
contains "with nothing missing" '"missing":0' "$V"

echo "$PASS passed, $FAIL failed"
[ "$FAIL" = 0 ]
