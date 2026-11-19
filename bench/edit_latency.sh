#!/bin/bash
#
# Edit latency during a backup: PUT /repos/<id>/sources timed while a first
# backup runs, against a daemon on a modelled slow device (bench/slow_dev.c).
#
# What it measures is the product promise of Manual 6.3, that a metadata
# write during a backup answers well under a second, on the path that broke
# it in M9 and M10: a backup holding its write transaction across pack
# writeback (B232), a checkpoint round syncing a backlog under the write lock
# (B233), and request reads queued on the shared connection behind a bulk
# COMMIT (B222). No real sync reaches the disk and the source tree is small;
# the shim paces source opens so the backup still lasts long enough to sample.
#
#   bench/edit_latency.sh [c-backupd] [slow_dev.so]
#
# Environment (defaults in brackets):
#   FILES [6000] small files, SIZE_MIN/SIZE_MAX [4096/16384] bytes, random
#   OPEN_MS [20]         per source open, so the store phase runs ~FILES*OPEN_MS/4
#   BW_MBS [6] LAT_MS [20] FLOCK [0]    the device model (slow_dev.c); 6 MiB/s
#                        keeps it busy with this workload, as FS2 was
#   WAL_PAGES [1000]     db.wal_autocheckpoint (the hard cap is 16 x this)
#   INTERVAL [0.2]       seconds between edits
#   BOUND_MS             exit 1 when p99 is above this
#   KEEP                 keep the scratch directory
#   OUT                  also write the samples, metrics and device report here
#
# Prints one summary line, "edit-latency n=.. p50=.. p90=.. p99=.. max=.. ms",
# then the daemon's lock and checkpoint metrics and the device report.

set -u

BIN="${1:-build/bins/c-backupd}"
SHIM="${2:-build/bins/slow_dev.so}"
FILES="${FILES:-6000}"
SIZE_MIN="${SIZE_MIN:-4096}"
SIZE_MAX="${SIZE_MAX:-16384}"
OPEN_MS="${OPEN_MS:-20}"
BW_MBS="${BW_MBS:-6}"
LAT_MS="${LAT_MS:-20}"
FLOCK="${FLOCK:-0}"
WAL_PAGES="${WAL_PAGES:-1000}"
INTERVAL="${INTERVAL:-0.2}"
BOUND_MS="${BOUND_MS:-}"

[ -x "$BIN" ] || { echo "edit_latency: no daemon at $BIN" >&2; exit 2; }
[ -f "$SHIM" ] || { echo "edit_latency: no shim at $SHIM (make $SHIM)" >&2; exit 2; }
SHIM="$(cd "$(dirname "$SHIM")" && pwd)/$(basename "$SHIM")"

TEST_ROOT="${C_BACKUP_TEST_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/c-backup-test}"
mkdir -p "$TEST_ROOT"
WORK="$(mktemp -d "$TEST_ROOT/edit_latency.XXXXXX")"
SOCK="$WORK/c.sock"

cleanup() {
    if [ -n "${DAEMON_PID:-}" ]; then
        kill "$DAEMON_PID" 2>/dev/null
        wait "$DAEMON_PID" 2>/dev/null
    fi
    [ -n "${KEEP:-}" ] && { echo "kept $WORK"; return; }
    rm -rf "$WORK"
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

# Random content so nothing deduplicates or compresses: every file is an
# append to a pack, which is the path B232 is on. Seeded, so a run is the
# same tree every time.
RANDOM=20261118
for ((i = 0; i < FILES; i++)); do
    d="$WORK/src/d$(printf '%03d' $((i % 100)))"
    [ -d "$d" ] || mkdir -p "$d"
    head -c $((SIZE_MIN + (RANDOM * 32768 + RANDOM) % (SIZE_MAX - SIZE_MIN + 1))) /dev/urandom \
        > "$d/f$(printf '%06d' "$i")" || exit 2
done

mkdir -p "$WORK/state" "$WORK/repo"
cat > "$WORK/config.yaml" <<EOF
db:
  path: $WORK/state/state.db
  wal_autocheckpoint: $WAL_PAGES
http:
  socket: $SOCK
  port: 0
log:
  level: info
EOF
chmod 600 "$WORK/config.yaml"

LD_PRELOAD="$SHIM" SLOWDEV_DIRS="$WORK/state:$WORK/repo" SLOWDEV_BW_MBS="$BW_MBS" \
SLOWDEV_LAT_MS="$LAT_MS" SLOWDEV_FLOCK="$FLOCK" SLOWDEV_SRC_DIR="$WORK/src" \
SLOWDEV_SRC_OPEN_MS="$OPEN_MS" SLOWDEV_REPORT="$WORK/device.txt" \
C_BACKUP_CONFIG_PATH="$WORK/config.yaml" "$BIN" > "$WORK/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 100); do [ -S "$SOCK" ] && break; sleep 0.1; done
[ -S "$SOCK" ] || { echo "edit_latency: daemon did not start" >&2; cat "$WORK/daemon.log" >&2; exit 2; }

api() {  # api <method> <path> [body]
    curl -s -X "$1" --unix-socket "$SOCK" ${3:+-d "$3"} "http://localhost$2"
}

R=$(api POST /repos "{\"path\":\"$WORK/repo\"}")
REPO=$(printf '%s' "$R" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
[ -n "$REPO" ] || { echo "edit_latency: repo add failed: $R" >&2; exit 2; }
EDIT="{\"sources\":[\"$WORK/src\"],\"exclude\":[\"*.tmp\"]}"
api PUT "/repos/$REPO/sources" "$EDIT" > /dev/null

R=$(api POST "/repos/$REPO/backup" "{\"paths\":[\"$WORK/src\"]}")
JOB=$(printf '%s' "$R" | sed -n 's/.*"job_id":\([0-9]*\).*/\1/p')
[ -n "$JOB" ] || { echo "edit_latency: backup submit failed: $R" >&2; exit 2; }

: > "$WORK/samples.txt"
T0=$(date +%s.%N)
STATE=""
while :; do
    t=$(curl -s -o /dev/null -w '%{http_code} %{time_total}' -X PUT --unix-socket "$SOCK" \
             -d "$EDIT" "http://localhost/repos/$REPO/sources")
    echo "$(awk -v a="$(date +%s.%N)" -v b="$T0" 'BEGIN{printf "%.2f", a-b}') $t" >> "$WORK/samples.txt"
    case "$(api GET "/jobs/$JOB")" in
        *'"state":"completed"'*) STATE=completed; break ;;
        *'"state":"failed"'*)    STATE=failed; break ;;
    esac
    sleep "$INTERVAL"
done
WALL=$(awk -v a="$(date +%s.%N)" -v b="$T0" 'BEGIN{printf "%.1f", a-b}')
api GET /metrics > "$WORK/metrics.txt"
kill "$DAEMON_PID" 2>/dev/null
wait "$DAEMON_PID" 2>/dev/null
DAEMON_PID=""

BAD=$(awk '$2 != 200' "$WORK/samples.txt" | wc -l)
SUMMARY=$(awk '{print $3 * 1000}' "$WORK/samples.txt" | sort -n | awk '
    { v[NR] = $1 }
    function q(p,  i) { i = int(p * NR + 0.999999); if (i < 1) i = 1; if (i > NR) i = NR; return v[i] }
    END { if (NR == 0) { print "n=0"; exit }
          printf "n=%d p50=%.0f p90=%.0f p99=%.0f max=%.0f", NR, q(0.50), q(0.90), q(0.99), v[NR] }')
echo "edit-latency $SUMMARY ms (backup $STATE, ${WALL} s, non-200: $BAD)"
grep -E '^cbackup_(db_txn_(wait_seconds_total|grants_total)|checkpoint_(lock_max_seconds|lock_seconds_total|presync_seconds_total|rounds_total|throttle_seconds_total)|wal_frames_max)' \
    "$WORK/metrics.txt" | sed 's/^/  /'
grep -E 'checkpointed holding' "$WORK/daemon.log" | sed 's/^/  log: /' | tail -5
sed 's/^/  device: /' "$WORK/device.txt" 2>/dev/null
if [ -n "${OUT:-}" ]; then
    mkdir -p "$OUT"
    cp "$WORK/samples.txt" "$WORK/metrics.txt" "$WORK/daemon.log" "$OUT/" 2>/dev/null
    cp "$WORK/device.txt" "$OUT/" 2>/dev/null
    echo "$SUMMARY" > "$OUT/summary.txt"
fi

[ "$STATE" = completed ] || { echo "edit_latency: the backup $STATE" >&2; exit 1; }
[ "$BAD" -eq 0 ] || { echo "edit_latency: $BAD edits failed" >&2; exit 1; }
if [ -n "$BOUND_MS" ]; then
    P99=$(printf '%s' "$SUMMARY" | sed -n 's/.*p99=\([0-9]*\).*/\1/p')
    if [ "${P99:-0}" -gt "$BOUND_MS" ]; then
        echo "edit_latency: p99 ${P99} ms is above the ${BOUND_MS} ms bound" >&2
        exit 1
    fi
fi
exit 0
