#!/bin/bash
#
# B215: the daemon as it is deployed, started as root and dropping itself to
# security.run_as_uid 990, keeps state.db's WAL bounded; and when something
# stops the WAL being checkpointed, the daemon says so in its log, in
# /readyz and in /metrics.
#
# On a test server the dropped daemon's WAL reached 524% of its cap and
# state.db stayed 4096 bytes: AppGuard creates state.db, -wal and -shm as
# root, 0600, before the drop; the checkpointer opened its own connection
# after it, which SQLite quietly opened read-only (uid 990 can read the
# files through CAP_DAC_READ_SEARCH but not write them), so every checkpoint
# was refused and counted as busy, and /readyz said ready.
#
# The drop needs root. CI's job runs as root; a developer's machine gets a
# user namespace where the current user is root and uid 990 maps to one of
# its subordinate uids (newuidmap), which is the same drop with the same
# capability sets, as far as the files in the namespace are concerned.
# Without either the suite is skipped, saying so.

set -u

DAEMON="$(realpath "${1:-build/bins/c-backupd}")"
CLI="$(realpath "${2:-build/bins/c-backup}")"
SELF="$(realpath "$0")"
export C_BACKUP_TXN_GUARD=abort
TEST_ROOT="${C_BACKUP_TEST_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/c-backup-test}"
mkdir -p "$TEST_ROOT"
THRESHOLD=1000

cap_bnd_has() {   # cap_bnd_has <bit>: in this process's bounding set
    local hex
    hex=$(sed -n 's/^CapBnd:[[:space:]]*//p' /proc/self/status)
    [ -n "$hex" ] && (( (16#$hex >> $1) & 1 ))
}

if [ -z "${B215_INNER:-}" ]; then
    export B215_INNER=1
    # Real root whose bounding set still holds CAP_DAC_READ_SEARCH (2), the
    # one capability the drop keeps effective: run as is.
    if [ "$(id -u)" = 0 ] && cap_bnd_has 2; then
        exec bash "$SELF" "$DAEMON" "$CLI"
    fi
    # Otherwise a user namespace. Root maps itself; anyone else maps itself
    # to 0 and its subordinate range above it, which needs uid 990 inside.
    if [ "$(id -u)" = 0 ]; then
        MAP=(--map-users=0:0:65536 --map-groups=0:0:65536)
    else
        SU=$(awk -F: -v u="$(id -un)" -v n="$(id -u)" '$1==u||$1==n {print $2":"$3; exit}' /etc/subuid 2>/dev/null)
        SG=$(awk -F: -v u="$(id -un)" -v n="$(id -u)" '$1==u||$1==n {print $2":"$3; exit}' /etc/subgid 2>/dev/null)
        if [ -z "$SU" ] || [ -z "$SG" ] || [ "${SU#*:}" -lt 1000 ] || [ "${SG#*:}" -lt 1000 ] ||
           ! command -v newuidmap >/dev/null 2>&1; then
            echo "privilege drop e2e: skipped (not root, and no subordinate uid range to map uid 990 into)"
            exit 0
        fi
        MAP=(--map-users="0:$(id -u):1" --map-users="1:${SU%%:*}:$(( ${SU#*:} < 65535 ? ${SU#*:} : 65535 ))"
             --map-groups="0:$(id -g):1" --map-groups="1:${SG%%:*}:$(( ${SG#*:} < 65535 ? ${SG#*:} : 65535 ))")
    fi
    if ! unshare "${MAP[@]}" --setuid 0 --setgid 0 true 2>/dev/null; then
        echo "privilege drop e2e: skipped (user namespaces are not available here)"
        exit 0
    fi
    exec unshare "${MAP[@]}" --setuid 0 --setgid 0 bash "$SELF" "$DAEMON" "$CLI"
fi

# ---- as root, or root of a user namespace, from here ----

WORK="$(mktemp -d "$TEST_ROOT/c_backup_privdrop_e2e.XXXXXX")"
SOCK="$WORK/run/d.sock"
PASS=0
FAIL=0

cleanup() {
    [ -n "${DAEMON_PID:-}" ] && kill "$DAEMON_PID" 2>/dev/null
    wait "${DAEMON_PID:-}" 2>/dev/null
    # Here, not outside: in a namespace, files uid 990 made belong to a
    # subordinate uid the invoking user cannot remove.
    rm -rf "$WORK"
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

ok()  { PASS=$((PASS+1)); printf '  ok   %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; }
check()    { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "$2" "$3"; fi; }
contains() { case "$3" in *"$2"*) ok "$1" ;; *) bad "$1" "contains '$2'" "$3" ;; esac; }

cb()     { "$CLI" --socket "$SOCK" "$@"; }
get()    { curl -s --unix-socket "$SOCK" "http://localhost$1"; }
metric() { get /metrics | sed -n "s/^$1 //p"; }
# The wal-index header (walformat.md): mxFrame at 16, nBackfill at 96.
shm_u32() { od -An -tu4 -j"$1" -N4 "$WORK/state/state.db-shm" | tr -d ' '; }

# The layout the Manual documents: state, socket directory and repository
# owned by 990, sources wherever they are.
mkdir -p "$WORK/state" "$WORK/run" "$WORK/repo" "$WORK/src"
chown 990:990 "$WORK/state" "$WORK/run" "$WORK/repo"
chmod 0700 "$WORK/state"
chmod 0755 "$WORK"
for d in $(seq 1 15); do
    mkdir -p "$WORK/src/d$d"
    for f in $(seq 1 200); do printf '%s %s %s\n' "$d" "$f" "$RANDOM" > "$WORK/src/d$d/f$f"; done
done

cat > "$WORK/c.yaml" <<EOF
db:
  path: $WORK/state/state.db
  wal_autocheckpoint: $THRESHOLD
http:
  socket: $SOCK
  port: 0
security:
  run_as_uid: 990
  key_dir: $WORK/state/keys
EOF
chmod 600 "$WORK/c.yaml"

C_BACKUP_CONFIG_PATH="$WORK/c.yaml" "$DAEMON" > "$WORK/d.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 100); do
    [ -S "$SOCK" ] && break
    kill -0 "$DAEMON_PID" 2>/dev/null || break
    sleep 0.1
done
if grep -q "CAP_DAC_READ_SEARCH is not available" "$WORK/d.log"; then
    echo "privilege drop e2e: skipped (CAP_DAC_READ_SEARCH is not available to keep here)"
    exit 0
fi

echo "the drop"
check    "the daemon is listening" "yes" "$([ -S "$SOCK" ] && echo yes || echo no)"
contains "it dropped to uid 990" "running as uid 990" "$(cat "$WORK/d.log")"
# What made B215 possible: the state files are root's, created before the
# drop. The daemon has to cope with that, since it is how every fresh
# deployment starts.
check    "state.db is root's, as AppGuard made it" "0" "$(stat -c %u "$WORK/state/state.db")"
check    "and so is its -shm" "0" "$(stat -c %u "$WORK/state/state.db-shm")"

echo "the WAL is checkpointed after the drop"
check "repo add" '{"id":1}' "$(cb repo add "$WORK/repo" main)"
for i in 1 2; do
    find "$WORK/src" -type f -name 'f1*' -exec sh -c 'echo "$RANDOM" >> "$1"' _ {} \;
    OUT=$(cb backup 1 "$WORK/src" 2>&1)
    contains "backup $i completes" '"state":"completed"' "$OUT"
done
MAX_SEEN=0
for _ in $(seq 100); do
    F=$(shm_u32 16)
    [ "${F:-0}" -gt "$MAX_SEEN" ] && MAX_SEEN=$F
    DB=$(stat -c %s "$WORK/state/state.db")
    [ "$DB" -gt 1048576 ] && [ "${F:-0}" -lt "$THRESHOLD" ] && break
    sleep 0.1
done
DB=$(stat -c %s "$WORK/state/state.db")
F=$(shm_u32 16)
echo "  state.db $DB bytes, WAL at $F frames ($(shm_u32 96) copied)"
check "state.db was written back (it stayed 4096 bytes in B215)" "yes" \
      "$([ "$DB" -gt 1048576 ] && echo yes || echo "no: $DB bytes")"
check "the WAL restarted below the threshold" "yes" \
      "$([ "${F:-99999}" -lt "$THRESHOLD" ] && echo yes || echo "no: $F frames")"
contains "/readyz is ready" '"status":"ready"' "$(get /readyz)"
check    "no checkpoint failures counted" "0" "$(metric cbackup_checkpoint_failures_total)"
check    "and none in progress" "0" "$(metric cbackup_checkpoint_failing)"

echo "a WAL index the daemon cannot read is loud"
# A symlink loop in place of -shm fails the daemon's open of it the way a
# file it may not read would (ELOOP; DAC_READ_SEARCH would read past any
# mode). The real file is kept by a second link and put back, so SQLite's
# own descriptors and locks never notice. The loop is a link to a link to
# itself, so it loops under either name and mv sees two different files.
SHM="$WORK/state/state.db-shm"
ln "$SHM" "$SHM.keep"
ln -s state.db-shm.self "$SHM.self"
ln -s state.db-shm.self "$SHM.loop"
mv -T "$SHM.loop" "$SHM"
R=""
for _ in $(seq 50); do
    R=$(get /readyz)
    case "$R" in *degraded*) break ;; esac
    sleep 0.1
done
contains "/readyz is degraded" '"status":"degraded"' "$R"
contains "and says the checkpoint is failing" '"state_db_checkpoint":"failing"' "$R"
contains "naming the reader and the file" "wal-index reader: cannot read $SHM" "$R"
contains "with the errno" "errno 40" "$R"
N=$(metric cbackup_checkpoint_failures_total)
check    "the failure is counted" "yes" "$([ "${N:-0}" -ge 1 ] && echo yes || echo "no: '$N'")"
check    "and in progress" "1" "$(metric cbackup_checkpoint_failing)"
contains "the log has it as an error" "state.db: wal-index reader failed: cannot read $SHM" \
         "$(grep wal_health "$WORK/d.log")"

mv -T "$SHM.keep" "$SHM"
rm -f "$SHM.self"
for _ in $(seq 50); do
    R=$(get /readyz)
    case "$R" in *'"status":"ready"'*) break ;; esac
    sleep 0.1
done
contains "put back, /readyz is ready again" '"status":"ready"' "$R"
check    "nothing in progress" "0" "$(metric cbackup_checkpoint_failing)"
contains "and the log says it recovered" "wal-index reader recovered" "$(cat "$WORK/d.log")"

kill "$DAEMON_PID"
wait "$DAEMON_PID"
check "the daemon stops cleanly" "0" "$?"
DAEMON_PID=

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
