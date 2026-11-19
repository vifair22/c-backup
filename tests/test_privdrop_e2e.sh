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
#
# B244: the hardened container runs with a read-only root, so SQLite has
# nowhere of its own to put temporary files, and the first start of M10
# failed in migration 009. Where a private mount namespace is available the
# whole suite runs that way: /tmp, /var/tmp and /usr/tmp read-only and the
# working directory too, so the state volume is the only place left. It also
# starts the daemon on a database an M9 daemon left at migration 008
# (tests/fixtures/state-at-008.sql), as FS1's upgrade did.

set -u

DAEMON="$(realpath "${1:-build/bins/c-backupd}")"
CLI="$(realpath "${2:-build/bins/c-backup}")"
SELF="$(realpath "$0")"
FIXTURE_008="$(realpath "$(dirname "$0")/fixtures/state-at-008.sql")"
export C_BACKUP_TXN_GUARD=abort
TEST_ROOT="${C_BACKUP_TEST_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/c-backup-test}"
mkdir -p "$TEST_ROOT"
THRESHOLD=1000
# journal_size_limit: the WAL file is trimmed to this at the first commit
# after a restart (4 x the threshold, 4 KiB pages; checkpointer.h).
LIMIT=$((THRESHOLD * 4 * 4096))

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
        if unshare --mount --propagation private true 2>/dev/null; then
            export B244_PRIVATE_MOUNTS=1
            exec unshare --mount --propagation private bash "$SELF" "$DAEMON" "$CLI"
        fi
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
    if unshare "${MAP[@]}" --setuid 0 --setgid 0 --mount --propagation private true 2>/dev/null; then
        export B244_PRIVATE_MOUNTS=1
        exec unshare "${MAP[@]}" --setuid 0 --setgid 0 --mount --propagation private \
             bash "$SELF" "$DAEMON" "$CLI"
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
    [ -n "${D8_PID:-}" ] && kill "$D8_PID" 2>/dev/null
    wait "${D8_PID:-}" 2>/dev/null
    # Here, not outside: in a namespace, files uid 990 made belong to a
    # subordinate uid the invoking user cannot remove. The read-only working
    # directory is a mount point (B244), so it is left and unmounted first.
    if mountpoint -q "$WORK/ro-cwd" 2>/dev/null; then
        cd "$WORK" && umount "$WORK/ro-cwd"
    fi
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
for d in $(seq 1 30); do
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

start_daemon() {
    C_BACKUP_CONFIG_PATH="$WORK/c.yaml" "$DAEMON" >> "$WORK/d.log" 2>&1 &
    DAEMON_PID=$!
    for _ in $(seq 100); do
        [ -S "$SOCK" ] && break
        kill -0 "$DAEMON_PID" 2>/dev/null || break
        sleep 0.1
    done
}
stop_daemon() {
    kill "$DAEMON_PID"
    wait "$DAEMON_PID"
    local rc=$?
    DAEMON_PID=
    return $rc
}
# Changed files, then a backup, n times.
backups() {
    local i OUT
    for i in $(seq 1 "$1"); do
        find "$WORK/src" -type f -name 'f1*' -exec sh -c 'echo "$RANDOM" >> "$1"' _ {} \;
        OUT=$(cb backup 1 "$WORK/src" 2>&1)
        contains "$2: backup $i completes" '"state":"completed"' "$OUT"
    done
}
# Until state.db is past $1 bytes and the WAL has restarted below the
# threshold, ten seconds at most; then say where they are.
wait_backfilled() {
    local db f
    for _ in $(seq 100); do
        db=$(stat -c %s "$WORK/state/state.db")
        f=$(shm_u32 16)
        [ "$db" -gt "$1" ] && [ "${f:-0}" -lt "$THRESHOLD" ] && break
        sleep 0.1
    done
    DB=$(stat -c %s "$WORK/state/state.db")
    F=$(shm_u32 16)
    echo "  state.db $DB bytes, WAL at $F frames ($(shm_u32 96) copied), file $(stat -c %s "$WORK/state/state.db-wal") bytes"
}

# B244: no writable directory of SQLite's own. Each is covered by a
# read-only tmpfs in this mount namespace only, and the working directory is
# one too, since SQLite falls back to it last.
NO_TMP=no
if [ "${B244_PRIVATE_MOUNTS:-}" = 1 ]; then
    NO_TMP=yes
    mkdir -p "$WORK/ro-cwd"
    for d in /tmp /var/tmp /usr/tmp "$WORK/ro-cwd"; do
        [ -d "$d" ] || continue
        mount -t tmpfs -o ro,size=4k tmpfs "$d" || { NO_TMP=no; break; }
    done
    cd "$WORK/ro-cwd" || NO_TMP=no
fi
if [ "$NO_TMP" = yes ]; then
    echo "no writable temporary directory (/tmp, /var/tmp, the working directory: read-only)"
else
    echo "B244 read-only temporary directories: skipped (no private mount namespace here)"
fi

start_daemon
if grep -q "CAP_DAC_READ_SEARCH is not available" "$WORK/d.log"; then
    echo "privilege drop e2e: skipped (CAP_DAC_READ_SEARCH is not available to keep here)"
    exit 0
fi

echo "the drop"
check    "the daemon is listening" "yes" "$([ -S "$SOCK" ] && echo yes || echo no)"
contains "it dropped to uid 990" "running as uid 990" "$(cat "$WORK/d.log")"
# B244: on fresh state every migration ran, 009 included, with no writable
# temporary directory but the state volume.
contains "the temporaries are in the state volume" \
         "sqlite temporary files: $WORK/state" "$(cat "$WORK/d.log")"
lacks    "no migration failed" "migrations failed" "$(cat "$WORK/d.log")"
# What made B215 possible: the state files are root's, created before the
# drop. The daemon has to cope with that, since it is how every fresh
# deployment starts.
check    "state.db is root's, as AppGuard made it" "0" "$(stat -c %u "$WORK/state/state.db")"
check    "and so is its -shm" "0" "$(stat -c %u "$WORK/state/state.db-shm")"

if [ "$NO_TMP" = yes ]; then
    echo "an M9 database at migration 008 upgrades with nowhere for SQLite's temporaries"
    mkdir -p "$WORK/state008"
    chown 990:990 "$WORK/state008"
    chmod 0700 "$WORK/state008"
    sqlite3 "$WORK/state008/state.db" < "$FIXTURE_008"
    sed -e "s#$WORK/state/#$WORK/state008/#" -e "s#$SOCK#$WORK/run/d008.sock#" \
        "$WORK/c.yaml" > "$WORK/c008.yaml"
    chmod 600 "$WORK/c008.yaml"
    C_BACKUP_CONFIG_PATH="$WORK/c008.yaml" "$DAEMON" > "$WORK/d008.log" 2>&1 &
    D8_PID=$!
    for _ in $(seq 100); do
        [ -S "$WORK/run/d008.sock" ] && break
        kill -0 "$D8_PID" 2>/dev/null || break
        sleep 0.1
    done
    L=$(cat "$WORK/d008.log")
    check    "it is listening" "yes" "$([ -S "$WORK/run/d008.sock" ] && echo yes || echo no)"
    lacks    "008 is not applied again" "applying 008_id_reserve.sql" "$L"
    contains "009 is applied" "applying 009_journal_autoincrement.sql" "$L"
    LAST_MIG=$(basename "$(ls "$(dirname "$SELF")"/../migrations/*.sql | tail -n 1)")
    contains "and the last one" "applying $LAST_MIG" "$L"
    contains "the temporaries are in the state volume" \
             "sqlite temporary files: $WORK/state008" "$L"
    lacks    "no migration failed" "migrations failed" "$L"
    kill "$D8_PID"
    wait "$D8_PID"
    check    "it stops cleanly" "0" "$?"
    D8_PID=
    check    "every migration is recorded" "$(ls "$(dirname "$SELF")"/../migrations/*.sql | wc -l)" \
             "$(sqlite3 "$WORK/state008/state.db" \
                "SELECT COUNT(*) FROM system_migrations WHERE filename NOT LIKE '\_lib/%' ESCAPE '\'")"
    check    "every journal entry survives, renumbered where ids collided" \
             "1:1 1:2 1:3 3:10 3:11 2:12 2:13" \
             "$(sqlite3 "$WORK/state008/state.db" \
                "SELECT repo_id || ':' || id FROM journal ORDER BY id" | tr '\n' ' ' | sed 's/ $//')"
fi

echo "the WAL is checkpointed after the drop"
check "repo add" '{"id":1}' "$(cb repo add "$WORK/repo" main)"
backups 2 "first run"
wait_backfilled 1048576
check "state.db was written back (it stayed 4096 bytes in B215)" "yes" \
      "$([ "$DB" -gt 1048576 ] && echo yes || echo "no: $DB bytes")"
check "the WAL restarted below the threshold" "yes" \
      "$([ "${F:-99999}" -lt "$THRESHOLD" ] && echo yes || echo "no: $F frames")"
contains "/readyz is ready" '"status":"ready"' "$(get /readyz)"
check    "no checkpoint failures counted" "0" "$(metric cbackup_checkpoint_failures_total)"
check    "and none in progress" "0" "$(metric cbackup_checkpoint_failing)"

# Every path that could replace -wal or -shm while the daemon runs keeps the
# file instead (checkpointer.h): a restart rewrites the header in place, the
# trim to journal_size_limit is an ftruncate, a RESTART checkpoint writes
# only the header. Were one to create a new file, it would be created by
# uid 990 for connections that hold the old one, or be root's and unwritable
# by any connection opened after the drop. So: the WAL grows past the limit,
# is checkpointed, restarted and trimmed back under it, and is the same file
# throughout, still root's, with nothing failing.
echo "the WAL is trimmed in place, never recreated"
WAL="$WORK/state/state.db-wal"
WAL_INO=$(stat -c %i "$WAL")
SHM_INO=$(stat -c %i "$WORK/state/state.db-shm")
DB0=$(stat -c %s "$WORK/state/state.db")
echo 0 > "$WORK/walpeak"
( peak=0
  while :; do
      sz=$(stat -c %s "$WAL" 2>/dev/null || echo 0)
      [ "$sz" -gt "$peak" ] && { peak=$sz; echo "$peak" > "$WORK/walpeak"; }
      sleep 0.005
  done ) &
PEAK_PID=$!
backups 3 "trim"
kill "$PEAK_PID"; wait "$PEAK_PID" 2>/dev/null
PEAK=$(cat "$WORK/walpeak")
for _ in $(seq 100); do
    [ "$(stat -c %s "$WAL")" -le "$LIMIT" ] && [ "$(shm_u32 16)" -lt "$THRESHOLD" ] && break
    sleep 0.1
done
wait_backfilled "$DB0"
check "the WAL file grew past journal_size_limit" "yes" \
      "$([ "$PEAK" -gt "$LIMIT" ] && echo yes || echo "no: peak $PEAK, limit $LIMIT")"
check "and was trimmed back to it" "yes" \
      "$([ "$(stat -c %s "$WAL")" -le "$LIMIT" ] && echo yes || echo "no: $(stat -c %s "$WAL") bytes")"
check "state.db took the new frames" "yes" "$([ "$DB" -gt "$DB0" ] && echo yes || echo "no: $DB0 -> $DB")"
check "the WAL restarted below the threshold" "yes" \
      "$([ "${F:-99999}" -lt "$THRESHOLD" ] && echo yes || echo "no: $F frames")"
check "-wal is the same file" "$WAL_INO" "$(stat -c %i "$WAL")"
check "-shm is the same file" "$SHM_INO" "$(stat -c %i "$WORK/state/state.db-shm")"
check "-wal is still root's" "0" "$(stat -c %u "$WAL")"
check "no checkpoint failures counted" "0" "$(metric cbackup_checkpoint_failures_total)"

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

stop_daemon
check "the daemon stops cleanly" "0" "$?"

# The other way -wal and -shm are made anew: the last connection closing
# deletes them, which is shutdown, and the next start creates them again
# as root, before the drop, on a state.db that is root's from the first
# start. Everything is opened before the drop again, so checkpoints work
# exactly as on a fresh start.
echo "a restart on root-owned state files"
check "state.db stayed root's" "0" "$(stat -c %u "$WORK/state/state.db")"
check "the clean stop removed the WAL" "no" "$([ -e "$WAL" ] && echo yes || echo no)"
DB0=$(stat -c %s "$WORK/state/state.db")
start_daemon
check    "the daemon is listening again" "yes" "$([ -S "$SOCK" ] && echo yes || echo no)"
check    "it dropped to uid 990 again" "2" "$(grep -c "running as uid 990" "$WORK/d.log")"
check    "the new -wal is root's" "0" "$(stat -c %u "$WAL" 2>/dev/null || echo missing)"
check    "the new -shm is root's" "0" "$(stat -c %u "$WORK/state/state.db-shm" 2>/dev/null || echo missing)"
backups 2 "after the restart"
wait_backfilled "$DB0"
check "state.db took the new frames" "yes" "$([ "$DB" -gt "$DB0" ] && echo yes || echo "no: $DB0 -> $DB")"
check "the WAL restarted below the threshold" "yes" \
      "$([ "${F:-99999}" -lt "$THRESHOLD" ] && echo yes || echo "no: $F frames")"
contains "/readyz is ready" '"status":"ready"' "$(get /readyz)"
check    "no checkpoint failures counted" "0" "$(metric cbackup_checkpoint_failures_total)"
stop_daemon
check "the restarted daemon stops cleanly" "0" "$?"

# B218: a stop with a large WAL and a running job, as uid 990. The stop
# took 53 s on a test server, past Docker's 10 s grace: the last connection's
# close copied the whole WAL back into state.db first. The stop now leaves a
# WAL too large to copy for the next start, and does not wait on the job.
# The threshold is raised to the most the config takes, so the checkpointer
# leaves the WAL alone; a fake unit's disk, 256 GiB of one byte, keeps a job
# reading for as long as the stop needs, reading nothing from disk and
# storing almost nothing, since every chunk is the same.
echo "a stop with a large WAL and a running job"
if ! command -v sqlite3 >/dev/null 2>&1; then
    echo "  (skipped: no sqlite3 to grow the WAL with)"
else
    cat > "$WORK/units.json" <<EOF
{"units":[{"id":"m10","name":"cbk-m10","state":"running","caps":["quiesce"],
  "blobs":[{"id":"cfg","path":".cbackup-unit/config/x","text":"m10"}],
  "disks":[{"id":"vda","size":274877906944,"alloc":"exact",
            "runs":[{"off":0,"len":274877906944,"kind":"fill","seed":90}]}]}]}
EOF
    sed -i "s/^  wal_autocheckpoint: .*/  wal_autocheckpoint: 1048576/" "$WORK/c.yaml"
    printf 'units:\n  allow: cbk-m10\n  fake_spec: %s\n' "$WORK/units.json" >> "$WORK/c.yaml"
    start_daemon
    check "the daemon is listening" "yes" "$([ -S "$SOCK" ] && echo yes || echo no)"
    SNAPS0=$(cb snapshots 1 | grep -o '"id":[0-9]*,' | wc -l)
    # About 300 MiB of WAL: a 512 KiB row rewritten 600 times, each its own
    # commit, then a marker the next start must still have.
    { echo "PRAGMA busy_timeout=10000; PRAGMA wal_autocheckpoint=0;"
      for _ in $(seq 600); do
          echo "INSERT INTO daemon_state (key, value) VALUES ('m10_filler', hex(randomblob(262144)))" \
               "ON CONFLICT(key) DO UPDATE SET value = excluded.value;"
      done
      echo "DELETE FROM daemon_state WHERE key = 'm10_filler';"
      echo "INSERT INTO daemon_state (key, value) VALUES ('m10_marker', 'kept');"
    } | sqlite3 "$WORK/state/state.db" > /dev/null
    WALSZ=$(stat -c %s "$WAL")
    echo "  WAL $WALSZ bytes, $(shm_u32 16) frames ($(shm_u32 96) copied)"
    check "the WAL is a few hundred MB" "yes" \
          "$([ "$WALSZ" -gt 200000000 ] && [ "$WALSZ" -lt 600000000 ] && echo yes || echo "no: $WALSZ")"
    JOB=$(cb unit backup 1 --provider fake cbk-m10 --no-wait | sed -n 's/.*"job_id":\([0-9]*\).*/\1/p')
    J=""
    for _ in $(seq 200); do
        J=$(cb jobs "$JOB")
        case "$J" in *'"state":"running"'*'"bytes_done":'[1-9]*) break ;; esac
        sleep 0.05
    done
    contains "a unit backup is running" '"state":"running"' "$J"
    T0=$(date +%s%N)
    stop_daemon
    RC=$?
    MS=$(( ($(date +%s%N) - T0) / 1000000 ))
    echo "  stopped in $MS ms"
    check "the daemon stops cleanly" "0" "$RC"
    check "within 8 s, well inside Docker's 10" "yes" \
          "$([ "$MS" -lt 8000 ] && echo yes || echo "no: $MS ms")"
    contains "the job was cancelled" "job $JOB cancelled" "$(cat "$WORK/d.log")"
    lacks    "not abandoned" "did not stop within" "$(cat "$WORK/d.log")"
    contains "the WAL was left for the next start" "WAL frames" "$(grep 'for the next start' "$WORK/d.log")"
    check    "it is still there" "yes" "$([ -s "$WAL" ] && echo yes || echo no)"
    check    "and still root's" "0" "$(stat -c %u "$WAL")"

    # The next start reads the WAL back, quickly, and nothing is missing.
    sed -i "s/^  wal_autocheckpoint: .*/  wal_autocheckpoint: $THRESHOLD/" "$WORK/c.yaml"
    DB0=$(stat -c %s "$WORK/state/state.db")
    T0=$(date +%s%N)
    start_daemon
    MS=$(( ($(date +%s%N) - T0) / 1000000 ))
    echo "  listening again in $MS ms"
    check "the next start is listening" "yes" "$([ -S "$SOCK" ] && echo yes || echo no)"
    check "within 5 s" "yes" "$([ "$MS" -lt 5000 ] && echo yes || echo "no: $MS ms")"
    check "every snapshot is still there" "$SNAPS0" "$(cb snapshots 1 | grep -o '"id":[0-9]*,' | wc -l)"
    contains "the job is recorded cancelled" '"state":"cancelled"' "$(cb jobs "$JOB")"
    lacks    "not interrupted" "interrupted by daemon restart" "$(cb jobs "$JOB")"
    check    "the last write before the stop survived" "kept" \
             "$(sqlite3 -cmd ".timeout 10000" "$WORK/state/state.db" \
                "SELECT value FROM daemon_state WHERE key = 'm10_marker'")"
    wait_backfilled "$DB0"
    check "the WAL was copied back and restarted" "yes" \
          "$([ "${F:-99999}" -lt "$THRESHOLD" ] && echo yes || echo "no: $F frames")"
    contains "/readyz is ready" '"status":"ready"' "$(get /readyz)"
    check    "no checkpoint failures counted" "0" "$(metric cbackup_checkpoint_failures_total)"
    stop_daemon
    check "and that daemon stops cleanly too" "0" "$?"
fi

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
