#!/bin/bash
# First-backup model (PERF.md, 2026-11-19): a fresh daemon and repository on
# scratch, one first backup of SRC, with bench/io_count.c preloaded. Writes
# OUT/summary.txt (wall, CPU), counters.0 (setup) and counters.1 (the
# backup), threads.raw (every thread's CPU, 4 samples a second), the daemon
# log, pagemap.txt (dbstat page -> b-tree) and row counts.
#
#   bench/first_backup_model.sh SCRATCH OUT SRC [extra config yaml]
#
# SCRATCH must not exist; it is removed afterwards unless KEEP=1. Put it on
# tmpfs: the point is to count bytes and syscalls, not to time a disk.
# DAEMON, CLI and SHIM override build/bins/c-backupd, c-backup, io_count.so;
# IOC_SRC, IOC_STREAM_MBS and IOC_DEV_MBS pass through to the shim.
# Summaries: bench/io_sites.py OUT/counters.1 (sync call sites),
# bench/io_threads.py OUT/threads.raw (CPU per thread name), and
#   awk -F'|' 'NR==FNR{m[$1]=$2; next} /^walpg/{split($0,f," "); c[m[f[2]]]+=f[3]}
#     END{for(k in c) print c[k], k}' OUT/pagemap.txt OUT/counters.1 | sort -rn
# for the WAL frames per b-tree.
set -u
W=$1; OUT=$2; SRC=$3; EXTRA=${4:-}
DAEMON=${DAEMON:-build/bins/c-backupd}; CLI=${CLI:-build/bins/c-backup}; SHIM=${SHIM:-build/bins/io_count.so}
[ -e "$W" ] && { echo "$W exists"; exit 1; }
DAEMON=$(realpath "$DAEMON"); CLI=$(realpath "$CLI"); SHIM=$(realpath "$SHIM")
mkdir -p "$W/ioc" "$W/state" "$W/keys" "$OUT"
cat > "$W/c.yaml" <<YAML
db:
  path: $W/state/state.db
http:
  socket: $W/c.sock
log:
  level: info
security:
  key_dir: $W/keys
$EXTRA
YAML
IOC_DIR=$W/ioc LD_PRELOAD=$SHIM C_BACKUP_CONFIG_PATH=$W/c.yaml "$DAEMON" > "$OUT/daemon.log" 2>&1 &
DP=$!
PP=
trap 'kill $DP 2>/dev/null; [ -n "$PP" ] && kill $PP 2>/dev/null; wait' EXIT
trap 'exit 143' TERM INT
for _ in $(seq 1 100); do [ -S "$W/c.sock" ] && break; sleep 0.1; done
cb() { "$CLI" --socket "$W/c.sock" "$@"; }
cb repo add "$W/repo" r --encrypt system > /dev/null || { echo "repo add failed"; exit 1; }
cb repo key ack 1 > /dev/null
busy() { cb jobs --limit 20 2>/dev/null | grep -oE '"state":"(queued|running)"' | wc -l; }
for _ in $(seq 1 600); do [ "$(busy)" = 0 ] && break; sleep 0.2; done
dumpwait() { touch "$W/ioc/dump"; while [ -e "$W/ioc/dump" ]; do sleep 0.1; done; sleep 0.3; }
dumpwait
( while kill -0 $DP 2>/dev/null; do
    t=$(date +%s.%N)
    for s in /proc/$DP/task/*/stat; do read -r line < "$s" 2>/dev/null && echo "$t $line"; done
    sleep 0.25
  done ) > "$OUT/threads.raw" 2>/dev/null &
PP=$!
read -r U0 S0 < <(awk '{print $14, $15}' /proc/$DP/stat)
t0=$(date +%s.%N)
cb backup 1 "$SRC" > "$OUT/backup.json" 2>&1; rc=$?
for _ in $(seq 1 3000); do [ "$(busy)" = 0 ] && break; sleep 0.1; done
t1=$(date +%s.%N)
read -r U1 S1 < <(awk '{print $14, $15}' /proc/$DP/stat)
dumpwait
awk -v rc=$rc -v a=$t0 -v b=$t1 -v u0=$U0 -v u1=$U1 -v s0=$S0 -v s1=$S1 \
  'BEGIN{printf "rc=%s wall=%.2f user_s=%.2f sys_s=%.2f\n", rc, b-a, (u1-u0)/100, (s1-s0)/100}' > "$OUT/summary.txt"
{ du -sb "$W/repo"; ls -l "$W/state"; } >> "$OUT/summary.txt"
cp "$W/ioc"/counters.* "$OUT/"
sqlite3 "$W/state/state.db" "select pageno, name from dbstat" > "$OUT/pagemap.txt" 2>/dev/null
for t in $(sqlite3 "$W/state/state.db" "select name from sqlite_master where type='table'"); do
  echo "$t $(sqlite3 "$W/state/state.db" "select count(*) from \"$t\"" 2>/dev/null)"; done > "$OUT/rows.txt"
cat "$OUT/summary.txt"
kill $DP 2>/dev/null; wait $DP 2>/dev/null
[ "${KEEP:-0}" = 1 ] || rm -rf --one-file-system "$W"
