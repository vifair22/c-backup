#!/bin/bash
#
# CLI against a real daemon.
#
# The CLI holds no repository logic: every command is an HTTP request over the
# unix socket. What is worth testing is therefore that each command reaches the
# right endpoint, that exit codes track HTTP status, and above all that a
# missing daemon produces a clear message rather than a crash or a hang.

set -u

DAEMON="${1:-build/bins/c-backupd}"
CLI="${2:-build/bins/c-backup}"
WORK="$(mktemp -d /tmp/c_backup_cli_e2e.XXXXXX)"
SOCK="$WORK/c.sock"
PASS=0
FAIL=0

cleanup() {
    [ -n "${DAEMON_PID:-}" ] && kill "$DAEMON_PID" 2>/dev/null
    wait "${DAEMON_PID:-}" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT

ok()  { PASS=$((PASS+1)); printf '  ok   %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; }

check()    { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "$2" "$3"; fi; }
contains() { case "$3" in *"$2"*) ok "$1" ;; *) bad "$1" "contains '$2'" "$3" ;; esac; }

cb() { "$CLI" --socket "$SOCK" "$@"; }

echo "no daemon"
# The most important case: a clear message, a non-zero exit, and no hang.
OUT=$("$CLI" --socket "$WORK/absent.sock" repo list 2>&1); RC=$?
check    "exits non-zero without a daemon" 1 "$RC"
contains "explains the daemon is not running" "daemon not running" "$OUT"
contains "suggests how to start it" "systemctl start c-backupd" "$OUT"

echo "offline commands"
contains "version" "c-backup" "$("$CLI" --version)"
contains "help lists commands" "backup <repo-id>" "$("$CLI" help)"
OUT=$("$CLI" nonsense 2>&1); RC=$?
check    "unknown command exits non-zero" 1 "$RC"
contains "unknown command explains itself" "unknown command" "$OUT"

mkdir -p "$WORK/src"
echo "content one" > "$WORK/src/one.txt"
echo "content two" > "$WORK/src/two.txt"

cat > "$WORK/config.yaml" <<EOF
db:
  path: $WORK/state.db
http:
  socket: $SOCK
  port: 18801
  bind: 127.0.0.1
log:
  level: warn
EOF

C_BACKUP_CONFIG_PATH="$WORK/config.yaml" "$DAEMON" > "$WORK/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
if [ ! -S "$SOCK" ]; then echo "daemon did not start"; cat "$WORK/daemon.log"; exit 1; fi

echo "with daemon"
contains "health" "ready" "$(cb health)"
contains "repo add" '"id":1' "$(cb repo add "$WORK/repo" main)"
contains "repo list" '"path"' "$(cb repo list)"
contains "repo show" '"uuid"' "$(cb repo show 1)"

contains "backup submits a job" '"job_id"' "$(cb backup 1 "$WORK/src")"
for _ in $(seq 1 60); do
    case "$(cb jobs 1)" in *'"state":"completed"'*|*'"state":"failed"'*) break ;; esac
    sleep 0.2
done
contains "job completed" '"state":"completed"' "$(cb jobs 1)"
contains "jobs list"     '"action"'            "$(cb jobs)"
contains "snapshots"     '"id":"1"'            "$(cb snapshots 1)"
contains "ls"            '"name"'              "$(cb ls 1 1)"
contains "search"        'one.txt'             "$(cb search 1 txt)"

contains "tag set"    '"ok"'      "$(cb tag set 1 keeper 1)"
contains "tag list"   'keeper'    "$(cb tag list 1)"
check    "tag remove" 0           "$(cb tag remove 1 keeper > /dev/null; echo $?)"

contains "schedule add"    '"id"' "$(cb schedule add 1 gc 3600)"
contains "schedule list"   'gc'   "$(cb schedule list 1)"
check    "schedule remove" 0      "$(cb schedule remove 1 1 > /dev/null; echo $?)"

contains "gc"      '"job_id"' "$(cb gc 1)"
contains "reindex" '"job_id"' "$(cb reindex 1)"
contains "gfs"     '"job_id"' "$(cb gfs 1)"
contains "journal" 'entries'  "$(cb journal 1)"

# Poll the restore's own job, not "is anything running". The maintenance jobs
# above may still be queued ahead of it, and a global check breaks out while
# the restore has not started.
RESTORE_OUT=$(cb restore 1 1 "$WORK/out")
contains "restore submits a job" '"job_id"' "$RESTORE_OUT"
RESTORE_JOB=$(printf '%s' "$RESTORE_OUT" | sed -n 's/.*"job_id":\([0-9]*\).*/\1/p')
# Three maintenance jobs were queued just above and there are two workers, so
# the restore can sit behind them. The old twenty-second budget ran out on a
# loaded machine and the loop fell through silently, leaving the tree
# comparison below to report a difference that was really "not finished yet".
RESTORE_DONE=0
for _ in $(seq 1 600); do
    case "$(cb jobs "$RESTORE_JOB")" in
        *'"state":"completed"'*|*'"state":"failed"'*|*'"state":"cancelled"'*)
            RESTORE_DONE=1; break ;;
    esac
    sleep 0.2
done
check "restore job reached a terminal state" 1 "$RESTORE_DONE"
if diff -r "$WORK/src" "$WORK/out$WORK/src" > /dev/null 2>&1; then
    ok "restored tree is identical"
else
    bad "restored tree is identical" "no differences" "diff reported changes"
fi

echo "error handling"
OUT=$(cb repo show 999 2>&1); RC=$?
check    "unknown repo exits non-zero" 1 "$RC"
contains "unknown repo reports status" "HTTP 404" "$OUT"
OUT=$(cb backup 2>&1); RC=$?
check    "missing arguments exit non-zero" 1 "$RC"
contains "missing arguments show usage" "usage:" "$OUT"

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
