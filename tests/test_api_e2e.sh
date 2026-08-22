#!/bin/bash
#
# End-to-end API test against a real daemon.
#
# The unit suites cover the code behind the routes; nothing covers the routes
# themselves, the auth middleware, or the job manager actually running work on
# a worker thread. Those only exist once a process is listening, so this drives
# a real daemon over both transports.
#
# Exits non-zero on the first failure so a broken endpoint stops the build.

set -u

BIN="${1:-build/bins/c-backupd}"
WORK="$(mktemp -d /tmp/c_backup_api_e2e.XXXXXX)"
SOCK="$WORK/c.sock"
PORT=18799
PASS=0
FAIL=0

cleanup() {
    [ -n "${DAEMON_PID:-}" ] && kill "$DAEMON_PID" 2>/dev/null
    wait "${DAEMON_PID:-}" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT

ok()   { PASS=$((PASS+1)); printf '  ok   %s\n' "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; }

check() {  # check <name> <expected> <actual>
    if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "$2" "$3"; fi
}

contains() {  # contains <name> <needle> <haystack>
    case "$3" in
        *"$2"*) ok "$1" ;;
        *)      bad "$1" "contains '$2'" "$3" ;;
    esac
}

sock_code() { curl -s -o /dev/null -w '%{http_code}' --unix-socket "$SOCK" "http://localhost$1"; }
sock_body() { curl -s --unix-socket "$SOCK" "http://localhost$1"; }
tcp_code()  { curl -s -o /dev/null -w '%{http_code}' "$@" "http://127.0.0.1:$PORT$1" 2>/dev/null; }

mkdir -p "$WORK/src/docs"
echo "alpha content"  > "$WORK/src/a.txt"
echo "beta content"   > "$WORK/src/b.txt"
echo "nested content" > "$WORK/src/docs/c.txt"

cat > "$WORK/config.yaml" <<EOF
db:
  path: $WORK/state.db
http:
  socket: $SOCK
  port: $PORT
  bind: 127.0.0.1
log:
  level: warn
EOF

C_BACKUP_CONFIG_PATH="$WORK/config.yaml" "$BIN" > "$WORK/daemon.log" 2>&1 &
DAEMON_PID=$!

for _ in $(seq 1 50); do
    [ -S "$SOCK" ] && break
    sleep 0.1
done
if [ ! -S "$SOCK" ]; then
    echo "daemon did not start; log follows:"
    cat "$WORK/daemon.log"
    exit 1
fi

echo "health"
check "healthz"            200 "$(sock_code /healthz)"
check "readyz"             200 "$(sock_code /readyz)"

echo "auth"
contains "setup unconfigured" '"configured":false' "$(sock_body /api/auth/setup)"
# First-run mode leaves TCP open so the operator can bootstrap.
check "tcp open before setup" 200 "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/repos")"
curl -s -X POST -d '{"password":"correcthorsebattery"}' \
     "http://127.0.0.1:$PORT/api/auth/setup" > /dev/null
check "tcp locked after setup" 401 "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/repos")"
check "wrong password" 401 "$(curl -s -o /dev/null -w '%{http_code}' -X POST \
     -d '{"password":"wrong"}' "http://127.0.0.1:$PORT/api/auth/login")"

TOKEN=$(curl -s -X POST -d '{"password":"correcthorsebattery"}' \
        "http://127.0.0.1:$PORT/api/auth/login" \
        | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
if [ -n "$TOKEN" ]; then ok "login returns a token"; else bad "login returns a token" "non-empty" "empty"; fi
check "bearer accepted" 200 "$(curl -s -o /dev/null -w '%{http_code}' \
     -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:$PORT/repos")"
# The socket is gated by filesystem permissions, not tokens.
check "socket bypasses auth" 200 "$(sock_code /repos)"

echo "repos"
contains "register" '"id":1' \
    "$(curl -s -X POST --unix-socket "$SOCK" -d "{\"path\":\"$WORK/repo\"}" http://localhost/repos)"
contains "get repo" '"uuid"' "$(sock_body /repos/1)"
check "unknown repo" 404 "$(sock_code /repos/99)"

mkdir -p "$WORK/foreign" && echo "not ours" > "$WORK/foreign/data.txt"
check "refuses foreign dir" 400 \
    "$(curl -s -o /dev/null -w '%{http_code}' -X POST --unix-socket "$SOCK" \
       -d "{\"path\":\"$WORK/foreign\"}" http://localhost/repos)"
check "foreign dir untouched" "data.txt" "$(ls "$WORK/foreign")"

echo "backup"
contains "submit" '"job_id"' \
    "$(curl -s -X POST --unix-socket "$SOCK" -d "{\"paths\":[\"$WORK/src\"]}" \
       http://localhost/repos/1/backup)"
for _ in $(seq 1 60); do
    case "$(sock_body /jobs/1)" in
        *'"state":"completed"'*|*'"state":"failed"'*) break ;;
    esac
    sleep 0.2
done
contains "job completed" '"state":"completed"' "$(sock_body /jobs/1)"
contains "snapshot listed" '"id":"1"' "$(sock_body /repos/1/snapshots)"
contains "tree lists root" '"name"' "$(sock_body '/repos/1/snapshots/1/tree?parent=0')"
contains "search finds file" 'a.txt' "$(sock_body '/repos/1/search?q=txt')"

echo "metadata"
check "set tag" 200 "$(curl -s -o /dev/null -w '%{http_code}' -X POST --unix-socket "$SOCK" \
     -d '{"name":"milestone","snap_id":1}' http://localhost/repos/1/tags)"
contains "tag listed" 'milestone' "$(sock_body /repos/1/tags)"
check "set note" 200 "$(curl -s -o /dev/null -w '%{http_code}' -X PUT --unix-socket "$SOCK" \
     -d '{"body":"first backup"}' http://localhost/repos/1/snapshots/1/note)"
contains "note readable" 'first backup' "$(sock_body /repos/1/snapshots/1/note)"
check "set policy" 200 "$(curl -s -o /dev/null -w '%{http_code}' -X PUT --unix-socket "$SOCK" \
     -d '{"key":"retention.keep_daily","value":"14"}' http://localhost/repos/1/policy)"
contains "policy readable" 'keep_daily' "$(sock_body /repos/1/policy)"
check "delete tag" 204 "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE --unix-socket "$SOCK" \
     http://localhost/repos/1/tags/milestone)"

echo "restore"
curl -s -X POST --unix-socket "$SOCK" -d "{\"dest\":\"$WORK/out\"}" \
     http://localhost/repos/1/snapshots/1/restore > /dev/null
for _ in $(seq 1 60); do
    case "$(sock_body /jobs/2)" in
        *'"state":"completed"'*|*'"state":"failed"'*) break ;;
    esac
    sleep 0.2
done
contains "restore completed" '"state":"completed"' "$(sock_body /jobs/2)"
if diff -r "$WORK/src" "$WORK/out$WORK/src" > /dev/null 2>&1; then
    ok "restored tree is identical"
else
    bad "restored tree is identical" "no differences" "diff reported changes"
fi

echo "maintenance"
check "gc" 202 "$(curl -s -o /dev/null -w '%{http_code}' -X POST --unix-socket "$SOCK" \
     http://localhost/repos/1/gc)"
check "reindex" 202 "$(curl -s -o /dev/null -w '%{http_code}' -X POST --unix-socket "$SOCK" \
     http://localhost/repos/1/reindex)"
check "gfs" 202 "$(curl -s -o /dev/null -w '%{http_code}' -X POST --unix-socket "$SOCK" \
     http://localhost/repos/1/gfs)"
sleep 1
contains "journal readable" 'entries' "$(sock_body /repos/1/journal)"

echo "shutdown"
kill -TERM "$DAEMON_PID" 2>/dev/null
for _ in $(seq 1 50); do
    kill -0 "$DAEMON_PID" 2>/dev/null || break
    sleep 0.1
done
if kill -0 "$DAEMON_PID" 2>/dev/null; then
    bad "clean SIGTERM shutdown" "exited" "still running"
    kill -9 "$DAEMON_PID" 2>/dev/null
else
    ok "clean SIGTERM shutdown"
fi
DAEMON_PID=""

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
