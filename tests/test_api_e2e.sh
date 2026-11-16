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
# Scratch goes where the Makefile says (C_BACKUP_TEST_DIR), and on disk when
# run by hand: /tmp is often a tmpfs, so dense scratch there is RAM.
TEST_ROOT="${C_BACKUP_TEST_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/c-backup-test}"
mkdir -p "$TEST_ROOT"
WORK="$(mktemp -d "$TEST_ROOT/c_backup_api_e2e.XXXXXX")"
SOCK="$WORK/c.sock"
# Per run, so two suites running at once (CI jobs on one runner, parallel
# worktrees) cannot answer each other's requests. A fixed port made a second
# daemon fail to bind, or worse, sent a request to the other run's daemon.
PORT=$((20000 + ($$ % 20000)))
# Skip past ports something already holds: the base is derived from the pid,
# and anything bound there (a listener, or an outgoing connection that drew
# it from the ephemeral range) made the daemon exit with "cannot start API
# server" about once in a few dozen runs.
port_busy() { ss -Htan "( sport = :$1 )" 2>/dev/null | grep -q .; }
while port_busy "$PORT"; do PORT=$((PORT + 1)); [ "$PORT" -gt 64990 ] && PORT=20000; done
PASS=0
FAIL=0

cleanup() {
    [ -n "${DAEMON_PID:-}" ] && kill "$DAEMON_PID" 2>/dev/null
    wait "${DAEMON_PID:-}" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT
# A signal would otherwise end the script without the EXIT trap: the scratch
# guard stops an over-budget suite with SIGTERM and still wants it gone.
trap 'exit 143' TERM
trap 'exit 130' INT

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

# A job that never reported progress used to serialise as "current":, which
# is not JSON. gc, gfs and reindex all hit it, so every client broke on
# exactly the maintenance jobs. No parser here, so the check is for an empty
# value after any key, which is what the bug produced.
well_formed() {  # well_formed <name> <body>
    case "$2" in
        *'":,'*|*'":}'*|*'":]'*) bad "$1" "no empty JSON values" "$2" ;;
        '') bad "$1" "a response body" "(empty)" ;;
        *) ok "$1" ;;
    esac
}

sock_code() { curl -s -o /dev/null -w '%{http_code}' --unix-socket "$SOCK" "http://localhost$1"; }
sock_body() { curl -s --unix-socket "$SOCK" "http://localhost$1"; }
tcp_code() {  # tcp_code <path> [curl args...]
    local path="$1"; shift
    curl -s -o /dev/null -w '%{http_code}' "$@" "http://127.0.0.1:$PORT$path" 2>/dev/null
}

# Job ids come from the submit response, never from counting submissions.
# Anything that fires a schedule consumes an id too, so a hardcoded id ends up
# polling somebody else's already-finished job and the wait returns instantly.
job_id() {  # job_id <submit-response-body>
    printf '%s' "$1" | sed -n 's/.*"job_id":\([0-9]*\).*/\1/p'
}

wait_job() {  # wait_job <id>
    # An empty id means the submit itself failed; polling /jobs/ for 20s to
    # discover that buries the real error under a timeout.
    [ -n "$1" ] || return 1
    for _ in $(seq 1 100); do
        case "$(sock_body "/jobs/$1")" in
            *'"state":"completed"'*|*'"state":"failed"'*) return 0 ;;
        esac
        sleep 0.2
    done
    return 1
}

mkdir -p "$WORK/src/docs"
echo "alpha content"  > "$WORK/src/a.txt"
echo "beta content"   > "$WORK/src/b.txt"
echo "nested content" > "$WORK/src/docs/c.txt"

echo "config refusal"
# Each of these used to start the daemon normally, running on defaults the
# operator never chose (UAT B081). Now each must stop it before it binds,
# naming the file, the line and what is wrong.
refuses() {  # refuses <name> <expected stderr fragment> <config lines...>
    local name="$1" want="$2"; shift 2
    local cfg="$WORK/bad.yaml"
    {
        printf 'db:\n  path: %s/bad.db\nhttp:\n  socket: %s/bad.sock\n  port: 0\n' "$WORK" "$WORK"
        printf '%s\n' "$@"
    } > "$cfg"
    chmod 600 "$cfg"
    local out rc
    out=$(C_BACKUP_CONFIG_PATH="$cfg" timeout 10 "$BIN" 2>&1)
    rc=$?
    if [ "$rc" -ne 1 ]; then
        bad "$name: refuses to start" "exit 1" "exit $rc: $out"
    else
        contains "$name: refuses to start" "$want" "$out"
    fi
    [ -e "$WORK/bad.sock" ] && bad "$name: never listened" "no socket" "socket created"
    rm -f "$WORK/bad.db" "$WORK/bad.db-wal" "$WORK/bad.db-shm" "$WORK/bad.sock"
}
refuses "malformed yaml" "bad.yaml:6: error: flow collections" 'log: [unclosed'
refuses "unknown log level" "bad.yaml:7: error: log.level: 'chatty' is not one of" \
    'log:' '  level: chatty'
refuses "unknown top-level key" "bad.yaml:6: error: 'verbose: true' at the top level" \
    'verbose: true'
refuses "unknown section" "bad.yaml:7: error: unknown key 'mystery.thing'" \
    'mystery:' '  thing: 1'
refuses "typo'd key" "unknown key 'security.key_dri' (did you mean 'security.key_dir'?)" \
    'security:' '  key_dri: /x'
refuses "integer key not an integer" "bad.yaml:7: error: jobs.max_parallel: 'eighty' is not an integer" \
    'jobs:' '  max_parallel: eighty'
refuses "nested mapping" "bad.yaml:8: error: indented deeper" \
    'jobs:' '  max_parallel: 2' '    extra: 1'
# Both lines, not just the first: the second problem is what a parser that
# stops at the first error would never reach. c-utils 1.5.1 no longer repeats
# the first problem with an "(and N more)" count; it points at the list.
refuses "every problem reported" "bad.yaml:8: error: unknown key 'jobs.max_paralel'" \
    'jobs:' '  max_parallel: x' '  max_paralel: 2'
# Values of the right type the daemon cannot honour (B081 reopened): each
# used to start, port 99999 logging "listening on 0.0.0.0:99999".
refuses "port out of range" "bad.yaml:6: error: http.port: '99999' is not between 0 and 65535" \
    '  port: 99999'
refuses "zero workers" "bad.yaml:7: error: jobs.max_parallel: '0' is not between 1 and 256" \
    'jobs:' '  max_parallel: 0'
refuses "negative retention" "retention.keep_daily: '-1' is not between 0 and" \
    'retention:' '  keep_daily: -1'
refuses "switch not 0 or 1" "auth.cookie_secure: '2' is not between 0 and 1" \
    'auth:' '  cookie_secure: 2'
refuses "bind not an address" "bad.yaml:6: error: http.bind: 'not-an-ip' is not an IPv4 address" \
    '  bind: not-an-ip'
refuses "file cache mode" "backup.file_cache_match: 'fast' is not one of auto, strict, relaxed" \
    'backup:' '  file_cache_match: fast'
refuses "unsettled policy" "backup.unsettled: 'maybe' is not one of store, skip" \
    'backup:' '  unsettled: maybe'
refuses "store workers out of range" "backup.store_workers: '65' is not between 0 and 64" \
    'backup:' '  store_workers: 65'
refuses "tab indentation" "bad.yaml:6: error: tab in the indentation" \
    "$(printf '\tbind: 127.0.0.1')"
# log.level among other problems: it was dropped from the list.
out=$(printf 'db:\n  path: %s/bad.db\nhttp:\n  socket: %s/bad.sock\n  port: abc\nlog:\n  level: verbose\njobs:\n  max_parallel: -1\n' \
      "$WORK" "$WORK" > "$WORK/bad.yaml"; chmod 600 "$WORK/bad.yaml"
      C_BACKUP_CONFIG_PATH="$WORK/bad.yaml" timeout 10 "$BIN" 2>&1)
contains "every problem: port" "bad.yaml:5: error: http.port: 'abc'" "$out"
contains "every problem: log.level" "bad.yaml:7: error: log.level: 'verbose'" "$out"
contains "every problem: max_parallel" "bad.yaml:9: error: jobs.max_parallel: '-1'" "$out"
# db.path is required: there is no default to fall back on.
printf 'http:\n  socket: %s/bad.sock\n  port: 0\n' "$WORK" > "$WORK/bad.yaml"
out=$(C_BACKUP_CONFIG_PATH="$WORK/bad.yaml" timeout 10 "$BIN" 2>&1)
contains "missing db.path refused, naming the file" \
    "bad.yaml: error: required config key 'db.path' is missing or empty" "$out"
# CRLF line endings and a UTF-8 byte order mark are legal YAML and start.
for kind in crlf bom; do
    body=$(printf 'db:\n  path: %s/bad.db\nhttp:\n  socket: %s/bad.sock\n  port: 0\n\nlog:\n  level: info\n' "$WORK" "$WORK")
    if [ "$kind" = crlf ]; then printf '%s\n' "$body" | sed 's/$/\r/' > "$WORK/bad.yaml"
    else { printf '\xEF\xBB\xBF'; printf '%s\n' "$body"; } > "$WORK/bad.yaml"; fi
    chmod 600 "$WORK/bad.yaml"
    C_BACKUP_CONFIG_PATH="$WORK/bad.yaml" timeout 3 "$BIN" > "$WORK/$kind.log" 2>&1
    rc=$?
    check "$kind config starts" "124" "$rc"
    case "$(cat "$WORK/$kind.log")" in
        *error:*) bad "$kind config: no errors" "none" "$(grep error: "$WORK/$kind.log")" ;;
        *) ok "$kind config: no errors" ;;
    esac
    rm -f "$WORK/bad.db" "$WORK/bad.db-wal" "$WORK/bad.db-shm" "$WORK/bad.sock" "$WORK/$kind.log"
done
# OWN-45-LIVE: the daemon applies backup.unsettled, store by default and
# skip when set; test_settle_fault shows each value's behaviour through the
# same parser.
for pol in default skip; do
    body=$(printf 'db:\n  path: %s/bad.db\nhttp:\n  socket: %s/bad.sock\n  port: 0\nlog:\n  level: info\n' "$WORK" "$WORK")
    [ "$pol" = skip ] && body=$(printf '%s\nbackup:\n  unsettled: skip\n' "$body")
    printf '%s\n' "$body" > "$WORK/bad.yaml"
    chmod 600 "$WORK/bad.yaml"
    C_BACKUP_CONFIG_PATH="$WORK/bad.yaml" timeout 3 "$BIN" > "$WORK/pol.log" 2>&1
    want=$([ "$pol" = skip ] && echo skip || echo store)
    contains "backup.unsettled $pol is applied as $want" "backup.unsettled: $want" "$(cat "$WORK/pol.log")"
    rm -f "$WORK/bad.db" "$WORK/bad.db-wal" "$WORK/bad.db-shm" "$WORK/bad.sock" "$WORK/pol.log"
done
# An environment override has no line, so it is named instead.
printf 'db:\n  path: %s/bad.db\nhttp:\n  socket: %s/bad.sock\n  port: 0\n' \
    "$WORK" "$WORK" > "$WORK/bad.yaml"
out=$(C_BACKUP_JOBS_MAX_PARALLEL=lots C_BACKUP_CONFIG_PATH="$WORK/bad.yaml" \
      timeout 10 "$BIN" 2>&1)
contains "bad environment override refused" \
    "environment C_BACKUP_JOBS_MAX_PARALLEL (jobs.max_parallel): 'lots' is not an integer" "$out"
check "and nothing was opened" "" "$(ls "$WORK"/bad.db* 2>/dev/null)"
rm -f "$WORK/bad.yaml"

# The config the packages and the image ship has to pass the same checks,
# or an edit to it would ship a daemon that will not start. Paths are
# redirected; everything else is as shipped.
sed -e "s#/var/lib/c-backupd/state.db#$WORK/pkg.db#" \
    -e "s#/run/c-backupd/c-backupd.sock#$WORK/pkg.sock#" \
    -e "s#^  port: 8765#  port: 0#" \
    "$(dirname "$0")/../packaging/config.yaml" > "$WORK/pkg.yaml"
chmod 600 "$WORK/pkg.yaml"
C_BACKUP_CONFIG_PATH="$WORK/pkg.yaml" C_BACKUP_SECURITY_KEY_DIR="$WORK/pkgkeys" \
    "$BIN" > "$WORK/pkg.log" 2>&1 &
PKG_PID=$!
for _ in $(seq 1 50); do [ -S "$WORK/pkg.sock" ] && break; sleep 0.1; done
if [ -S "$WORK/pkg.sock" ]; then ok "shipped config starts the daemon"
else bad "shipped config starts the daemon" "a socket" "$(cat "$WORK/pkg.log")"; fi
kill -TERM "$PKG_PID" 2>/dev/null
wait "$PKG_PID" 2>/dev/null

cat > "$WORK/config.yaml" <<EOF
db:
  path: $WORK/state.db
http:
  socket: $SOCK
  port: $PORT
  bind: 127.0.0.1
log:
  level: warn
auth:
  cookie_secure: 1
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
# Remote clients: the desktop app on another machine. Everything below the
# setup checks goes over TCP unless it says socket.
tcp_body() { curl -s "$@" 2>/dev/null; }
URL="http://127.0.0.1:$PORT"

contains "setup unconfigured" '"configured":false' "$(sock_body /api/auth/setup)"
# Before setup the network gets the probes and a 403 that says what to do -
# not the whole API, which is what the lifted airies-ups behaviour gave it.
check "tcp healthz before setup"   200 "$(tcp_code /healthz)"
check "tcp readyz before setup"    200 "$(tcp_code /readyz)"
check "tcp metrics before setup"   200 "$(tcp_code /metrics)"
contains "tcp setup state readable" '"configured":false' "$(tcp_body "$URL/api/auth/setup")"
check "tcp api refused before setup" 403 "$(tcp_code /repos)"
contains "refusal names the fix" "c-backup auth setup" "$(tcp_body "$URL/repos")"
check "tcp login refused before setup" 403 "$(tcp_code /api/auth/login -X POST \
     -d '{"password":"correcthorsebattery"}')"
# The first password is set on the host. Over TCP that would hand the daemon
# to whoever reached the port first.
check "tcp setup refused" 403 "$(tcp_code /api/auth/setup -X POST \
     -d '{"password":"correcthorsebattery"}')"
contains "still unconfigured" '"configured":false' "$(sock_body /api/auth/setup)"

check "short password refused" 400 "$(curl -s -o /dev/null -w '%{http_code}' -X POST \
     --unix-socket "$SOCK" -d '{"password":"short"}' http://localhost/api/auth/setup)"
# B112: a password of the wrong type was told it was required.
check "a non-string password is named" '{"error":"password must be a string"} 400' \
    "$(curl -s -w ' %{http_code}' -X POST --unix-socket "$SOCK" \
       -d '{"password":["correcthorsebattery"]}' http://localhost/api/auth/setup)"
check "socket setup" 200 "$(curl -s -o /dev/null -w '%{http_code}' -X POST \
     --unix-socket "$SOCK" -d '{"password":"correcthorsebattery"}' http://localhost/api/auth/setup)"
check "setup twice refused" 409 "$(curl -s -o /dev/null -w '%{http_code}' -X POST \
     --unix-socket "$SOCK" -d '{"password":"anotherpassword"}' http://localhost/api/auth/setup)"

check "tcp locked after setup" 401 "$(tcp_code /repos)"
# The probes stay public once a password exists. The desktop app asks /readyz
# before it logs in, so a 401 here made every remote connection fail.
check "tcp healthz after setup"  200 "$(tcp_code /healthz)"
check "tcp readyz after setup"   200 "$(tcp_code /readyz)"
check "tcp metrics after setup"  200 "$(tcp_code /metrics)"
check "wrong password" 401 "$(tcp_code /api/auth/login -X POST -d '{"password":"wrong"}')"

TOKEN=$(tcp_body -X POST -d '{"password":"correcthorsebattery"}' "$URL/api/auth/login" \
        | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
if [ -n "$TOKEN" ]; then ok "login returns a token"; else bad "login returns a token" "non-empty" "empty"; fi
check "bearer accepted" 200 "$(tcp_code /repos -H "Authorization: Bearer $TOKEN")"
check "bare token accepted" 200 "$(tcp_code /repos -H "Authorization: $TOKEN")"
check "bad token refused" 401 "$(tcp_code /repos -H "Authorization: Bearer nonesuch")"
check "cookie accepted" 200 "$(tcp_code /repos -b "auth=$TOKEN")"
# B082: the key was read into the route context and then cleared when the
# routes registered, so the flag was parsed and never honoured.
LOGIN_HDRS=$(curl -s -D - -o /dev/null -X POST -d '{"password":"correcthorsebattery"}' \
             "http://127.0.0.1:$PORT/api/auth/login")
contains "cookie_secure sets Secure on the session cookie" "; Secure" \
    "$(printf '%s' "$LOGIN_HDRS" | grep -i '^set-cookie:')"
# No CORS: nothing here is a browser client, and a wildcard origin let any
# page in a browser on the host read the API.
contains "no CORS header" "no-cors" \
    "$(curl -s -D - -o /dev/null -H "Authorization: Bearer $TOKEN" "$URL/repos" \
       | grep -qi '^access-control-allow-origin' && echo cors || echo no-cors)"
check "preflight is not an auth bypass" 401 "$(tcp_code /repos -X OPTIONS)"
# The socket is gated by filesystem permissions, not tokens.
check "socket bypasses auth" 200 "$(sock_code /repos)"

echo "repos"
contains "register" '"id":1' \
    "$(curl -s -X POST --unix-socket "$SOCK" -d "{\"path\":\"$WORK/repo\"}" http://localhost/repos)"
contains "get repo" '"uuid"' "$(sock_body /repos/1)"
# A repository with no snapshot is counted. Its age runs from registration,
# so it trips an age alert like any stale repository instead of reading 0.
contains "never backed up is counted" "cbackup_repos_never_backed_up 1" "$(sock_body /metrics)"
# Alert delivery is countable (UAT B083): present, as zeros, with Pushover
# unconfigured, so an alert rule on them is never matching an absent series.
METRICS=$(sock_body /metrics)
contains "alerts sent metric"    "cbackup_alerts_sent_total 0" "$METRICS"
contains "alerts failed metric"  "cbackup_alerts_failed_total 0" "$METRICS"
contains "alerts failed is a counter" "# TYPE cbackup_alerts_failed_total counter" "$METRICS"
contains "alerts pending metric" "cbackup_alerts_pending 0" "$METRICS"
# B092: the ages are the stalest repository's, and an info series says which
# one, so an alert can name it without a series per repository.
contains "stalest backup is named" \
    'cbackup_stalest_snapshot_repo_info{repo_id="1",label=""} 1' "$METRICS"
contains "stalest verify is named" \
    'cbackup_stalest_verify_repo_info{repo_id="1",label=""} 1' "$METRICS"
check "unknown repo" 404 "$(sock_code /repos/99)"

echo "methods"
# B085: every method the router did not recognise was served as a GET, so
# PATCH /repos/1 answered 200 with the repository and looked like an update
# that changed nothing. A known path asked with the wrong method is a 405
# naming the methods it does answer; an unknown path stays a 404.
sock_method() {  # sock_method <METHOD> <path> -> "<code> <Allow value>"
    # HEAD through -I: with -X HEAD curl waits for a body that never comes.
    local how=(-X "$1"); [ "$1" = HEAD ] && how=(-I)
    curl -s -o /dev/null -D - "${how[@]}" --max-time 5 --unix-socket "$SOCK" "http://localhost$2" \
        | tr -d '\r' | awk 'NR==1 {c=$2} tolower($1)=="allow:" {sub(/^[^:]*: */,""); a=$0}
                          END {print c, a}'
}
check "PATCH a repo"            "405 GET, DELETE" "$(sock_method PATCH /repos/1)"
check "repo still there after"  200               "$(sock_code /repos/1)"
check "PUT a repo"              "405 GET, DELETE" "$(sock_method PUT /repos/1)"
check "POST a repo"             "405 GET, DELETE" "$(sock_method POST /repos/1)"
check "PATCH the repo list"     "405 GET, POST"   "$(sock_method PATCH /repos)"
check "DELETE the repo list"    "405 GET, POST"   "$(sock_method DELETE /repos)"
check "POST a read-only path"   "405 GET"         "$(sock_method POST /repos/1/journal)"
check "DELETE a read-only path" "405 GET"         "$(sock_method DELETE /repos/1/summary)"
check "GET a job submission"    "405 POST"        "$(sock_method GET /repos/1/backup)"
check "OPTIONS on schedules"    "405 GET, POST"   "$(sock_method OPTIONS /repos/1/schedules)"
check "GET one schedule"        "405 PUT, DELETE" "$(sock_method GET /repos/1/schedules/5)"
check "PUT a snapshot"          "405 DELETE"      "$(sock_method PUT /repos/1/snapshots/5)"
check "POST a note"             "405 GET, PUT"    "$(sock_method POST /repos/1/snapshots/5/note)"
check "GET a cancel"            "405 POST"        "$(sock_method GET /jobs/5/cancel)"
check "POST a job"              "405 GET"         "$(sock_method POST /jobs/5)"
check "PUT a job stream"        "405 GET"         "$(sock_method PUT /jobs/stream/5)"
check "PUT healthz"             "405 GET"         "$(sock_method PUT /healthz)"
check "PATCH an auth route"     "405 GET, POST"   "$(sock_method PATCH /api/auth/setup)"
check "HEAD is a GET"           "200 "            "$(sock_method HEAD /repos/1)"
check "unknown path, any method" "404 "           "$(sock_method PATCH /nonexistent)"
check "unknown repo sub-path"   "404 "            "$(sock_method POST /repos/1/nonexistent)"
check "PATCH an unknown repo"   "404 "            "$(sock_method PATCH /repos/999)"
# And DELETE of a repository that does not exist is a 404, not a 204.
check "DELETE an unknown repo"  "404 "            "$(sock_method DELETE /repos/999)"
# And every route under it: these answered 200 with empty results, which a
# script cannot tell from an empty repository that exists.
check "unknown repo: summary" 404 "$(sock_code /repos/99/summary)"
check "unknown repo: snapshots" 404 "$(sock_code /repos/99/snapshots)"
check "unknown repo: backup" 404 "$(curl -s -o /dev/null -w '%{http_code}' -X POST --unix-socket "$SOCK" \
     -d '{"paths":["/tmp"]}' http://localhost/repos/99/backup)"
check "unknown repo: policy" 404 "$(curl -s -o /dev/null -w '%{http_code}' -X PUT --unix-socket "$SOCK" \
     -d '{"key":"retention.keep_daily","value":"1"}' http://localhost/repos/99/policy)"

mkdir -p "$WORK/foreign" && echo "not ours" > "$WORK/foreign/data.txt"
check "refuses foreign dir" 400 \
    "$(curl -s -o /dev/null -w '%{http_code}' -X POST --unix-socket "$SOCK" \
       -d "{\"path\":\"$WORK/foreign\"}" http://localhost/repos)"
check "foreign dir untouched" "data.txt" "$(ls "$WORK/foreign")"
# purge was accepted and deleted nothing. Refused now, and refused before
# anything happens: the repository is still registered afterwards.
check "purge is refused" 400 "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE \
     --unix-socket "$SOCK" 'http://localhost/repos/1?purge=1')"
check "a refused purge deregisters nothing" 200 "$(sock_code /repos/1)"

echo "backup"
BACKUP_RESP=$(curl -s -X POST --unix-socket "$SOCK" -d "{\"paths\":[\"$WORK/src\"]}" \
              http://localhost/repos/1/backup)
contains "submit" '"job_id"' "$BACKUP_RESP"
BACKUP_JOB=$(job_id "$BACKUP_RESP")
wait_job "$BACKUP_JOB"
contains "job completed" '"state":"completed"' "$(sock_body "/jobs/$BACKUP_JOB")"
# A finished job says what it did, and how big and how long. It said only
# "completed" with the last phase's entry count.
JOB_BODY=$(sock_body "/jobs/$BACKUP_JOB")
contains "backup result names its snapshot" '"result":{"snapshot_id":1,' "$JOB_BODY"
contains "backup result counts files" '"files":3,' "$JOB_BODY"
contains "no large files, no store pool" '"store_workers":1,' "$JOB_BODY"
# B086: what the backup wrote, by category. Every object goes into a pack,
# so the pack payload and its parity are the only categories there are.
contains "backup result counts bytes written" '"bytes_written":' "$JOB_BODY"
BW=$(printf '%s' "$JOB_BODY" | sed -n 's/.*"bytes_written":\([0-9]*\),.*/\1/p')
BWP=$(printf '%s' "$JOB_BODY" | sed -n 's/.*"bytes_written_pack":\([0-9]*\),.*/\1/p')
BWPP=$(printf '%s' "$JOB_BODY" | sed -n 's/.*"bytes_written_pack_parity":\([0-9]*\)[,}].*/\1/p')
check "bytes written is pack plus parity" "${BW:-none}" "$(( ${BWP:-0} + ${BWPP:-0} ))"
# The result is exactly these keys, in this order: nothing for loose
# objects or a pack pass, which no longer exist, and the files left out
# (B118) and stored as read (OWN-45).
RESULT_KEYS=$(printf '%s' "$JOB_BODY" | sed -n 's/.*"result":{\([^}]*\)}.*/\1/p' \
              | grep -o '"[a-z_]*":' | tr -d '":' | tr '\n' ' ')
check "the backup result carries exactly its keys" \
    "snapshot_id files dirs files_unchanged bytes_logical objects_new bytes_stored trees_new trees_reused store_workers bytes_written bytes_written_pack bytes_written_pack_parity files_skipped skipped skipped_listed files_changed changed changed_listed changed_unsettled unsettled unsettled_listed " \
    "$RESULT_KEYS"
contains "and a clean backup left nothing out" '"files_skipped":0,"skipped":[],"skipped_listed":0,' "$JOB_BODY"
contains "and saw nothing change under it" '"files_changed":0,"changed":[],"changed_listed":0,' "$JOB_BODY"
contains "and kept nothing that never settled" '"changed_unsettled":0,"unsettled":[],"unsettled_listed":0}' "$JOB_BODY"
contains "job reports bytes" '"bytes_total":' "$JOB_BODY"
contains "job reports when it started" '"started":1' "$JOB_BODY"
contains "job ids are numbers" "\"id\":$BACKUP_JOB," "$JOB_BODY"
contains "the job list carries the same detail" '"elapsed_s":' "$(sock_body /jobs)"
contains "snapshot listed" '"id":1,' "$(sock_body /repos/1/snapshots)"
contains "tree lists root" '"name"' "$(sock_body '/repos/1/snapshots/1/tree?parent=0')"
contains "search finds file" 'a.txt' "$(sock_body '/repos/1/search?q=txt')"
contains "a search result says whether it may be torn" '"may_be_torn":false' \
    "$(sock_body '/repos/1/search?q=txt')"

echo "sources"
# B078: anything the loop could not read was skipped, and the route replaces
# wholesale - so a malformed request answered 204 having deleted the
# configuration. Every element is checked now and a refusal changes nothing.
put_sources() { curl -s -o /dev/null -w '%{http_code}' -X PUT --unix-socket "$SOCK" \
                     -d "$1" http://localhost/repos/1/sources; }
check "sources set" 200 "$(put_sources "{\"sources\":[\"$WORK/src\"],\"exclude\":[\"*.tmp\"]}")"
check "sources as a string refused"   400 "$(put_sources '{"sources":"/x"}')"
check "missing sources refused"       400 "$(put_sources '{}')"
check "a non-string source refused"   400 "$(put_sources '{"sources":[123]}')"
check "an empty source refused"       400 "$(put_sources '{"sources":[""]}')"
check "a relative source refused"     400 "$(put_sources '{"sources":["rel/path"]}')"
check "an empty exclude refused"      400 "$(put_sources "{\"sources\":[\"$WORK/src\"],\"exclude\":[\"\"]}")"
contains "a refusal leaves the configuration alone" "$WORK/src" "$(sock_body /repos/1/sources)"
ABSENT=$(curl -s -X PUT --unix-socket "$SOCK" -d '{"sources":["/no/such/mount"]}' \
       http://localhost/repos/1/sources)
contains "an absent source is accepted with a warning" "does not exist on the daemon's host" \
    "$ABSENT"
# B097: the warning promised the source would be skipped, but a backup with
# a missing source fails (B077). It has to say what actually happens.
contains "and the warning says a backup fails" "a backup fails while it is missing" "$ABSENT"
check "and does not promise a skip" "" "$(printf '%s' "$ABSENT" | grep -o 'skip')"
contains "sources are stored normalised" '"sources":["/a/b"]' \
    "$(put_sources '{"sources":["/a//b/./"]}' >/dev/null; sock_body /repos/1/sources)"
check "sources cleared" 200 "$(put_sources '{"sources":[]}')"

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
check "delete tag" 200 "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE --unix-socket "$SOCK" \
     http://localhost/repos/1/tags/milestone)"

# B037, B038: metadata for a snapshot that does not exist, a tag with no
# name, and removing a tag that is not there were all accepted in silence.
sock_send() {  # sock_send <method> <path> [body] -> "<code> <body>"
    curl -s -w ' %{http_code}' -X "$1" --unix-socket "$SOCK" ${3:+-d "$3"} "http://localhost$2"
}
check "tag on a missing snapshot is a 404" 404 "$(curl -s -o /dev/null -w '%{http_code}' -X POST \
     --unix-socket "$SOCK" -d '{"name":"ghost","snap_id":999}' http://localhost/repos/1/tags)"
check "note on a missing snapshot is a 404" 404 "$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
     --unix-socket "$SOCK" -d '{"body":"x"}' http://localhost/repos/1/snapshots/999/note)"
check "an empty tag name is a 400" 400 "$(curl -s -o /dev/null -w '%{http_code}' -X POST \
     --unix-socket "$SOCK" -d '{"name":"","snap_id":1}' http://localhost/repos/1/tags)"
contains "removing a missing tag is a 404" '{"error":"no such tag"} 404' \
    "$(sock_send DELETE /repos/1/tags/milestone)"
check "nothing was stored for the missing snapshot" '{"tags":[]}' "$(sock_body /repos/1/tags)"

# B036: a tag name is a path segment in DELETE, so one that cannot be a
# segment ("a b", "a/b", "x?y") could be set and then never removed.
for n in 'a/b' 'a b' 'x?y' '.hidden' '..'; do
    check "tag name '$n' is refused" 400 "$(curl -s -o /dev/null -w '%{http_code}' -X POST \
         --unix-socket "$SOCK" -d "{\"name\":\"$n\",\"snap_id\":1}" http://localhost/repos/1/tags)"
done
check "a 65-character tag name is refused" 400 "$(curl -s -o /dev/null -w '%{http_code}' -X POST \
     --unix-socket "$SOCK" -d "{\"name\":\"$(printf 'a%.0s' $(seq 1 65))\",\"snap_id\":1}" \
     http://localhost/repos/1/tags)"
check "a dotted, dashed name is fine" 200 "$(curl -s -o /dev/null -w '%{http_code}' -X POST \
     --unix-socket "$SOCK" -d '{"name":"v1.2_pre-upgrade","snap_id":1}' http://localhost/repos/1/tags)"
check "and removable" 200 "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE \
     --unix-socket "$SOCK" http://localhost/repos/1/tags/v1.2_pre-upgrade)"

# B034: policy stored any key and any value and said ok. Each of these was
# accepted and then ignored or misread by the retention pass.
put_policy() { sock_send PUT /repos/1/policy "{\"key\":\"$1\",\"value\":\"$2\"}"; }
OUT=$(put_policy retention.keep_hourly 5)
contains "an unknown policy key is a 400" ' 400' "$OUT"
contains "and the refusal lists the valid keys" 'retention.keep_yearly' "$OUT"
contains "a non-numeric count is a 400" ' 400' "$(put_policy retention.keep_daily banana)"
contains "a negative count is a 400" ' 400' "$(put_policy retention.keep_weekly -3)"
contains "a signed count is a 400" ' 400' "$(put_policy retention.keep_weekly +3)"
# maintenance.auto_pack went with the pack job: there is no pack pass left
# for it to switch. Refused as unknown, not stored and ignored.
OUT=$(put_policy maintenance.auto_pack false)
contains "the retired auto_pack key is a 400" ' 400' "$OUT"
contains "refused as unknown" "unknown policy key 'maintenance.auto_pack'" "$OUT"
POL=$(sock_body /repos/1/policy)
contains "a refused value leaves the stored one" '"key":"retention.keep_daily","value":14' "$POL"
case "$POL" in *keep_hourly*|*auto_pack*) bad "an unknown key is not stored" "absent" "$POL" ;;
               *) ok "an unknown key is not stored" ;; esac

# B040: the defaults list every key in force where a repository has not set
# it, which with auto_pack gone is exactly the retention keys.
check "policy defaults are the retention keys" \
    '"defaults":[{"key":"retention.keep_snaps","value":1},{"key":"retention.keep_daily","value":7},{"key":"retention.keep_weekly","value":4},{"key":"retention.keep_monthly","value":12},{"key":"retention.keep_yearly","value":3}]}' \
    "$(sock_body /repos/1/policy | sed -n 's/.*\("defaults":.*\)/\1/p')"

# B008: values are typed, so the PUT takes them as the GET serves them.
check "a count may be sent as a number" '{"ok":true} 200' \
    "$(sock_send PUT /repos/1/policy '{"key":"retention.keep_monthly","value":6}')"
contains "but a boolean is not a count" ' 400' \
    "$(sock_send PUT /repos/1/policy '{"key":"retention.keep_monthly","value":true}')"
contains "and both read back typed" '"key":"retention.keep_monthly","value":6' \
    "$(sock_body /repos/1/policy)"

echo "request field types"
# B112: a field present with the wrong JSON type read as absent. An array
# label registered a repository with no label and answered 201, and a
# snap_id of 1e999 was told it was required. Each is a 400 naming the field
# and what it must be, and nothing is created.
for lbl in '["a"]' '{"a":1}' '5' 'true' 'null'; do
    R=$(sock_send POST /repos "{\"path\":\"$WORK/typedrepo\",\"label\":$lbl}")
    check "label $lbl is refused" '{"error":"label must be a string"} 400' "$R"
done
check "and no repository was made" "absent" \
    "$([ -e "$WORK/typedrepo" ] && echo present || echo absent)"
case "$(sock_body /repos)" in *typedrepo*) bad "and none was registered" "absent" "listed" ;;
                              *) ok "and none was registered" ;; esac
check "a non-string path is refused" '{"error":"path must be a string"} 400' \
    "$(sock_send POST /repos '{"path":["/x"]}')"
check "a missing path is still required" '{"error":"path is required"} 400' \
    "$(sock_send POST /repos '{"label":"x"}')"
check "a non-string encrypt is refused" '{"error":"encrypt must be a string"} 400' \
    "$(sock_send POST /repos "{\"path\":\"$WORK/typedrepo\",\"encrypt\":1}")"

tag_snap() { sock_send POST /repos/1/tags "{\"name\":\"typed\",\"snap_id\":$1}"; }
RANGE='snap_id is out of range; it must be from 1 to 4294967295'
check "snap_id 1e999 is out of range"   "{\"error\":\"$RANGE\"} 400" "$(tag_snap 1e999)"
check "snap_id -1 is out of range"      "{\"error\":\"$RANGE\"} 400" "$(tag_snap -1)"
check "snap_id 0 is out of range"       "{\"error\":\"$RANGE\"} 400" "$(tag_snap 0)"
check "snap_id past uint32 is out of range" "{\"error\":\"$RANGE\"} 400" "$(tag_snap 4294967296)"
check "snap_id 99999999999999999999 is out of range" "{\"error\":\"$RANGE\"} 400" \
    "$(tag_snap 99999999999999999999)"
check "snap_id 1.5 is not whole" '{"error":"snap_id must be a whole number"} 400' "$(tag_snap 1.5)"
check "snap_id as a string" '{"error":"snap_id must be a number"} 400' "$(tag_snap '"1"')"
check "snap_id as a list"   '{"error":"snap_id must be a number"} 400' "$(tag_snap '[1]')"
check "a missing snap_id is still required" '{"error":"snap_id is required"} 400' \
    "$(sock_send POST /repos/1/tags '{"name":"typed"}')"
check "move must be a boolean" '{"error":"move must be true or false"} 400' \
    "$(sock_send POST /repos/1/tags '{"name":"typed","snap_id":1,"move":1}')"
check "no typed tag was stored" '{"tags":[]}' "$(sock_body /repos/1/tags)"

check "a note body must be a string" '{"error":"body must be a string"} 400' \
    "$(sock_send PUT /repos/1/snapshots/1/note '{"body":["x"]}')"
check "a fractional policy value" '{"error":"value must be a whole number"} 400' \
    "$(sock_send PUT /repos/1/policy '{"key":"retention.keep_daily","value":1.5}')"
check "a policy value of 1e999" \
    '{"error":"value is out of range; it must be from -9007199254740992 to 9007199254740992"} 400' \
    "$(sock_send PUT /repos/1/policy '{"key":"retention.keep_daily","value":1e999}')"
check "a policy value that is a list" \
    '{"error":"value must be a string, a whole number, or true or false"} 400' \
    "$(sock_send PUT /repos/1/policy '{"key":"retention.keep_daily","value":[1]}')"
check "a non-string policy key" '{"error":"key must be a string"} 400' \
    "$(sock_send PUT /repos/1/policy '{"key":7,"value":1}')"
contains "and the stored value is untouched" '"key":"retention.keep_daily","value":14' \
    "$(sock_body /repos/1/policy)"

# The backup body: each of these used to be dropped and the backup run
# anyway, from the configured sources or with the wrong options.
check "a backup label must be a string" '{"error":"label must be a string"} 400' \
    "$(sock_send POST /repos/1/backup '{"label":["x"]}')"
# OWN-33: a long label overran the record writer's buffer and put daemon
# stack into the repository; a control character is not text.
LONG_LABEL=$(head -c 1000 /dev/zero | tr '\0' x)
LABEL_RULE='{"error":"label must be at most 256 bytes of printable text"} 400'
check "a 1,000-byte backup label is refused" "$LABEL_RULE" \
    "$(sock_send POST /repos/1/backup "{\"paths\":[\"$WORK/src\"],\"label\":\"$LONG_LABEL\"}")"
check "a label with a newline is refused" "$LABEL_RULE" \
    "$(sock_send POST /repos/1/backup "{\"paths\":[\"$WORK/src\"],\"label\":\"a\\nb\"}")"
check "a label with a C1 control is refused" "$LABEL_RULE" \
    "$(sock_send POST /repos/1/backup "{\"paths\":[\"$WORK/src\"],\"label\":\"a\\u009bb\"}")"
# B115: cJSON decodes \u0000 into a C string that ends there, so the label
# was stored as "nul" with a 202 and a path named a shorter one.
check "a label with an escaped NUL is refused" "$LABEL_RULE" \
    "$(sock_send POST /repos/1/backup "{\"paths\":[\"$WORK/src\"],\"label\":\"nul\\u0000here\"}")"
check "a backup path with an escaped NUL is refused" \
    '{"error":"paths must not contain a NUL character"} 400' \
    "$(sock_send POST /repos/1/backup "{\"paths\":[\"$WORK/src\\u0000/x\"]}")"
check "a schedule label with an escaped NUL is refused" "$LABEL_RULE" \
    "$(sock_send POST /repos/1/schedules '{"action":"backup","label":"a\u0000b","trigger":{"type":"interval","seconds":60}}')"
check "a tag name with an escaped NUL is refused" \
    '{"error":"name must not contain a NUL character"} 400' \
    "$(sock_send POST /repos/1/tags '{"name":"t\u0000x","snap_id":1}')"
check "a NUL in a nested trigger field is refused by name" \
    '{"error":"every must not contain a NUL character"} 400' \
    "$(sock_send POST /repos/1/schedules '{"action":"gc","trigger":{"type":"calendar","every":"day\u0000","at":"01:00"}}')"
OUT=$(sock_send POST /repos/1/tags '{"name":"t\\u0000","snap_id":1}')
case "$OUT" in
    *NUL*) bad "an escaped backslash before u0000 is not a NUL" "the tag-name rule" "$OUT" ;;
    *400)  ok "an escaped backslash before u0000 is not a NUL" ;;
    *)     bad "an escaped backslash before u0000 is not a NUL" "a 400" "$OUT" ;;
esac
check "a query value with %00 is refused" \
    '{"error":"q must not contain a NUL character"} 400' \
    "$(sock_send GET '/repos/1/search?q=a%00b')"
check "backup paths must be a list" '{"error":"paths must be a list of strings"} 400' \
    "$(sock_send POST /repos/1/backup '{"paths":"/x"}')"
check "a non-string backup path" \
    '{"error":"paths must be a list of strings; entry 2 is not one"} 400' \
    "$(sock_send POST /repos/1/backup "{\"paths\":[\"$WORK/src\",5]}")"
check "one_filesystem must be a flag" '{"error":"one_filesystem must be true or false"} 400' \
    "$(sock_send POST /repos/1/backup "{\"paths\":[\"$WORK/src\"],\"one_filesystem\":\"yes\"}")"
check "restore overwrite must be a flag" '{"error":"overwrite must be true or false"} 400' \
    "$(sock_send POST /repos/1/snapshots/1/restore "{\"dest\":\"$WORK/typed\",\"overwrite\":\"yes\"}")"
check "restore subpath must be a string" '{"error":"subpath must be a string"} 400' \
    "$(sock_send POST /repos/1/snapshots/1/restore "{\"dest\":\"$WORK/typed\",\"subpath\":[\"/a\"]}")"
check "restore dest must be a string" '{"error":"dest must be a string"} 400' \
    "$(sock_send POST /repos/1/snapshots/1/restore '{"dest":5}')"
check "grep max_files must be a number" '{"error":"max_files must be a number"} 400' \
    "$(sock_send POST /repos/1/grep '{"pattern":"x","max_files":"10"}')"
check "grep snapshot of 1e999" \
    '{"error":"snapshot is out of range; it must be from 1 to 4294967295"} 400' \
    "$(sock_send POST /repos/1/grep '{"pattern":"x","snapshot":1e999}')"
check "nothing was restored by a refusal" "absent" \
    "$([ -e "$WORK/typed" ] && echo present || echo absent)"

# B112: the body is checked before the repository. Repository 1 has no
# sources configured (cleared above), and a backup schedule naming none
# was refused for that even when its trigger was malformed, so the client
# fixed the sources and then learned about the trigger.
contains "repository 1 has no sources here" '"sources":[]' "$(sock_body /repos/1/sources)"
sched() { sock_send POST /repos/1/schedules "$1"; }
check "a malformed trigger is reported before missing sources" \
    "{\"error\":\"trigger: unknown type 'cron'\"} 400" \
    "$(sched '{"action":"backup","trigger":{"type":"cron"}}')"
check "a trigger of the wrong type" '{"error":"trigger must be an object"} 400' \
    "$(sched '{"action":"backup","trigger":"daily"}')"
check "a missing trigger" '{"error":"trigger is required"} 400' \
    "$(sched '{"action":"backup"}')"
check "trigger seconds of the wrong type" \
    '{"error":"interval trigger: trigger.seconds must be a number"} 400' \
    "$(sched '{"action":"backup","trigger":{"type":"interval","seconds":"3600"}}')"
check "trigger seconds of 1e999" \
    '{"error":"interval trigger: trigger.seconds is out of range; it must be from 1 to 9007199254740992"} 400' \
    "$(sched '{"action":"backup","trigger":{"type":"interval","seconds":1e999}}')"
check "a bad on_overlap is reported before missing sources" \
    '{"error":"on_overlap must be skip, queue or cancel_running"} 400' \
    "$(sched '{"action":"backup","on_overlap":"x","trigger":{"type":"interval","seconds":60}}')"
check "a bad catchup is reported before missing sources" \
    '{"error":"catchup must be skip or run_once"} 400' \
    "$(sched '{"action":"backup","catchup":["skip"],"trigger":{"type":"interval","seconds":60}}')"
check "non-string schedule paths are reported before missing sources" \
    '{"error":"paths must be a list of strings; entry 1 is not one"} 400' \
    "$(sched '{"action":"backup","paths":[1],"trigger":{"type":"interval","seconds":60}}')"
check "a schedule's 1,000-byte backup label is refused" "$LABEL_RULE" \
    "$(sched "{\"action\":\"backup\",\"label\":\"$LONG_LABEL\",\"trigger\":{\"type\":\"interval\",\"seconds\":60}}")"
check "a label belongs to a backup schedule only" \
    '{"error":"label applies only to a backup schedule, not gc"} 400' \
    "$(sched '{"action":"gc","label":"x","trigger":{"type":"interval","seconds":60}}')"
check "a sound body then hears about the sources" \
    '{"error":"this repository has no backup sources configured, and none were given"} 400' \
    "$(sched '{"action":"backup","trigger":{"type":"interval","seconds":60}}')"
check "a non-string action" '{"error":"action must be a string"} 400' \
    "$(sched '{"action":1,"trigger":{"type":"interval","seconds":60}}')"
check "a non-string grep pattern" '{"error":"pattern must be a string"} 400' \
    "$(sched '{"action":"grep","pattern":5,"trigger":{"type":"interval","seconds":60}}')"
case "$(sock_body /repos/1/schedules)" in
    *'"action"'*) bad "no schedule was created by any of them" "none" "$(sock_body /repos/1/schedules)" ;;
    *) ok "no schedule was created by any of them" ;;
esac

# B112 (1), (2): the parser stopped at the end of the first value, so data
# after it was never read, and a repeated key silently took one of its
# values. c-utils 1.6.0 refuses both, at any depth, before a route sees
# the body.
code_of() { printf '%s' "$1" | awk '{print $NF}'; }
check "trailing junk after a repo body is a 400" 400 \
    "$(code_of "$(sock_send POST /repos "{\"path\":\"$WORK/junkrepo\",\"label\":\"x\"} junk")")"
check "a second object after a repo body is a 400" 400 \
    "$(code_of "$(sock_send POST /repos "{\"path\":\"$WORK/junkrepo\"}{}")")"
check "a bare {}{} is a 400" 400 "$(code_of "$(sock_send POST /repos '{}{}')")"
check "a duplicate top-level key is a 400" 400 \
    "$(code_of "$(sock_send POST /repos "{\"path\":\"$WORK/junkrepo\",\"path\":\"$WORK/junkrepo2\"}")")"
check "trailing whitespace is still fine to send" '{"error":"path is required"} 400' \
    "$(sock_send POST /repos "{\"label\":\"x\"}"$' \t\n')"
check "and no repository was made by any of them" "absent absent" \
    "$([ -e "$WORK/junkrepo" ] && echo present || echo absent) $([ -e "$WORK/junkrepo2" ] && echo present || echo absent)"
case "$(sock_body /repos)" in *junkrepo*) bad "and none was registered" "absent" "listed" ;;
                              *) ok "and none was registered" ;; esac
check "a duplicate key inside a trigger is a 400" 400 \
    "$(code_of "$(sched '{"action":"gc","trigger":{"type":"interval","seconds":60,"seconds":3600}}')")"
check "trailing data after a schedule is a 400" 400 \
    "$(code_of "$(sched '{"action":"gc","trigger":{"type":"interval","seconds":60}} x')")"
check "a duplicate top-level schedule key is a 400" 400 \
    "$(code_of "$(sched '{"action":"gc","action":"verify","trigger":{"type":"interval","seconds":60}}')")"
# The pack job is gone (every object is written into a pack as it is
# stored), so "pack" is no longer an action a schedule can name.
check "a pack schedule is a 400" 400 \
    "$(code_of "$(sched '{"action":"pack","trigger":{"type":"interval","seconds":60}}')")"
case "$(sock_body /repos/1/schedules)" in
    *'"action"'*) bad "and no schedule was created" "none" "$(sock_body /repos/1/schedules)" ;;
    *) ok "and no schedule was created" ;;
esac
# NIT-8: one wording on every route, and it names the problem. POST /repos
# said "body is not valid JSON", the rest "invalid JSON", neither the key.
check "a duplicate repo key is named" \
    "{\"error\":\"invalid JSON: duplicate key 'path' at the top level\"} 400" \
    "$(sock_send POST /repos "{\"path\":\"$WORK/junkrepo\",\"path\":\"$WORK/junkrepo2\"}")"
check "a duplicate key in a trigger is named, with where it is" \
    "{\"error\":\"invalid JSON: duplicate key 'seconds' in object 'trigger'\"} 400" \
    "$(sched '{"action":"gc","trigger":{"type":"interval","seconds":60,"seconds":3600}}')"
check "a duplicate backup key is named" \
    "{\"error\":\"invalid JSON: duplicate key 'label' at the top level\"} 400" \
    "$(sock_send POST /repos/1/backup '{"label":"a","label":"b"}')"
check "malformed JSON on POST /repos says invalid JSON" '{"error":"invalid JSON"} 400' \
    "$(sock_send POST /repos '{"path":')"

echo "restore"
# A snapshot that does not exist is refused up front, not queued as a job
# that fails telling the operator to reindex.
check "restore of an unknown snapshot is a 404" 404 "$(curl -s -o /dev/null -w '%{http_code}' \
     -X POST --unix-socket "$SOCK" -d "{\"dest\":\"$WORK/nowhere\"}" \
     http://localhost/repos/1/snapshots/999/restore)"
RESTORE_JOB=$(job_id "$(curl -s -X POST --unix-socket "$SOCK" -d "{\"dest\":\"$WORK/out\"}" \
                        http://localhost/repos/1/snapshots/1/restore)")
wait_job "$RESTORE_JOB"
contains "restore completed" '"state":"completed"' "$(sock_body "/jobs/$RESTORE_JOB")"
contains "restore result counts what it wrote" '"result":{"files":3,' "$(sock_body "/jobs/$RESTORE_JOB")"
# B093: a finished job's count is the last report, and it ends at its total.
RJ_BODY=$(sock_body "/jobs/$RESTORE_JOB")
RJ_CUR=$(printf '%s' "$RJ_BODY" | sed -n 's/.*"current":\([0-9]*\).*/\1/p')
RJ_TOT=$(printf '%s' "$RJ_BODY" | sed -n 's/.*"total":\([0-9]*\).*/\1/p')
check "a finished restore's count reaches its total" "$RJ_TOT" "$RJ_CUR"
if diff -r "$WORK/src" "$WORK/out$WORK/src" > /dev/null 2>&1; then
    ok "restored tree is identical"
else
    bad "restored tree is identical" "no differences" "diff reported changes"
fi

echo "schedules"
check "create schedule" 201 "$(curl -s -o /dev/null -w '%{http_code}' -X POST --unix-socket "$SOCK" \
     -d '{"action":"gc","trigger":{"type":"interval","seconds":3600}}' \
     http://localhost/repos/1/schedules)"
# The trigger is validated at creation rather than at fire time, so a typo is
# reported to whoever made it instead of quietly disabling the schedule later.
check "reject bad trigger" 400 "$(curl -s -o /dev/null -w '%{http_code}' -X POST --unix-socket "$SOCK" \
     -d '{"action":"gc","trigger":{"type":"cron","spec":"* * * * *"}}' \
     http://localhost/repos/1/schedules)"
contains "schedule listed" '"action":"gc"' "$(sock_body /repos/1/schedules)"
# B051/B049: the trigger is an object rather than a string of JSON, and the
# list says which zone calendar triggers run in, with a readable next fire.
SL=$(sock_body /repos/1/schedules)
contains "trigger is an object" '"trigger":{"type":"interval","seconds":3600}' "$SL"
if [ -n "${TZ:-}" ]; then WANT_TZ="${TZ#:}"; else WANT_TZ="$(readlink /etc/localtime 2>/dev/null | sed 's|.*zoneinfo/||')"; fi
[ -n "$WANT_TZ" ] && contains "list names the daemon's zone" "\"timezone\":\"$WANT_TZ\"" "$SL"
OFF=$(date +%z | sed 's/\(..\)$/:\1/')
case "$SL" in
    *'"next_due_iso":"'[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]"$OFF"'"'*)
        ok "next fire is ISO 8601 with the local offset" ;;
    *) bad "next fire is ISO 8601 with the local offset" "...$OFF" "$SL" ;;
esac
check "disable schedule" 200 "$(curl -s -o /dev/null -w '%{http_code}' -X PUT --unix-socket "$SOCK" \
     -d '{"enabled":0}' http://localhost/repos/1/schedules/1)"
contains "schedule disabled" '"enabled":false' "$(sock_body /repos/1/schedules)"
# B091: {"enabled":false} was refused with "enabled must be 0 or 1". JSON
# booleans and 0/1 both work, both ways, and anything else is a 400.
sched_put() { curl -s -o /dev/null -w '%{http_code}' -X PUT --unix-socket "$SOCK" \
                   -d "$1" http://localhost/repos/1/schedules/1; }
check "enable with true"   200 "$(sched_put '{"enabled":true}')"
contains "enabled by true"  '"enabled":true'  "$(sock_body /repos/1/schedules)"
check "disable with false" 200 "$(sched_put '{"enabled":false}')"
contains "disabled by false" '"enabled":false' "$(sock_body /repos/1/schedules)"
check "enable with 1"      200 "$(sched_put '{"enabled":1}')"
contains "enabled by 1"     '"enabled":true'  "$(sock_body /repos/1/schedules)"
check "disable with 0"     200 "$(sched_put '{"enabled":0}')"
contains "disabled by 0"    '"enabled":false' "$(sock_body /repos/1/schedules)"
check "enabled 2 refused"  400 "$(sched_put '{"enabled":2}')"
check "enabled string refused" 400 "$(sched_put '{"enabled":"yes"}')"
contains "a refusal changes nothing" '"enabled":false' "$(sock_body /repos/1/schedules)"

# A schedule already due must fire on the next tick and produce a real job.
curl -s -X POST --unix-socket "$SOCK" \
     -d '{"action":"gc","trigger":{"type":"interval","seconds":1}}' \
     http://localhost/repos/1/schedules > /dev/null
JOBS_BEFORE=$(sock_body /jobs | grep -o '"id"' | wc -l)
sleep 3
JOBS_AFTER=$(sock_body /jobs | grep -o '"id"' | wc -l)
if [ "$JOBS_AFTER" -gt "$JOBS_BEFORE" ]; then
    ok "due schedule submitted a job"
else
    bad "due schedule submitted a job" "more than $JOBS_BEFORE jobs" "$JOBS_AFTER"
fi
check "delete schedule" 200 "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE --unix-socket "$SOCK" \
     http://localhost/repos/1/schedules/1)"
# B065: these answered success for a schedule that does not exist.
check "delete a missing schedule" 404 "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE \
     --unix-socket "$SOCK" http://localhost/repos/1/schedules/1)"
check "disable a missing schedule" 404 "$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
     --unix-socket "$SOCK" -d '{"enabled":0}' http://localhost/repos/1/schedules/999)"
check "yearly trigger on 30 February refused" 400 "$(curl -s -o /dev/null -w '%{http_code}' \
     -X POST --unix-socket "$SOCK" \
     -d '{"action":"gc","trigger":{"type":"calendar","every":"year","on_month":2,"on_day":30,"at":"05:00"}}' \
     http://localhost/repos/1/schedules)"

echo "introspection"
SUMMARY=$(sock_body /repos/1/summary)
contains "summary"      '"snapshots"'  "$SUMMARY"
# There are no loose objects any more. What a client can see short of a
# sealed pack is the staged count, which is zero once a backup has finished.
contains "summary counts staged objects" '"staged":0' "$SUMMARY"
case "$SUMMARY" in *'"loose"'*) bad "summary has no loose count" "absent" "$SUMMARY" ;;
                   *) ok "summary has no loose count" ;; esac
contains "stats"        '"per_type"'   "$(sock_body /repos/1/stats)"
# The staged scope is legitimately empty here, every backup having sealed its
# pack. What matters is that the scoped query answers rather than errors.
well_formed "staged stats scope answers" "$(sock_body '/repos/1/stats?scope=staged')"
contains "and narrows to nothing" '"per_type":[]' "$(sock_body '/repos/1/stats?scope=staged')"
# loose is not a scope now; like any unknown scope it is refused rather
# than answered unscoped.
check "the loose scope is refused" 400 "$(sock_code '/repos/1/stats?scope=loose')"
contains "objects"      '"hash"'       "$(sock_body '/repos/1/objects?limit=2')"
contains "packs"        '{'            "$(sock_body /repos/1/packs)"
# B074: values that cannot mean anything were ignored, so the default page
# came back as though the request had been understood.
check "unknown object location is a 400" 400 "$(sock_code '/repos/1/objects?location=nowhere')"
check "location=all is accepted"         200 "$(sock_code '/repos/1/objects?location=all')"
check "location=staged is accepted"      200 "$(sock_code '/repos/1/objects?location=staged')"
check "location=packed is accepted"      200 "$(sock_code '/repos/1/objects?location=packed')"
check "location=loose is a 400 now"      400 "$(sock_code '/repos/1/objects?location=loose')"
contains "nothing is staged after a backup" '"objects":[]' \
    "$(sock_body '/repos/1/objects?location=staged')"
check "negative limit is a 400"          400 "$(sock_code '/repos/1/snapshots?limit=-1')"
check "zero limit is a 400"              400 "$(sock_code '/repos/1/snapshots?limit=0')"
check "non-numeric limit is a 400"       400 "$(sock_code '/repos/1/objects?limit=lots')"
check "negative offset is a 400"         400 "$(sock_code '/repos/1/snapshots?offset=-5')"
check "bad limit on the journal is a 400" 400 "$(sock_code '/repos/1/journal?limit=x')"
check "bad limit on grep runs is a 400"  400 "$(sock_code '/repos/1/grep?limit=-1')"
check "bad limit on jobs is a 400"       400 "$(sock_code '/jobs?limit=-1')"
check "bad offset on jobs is a 400"      400 "$(sock_code '/jobs?offset=x')"
check "jobs pages with limit and offset" 200 "$(sock_code '/jobs?limit=1&offset=1')"
check "a large limit is clamped, not refused" 200 "$(sock_code '/repos/1/snapshots?limit=999999')"
# NIT-6: past nine digits a limit was refused as "1 or more", true of what
# was sent; it is a large limit like any other. And a refusal names the
# route's range, which differs by route.
check "an overflowing limit is clamped too" 200 "$(sock_code '/jobs?limit=99999999999')"
check "an overflowing offset is an empty page" 200 "$(sock_code '/jobs?offset=99999999999')"
check "a bad limit names the range" \
    '{"error":"limit is out of range; it must be from 1 to 1000 (a larger one, up to 9007199254740992, is reduced to 1000)"}' \
    "$(sock_body '/jobs?limit=0')"
# B112: every numeric query parameter reads numbers the same way and words
# a refusal the way a body field does. An overflowing id used to be clamped
# by strtoll and answered 404, as though it were merely unknown.
check "a non-numeric limit says whole number" '{"error":"limit must be a whole number"} 400' \
    "$(sock_send GET '/jobs?limit=lots')"
check "a fractional limit says whole number" '{"error":"limit must be a whole number"} 400' \
    "$(sock_send GET '/jobs?limit=1.5')"
check "a negative offset is out of range" \
    '{"error":"offset is out of range; it must be from 0 to 2147483647 (a larger one, up to 9007199254740992, is past every row)"} 400' \
    "$(sock_send GET '/jobs?offset=-5')"
# NIT-8: a number too large to represent is out of range, as it is in a
# body; these were read as the maximum and answered 200.
LRANGE='limit is out of range; it must be from 1 to 1000 (a larger one, up to 9007199254740992, is reduced to 1000)'
for v in 1e999 99999999999999999999 9007199254740994; do
    check "limit=$v is out of range" "{\"error\":\"$LRANGE\"} 400" "$(sock_send GET "/jobs?limit=$v")"
done
check "limit=9007199254740992 is still clamped" 200 "$(sock_code '/jobs?limit=9007199254740992')"
check "an unrepresentable offset is out of range" 400 "$(sock_code '/jobs?offset=1e999')"
# NIT-8: an id in the path is parsed, not routed. These were a 405, a 404
# and a 404, while /repos/-1 was already a 400.
for v in 99999999999999999999 1e999 -1 0 1.5 abc; do
    check "snapshot id $v is invalid" '{"error":"invalid snapshot id"} 400' \
        "$(sock_send DELETE "/repos/1/snapshots/$v")"
done
check "and under a suffix too" '{"error":"invalid snapshot id"} 400' \
    "$(sock_send GET /repos/1/snapshots/1e999/tree)"
for v in 99999999999999999999 1e999 -1 0; do
    check "repo id $v is invalid" '{"error":"invalid repo id"} 400' "$(sock_send GET "/repos/$v")"
    check "job id $v is invalid" '{"error":"invalid job id"} 400' "$(sock_send GET "/jobs/$v")"
done
check "a cancel with a bad job id" '{"error":"invalid job id"} 400' \
    "$(sock_send POST /jobs/-1/cancel)"
check "a stream with a bad job id" 400 "$(sock_code /jobs/stream/99999999999999999999)"
check "a schedule id is parsed" '{"error":"invalid schedule id"} 400' \
    "$(sock_send DELETE /repos/1/schedules/1e999)"
check "a grep run id is parsed" '{"error":"invalid grep run id"} 400' \
    "$(sock_send GET /repos/1/grep/-1)"
check "a verify run id is parsed" '{"error":"invalid verify run id"} 400' \
    "$(sock_send GET /repos/1/verify/99999999999999999999)"
check "a pack id is parsed" '{"error":"invalid pack id"} 400' \
    "$(sock_send GET /repos/1/packs/-1/entries)"
check "an unknown valid snapshot id is still a 404" 404 \
    "$(sock_code /repos/1/snapshots/999999/tree)"
check "an unknown valid job id is still a 404" 404 "$(sock_code /jobs/999999)"
check "a leading zero still reads" 200 "$(sock_code '/jobs?limit=010')"
SRANGE='snapshot is out of range; it must be from 1 to 4294967295'
for v in 1e999 -1 0 99999999999999999999; do
    check "search ?snapshot=$v is out of range" "{\"error\":\"$SRANGE\"} 400" \
        "$(sock_send GET "/repos/1/search?q=txt&snapshot=$v")"
done
for v in abc 1.5 +5; do
    check "search ?snapshot=$v is not a whole number" \
        '{"error":"snapshot must be a whole number"} 400' \
        "$(sock_send GET "/repos/1/search?q=txt&snapshot=$v")"
done
check "search ?snapshot= of an unknown id is still 404" '{"error":"no such snapshot"} 404' \
    "$(sock_send GET '/repos/1/search?q=txt&snapshot=4000000000')"
PRANGE='parent is out of range; it must be from 0 to 9007199254740992'
for v in 1e999 -1 99999999999999999999; do
    check "tree ?parent=$v is out of range" "{\"error\":\"$PRANGE\"} 400" \
        "$(sock_send GET "/repos/1/snapshots/1/tree?parent=$v")"
done
check "tree ?parent=x is not a whole number" '{"error":"parent must be a whole number"} 400' \
    "$(sock_send GET '/repos/1/snapshots/1/tree?parent=x')"
check "tree ?parent= of an unknown node is still 404" \
    '{"error":"no such directory in this snapshot"} 404' \
    "$(sock_send GET '/repos/1/snapshots/1/tree?parent=9000000000')"
check "tree ?parent=0 is the root" 200 "$(sock_code '/repos/1/snapshots/1/tree?parent=0')"
check "diff ?from=1e999 is out of range" \
    '{"error":"from is out of range; it must be from 1 to 4294967295"} 400' \
    "$(sock_send GET '/repos/1/diff?from=1e999&to=1')"
check "diff ?to=x is not a whole number" '{"error":"to must be a whole number"} 400' \
    "$(sock_send GET '/repos/1/diff?from=1&to=x')"
check "diff of an unknown snapshot is still 404" \
    "{\"error\":\"no such snapshot in 'to'\"} 404" \
    "$(sock_send GET '/repos/1/diff?from=1&to=4000000000')"
contains "fanout"       '"buckets"'    "$(sock_body /repos/1/fanout)"

# type 1 is a file. Tree objects are referenced by subtree_hash rather than
# content_hash, so they have no refs and would be a misleading probe.
OBJ=$(sock_body '/repos/1/objects?limit=200' \
      | tr '}' '\n' | grep '"type":1,' | head -1 \
      | sed -n 's/.*"hash":"\([a-f0-9]*\)".*/\1/p')
contains "object locate"  '"found":true' "$(sock_body "/repos/1/objects/$OBJ")"
contains "object refs"    '"refs"'       "$(sock_body "/repos/1/objects/$OBJ/refs")"
# B076: refs named the file but not where it is.
contains "object refs carry full paths" "\"path\":\"$WORK/src/" "$(sock_body "/repos/1/objects/$OBJ/refs")"
contains "object layout"  '"header_size"' "$(sock_body "/repos/1/objects/$OBJ/layout")"
contains "object content" '"base64"'     "$(sock_body "/repos/1/objects/$OBJ/content")"
# A malformed hash must be rejected rather than reaching the store.
check    "rejects bad hash" 400 "$(sock_code /repos/1/objects/nothexatall)"

# Diff walks manifest trees: an unchanged subtree hash prunes the whole
# subtree, so a second snapshot with one change reports exactly one change.
echo "changed" > "$WORK/src/a.txt"
DIFF_JOB=$(job_id "$(curl -s -X POST --unix-socket "$SOCK" -d "{\"paths\":[\"$WORK/src\"]}" \
                      http://localhost/repos/1/backup)")
wait_job "$DIFF_JOB"
contains "second backup completed" '"state":"completed"' "$(sock_body "/jobs/$DIFF_JOB")"
DIFF=$(sock_body '/repos/1/diff?from=1&to=2')
contains "diff reports the modification" '"change":"modified"' "$DIFF"
contains "diff counts one change"        '"modified":1'        "$DIFF"
contains "diff adds nothing"             '"added":0'           "$DIFF"
contains "diff truncation is a boolean"  '"truncated":false'   "$DIFF"


echo "packs"
# There is no pack job: every object is written into a pack as it is stored.
check "the pack route is gone" 404 "$(curl -s -o /dev/null -w '%{http_code}' -X POST \
     --unix-socket "$SOCK" http://localhost/repos/1/pack)"
contains "packs listed"  '"filename"'            "$(sock_body /repos/1/packs)"
# A repository holds packs and the snapshot-id high-water mark, and no
# objects directory: nothing writes outside a pack.
check "the repository has no objects directory" "absent" \
    "$([ -e "$WORK/repo/objects" ] && echo present || echo absent)"
check "and keeps its snapshot-id mark" "present" \
    "$([ -f "$WORK/repo/snapshot-ids" ] && echo present || echo absent)"
contains "pack entries"  '"entry_index"'         "$(sock_body '/repos/1/packs/0/entries?limit=1')"
# B075: missing ids answered as empty successes, which reads as "empty".
check "entries of a missing pack"   404 "$(sock_code '/repos/1/packs/99999/entries')"
check "tree of a missing snapshot"  404 "$(sock_code '/repos/1/snapshots/99999/tree?parent=0')"
check "tree under a missing node"   404 "$(sock_code '/repos/1/snapshots/1/tree?parent=99999999')"
check "findings of a missing verify run" 404 "$(sock_code '/repos/1/verify/99999')"
check "hits of a missing search"    404 "$(sock_code '/repos/1/grep/99999')"
# Every object is in a pack, so every read path has to resolve through the
# pack index.
PACKED=$(sock_body '/repos/1/objects?location=pack&limit=1' \
         | sed -n 's/.*"hash":"\([a-f0-9]*\)".*/\1/p')
contains "packed object locates" '"found":true'  "$(sock_body "/repos/1/objects/$PACKED")"
contains "packed object layout"  '"header_size"' "$(sock_body "/repos/1/objects/$PACKED/layout")"
# B076: the docs said 56 bytes. A packed object sits behind the 51-byte pack
# entry header.
contains "packed header is the 51-byte entry header" '"header_size":51' \
    "$(sock_body "/repos/1/objects/$PACKED/layout")"
# B102: a packed object's phys_size is its stored payload, the entry header
# and parity being charged to the pack, so it equals layout's payload_size
# and is exactly 51 less than layout's physical_size (docs/api.md).
P_PHYS=$(sock_body "/repos/1/objects/$PACKED" | sed -n 's/.*"phys_size":\([0-9]*\).*/\1/p')
P_LAY=$(sock_body "/repos/1/objects/$PACKED/layout")
P_REC=$(printf '%s' "$P_LAY" | sed -n 's/.*"physical_size":\([0-9]*\).*/\1/p')
P_PAY=$(printf '%s' "$P_LAY" | sed -n 's/.*"payload_size":\([0-9]*\).*/\1/p')
check "packed phys_size is layout's payload_size" "$P_PAY" "$P_PHYS"
check "and the record is that plus the entry header" "$P_REC" "$((P_PHYS + 51))"
contains "packed object content" '"base64"'      "$(sock_body "/repos/1/objects/$PACKED/content")"
contains "diff over packs"       '"modified"'    "$(sock_body '/repos/1/diff?from=1&to=2')"

# The sharpest check on packing: every object still readable afterwards. A
# lost pack_objects row leaves bytes on disk that nothing can resolve, and a
# full restore is the only thing that notices. Snapshot 2, because the diff
# section above changed a.txt and snapshot 1 no longer matches the source.
REPACK_JOB=$(job_id "$(curl -s -X POST --unix-socket "$SOCK" \
             -d "{\"dest\":\"$WORK/packed_out\"}" \
             http://localhost/repos/1/snapshots/2/restore)")
wait_job "$REPACK_JOB"
contains "restore after packing completed" '"state":"completed"' \
         "$(sock_body "/jobs/$REPACK_JOB")"
if diff -r "$WORK/src" "$WORK/packed_out$WORK/src" > /dev/null 2>&1; then
    ok "restore from packs is identical"
else
    bad "restore from packs is identical" "no differences" "diff reported changes"
fi

echo "partial restore"
SUB_JOB=$(job_id "$(curl -s -X POST --unix-socket "$SOCK" \
          -d "{\"dest\":\"$WORK/sub\",\"subpath\":\"$WORK/src/docs\"}" \
          http://localhost/repos/1/snapshots/1/restore)")
wait_job "$SUB_JOB"
contains "subpath restore completed" '"state":"completed"' "$(sock_body "/jobs/$SUB_JOB")"
# B093: the count was the snapshot's entries, so a one-file restore said
# 104 of 104. The subtree is docs/ and c.txt: two entries of work.
contains "a subpath restore counts the subtree" '"phase":"restore","current":2,"total":2' \
    "$(sock_body "/jobs/$SUB_JOB")"
# Only the requested subtree, so the siblings must not be there.
if [ -f "$WORK/sub$WORK/src/docs/c.txt" ]; then ok "subpath restored its file"
else bad "subpath restored its file" "$WORK/sub$WORK/src/docs/c.txt exists" "missing"; fi
if [ -f "$WORK/sub$WORK/src/a.txt" ]; then
    bad "subpath excluded the rest" "no a.txt outside the subpath" "a.txt restored"
else ok "subpath excluded the rest"; fi

# A subpath is normalised before it is matched, so the doubled and trailing
# slashes here name the same directory as the plain spelling.
NORM_JOB=$(job_id "$(curl -s -X POST --unix-socket "$SOCK" \
           -d "{\"dest\":\"$WORK/subnorm\",\"subpath\":\"$WORK//src/./docs/\"}" \
           http://localhost/repos/1/snapshots/1/restore)")
wait_job "$NORM_JOB"
contains "normalised subpath restore completed" '"state":"completed"' "$(sock_body "/jobs/$NORM_JOB")"
if [ -f "$WORK/subnorm$WORK/src/docs/c.txt" ]; then ok "normalised subpath restored its file"
else bad "normalised subpath restored its file" "c.txt restored" "missing"; fi

echo "restore request validation"
restore_req() {  # restore_req <json> -> "<code> <body>"
    curl -s -w ' %{http_code}' -X POST --unix-socket "$SOCK" -d "$1" \
         http://localhost/repos/1/snapshots/1/restore
}
# A subpath that matches nothing used to run, restore nothing, and report
# success. The index can answer the question before a job exists.
R=$(restore_req "{\"dest\":\"$WORK/nothing\",\"subpath\":\"/no/such/path\"}")
contains "missing subpath is 404"        ' 404'                                   "$R"
contains "missing subpath says so"       'subpath /no/such/path matches nothing in snapshot 1' "$R"
check    "missing subpath wrote nothing" "absent" "$([ -e "$WORK/nothing" ] && echo present || echo absent)"
# A relative dest resolves against the daemon's cwd. It used to become a job
# that then failed; it is refused at the request instead.
R=$(restore_req '{"dest":"relative/out"}')
contains "relative dest is 400"          ' 400'                       "$R"
contains "relative dest says why"        'dest must be an absolute path' "$R"
# Restoring into a repository writes beside packs/ and snapshots/. Refused,
# however the path is spelled: directly, through "..", and through a symlink,
# which is why the comparison is on canonical paths.
R=$(restore_req "{\"dest\":\"$WORK/repo/restored\"}")
contains "dest inside the repo is 400"   ' 400'                       "$R"
contains "dest inside the repo says so"  'inside the repository'      "$R"
R=$(restore_req "{\"dest\":\"$WORK/src/../repo/x\"}")
contains "dotdot into the repo is 400"   ' 400'                       "$R"
ln -s "$WORK/repo" "$WORK/repolink"
R=$(restore_req "{\"dest\":\"$WORK/repolink/x\"}")
contains "symlink into the repo is 400"  ' 400'                       "$R"
R=$(restore_req "{\"dest\":\"$WORK/repo\"}")
contains "the repo itself is 400"        ' 400'                       "$R"
check    "repo untouched by refusals"    "absent" \
         "$([ -e "$WORK/repo/restored" ] || [ -e "$WORK/repo/x" ] && echo present || echo absent)"
# A sibling whose name merely starts with the repo's is not inside it.
R=$(restore_req "{\"dest\":\"$WORK/repo-sibling\"}")
contains "repo-prefixed sibling accepted" '"job_id"'                  "$R"
wait_job "$(job_id "$R")"

echo "maintenance"
GC_JOB=$(job_id "$(curl -s -X POST --unix-socket "$SOCK" http://localhost/repos/1/gc)")
check "gc submitted" "1" "$([ -n "$GC_JOB" ] && echo 1 || echo 0)"
check "reindex" 202 "$(curl -s -o /dev/null -w '%{http_code}' -X POST --unix-socket "$SOCK" \
     http://localhost/repos/1/reindex)"
check "gfs" 202 "$(curl -s -o /dev/null -w '%{http_code}' -X POST --unix-socket "$SOCK" \
     http://localhost/repos/1/gfs)"
wait_job "$GC_JOB"
# The maintenance jobs are the ones that report no progress, so they are the
# ones the empty-value bug hit.
well_formed "gc job body is well formed" "$(sock_body "/jobs/$GC_JOB")"
well_formed "job list is well formed"    "$(sock_body /jobs)"
sleep 1
contains "journal readable" 'entries' "$(sock_body /repos/1/journal)"

# Exclusions reach the engine only if the route parses them. It did not: the
# field looked accepted and was ignored, so every backup took the whole tree.
#
# Its own repo and its own source tree on purpose. Backing up into repo 1
# would shift its snapshot numbering and add files the earlier restore and
# diff assertions compare against, which is exactly what happened the first
# time this was written.
echo "exclude"
mkdir -p "$WORK/exsrc/node_modules" "$WORK/exsrc/build"
echo "kept"     > "$WORK/exsrc/kept.txt"
echo "vendored" > "$WORK/exsrc/node_modules/dep.js"
echo "object"   > "$WORK/exsrc/build/out.o"
echo "artifact" > "$WORK/exsrc/disk.iso"
check "register exclude repo" 201 \
    "$(curl -s -o /dev/null -w '%{http_code}' -X POST --unix-socket "$SOCK" \
       -d "{\"path\":\"$WORK/exrepo\"}" http://localhost/repos)"
EX_RESP=$(curl -s -X POST --unix-socket "$SOCK" \
          -d "{\"paths\":[\"$WORK/exsrc\"],\"exclude\":[\"node_modules\",\"*.iso\",\"build\"]}" \
          http://localhost/repos/2/backup)
EX_JOB=$(job_id "$EX_RESP")
wait_job "$EX_JOB"
contains "excluding backup completed" '"state":"completed"' "$(sock_body "/jobs/$EX_JOB")"
contains "unexcluded file backed up" 'kept.txt' "$(sock_body '/repos/2/search?q=kept')"
case "$(sock_body '/repos/2/search?q=dep')" in
    *dep.js*) bad "node_modules excluded" "absent" "present" ;;
    *)        ok  "node_modules excluded" ;;
esac
case "$(sock_body '/repos/2/search?q=disk')" in
    *disk.iso*) bad "glob *.iso excluded" "absent" "present" ;;
    *)          ok  "glob *.iso excluded" ;;
esac
case "$(sock_body '/repos/2/search?q=out')" in
    *out.o*) bad "build dir excluded" "absent" "present" ;;
    *)       ok  "build dir excluded" ;;
esac
# Excluding everything used to complete with a snapshot of nothing but the
# source's ancestor directories. It fails instead, and writes no snapshot.
SNAPS_BEFORE=$(sock_body /repos/2/snapshots | grep -o '"id"' | wc -l)
ALL_JOB=$(job_id "$(curl -s -X POST --unix-socket "$SOCK" \
          -d "{\"paths\":[\"$WORK/exsrc\"],\"exclude\":[\"*\"]}" \
          http://localhost/repos/2/backup)")
wait_job "$ALL_JOB"
ALL_BODY=$(sock_body "/jobs/$ALL_JOB")
contains "exclude-everything backup fails"  '"state":"failed"'     "$ALL_BODY"
contains "exclude-everything says why"      'nothing to back up'   "$ALL_BODY"
check    "exclude-everything wrote no snapshot" "$SNAPS_BEFORE" \
         "$(sock_body /repos/2/snapshots | grep -o '"id"' | wc -l)"

# Verify had unit coverage and no end-to-end coverage: the route, the job
# wiring and the level parameter were never exercised through the daemon.
echo "verify"
VERIFY_RESP=$(curl -s -X POST --unix-socket "$SOCK" \
              "http://localhost/repos/1/verify?level=index")
contains "verify submit" '"job_id"' "$VERIFY_RESP"
VERIFY_JOB=$(job_id "$VERIFY_RESP")
wait_job "$VERIFY_JOB"
contains "verify job completed" '"state":"completed"' "$(sock_body "/jobs/$VERIFY_JOB")"
DEEP_RESP=$(curl -s -X POST --unix-socket "$SOCK" \
            "http://localhost/repos/1/verify?level=deep")
# Asserted, not just parsed: this failed once in CI with no job id and the
# log said only "no such job", because the response that explained it was
# thrown away.
contains "deep verify submit" '"job_id"' "$DEEP_RESP"
DEEP_JOB=$(job_id "$DEEP_RESP")
wait_job "$DEEP_JOB"
contains "deep verify completed" '"state":"completed"' "$(sock_body "/jobs/$DEEP_JOB")"
# B106: the job says what kind of run it was. A dry run's counts on a healthy
# repository are identical to a repairing run's, so nothing else does.
contains "a plain verify is deep and repairing" \
    '"level":"deep","repair":true,"dry_run":false}' "$(sock_body "/jobs/$DEEP_JOB")"
DRY_RESP=$(curl -s -X POST --unix-socket "$SOCK" \
           "http://localhost/repos/1/verify?dry_run=1")
DRY_JOB=$(job_id "$DRY_RESP")
wait_job "$DRY_JOB"
contains "a dry run is the same level, with repair off" \
    '"level":"deep","repair":false,"dry_run":true}' "$(sock_body "/jobs/$DRY_JOB")"
check "unknown verify level rejected" 400 \
    "$(curl -s -o /dev/null -w '%{http_code}' -X POST --unix-socket "$SOCK" \
       "http://localhost/repos/1/verify?level=sideways")"
contains "verify age metric present" 'cbackup_last_verify_age_seconds' "$(sock_body /metrics)"

# B008: every number the API serves is a JSON number. Rows used to be
# copied out of the database as text, so ids, counts, sizes, timestamps,
# modes and flags all arrived quoted and every client had to know which
# strings were numbers. Swept across the repository-scoped reads, after
# backup, tag, policy, schedule and verify have all left rows behind.
# A string of nothing but digits is what the bug looked like; none of the
# names, paths, labels or hashes in this fixture is one. The fanout's
# "prefix" is the exception that proves it: two hex digits, which can be
# all decimal ones.
echo "typed values"
NEWEST=$(sock_body '/repos/1/snapshots?limit=1' | grep -oE '"id":[0-9]+' | head -1 | tr -dc '0-9')
for path in /repos /repos/1 /repos/1/snapshots "/repos/1/snapshots/$NEWEST/tree?parent=0" \
            '/repos/1/search?q=txt' /repos/1/summary /repos/1/stats \
            '/repos/1/objects?limit=5' "/repos/1/objects/$OBJ" "/repos/1/objects/$OBJ/refs" \
            /repos/1/packs '/repos/1/packs/0/entries?limit=5' /repos/1/fanout \
            '/repos/1/diff?from=1&to=2' /repos/1/tags /repos/1/policy /repos/1/journal \
            /repos/1/schedules /repos/1/verify /repos/1/grep; do
    BODY=$(sock_body "$path")
    if printf '%s' "$BODY" | grep -oE '"[a-z_]+":"-?[0-9]+"' | grep -qv '^"prefix":'; then
        bad "no quoted numbers in $path" "numbers unquoted" \
            "$(printf '%s' "$BODY" | grep -oE '"[a-z_]+":"-?[0-9]+"' | head -3 | tr '\n' ' ')"
    else
        ok "no quoted numbers in $path"
    fi
done
REPO1=$(sock_body /repos/1)
contains "a repo id is a number"          '"id":1,'               "$REPO1"
contains "key_escrowed is a boolean"      '"key_escrowed":false'  "$REPO1"
contains "an unset label is null"         '"label":null'          "$(sock_body /repos/2)"
TREE=$(sock_body "/repos/1/snapshots/$NEWEST/tree?parent=0")
contains "a tree type is a number"        '"type":2,'             "$TREE"
contains "a tree mode is the st_mode"     '"type":2,"mode":1'      "$TREE"  # S_IFDIR 040000 is 16384, so 16384-16895
contains "mtime is whole seconds"         '"mtime":1'             "$TREE"
contains "with the nanoseconds apart"     '"mtime_nsec":'         "$TREE"
contains "has_children is a boolean"      '"has_children":true'   "$TREE"
contains "may_be_torn is a boolean"       '"may_be_torn":false'   "$TREE"
contains "a directory has a null hash"    '"hash":null'           "$TREE"
contains "materialized is a boolean"      '"materialized":true'   "$(sock_body /repos/1/snapshots)"
contains "the last snapshot time is a number" '"last_snapshot_ts":1' "$(sock_body /repos/1/summary)"
LOC=$(sock_body "/repos/1/objects/$OBJ")
contains "a packed object says so"        '"location":"packed","pack_id":' "$LOC"
contains "and the listing names its pack" '"location":"packed","pack_id":' \
    "$(sock_body '/repos/1/objects?limit=5')"
contains "verify completion is a boolean" '"completed":true'      "$(sock_body /repos/1/verify)"
contains "an object type is a number"     '"type":1,'             "$LOC"

# An empty collection must still carry its key. The array is created lazily
# on first append, so a query with no rows used to serialise as {} and every
# client had to handle two shapes depending on whether data happened to exist.
echo "empty collections"
contains "empty journal filter keeps its key" '"entries"' \
    "$(sock_body '/repos/1/journal?result=nonesuch')"
contains "empty search keeps its key" '"results"' \
    "$(sock_body '/repos/1/search?q=zzzznotathing')"
contains "empty object refs keep their key" '"refs"' \
    "$(sock_body '/repos/1/objects/0000000000000000000000000000000000000000000000000000000000000000/refs')"

# B123: backups submitted while snapshot deletes ran against the same
# repository failed "repo N is busy" whenever a delete held the repo lock at
# the moment the job was dispatched, each with a failure alert. The UAT
# trigger, smaller: backups and forced deletes shuffled through xargs -P 12.
# A delete may still be refused with 503 while a backup holds the repository;
# a job must never fail because a delete held it.
echo "jobs racing snapshot deletes"
RACE_BODY=$(curl -s -X POST --unix-socket "$SOCK" \
            -d "{\"path\":\"$WORK/racerepo\",\"label\":\"race\"}" http://localhost/repos)
RACE=$(printf '%s' "$RACE_BODY" | sed -n 's/^{"id":\([0-9]*\).*/\1/p')
contains "race repository registered" '"id":' "$RACE_BODY"
for _ in 1 2 3 4 5 6 7 8; do
    wait_job "$(job_id "$(curl -s -X POST --unix-socket "$SOCK" \
        -d "{\"paths\":[\"$WORK/src\"]}" "http://localhost/repos/$RACE/backup")")"
done
RACE_SNAPS=$(sock_body "/repos/$RACE/snapshots?limit=100" | grep -oE '"id":[0-9]+' | tr -dc '0-9\n')
check "eight snapshots to delete" 8 "$(printf '%s\n' "$RACE_SNAPS" | grep -c .)"
race_op() {  # race_op b | race_op <snapshot id>
    if [ "$1" = b ]; then
        # One id per line: the body has no trailing newline, and twelve
        # unterminated ids written in parallel would run together.
        printf '%s\n' "$(curl -s -X POST --unix-socket "$SOCK" -d "{\"paths\":[\"$WORK/src\"]}" \
             "http://localhost/repos/$RACE/backup" | sed -n 's/.*"job_id":\([0-9]*\).*/\1/p')"
    else
        curl -s -o /dev/null -X DELETE --unix-socket "$SOCK" \
             "http://localhost/repos/$RACE/snapshots/$1?force=1"
    fi
}
export -f race_op
export SOCK WORK RACE
RACE_JOBS=$({ for _ in $(seq 1 12); do echo b; done; printf '%s\n' "$RACE_SNAPS"; } \
            | shuf --random-source=<(yes) | timeout 60 xargs -P 12 -n 1 bash -c 'race_op "$0"')
check "every racing backup was accepted" 12 "$(printf '%s\n' "$RACE_JOBS" | grep -c .)"
RACE_FAILED=0
RACE_DONE=0
for j in $RACE_JOBS; do
    wait_job "$j"
    case "$(sock_body "/jobs/$j")" in
        *'"state":"completed"'*) RACE_DONE=$((RACE_DONE + 1)) ;;
        *) RACE_FAILED=$((RACE_FAILED + 1)); sock_body "/jobs/$j"; echo ;;
    esac
done
check "every racing backup completed" 12 "$RACE_DONE"
check "none failed on a delete holding the repository" 0 "$RACE_FAILED"

# Registration edge cases from UAT. Kept after every hardcoded /repos/<id>
# above, because each one registers a repository and takes an id.
echo "repo lifecycle"
repo_post_code() { curl -s -o /dev/null -w '%{http_code}' -X POST --unix-socket "$SOCK" \
                   -d "$1" http://localhost/repos; }
repo_post_body() { curl -s -X POST --unix-socket "$SOCK" -d "$1" http://localhost/repos; }

# B013: missing parents are created, and a path that cannot exist is the
# caller's mistake, not a 500.
check "missing parents are created" 201 "$(repo_post_code "{\"path\":\"$WORK/deep/a/b/r\"}")"
check "the repository is private" 700 "$(stat -c %a "$WORK/deep/a/b/r")"
echo "a file" > "$WORK/plainfile"
check "a file as a parent is a 400" 400 "$(repo_post_code "{\"path\":\"$WORK/plainfile/r\"}")"

# B012: a repository inside, or around, a registered one was accepted.
contains "a repo inside a repo is refused" "is inside repository" \
    "$(repo_post_body "{\"path\":\"$WORK/repo/nested\"}")"
check "and is a 400" 400 "$(repo_post_code "{\"path\":\"$WORK/repo/nested2\"}")"
check "and creates nothing" 1 "$([ -e "$WORK/repo/nested" ]; echo $?)"
contains "a repo around a repo is refused" "contains repository" \
    "$(repo_post_body "{\"path\":\"$WORK/deep/a\"}")"

# B015: re-registering answered {"id":1} and dropped the new label. It is
# still a success, because that is how a moved repository re-links, but it
# says so and applies the label.
RE=$(curl -s -w ' %{http_code}' -X POST --unix-socket "$SOCK" \
     -d "{\"path\":\"$WORK/repo\",\"label\":\"renamed\"}" http://localhost/repos)
contains "re-register answers 200" ' 200' "$RE"
contains "and says it was already registered" '"already_registered":true' "$RE"
contains "and that the label changed" '"label_updated":true' "$RE"
contains "the new label is stored" '"label":"renamed"' "$(sock_body /repos/1)"
contains "a trailing slash is the same repository" '"id":1,"already_registered":true' \
    "$(repo_post_body "{\"path\":\"$WORK/repo/\"}")"
contains "and the label survives an empty one" '"label":"renamed"' "$(sock_body /repos/1)"

# A new repository is format 3 and keeps the snapshot-id mark from the
# start. An older marker is refused at registration with what to do, not
# opened by a build that has no reader for it.
contains "a new marker is format 3" '"version":3' "$(cat "$WORK/deep/a/b/r/.cbackup")"
check "and the snapshot-id mark is created with it" "present" \
    "$([ -f "$WORK/deep/a/b/r/snapshot-ids" ] && echo present || echo absent)"
mkdir -p "$WORK/oldfmt" && chmod 700 "$WORK/oldfmt"
sed -e 's/"version":3/"version":2/' -e 's/"uuid":"[^"]*"/"uuid":"00000000-0000-4000-8000-000000000002"/' \
    "$WORK/deep/a/b/r/.cbackup" > "$WORK/oldfmt/.cbackup"
OUT=$(curl -s -w ' %{http_code}' -X POST --unix-socket "$SOCK" \
      -d "{\"path\":\"$WORK/oldfmt\"}" http://localhost/repos)
contains "a format 2 repository is refused" ' 400' "$OUT"
contains "and the refusal names the formats" \
    "is repository format 2; this build reads only format 3" "$OUT"
case "$(sock_body /repos)" in *oldfmt*) bad "and it is not registered" "absent" "listed" ;;
                              *) ok "and it is not registered" ;; esac
# B116: a repository label follows the snapshot label rule (OWN-33). Both
# of these registered with a 201 and were listed as given.
check "a 257-byte repository label is refused" \
    '{"error":"label must be at most 256 bytes of printable text"} 400' \
    "$(sock_send POST /repos "{\"path\":\"$WORK/lbl257\",\"label\":\"$(head -c 257 /dev/zero | tr '\0' a)\"}")"
check "a repository label with a newline is refused" \
    '{"error":"label must be at most 256 bytes of printable text"} 400' \
    "$(sock_send POST /repos "{\"path\":\"$WORK/lblnl\",\"label\":\"a\\nb\"}")"
check "and neither left a directory behind" "absent absent" \
    "$([ -e "$WORK/lbl257" ] && echo present || echo absent) $([ -e "$WORK/lblnl" ] && echo present || echo absent)"
check "a relabel is held to the rule too" \
    '{"error":"label must be at most 256 bytes of printable text"} 400' \
    "$(sock_send POST /repos "{\"path\":\"$WORK/repo\",\"label\":\"a\\tb\"}")"
contains "and leaves the label alone" '"label":"renamed"' "$(sock_body /repos/1)"

echo "remote"
# The same operations the desktop app drives, over TCP with a token, after
# the socket did the setup.
AUTH=(-H "Authorization: Bearer $TOKEN")
RB=$(tcp_body "${AUTH[@]}" -X POST -d "{\"paths\":[\"$WORK/src\"]}" "$URL/repos/1/backup")
contains "tcp backup submits" '"job_id"' "$RB"
RJOB=$(job_id "$RB")
wait_job "$RJOB"
contains "tcp job readable" '"state":"completed"' "$(tcp_body "${AUTH[@]}" "$URL/jobs/$RJOB")"
contains "tcp snapshots" '"snapshots"' "$(tcp_body "${AUTH[@]}" "$URL/repos/1/snapshots")"
# Progress streams over TCP too. A finished job gets one frame with its final
# state and the stream ends, so this cannot hang the suite.
contains "tcp job stream" '"state":"completed"' \
    "$(curl -s --max-time 5 "${AUTH[@]}" "$URL/jobs/stream/$RJOB")"
contains "tcp stream unknown job" 'no such job' \
    "$(curl -s --max-time 5 "${AUTH[@]}" "$URL/jobs/stream/999999")"
check "tcp stream needs a token" 401 "$(tcp_code /jobs/stream/$RJOB --max-time 5)"
# A 405 comes after auth: the network learns nothing about the API's shape
# without a token.
check "tcp 405 needs a token" 401 "$(tcp_code /repos/1 -X PATCH)"
check "tcp 405 with a token" 405 "$(tcp_code /repos/1 -X PATCH "${AUTH[@]}")"

# B053: a second client's session must not survive the password change that
# was meant to shut it out, while the session that made the change does.
OTHER=$(tcp_body -X POST -d '{"password":"correcthorsebattery"}' "$URL/api/auth/login" \
        | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
check "second session works" 200 "$(tcp_code /repos -H "Authorization: Bearer $OTHER")"
check "password change needs current" 401 "$(tcp_code /api/auth/password "${AUTH[@]}" -X POST \
     -d '{"current":"wrong","new":"brandnewpassword"}')"
check "password change too short" 400 "$(tcp_code /api/auth/password "${AUTH[@]}" -X POST \
     -d '{"current":"correcthorsebattery","new":"short"}')"
check "password change" 200 "$(tcp_code /api/auth/password "${AUTH[@]}" -X POST \
     -d '{"current":"correcthorsebattery","new":"brandnewpassword"}')"
check "old password refused" 401 "$(tcp_code /api/auth/login -X POST \
     -d '{"password":"correcthorsebattery"}')"
check "new password accepted" 200 "$(tcp_code /api/auth/login -X POST \
     -d '{"password":"brandnewpassword"}')"
check "other session revoked by the change" 401 "$(tcp_code /repos -H "Authorization: Bearer $OTHER")"
check "changing session survives" 200 "$(tcp_code /repos "${AUTH[@]}")"

# logout-all needs a credential and takes every session, the caller's too.
check "logout-all needs a token" 401 "$(tcp_code /api/auth/logout-all -X POST)"
T2=$(tcp_body -X POST -d '{"password":"brandnewpassword"}' "$URL/api/auth/login" \
     | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
T3=$(tcp_body -X POST -d '{"password":"brandnewpassword"}' "$URL/api/auth/login" \
     | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
contains "logout-all reports what it revoked" '"revoked":' \
    "$(tcp_body -H "Authorization: Bearer $T2" -X POST "$URL/api/auth/logout-all")"
check "logout-all ends the caller" 401 "$(tcp_code /repos -H "Authorization: Bearer $T2")"
check "logout-all ends the others" 401 "$(tcp_code /repos -H "Authorization: Bearer $T3")"
check "logout-all ends the first session" 401 "$(tcp_code /repos "${AUTH[@]}")"
TOKEN=$(tcp_body -X POST -d '{"password":"brandnewpassword"}' "$URL/api/auth/login" \
        | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
AUTH=(-H "Authorization: Bearer $TOKEN")

# B055: sessions are a TCP concept. Over the socket a login minted a token
# nothing used, and a logout with nothing to revoke answered 204.
sock_auth() {  # sock_auth <path> [curl args]: status and body over the socket
    local p=$1; shift
    curl -s -w ' %{http_code}' -X POST --unix-socket "$SOCK" "$@" "http://localhost$p"
}
OUT=$(sock_auth /api/auth/login -d '{"password":"brandnewpassword"}')
contains "socket login refused" " 400" "$OUT"
contains "and says the socket needs no credential" "needs no credential" "$OUT"
check "and mints no token" "" "$(printf '%s' "$OUT" | grep -o '"token"')"
OUT=$(sock_auth /api/auth/logout)
contains "socket logout with no token refused" " 400" "$OUT"
contains "and points at logout-all" "logout-all" "$OUT"
# A token named over the socket is revoked: the host's way to end one
# session without ending all of them.
SOCKREV=$(tcp_body -X POST -d '{"password":"brandnewpassword"}' "$URL/api/auth/login" \
          | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
check "socket logout revokes a named token" 204 "$(curl -s -o /dev/null -w '%{http_code}' \
     -X POST --unix-socket "$SOCK" -H "Authorization: Bearer $SOCKREV" \
     http://localhost/api/auth/logout)"
check "and that token is dead" 401 "$(tcp_code /repos -H "Authorization: Bearer $SOCKREV")"
check "while the caller's own is not" 200 "$(tcp_code /repos "${AUTH[@]}")"

check "logout" 204 "$(tcp_code /api/auth/logout "${AUTH[@]}" -X POST)"
check "token dead after logout" 401 "$(tcp_code /repos "${AUTH[@]}")"

echo "login throttle"
# B054: 100 wrong passwords took 2.66 s and the right one still worked
# afterwards. Now the first few failures are free and then the address is
# locked out, for longer each time, and the lockout holds even for the right
# password - otherwise it would not slow guessing at all.
#
# The waits come from the 429's own Retry-After, not fixed sleeps. The
# throttle counts in whole seconds, so a failure just before a second
# boundary can find the previous lockout already over, count once more and
# double it; a fixed 1.2 s sleep then lands inside a 2 s lockout. That
# flaked on CI.
login_hdrs() {  # login_hdrs <password>: status line and headers, lowercased
    curl -s -D - -o /dev/null -X POST -d "{\"password\":\"$1\"}" \
         "$URL/api/auth/login" | tr -d '\r' | tr 'A-Z' 'a-z'
}
status_of()      { printf '%s\n' "$1" | sed -n '1s/^http\/[0-9.]* \([0-9]*\).*/\1/p'; }
wait_retry_after() {  # sleep what a 429 asked for, plus a second for rounding
    local ra
    ra=$(printf '%s\n' "$1" | sed -n 's/^retry-after: *\([0-9]*\).*/\1/p')
    sleep $(( ${ra:-1} + 1 ))
}
CODES=""
LAST=""
for _ in $(seq 1 12); do
    LAST=$(login_hdrs guessing)
    c=$(status_of "$LAST")
    CODES="$CODES $c"
    [ "$c" = 429 ] && break
done
contains "wrong logins end in 429" "429" "$CODES"
contains "after the free attempts" "401 401 401 401 401 401" "$CODES"
wait_retry_after "$LAST"
check "another failure once the lockout lapses" 401 \
    "$(tcp_code /api/auth/login -X POST -d '{"password":"guessing"}')"
LAST=$(login_hdrs brandnewpassword)
check "the right password is refused while locked out" 429 "$(status_of "$LAST")"
contains "429 carries Retry-After" "retry-after:" "$LAST"
check "the socket is never locked out" 200 "$(curl -s -o /dev/null -w '%{http_code}' \
     --unix-socket "$SOCK" http://localhost/repos)"
wait_retry_after "$LAST"
check "the right password works once it lapses" 200 \
    "$(tcp_code /api/auth/login -X POST -d '{"password":"brandnewpassword"}')"

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
# And it was the daemon's shutdown that ran, not AppGuard's exit: that one
# closed the database under running jobs and skipped everything below,
# which left the socket behind.
if [ -e "$SOCK" ]; then bad "shutdown removes the socket" "no $SOCK" "still there"
else ok "shutdown removes the socket"; fi

# Retention defaults come from config.yaml. They were database keys nothing
# could set and the GFS job never read, so every pass used compiled-in
# numbers whatever the operator configured. A second daemon with keep_snaps
# at 2: four same-day backups hold no tier yet, so only the recency window
# keeps any, and the default of 1 would leave one.
echo "retention config"
R="$WORK/ret"
mkdir -p "$R/src"
echo one > "$R/src/f"
cat > "$R/config.yaml" <<EOF
db:
  path: $R/state.db
http:
  socket: $R/sock
  port: 0
log:
  level: warn
security:
  key_dir: $R/keys
retention:
  keep_snaps: 2
backup:
  store_workers: 2
EOF
C_BACKUP_CONFIG_PATH="$R/config.yaml" "$BIN" > "$R/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 50); do [ -S "$R/sock" ] && break; sleep 0.1; done
MAIN_SOCK="$SOCK"
SOCK="$R/sock"
sock_post() { curl -s -X POST --unix-socket "$SOCK" "http://localhost$1" ${2:+-d "$2"}; }
sock_post /repos "{\"path\":\"$R/repo\",\"label\":\"r\"}" > /dev/null
for i in 1 2 3 4; do
    echo "$i" >> "$R/src/f"
    wait_job "$(job_id "$(sock_post /repos/1/backup "{\"paths\":[\"$R/src\"]}")")"
done

# B033: setting a tag name already on another snapshot moved it silently,
# taking the protection off the first snapshot. Refused now, naming where
# the tag is, unless the request says move; the same snapshot again is
# fine. Snapshots 3 and 4 so retention below is unaffected.
sock_tag() { curl -s -w ' %{http_code}' -X POST --unix-socket "$SOCK" -d "$1" http://localhost/repos/1/tags; }
check "tag set" '{"ok":true} 200' "$(sock_tag '{"name":"keep","snap_id":3}')"
check "the same snapshot again is fine" '{"ok":true} 200' "$(sock_tag '{"name":"keep","snap_id":3}')"
OUT=$(sock_tag '{"name":"keep","snap_id":4}')
contains "an existing tag on another snapshot is a 409" ' 409' "$OUT"
contains "naming where the tag is" 'already on snapshot 3' "$OUT"
contains "and the tag did not move" '"snap_id":3' "$(sock_body /repos/1/tags)"
check "move moves it" '{"ok":true} 200' "$(sock_tag '{"name":"keep","snap_id":4,"move":true}')"
contains "to the new snapshot" '"snap_id":4' "$(sock_body /repos/1/tags)"
curl -s -o /dev/null -X DELETE --unix-socket "$SOCK" http://localhost/repos/1/tags/keep

# B035: a dry run's answer was in the daemon log only. The job reports the
# ids it would remove and the retention it used, and removes nothing.
DRY=$(job_id "$(sock_post '/repos/1/gfs?dry_run=1')")
wait_job "$DRY"
DRY_BODY=$(sock_body "/jobs/$DRY")
contains "gfs dry run names what it would remove" \
    '"result":{"dry_run":true,"would_remove":[1,2],"count":2,' "$DRY_BODY"
contains "and the retention it applied" '"retention":{"keep_snaps":2,' "$DRY_BODY"
check "and removes nothing" 4 \
    "$(sock_body /repos/1/snapshots | grep -o '"id":[0-9]*' | wc -l | tr -d ' ')"

GFS=$(job_id "$(sock_post /repos/1/gfs)")
wait_job "$GFS"
contains "a real pass names what it removed" \
    '"result":{"dry_run":false,"removed":[1,2],"count":2,' "$(sock_body "/jobs/$GFS")"
check "retention reads keep_snaps from config" 2 \
    "$(sock_body /repos/1/snapshots | grep -o '"id":[0-9]*' | wc -l | tr -d ' ')"

# B039: the journal entry for a retention pass had an empty message.
JOURNAL=$(sock_body '/repos/1/journal?limit=2')
contains "the journal says what a pass removed" \
    '"operation":"gfs","result":"success","duration_ms":' "$JOURNAL"
contains "with its ids" '"message":"removed 2 snapshots: 1, 2"' "$JOURNAL"
contains "and a dry run says it was one" '"message":"dry run: would remove 2 snapshots: 1, 2"' "$JOURNAL"
check "other jobs journal as they did" 4 "$(sock_body '/repos/1/journal?limit=10' \
    | grep -o '"operation":"backup","result":"success","duration_ms":[0-9]*,"message":null' \
    | wc -l | tr -d ' ')"

# B034, read side: a value stored before the route validated must not be
# read as atoi's 0, which for a tier means unlimited. Written straight into
# the database, since the route refuses it now.
if command -v sqlite3 > /dev/null 2>&1; then
    sqlite3 "$R/state.db" "INSERT INTO policy_values (repo_id, key, value)
                           VALUES (1, 'retention.keep_daily', 'banana')" 2>/dev/null
    BAD=$(job_id "$(sock_post '/repos/1/gfs?dry_run=1')")
    wait_job "$BAD"
    contains "a stored invalid count falls back to the default" \
        '"keep_daily":7,' "$(sock_body "/jobs/$BAD")"
else
    ok "a stored invalid count falls back to the default (skipped: no sqlite3)"
fi

# B094: tiers close on the clock, not on a later snapshot. Every snapshot
# showed gfs_flags 0 after a pass because the newest one's day never closed
# and same-day snapshots hold no tier. Moved to the last Saturday and Sunday
# of 2024 (noon UTC, so the same local day in any zone within 11 hours), the
# Sunday is that year's last daily, weekly, monthly and yearly: 15. Read
# back through the listing, which is what clients see.
if command -v sqlite3 > /dev/null 2>&1; then
    sqlite3 "$R/state.db" "UPDATE snapshots SET ts = 1735387200 WHERE repo_id = 1 AND id = 3;
                           UPDATE snapshots SET ts = 1735473600 WHERE repo_id = 1 AND id = 4"
    TIERS=$(job_id "$(sock_post /repos/1/gfs)")
    wait_job "$TIERS"
    LIST=$(sock_body /repos/1/snapshots)
    contains "the newest of a closed year holds every tier" '"id":4,"ts":1735473600,' "$LIST"
    contains "gfs_flags 15 on it" '"gfs_flags":15,' \
        "$(printf '%s' "$LIST" | grep -o '{"id":4,[^}]*}')"
    contains "and the day before is a daily" '"gfs_flags":1,' \
        "$(printf '%s' "$LIST" | grep -o '{"id":3,[^}]*}')"
else
    ok "tier flags reach the listing (skipped: no sqlite3)"
fi

# B092: the info series follows the stalest repository, and a label is
# free text, so its value is escaped as the exposition format requires.
# Repository 1's newest snapshot is now from 2024; a second one registered
# at the epoch is staler still.
sock_post /repos "{\"path\":\"$R/repo2\",\"label\":\"q\\\"u\\\\o\"}" > /dev/null
contains "the staler of two is named" \
    'cbackup_stalest_snapshot_repo_info{repo_id="1",label="r"} 1' "$(sock_body /metrics)"
if command -v sqlite3 > /dev/null 2>&1; then
    sqlite3 "$R/state.db" "UPDATE repos SET added_at = 0 WHERE id = 2"
    METRICS=$(sock_body /metrics)
    contains "and it moves when another becomes staler, label escaped" \
        'cbackup_stalest_snapshot_repo_info{repo_id="2",label="q\"u\\o"} 1' "$METRICS"
    check "one series, not one per repository" 1 \
        "$(printf '%s\n' "$METRICS" | grep -c '^cbackup_stalest_snapshot_repo_info{')"
else
    ok "the info series follows the stalest repository (skipped: no sqlite3)"
fi
# And a client can see them: the app shows the value in force, and these
# live in a file on this host that it cannot read.
contains "policy reports the configured defaults" \
    '"key":"retention.keep_snaps","value":2' "$(sock_body /repos/1/policy)"

# One object holds at most 16 MiB (OBJECT_MAX_BYTES); a larger file is a
# chunk list of content-defined chunks. B086: backup.store_workers reaches
# the backup. Three files over 16 MiB, and the configured 2 caps the pool
# where automatic would have used 3 on any machine with five CPUs or more;
# the main daemon, on automatic, stored its three small files serially.
mkdir -p "$R/big"
for i in 1 2 3; do head -c $((17 * 1024 * 1024)) /dev/urandom > "$R/big/f$i"; done
SW=$(job_id "$(sock_post /repos/2/backup "{\"paths\":[\"$R/big\"]}")")
wait_job "$SW"
SW_BODY=$(sock_body "/jobs/$SW")
contains "files over one object back up" '"state":"completed"' "$SW_BODY"
contains "store_workers from config sizes the pool" '"store_workers":2,' "$SW_BODY"
contains "and the backup stores all three" '"files":3,' "$SW_BODY"
rm -f "$R/big/f1" "$R/big/f2" "$R/big/f3"
rmdir "$R/big"

# B101: deleting a snapshot. A tag protects it unless force says otherwise,
# the 409 names the tags, a missing snapshot is a 404, and each delete is
# journalled. Snapshots 3 and 4 are what retention left.
sock_tag '{"name":"guard","snap_id":3}' > /dev/null
sock_tag '{"name":"also","snap_id":3}' > /dev/null
OUT=$(sock_send DELETE /repos/1/snapshots/3)
contains "a tagged snapshot is a 409" ' 409' "$OUT"
contains "naming its tags" 'tagged (also, guard)' "$OUT"
contains "and it is still listed" '"id":3,' "$(sock_body /repos/1/snapshots)"
contains "a bad force is a 400" ' 400' "$(sock_send DELETE '/repos/1/snapshots/3?force=maybe')"
check "force deletes it" '{"removed":true,"id":3,"tags_removed":2} 200' \
    "$(sock_send DELETE '/repos/1/snapshots/3?force=1')"
check "and its tags with it" '{"tags":[]}' "$(sock_body /repos/1/tags)"
contains "the repository's tag file loses them too" '"tags":[]' "$(cat "$R/repo/tags")"
contains "journalled" \
    '"operation":"snapshot-delete","result":"success"' "$(sock_body '/repos/1/journal?limit=1')"
contains "with what it removed" 'removed snapshot 3 and its tags also, guard (forced)' \
    "$(sock_body '/repos/1/journal?limit=1')"
check "deleting it again is a 404" '{"error":"no such snapshot"} 404' \
    "$(sock_send DELETE /repos/1/snapshots/3)"
check "so is one that never existed" '{"error":"no such snapshot"} 404' \
    "$(sock_send DELETE /repos/1/snapshots/999)"
check "an untagged snapshot needs no force" '{"removed":true,"id":4,"tags_removed":0} 200' \
    "$(sock_send DELETE /repos/1/snapshots/4)"

# Snapshot 4 was the newest, and ids were MAX(id) + 1 over the snapshots
# present: the next backup was 4 again, and anything still naming the
# deleted one named it. Ids are never reused now.
NEXT_JOB=$(job_id "$(sock_post /repos/1/backup "{\"paths\":[\"$R/src\"]}")")
wait_job "$NEXT_JOB"
contains "a deleted snapshot's id is not reused" '"result":{"snapshot_id":5,' \
    "$(sock_body "/jobs/$NEXT_JOB")"

# B113: removing a repository purged its job rows, and the next job took
# the id of the removed repository's newest one. Give repository 2 the
# newest job, remove it, wait for the purge to take that job's row, and the
# next job must still be above it.
GONE_JOB=$(job_id "$(sock_post /repos/2/backup "{\"paths\":[\"$R/src\"]}")")
wait_job "$GONE_JOB"
contains "remove repository 2" ' 200' "$(sock_send DELETE /repos/2)"
for _ in $(seq 1 100); do
    [ "$(sock_code "/jobs/$GONE_JOB")" = 404 ] && break
    sleep 0.1
done
check "the purge took its job row" 404 "$(sock_code "/jobs/$GONE_JOB")"
AFTER_JOB=$(job_id "$(sock_post /repos/1/backup "{\"paths\":[\"$R/src\"]}")")
wait_job "$AFTER_JOB"
if [ "$AFTER_JOB" -gt "$GONE_JOB" ]; then
    check "a purged job's id is not reused" ok ok
else
    check "a purged job's id is not reused" "above $GONE_JOB" "$AFTER_JOB"
fi
# OWN-33: a label with quotes, a backslash and non-ASCII text was written
# into the snapshot record unescaped, so the record was not JSON and a
# reindex lost the snapshot. It survives the listing and, with the index
# rows gone, a reindex from the repository alone. Sent as \u escapes; the
# listing returns UTF-8.
LBL_JSON='q\"u\\o caf\u00e9 \u2603'
LBL_OUT='q\"u\\o '$(printf 'caf\303\251 \342\230\203')
LJ=$(job_id "$(sock_post /repos/1/backup "{\"paths\":[\"$R/src\"],\"label\":\"$LBL_JSON\"}")")
wait_job "$LJ"
LSID=$(sock_body "/jobs/$LJ" | sed -n 's/.*"result":{"snapshot_id":\([0-9]*\).*/\1/p')
contains "an escaped label is listed intact" "\"id\":$LSID,\"ts\":" "$(sock_body /repos/1/snapshots)"
contains "with its label" "\"label\":\"$LBL_OUT\"" "$(sock_body /repos/1/snapshots)"
contains "the record holds it escaped" "\"label\":\"$LBL_OUT\"}" "$(cat "$R/repo/snapshots/$LSID")"
if command -v sqlite3 > /dev/null 2>&1; then
    sqlite3 "$R/state.db" "DELETE FROM nodes WHERE repo_id = 1;
                           DELETE FROM node_fts WHERE repo_id = 1;
                           DELETE FROM snapshots WHERE repo_id = 1"
    check "no snapshots listed once the rows are gone" 0 \
        "$(sock_body /repos/1/snapshots | grep -o '"id":[0-9]*' | wc -l | tr -d ' ')"
fi
RX=$(job_id "$(sock_post /repos/1/reindex)")
wait_job "$RX"
contains "the reindex succeeds" '"state":"completed"' "$(sock_body "/jobs/$RX")"
# The mark lives in the repository, not the index rows just deleted, so it
# still covers every id handed out, the deleted 3 and 4 included, and
# nothing needed repairing.
contains "the reindex reports the snapshot-id mark" "\"snapshot_mark\":$LSID," \
    "$(sock_body "/jobs/$RX")"
contains "and that the mark needed no repair" '"snapshot_ids_repaired":false' \
    "$(sock_body "/jobs/$RX")"
contains "and the label comes back from the record" "\"label\":\"$LBL_OUT\"" \
    "$(sock_body /repos/1/snapshots)"

# B118: a file the backup cannot read is left out, counted and named, in
# the result and in the journal. It used to stay in the snapshot with no
# content, and the job said nothing. Root reads through mode 000.
if [ "$(id -u)" != 0 ]; then
    echo "locked" > "$R/src/locked.txt"
    chmod 000 "$R/src/locked.txt"
    SJ=$(job_id "$(sock_post /repos/1/backup "{\"paths\":[\"$R/src\"]}")")
    wait_job "$SJ"
    chmod 600 "$R/src/locked.txt"
    SBODY=$(sock_body "/jobs/$SJ")
    contains "a backup with an unreadable file still completes" '"state":"completed"' "$SBODY"
    contains "and counts it" '"files_skipped":1,' "$SBODY"
    contains "and names it, with the reason" \
        "\"skipped\":[{\"path\":\"$R/src/locked.txt\",\"reason\":\"cannot open: Permission denied\"}],\"skipped_listed\":1," \
        "$SBODY"
    contains "and the journal says so" \
        "1 skipped (not in the snapshot); first: $R/src/locked.txt: cannot open: Permission denied" \
        "$(sock_body '/repos/1/journal?limit=1')"
    # And alerted, as a warning, without anyone opening the job. The worker
    # writes the line after the state, so it is waited for. No Pushover is
    # configured here, so the line must say it was not sent (NIT-11).
    WLINE="warning notification for job $SJ not sent, Pushover is not configured: "
    for _ in $(seq 1 100); do
        grep -q "$WLINE" "$R/daemon.log" && break
        sleep 0.1
    done
    contains "and the operator is alerted, naming the count and the first path" \
        "completed with a warning: 1 file(s) could not be read and are not in snapshot" \
        "$(grep -A1 "$WLINE" "$R/daemon.log")"
    contains "the alert names the file" "first: $R/src/locked.txt: cannot open: Permission denied" \
        "$(grep -A1 "$WLINE" "$R/daemon.log")"
    check "and does not claim it was sent" 0 \
        "$(grep -c "sending warning notification for job $SJ:" "$R/daemon.log")"
    rm -f "$R/src/locked.txt"
fi
# A clean backup alerts nobody: the label backup above skipped nothing.
check "a clean backup sends no warning" 0 \
    "$(grep -c "warning notification for job $LJ" "$R/daemon.log")"

# A backup schedule carries its label, checked when it is made.
SCH=$(sock_send POST /repos/1/schedules \
    "{\"action\":\"backup\",\"paths\":[\"$R/src\"],\"label\":\"$LBL_JSON\",\"trigger\":{\"type\":\"interval\",\"seconds\":86400}}")
contains "a labelled backup schedule is created" ' 201' "$SCH"
contains "and stores the label" "\"label\":\"$LBL_OUT\"" "$(sock_body /repos/1/schedules)"

kill -TERM "$DAEMON_PID" 2>/dev/null
wait "$DAEMON_PID" 2>/dev/null
DAEMON_PID=""
SOCK="$MAIN_SOCK"

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
