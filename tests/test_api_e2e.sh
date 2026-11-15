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
contains "job reports bytes" '"bytes_total":' "$JOB_BODY"
contains "job reports when it started" '"started":1' "$JOB_BODY"
contains "job ids are numbers" "\"id\":$BACKUP_JOB," "$JOB_BODY"
contains "the job list carries the same detail" '"elapsed_s":' "$(sock_body /jobs)"
contains "snapshot listed" '"id":1,' "$(sock_body /repos/1/snapshots)"
contains "tree lists root" '"name"' "$(sock_body '/repos/1/snapshots/1/tree?parent=0')"
contains "search finds file" 'a.txt' "$(sock_body '/repos/1/search?q=txt')"

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
contains "an absent source is accepted with a warning" "does not exist on the daemon's host" \
    "$(curl -s -X PUT --unix-socket "$SOCK" -d '{"sources":["/no/such/mount"]}' \
       http://localhost/repos/1/sources)"
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
check "delete tag" 204 "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE --unix-socket "$SOCK" \
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
check "and removable" 204 "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE \
     --unix-socket "$SOCK" http://localhost/repos/1/tags/v1.2_pre-upgrade)"

# B034: policy stored any key and any value and said ok. Each of these was
# accepted and then ignored or misread by the retention pass.
put_policy() { sock_send PUT /repos/1/policy "{\"key\":\"$1\",\"value\":\"$2\"}"; }
OUT=$(put_policy retention.keep_hourly 5)
contains "an unknown policy key is a 400" ' 400' "$OUT"
contains "and the refusal lists the valid keys" 'maintenance.auto_pack' "$OUT"
contains "a non-numeric count is a 400" ' 400' "$(put_policy retention.keep_daily banana)"
contains "a negative count is a 400" ' 400' "$(put_policy retention.keep_weekly -3)"
contains "a signed count is a 400" ' 400' "$(put_policy retention.keep_weekly +3)"
contains "a non-boolean flag is a 400" ' 400' "$(put_policy maintenance.auto_pack maybe)"
contains "a boolean in another spelling is accepted" ' 200' "$(put_policy maintenance.auto_pack OFF)"
POL=$(sock_body /repos/1/policy)
contains "and stored in one spelling" '"key":"maintenance.auto_pack","value":false' "$POL"
contains "a refused value leaves the stored one" '"key":"retention.keep_daily","value":14' "$POL"
case "$POL" in *keep_hourly*) bad "an unknown key is not stored" "absent" "$POL" ;;
               *) ok "an unknown key is not stored" ;; esac
put_policy maintenance.auto_pack true > /dev/null

# B040: the defaults listed every retention key but not auto_pack, which is
# in force everywhere a repository has not set it.
contains "policy defaults include auto_pack, on" \
    '{"key":"maintenance.auto_pack","value":true}]}' "$(sock_body /repos/1/policy)"

# B008: values are typed, so the PUT takes them as the GET serves them.
check "a count may be sent as a number" '{"ok":true} 200' \
    "$(sock_send PUT /repos/1/policy '{"key":"retention.keep_monthly","value":6}')"
check "and a flag as a boolean" '{"ok":true} 200' \
    "$(sock_send PUT /repos/1/policy '{"key":"maintenance.auto_pack","value":true}')"
contains "and both read back typed" '"key":"retention.keep_monthly","value":6' \
    "$(sock_body /repos/1/policy)"

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
check "delete schedule" 204 "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE --unix-socket "$SOCK" \
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
contains "summary"      '"snapshots"'  "$(sock_body /repos/1/summary)"
contains "stats"        '"per_type"'   "$(sock_body /repos/1/stats)"
# Backups pack on completion, so the loose scope is legitimately empty here.
# What matters is that the scoped query answers rather than errors.
well_formed "loose stats scope answers" "$(sock_body '/repos/1/stats?scope=loose')"
contains "objects"      '"hash"'       "$(sock_body '/repos/1/objects?limit=2')"
contains "packs"        '{'            "$(sock_body /repos/1/packs)"
# B074: values that cannot mean anything were ignored, so the default page
# came back as though the request had been understood.
check "unknown object location is a 400" 400 "$(sock_code '/repos/1/objects?location=nowhere')"
check "location=all is accepted"         200 "$(sock_code '/repos/1/objects?location=all')"
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
contains "fanout"       '"buckets"'    "$(sock_body /repos/1/fanout)"

# type 1 is a file. Tree objects are referenced by subtree_hash rather than
# content_hash, so they have no refs and would be a misleading probe. No
# location filter: a backup packs on completion, so which side of the loose
# and packed split an object lands on is not this test's business.
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


echo "packing"
PACK_JOB=$(job_id "$(curl -s -X POST --unix-socket "$SOCK" http://localhost/repos/1/pack)")
check "pack submitted" "1" "$([ -n "$PACK_JOB" ] && echo 1 || echo 0)"
wait_job "$PACK_JOB"
contains "pack completed" '"state":"completed"' "$(sock_body "/jobs/$PACK_JOB")"
contains "packs listed"  '"filename"'            "$(sock_body /repos/1/packs)"
contains "pack entries"  '"entry_index"'         "$(sock_body '/repos/1/packs/0/entries?limit=1')"
# B075: missing ids answered as empty successes, which reads as "empty".
check "entries of a missing pack"   404 "$(sock_code '/repos/1/packs/99999/entries')"
check "tree of a missing snapshot"  404 "$(sock_code '/repos/1/snapshots/99999/tree?parent=0')"
check "tree under a missing node"   404 "$(sock_code '/repos/1/snapshots/1/tree?parent=99999999')"
check "findings of a missing verify run" 404 "$(sock_code '/repos/1/verify/99999')"
check "hits of a missing search"    404 "$(sock_code '/repos/1/grep/99999')"
# Objects are in packs now, so every read path has to resolve through the
# index rather than the objects directory.
PACKED=$(sock_body '/repos/1/objects?location=pack&limit=1' \
         | sed -n 's/.*"hash":"\([a-f0-9]*\)".*/\1/p')
contains "packed object locates" '"found":true'  "$(sock_body "/repos/1/objects/$PACKED")"
contains "packed object layout"  '"header_size"' "$(sock_body "/repos/1/objects/$PACKED/layout")"
# B076: the docs said 56 bytes. A packed object sits behind the 51-byte pack
# entry header (a loose one behind the 64-byte object header).
contains "packed header is the 51-byte entry header" '"header_size":51' \
    "$(sock_body "/repos/1/objects/$PACKED/layout")"
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
# Restoring into a repository writes beside objects/ and packs/. Refused,
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
check "unknown verify level rejected" 400 \
    "$(curl -s -o /dev/null -w '%{http_code}' -X POST --unix-socket "$SOCK" \
       "http://localhost/repos/1/verify?level=sideways")"
contains "verify age metric present" 'cbackup_last_verify_age_seconds' "$(sock_body /metrics)"

# B008: every number the API serves is a JSON number. Rows used to be
# copied out of the database as text, so ids, counts, sizes, timestamps,
# modes and flags all arrived quoted and every client had to know which
# strings were numbers. Swept across the repository-scoped reads, after
# backup, pack, tag, policy, schedule and verify have all left rows behind.
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

check "logout" 204 "$(tcp_code /api/auth/logout "${AUTH[@]}" -X POST)"
check "token dead after logout" 401 "$(tcp_code /repos "${AUTH[@]}")"

echo "login throttle"
# B054: 100 wrong passwords took 2.66 s and the right one still worked
# afterwards. Now the first few failures are free and then the address is
# locked out, for longer each time, and the lockout holds even for the right
# password - otherwise it would not slow guessing at all.
CODES=""
for _ in $(seq 1 12); do
    c=$(tcp_code /api/auth/login -X POST -d '{"password":"guessing"}')
    CODES="$CODES $c"
    [ "$c" = 429 ] && break
done
contains "wrong logins end in 429" "429" "$CODES"
contains "after the free attempts" "401 401 401 401 401 401" "$CODES"
sleep 1.2
check "another failure once the lockout lapses" 401 \
    "$(tcp_code /api/auth/login -X POST -d '{"password":"guessing"}')"
check "the right password is refused while locked out" 429 \
    "$(tcp_code /api/auth/login -X POST -d '{"password":"brandnewpassword"}')"
contains "429 carries Retry-After" "retry-after:" \
    "$(curl -s -D - -o /dev/null -X POST -d '{"password":"brandnewpassword"}' \
       "$URL/api/auth/login" | tr 'A-Z' 'a-z')"
check "the socket is never throttled" 200 "$(curl -s -o /dev/null -w '%{http_code}' -X POST \
     --unix-socket "$SOCK" -d '{"password":"brandnewpassword"}' http://localhost/api/auth/login)"
sleep 2.2
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
# And a client can see them: the app shows the value in force, and these
# live in a file on this host that it cannot read.
contains "policy reports the configured defaults" \
    '"key":"retention.keep_snaps","value":2' "$(sock_body /repos/1/policy)"
kill -TERM "$DAEMON_PID" 2>/dev/null
wait "$DAEMON_PID" 2>/dev/null
DAEMON_PID=""
SOCK="$MAIN_SOCK"

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
