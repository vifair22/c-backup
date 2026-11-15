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
# A snapshot that does not exist is refused up front, not queued as a job
# that fails telling the operator to reindex.
check "restore of an unknown snapshot is a 404" 404 "$(curl -s -o /dev/null -w '%{http_code}' \
     -X POST --unix-socket "$SOCK" -d "{\"dest\":\"$WORK/nowhere\"}" \
     http://localhost/repos/1/snapshots/999/restore)"
RESTORE_JOB=$(job_id "$(curl -s -X POST --unix-socket "$SOCK" -d "{\"dest\":\"$WORK/out\"}" \
                        http://localhost/repos/1/snapshots/1/restore)")
wait_job "$RESTORE_JOB"
contains "restore completed" '"state":"completed"' "$(sock_body "/jobs/$RESTORE_JOB")"
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
check "disable schedule" 200 "$(curl -s -o /dev/null -w '%{http_code}' -X PUT --unix-socket "$SOCK" \
     -d '{"enabled":0}' http://localhost/repos/1/schedules/1)"
contains "schedule disabled" '"enabled":"0"' "$(sock_body /repos/1/schedules)"

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

echo "introspection"
contains "summary"      '"snapshots"'  "$(sock_body /repos/1/summary)"
contains "stats"        '"per_type"'   "$(sock_body /repos/1/stats)"
# Backups pack on completion, so the loose scope is legitimately empty here.
# What matters is that the scoped query answers rather than errors.
well_formed "loose stats scope answers" "$(sock_body '/repos/1/stats?scope=loose')"
contains "objects"      '"hash"'       "$(sock_body '/repos/1/objects?limit=2')"
contains "packs"        '{'            "$(sock_body /repos/1/packs)"
contains "fanout"       '"buckets"'    "$(sock_body /repos/1/fanout)"

# type 1 is a file. Tree objects are referenced by subtree_hash rather than
# content_hash, so they have no refs and would be a misleading probe. No
# location filter: a backup packs on completion, so which side of the loose
# and packed split an object lands on is not this test's business.
OBJ=$(sock_body '/repos/1/objects?limit=200' \
      | tr '}' '\n' | grep '"type":"1"' | head -1 \
      | sed -n 's/.*"hash":"\([a-f0-9]*\)".*/\1/p')
contains "object locate"  '"found":true' "$(sock_body "/repos/1/objects/$OBJ")"
contains "object refs"    '"refs"'       "$(sock_body "/repos/1/objects/$OBJ/refs")"
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
contains "diff counts one change"        '"modified":"1"'      "$DIFF"
contains "diff adds nothing"             '"added":"0"'         "$DIFF"


echo "packing"
PACK_JOB=$(job_id "$(curl -s -X POST --unix-socket "$SOCK" http://localhost/repos/1/pack)")
check "pack submitted" "1" "$([ -n "$PACK_JOB" ] && echo 1 || echo 0)"
wait_job "$PACK_JOB"
contains "pack completed" '"state":"completed"' "$(sock_body "/jobs/$PACK_JOB")"
contains "packs listed"  '"filename"'            "$(sock_body /repos/1/packs)"
contains "pack entries"  '"entry_index"'         "$(sock_body '/repos/1/packs/0/entries?limit=1')"
# Objects are in packs now, so every read path has to resolve through the
# index rather than the objects directory.
PACKED=$(sock_body '/repos/1/objects?location=pack&limit=1' \
         | sed -n 's/.*"hash":"\([a-f0-9]*\)".*/\1/p')
contains "packed object locates" '"found":true'  "$(sock_body "/repos/1/objects/$PACKED")"
contains "packed object layout"  '"header_size"' "$(sock_body "/repos/1/objects/$PACKED/layout")"
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

check "logout" 204 "$(tcp_code /api/auth/logout "${AUTH[@]}" -X POST)"
check "token dead after logout" 401 "$(tcp_code /repos "${AUTH[@]}")"

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
wait_job "$(job_id "$(sock_post /repos/1/gfs)")"
check "retention reads keep_snaps from config" 2 \
    "$(sock_body /repos/1/snapshots | grep -o '"id":"[0-9]*"' | wc -l | tr -d ' ')"
# And a client can see them: the app shows the value in force, and these
# live in a file on this host that it cannot read.
contains "policy reports the configured defaults" \
    '"key":"retention.keep_snaps","value":"2"' "$(sock_body /repos/1/policy)"
kill -TERM "$DAEMON_PID" 2>/dev/null
wait "$DAEMON_PID" 2>/dev/null
DAEMON_PID=""
SOCK="$MAIN_SOCK"

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
