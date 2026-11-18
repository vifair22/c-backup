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
# A write made outside the write lock aborts the daemon here rather than
# landing in another thread's transaction unnoticed (B157, db_write.h).
export C_BACKUP_TXN_GUARD=abort
CLI="${2:-build/bins/c-backup}"
# Scratch goes where the Makefile says (C_BACKUP_TEST_DIR), and on disk when
# run by hand: /tmp is often a tmpfs, so dense scratch there is RAM.
TEST_ROOT="${C_BACKUP_TEST_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/c-backup-test}"
mkdir -p "$TEST_ROOT"
WORK="$(mktemp -d "$TEST_ROOT/c_backup_cli_e2e.XXXXXX")"
# Ports per run, so concurrent suites (CI jobs on one runner, parallel
# worktrees) never reach each other's daemons: one for the main daemon, one
# for the second daemon that adopts a replica, one for the bind test. They
# came from the pid, skipping any `ss` showed busy, which still raced the
# bind; the kernel chooses them now (FLAKE-13).
. "$(dirname "$0")/free_port.sh"
mapfile -t PORTS < <(free_ports 3 "$WORK")
[ "${#PORTS[@]}" -eq 3 ] || { echo "cannot choose TCP ports"; exit 1; }
MAIN_PORT=${PORTS[0]}
PORT2=${PORTS[1]}
BINDPORT=${PORTS[2]}
SOCK="$WORK/c.sock"
PASS=0
FAIL=0

cleanup() {
    [ -n "${DAEMON2_PID:-}" ] && kill "$DAEMON2_PID" 2>/dev/null
    wait "${DAEMON2_PID:-}" 2>/dev/null
    [ -n "${DAEMON_PID:-}" ] && kill "$DAEMON_PID" 2>/dev/null
    wait "${DAEMON_PID:-}" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT
# A signal would otherwise end the script without the EXIT trap: the scratch
# guard stops an over-budget suite with SIGTERM and still wants it gone.
trap 'exit 143' TERM
trap 'exit 130' INT

ok()  { PASS=$((PASS+1)); printf '  ok   %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; }

check()    { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "$2" "$3"; fi; }
contains() { case "$3" in *"$2"*) ok "$1" ;; *) bad "$1" "contains '$2'" "$3" ;; esac; }

cb() { "$CLI" --socket "$SOCK" "$@"; }
# Does FILE contain the first 32 bytes of SAMPLE, verbatim? Compared as hex
# with a fixed-string search: the bytes are random, and handed to grep as a
# pattern they were a random regular expression - metacharacters, or a
# newline that split it into several patterns, one of which could be empty
# and match anything. That made the plaintext checks fail at random.
contains_bytes() {  # contains_bytes <sample> <file>
    [ -f "$1" ] && [ -f "$2" ] || return 1
    local pat; pat=$(head -c 32 "$1" | od -An -tx1 -v | tr -d ' \n')
    od -An -tx1 -v "$2" | tr -d ' \n' | grep -qF "$pat"
}

echo "no daemon"
# The most important case: a clear message, a non-zero exit, and no hang.
OUT=$("$CLI" --socket "$WORK/absent.sock" repo list 2>&1); RC=$?
check    "exits non-zero without a daemon" 1 "$RC"
contains "explains the daemon is not running" "daemon not running" "$OUT"
# B080: the advice fits where the CLI runs. It said systemctl inside the
# Docker image, which has no systemd. CI itself may be a container, so the
# expectation follows the same signals the CLI reads.
# B103: systemd with the unit installed wins even inside a container, and a
# container booting systemd without the unit gets both commands.
UNIT=0
for u in /etc/systemd/system /run/systemd/system /usr/local/lib/systemd/system \
         /usr/lib/systemd/system /lib/systemd/system; do
    [ -e "$u/c-backupd.service" ] && UNIT=1
done
if [ -d /run/systemd/system ] && [ "$UNIT" = 1 ]; then
    contains "suggests how to start it" "systemctl start c-backupd" "$OUT"
elif [ -e /.dockerenv ] || [ -e /run/.containerenv ] || [ -n "${container:-}" ]; then
    contains "suggests how to start it" "start that container" "$OUT"
elif [ -d /run/systemd/system ]; then
    contains "suggests how to start it" "systemctl start c-backupd" "$OUT"
else
    contains "suggests how to start it" "Start c-backupd." "$OUT"
fi
contains "names the socket options" "--socket PATH" "$OUT"
contains "and the variable" "C_BACKUP_SOCKET" "$OUT"
OUT=$(container=podman "$CLI" --socket "$WORK/absent.sock" repo list 2>&1)
if [ -d /run/systemd/system ] && [ "$UNIT" = 1 ]; then
    contains "a container with the unit installed says systemctl" "systemctl start c-backupd" "$OUT"
elif [ -d /run/systemd/system ]; then
    # The marker alone cannot be faked on a host booted with systemd; the
    # decision table itself is covered by test_start_advice.
    contains "a container running systemd gives both: docker" "docker start c-backupd" "$OUT"
    contains "and systemctl" "systemctl start c-backupd" "$OUT"
else
    contains "in a container it says to start the container" "docker start c-backupd" "$OUT"
    case "$OUT" in
        *systemctl*) bad "and not systemctl" "no systemctl" "$OUT" ;;
        *)           ok  "and not systemctl" ;;
    esac
fi
# jobs watch had its own copy of the advice, which always said systemctl.
OUT=$(container=podman "$CLI" --socket "$WORK/absent.sock" jobs watch 1 2>&1); RC=$?
check    "watch without a daemon exits non-zero" 1 "$RC"
contains "watch gives the same advice" "--socket PATH" "$OUT"

echo "offline commands"
contains "version" "c-backup" "$("$CLI" --version)"
contains "help lists commands" "backup <repo-id>" "$("$CLI" help)"
OUT=$("$CLI" nonsense 2>&1); RC=$?
check    "unknown command exits non-zero" 1 "$RC"
contains "unknown command explains itself" "unknown command" "$OUT"

mkdir -p "$WORK/src"
echo "content one" > "$WORK/src/one.txt"
# Incompressible, so it is stored raw: compression would hide plaintext from
# the encryption check further down regardless of whether it was encrypted.
head -c 65536 /dev/urandom > "$WORK/src/random.bin"
echo "content two" > "$WORK/src/two.txt"

# info, not warn: the OWN-31 checks read the line the job manager logs for
# a failure notification, the only trace that path leaves with Pushover
# unconfigured (where the line says it was not sent, NIT-11).
cat > "$WORK/config.yaml" <<EOF
db:
  path: $WORK/state.db
http:
  socket: $SOCK
  port: $MAIN_PORT
  bind: 127.0.0.1
log:
  level: info
security:
  key_dir: $WORK/keys
EOF

C_BACKUP_CONFIG_PATH="$WORK/config.yaml" "$DAEMON" > "$WORK/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
if [ ! -S "$SOCK" ]; then echo "daemon did not start"; cat "$WORK/daemon.log"; exit 1; fi

echo "with daemon"
contains "health" "ready" "$(cb health)"
contains "repo add" '"id":1' "$(cb repo add "$WORK/repo" main)"
check "an unknown repo id is an error" 1 "$(cb summary 99 >/dev/null 2>&1; echo $?)"
# Errors go to stderr, so stdout piped to a JSON reader never holds one.
check "an error body stays off stdout" "" "$(cb summary 99 2>/dev/null)"
contains "and is on stderr" '"error"' "$(cb summary 99 2>&1 >/dev/null)"
# --help after a command answers locally, before any validation or request.
contains "command --help shows its usage" "usage: c-backup repo" "$(cb repo add --help)"
check    "command --help exits 0" 0 "$(cb backup --help >/dev/null; echo $?)"
contains "help <command>" "usage: c-backup restore" "$(cb help restore)"
contains "repo list" '"path"' "$(cb repo list)"
contains "repo show" '"uuid"' "$(cb repo show 1)"

# Job commands wait for their job and report what happened, so there is
# nothing to poll and the assertion is on the outcome rather than on the
# daemon having accepted the request.
contains "backup waits and completes" '"state":"completed"' "$(cb backup 1 "$WORK/src")"
contains "jobs list"     '"action"'            "$(cb jobs)"
contains "snapshots"     '"id":1,'             "$(cb snapshots 1)"
contains "ls"            '"name"'              "$(cb ls 1 1)"
contains "search"        'one.txt'             "$(cb search 1 txt)"
# B074: these were silently replaced by the defaults.
# Written --limit=-1: spaced, a value starting with a dash is refused by the
# CLI as a missing one (B127), and this checks what the daemon says.
OUT=$(cb snapshots 1 --limit=-1 2>&1); RC=$?
check    "snapshots --limit -1 fails" 1 "$RC"
contains "and says why" "limit is out of range; it must be from 1 to" "$OUT"
OUT=$(cb objects 1 --location nowhere 2>&1); RC=$?
check    "objects --location nowhere fails" 1 "$RC"

contains "tag set"    '"ok"'      "$(cb tag set 1 keeper 1)"
contains "tag list"   'keeper'    "$(cb tag list 1)"
# B095: the removes printed nothing on success, against the contract of one
# JSON document per command.
OUT=$(cb tag remove 1 keeper); RC=$?
check    "tag remove" 0           "$RC"
check    "tag remove prints what it removed" '{"removed":true,"name":"keeper"}' "$OUT"

# B038: removing a tag that is not there exited 0 with no output.
OUT=$(cb tag remove 1 keeper 2>&1); RC=$?
check    "tag remove of a missing tag fails" 1 "$RC"
contains "and says so" 'no such tag' "$OUT"
# B037: a nonexistent snapshot, or no name at all, was accepted.
check "tag set on a missing snapshot fails" 1 "$(cb tag set 1 ghost 999 > /dev/null 2>&1; echo $?)"
check "note on a missing snapshot fails" 1 "$(cb snapshot note 1 999 x > /dev/null 2>&1; echo $?)"
check "tag set with an empty name fails" 1 "$(cb tag set 1 '' 1 > /dev/null 2>&1; echo $?)"
# B036: the name went into the path unencoded, so "a b" made a request line
# the server answered with an HTML page and a 301. Encoded, the daemon sees
# the name and answers in JSON. And a name like that cannot be set now.
contains "tag remove encodes the name" '{"error":"no such tag"}' "$(cb tag remove 1 'a b' 2>&1)"
contains "a name that is not a path segment is refused" 'invalid tag name' \
    "$(cb tag set 1 'a/b' 1 2>&1)"

contains "schedule add"    '"id"' "$(cb schedule add 1 gc 3600)"
contains "schedule list"   'gc'   "$(cb schedule list 1)"
OUT=$(cb schedule remove 1 1); RC=$?
check    "schedule remove" 0      "$RC"
check    "schedule remove prints what it removed" '{"removed":true,"id":1}' "$OUT"

contains "gc"      '"state":"completed"' "$(cb gc 1)"
REINDEX_OUT=$(cb reindex 1)
contains "reindex" '"state":"completed"' "$REINDEX_OUT"
# The reindex reconciles the repository's snapshot-id mark with the
# snapshots it holds: one snapshot here, and a mark already covering it.
contains "reindex reports the snapshot-id mark" '"snapshot_mark":1,' "$REINDEX_OUT"
contains "and that it needed no repair" '"snapshot_ids_repaired":false' "$REINDEX_OUT"
contains "gfs"     '"state":"completed"' "$(cb gfs 1)"
# B035: a dry run printed the job record and nothing about what it would
# remove. One snapshot, so the answer is nothing, but it is an answer.
GFS_OUT=$(cb gfs 1 --dry-run)
contains "gfs dry run says what it would remove" '"would_remove":[],"count":0' "$GFS_OUT"
contains "and the retention it used" '"retention":{"keep_snaps":' "$GFS_OUT"
contains "journal" 'entries'  "$(cb journal 1)"

RESTORE_OUT=$(cb restore 1 1 "$WORK/out")
contains "restore waits and completes" '"state":"completed"' "$RESTORE_OUT"

if diff -r "$WORK/src" "$WORK/out$WORK/src" > /dev/null 2>&1; then
    ok "restored tree is identical"
else
    bad "restored tree is identical" "no differences" "diff reported changes"
fi

# Every command in the table gets exercised. The CLI is the only interface an
# operator has on a server, so a capability reachable through the API and not
# through here is a capability they cannot use without hand-writing curl.
echo "full command coverage"
contains "stats"    '"per_type"' "$(cb stats 1)"
contains "summary"  '"'          "$(cb summary 1)"
contains "objects"  '"'          "$(cb objects 1 --limit 5)"
contains "objects filtered by location" '"objects"' "$(cb objects 1 --location packed --limit 5)"
# Nothing is staged once a backup has sealed its pack, so this is empty.
contains "empty location filter keeps its key" '"objects":[]' \
    "$(cb objects 1 --location staged --limit 5)"
# There are no loose objects; the daemon refuses the old location.
check "objects --location loose fails" 1 "$(cb objects 1 --location loose >/dev/null 2>&1; echo $?)"
contains "packs"    '"'          "$(cb packs 1)"
# B075: these printed an empty listing and exited 0 for ids that do not exist.
check "packs on a missing pack fails"      1 "$(cb packs 1 99999 >/dev/null 2>&1; echo $?)"
check "ls on a missing snapshot fails"     1 "$(cb ls 1 99999 >/dev/null 2>&1; echo $?)"
check "verify show on a missing run fails" 1 "$(cb verify show 1 99999 >/dev/null 2>&1; echo $?)"
# The B084 audit: routes and parameters with no command or flag.
contains "stats --scope staged" '"per_type"' "$(cb stats 1 --scope staged)"
check "stats --scope takes only staged" 1 "$(cb stats 1 --scope packed >/dev/null 2>&1; echo $?)"
check "and not loose any more" 1 "$(cb stats 1 --scope loose >/dev/null 2>&1; echo $?)"
check "jobs --limit pages" 1 "$(cb jobs --limit 1 | grep -o '"action":' | wc -l | tr -d ' ')"
# A bad offset reaching the daemon's 400 proves the flag is sent at all.
OUT=$(cb jobs --offset x 2>&1); RC=$?
check    "jobs --offset is sent" 1 "$RC"
contains "and checked by the daemon" "offset must be" "$OUT"
contains "fanout"   '"'          "$(cb fanout 1)"
contains "metrics"  'cbackup_'   "$(cb metrics)"
contains "health live" 'ok'      "$(cb health live)"

# An object hash out of the listing, then every view of it.
OBJ_HASH=$(cb objects 1 --limit 1 | sed -n 's/.*"hash":"\([0-9a-f]*\)".*/\1/p' | head -1)
if [ -n "$OBJ_HASH" ]; then
    contains "object locate" '"' "$(cb object 1 "$OBJ_HASH")"
    # A tree object has no node referencing it, so this may legitimately be
    # empty - but the key is present either way now.
    contains "object refs" '"refs"' "$(cb object 1 "$OBJ_HASH" refs)"
    contains "object layout" '"' "$(cb object 1 "$OBJ_HASH" layout)"
else
    bad "object listing yields a hash" "a hash" "none"
fi

contains "policy set"  '"ok"'         "$(cb policy set 1 retention.keep_daily 14)"
contains "policy list" 'keep_daily'   "$(cb policy list 1)"
# B034: any key and any value were stored.
OUT=$(cb policy set 1 retention.keep_hourly 5 2>&1); RC=$?
check    "an unknown policy key fails" 1 "$RC"
contains "and lists the valid keys" 'retention.keep_daily' "$OUT"
check "a non-numeric count fails" 1 "$(cb policy set 1 retention.keep_daily banana > /dev/null 2>&1; echo $?)"

contains "snapshot note set" '"ok"'   "$(cb snapshot note 1 1 'checked by hand')"
contains "snapshot note get" 'checked by hand' "$(cb snapshot note 1 1)"
contains "snapshot tree"     '"name"' "$(cb snapshot tree 1 1)"

# The pack job is gone: every object is written into a pack as it is
# stored, so there is nothing left for a pack command to do.
OUT=$(cb pack 1 2>&1); RC=$?
check    "pack is no longer a command" 1 "$RC"
contains "and says so" "unknown command 'pack'" "$OUT"
check "a pack schedule is refused" 1 "$(cb schedule add 1 pack 3600 >/dev/null 2>&1; echo $?)"
contains "verify" '"level":"index"'     "$(cb verify 1 --level index)"
check "verify list --offset moves on" 1 \
    "$([ "$(cb verify list 1 --limit 1 --offset 1 | grep -o '"level":' | wc -l | tr -d ' ')" -le 1 ] && echo 1 || echo 0)"
VERIFY_OUT=$(cb verify 1 --level deep)
contains "deep verify reports its run" '"level":"deep"' "$VERIFY_OUT"
contains "deep verify checked objects" '"checked"'      "$VERIFY_OUT"
contains "gc dry run" '"state":"completed"' "$(cb gc 1 --dry-run)"
contains "journal result filter" '"entries"' "$(cb journal 1 --result success --limit 5)"

echo "job reporting"
# B062: gc and its dry run printed a bare job; the counts were only logged.
GC_OUT=$(cb gc 1)
contains "gc reports what it swept" '"objects_swept":' "$GC_OUT"
contains "and what compaction did" '"packs_rewritten":' "$GC_OUT"
contains "and what coalescing did (B122)" '"packs_removed_by_coalesce":' "$GC_OUT"
contains "and the summary says so" ' coalesced into ' "$(cb journal 1)"
contains "and the stale temp files it removed" '"tmp_files_removed":' "$GC_OUT"
contains "gc dry run says it is one" '"dry_run":true' "$(cb gc 1 --dry-run)"
contains "gc journals a summary" '"message":"swept ' "$(cb journal 1)"

# B072: every entry said "daemon", so a manual run and a scheduled one could
# not be told apart.
contains "journal says a manual job came over the api" '"source":"api"' "$(cb journal 1)"
check    "and no entry claims the daemon asked" "" \
    "$(cb journal 1 | grep -o '"source":"daemon"')"
contains "the job record says so too" '"origin":"api"' "$(cb jobs)"

# B077: a source that does not exist failed nothing; the backup completed
# with the other source's files and a log line.
OUT=$(cb backup 1 "$WORK/src" "$WORK/no-such-dir" 2>&1); RC=$?
check    "a missing source fails the backup" 1 "$RC"
contains "naming the source" "no-such-dir" "$OUT"

# B058: `jobs watch` on a running job printed nothing until the job ended,
# because the CLI read the whole response before printing any of it. A job
# row nothing will finish holds the stream open for as long as the test
# needs, so the assertion is on what arrived, not on a race with real work.
if command -v sqlite3 > /dev/null 2>&1; then
    sqlite3 -cmd ".timeout 10000" "$WORK/state.db" "INSERT INTO jobs (id, repo_id, action, state, started,
        progress_phase) VALUES (9999, 1, 'backup', 'running', strftime('%s','now'),
        'store')"
    "$CLI" --socket "$SOCK" jobs watch 9999 > "$WORK/watch.out" 2> "$WORK/watch.err" &
    WPID=$!
    # The row stays running until this test completes it, so whatever the
    # watch prints before then arrived while the job ran, which is what B058
    # is about. A fixed sleep 1 guessed how long the CLI takes to start and
    # connect, and lost under load (FLAKE-17); this waits up to 10 s for it.
    for _ in $(seq 1 100); do
        grep -q '"state":"running"' "$WORK/watch.out" 2>/dev/null && break
        sleep 0.1
    done
    contains "jobs watch prints a running job at once" '"state":"running"' \
        "$(cat "$WORK/watch.out")"
    sqlite3 -cmd ".timeout 10000" "$WORK/state.db" "UPDATE jobs SET state = 'completed',
        ended = strftime('%s','now') WHERE id = 9999"
    for _ in $(seq 1 50); do kill -0 "$WPID" 2>/dev/null || break; sleep 0.1; done
    if kill -0 "$WPID" 2>/dev/null; then
        bad "jobs watch ends with the job" "exited" "still running"
        kill "$WPID" 2>/dev/null
    else
        ok "jobs watch ends with the job"
    fi
    wait "$WPID"; WRC=$?
    check    "and exits 0" 0 "$WRC"
    contains "and prints how it ended" '"state":"completed"' "$(cat "$WORK/watch.out")"
    # B096: it printed the SSE framing, "data: " before each document and a
    # blank line after it. Every line is now one JSON document.
    check "jobs watch prints no SSE framing" "" "$(grep -n '^data:\|^event:\|^$' "$WORK/watch.out")"
    check "every line is a JSON document" "" "$(grep -v '^{.*}$' "$WORK/watch.out")"
    check "and nothing on stderr" "" "$(cat "$WORK/watch.err")"
else
    ok "jobs watch prints a running job at once (skipped: no sqlite3)"
fi
# The daemon ends the stream with a named error event for a job it does not
# have. That is a failure, its document an error: stderr, not stdout.
"$CLI" --socket "$SOCK" jobs watch 987654 > "$WORK/watch.out" 2> "$WORK/watch.err"; RC=$?
check    "jobs watch of a missing job fails" 1 "$RC"
check    "and prints nothing on stdout" "" "$(cat "$WORK/watch.out")"
check    "and the error is bare JSON on stderr" '{"error":"no such job"}' "$(cat "$WORK/watch.err")"

# B057: `jobs cancel` printed nothing. A gc queued behind a backup of a lot
# of data is still cancellable when the request lands, whatever the machine.
# 512 MiB, in 32 files of 16 MiB.
mkdir -p "$WORK/big"
for i in $(seq 1 32); do
    dd if=/dev/zero of="$WORK/big/big$i.bin" bs=1M count=16 status=none
done
BJ=$(cb backup 1 "$WORK/big" --no-wait | sed -n 's/.*"job_id":\([0-9]*\).*/\1/p')
GJ=$(cb gc 1 --no-wait | sed -n 's/.*"job_id":\([0-9]*\).*/\1/p')
OUT=$(cb jobs cancel "$GJ")
contains "jobs cancel prints the job" "\"id\":$GJ" "$OUT"
contains "and that the cancel landed" '"cancel_requested":true' "$OUT"
cb jobs cancel "$BJ" > /dev/null 2>&1
for _ in $(seq 1 100); do
    cb jobs "$GJ" | grep -q '"state":"cancelled"' && break
    sleep 0.1
done
contains "the cancelled gc ends cancelled" '"state":"cancelled"' "$(cb jobs "$GJ")"
contains "and the journal says so" '"operation":"gc","result":"cancelled"' "$(cb journal 1)"
for _ in $(seq 1 100); do
    cb jobs "$BJ" | grep -Eq '"state":"(cancelled|completed)"' && break
    sleep 0.1
done
# tmp/.lock is the writers' claim on tmp/ (B133), not staging: it stays.
check "a cancelled backup leaves no staging files" "" \
    "$(find "$WORK/repo/tmp" -type f ! -name .lock 2>/dev/null)"
rm -f "$WORK"/big/big*.bin

# A second snapshot so diff has two sides, and so snapshot delete has
# something to remove that is not the only copy.
echo "content three" > "$WORK/src/three.txt"
contains "second backup completed" '"state":"completed"' "$(cb backup 1 "$WORK/src")"
contains "diff" '"added"' "$(cb diff 1 1 2)"

# B033: a tag set on a name already in use moved it silently, taking the
# retention protection off the snapshot it was on. --move says it is meant.
contains "tag set" '"ok"' "$(cb tag set 1 pinned 1)"
OUT=$(cb tag set 1 pinned 2 2>&1); RC=$?
check    "a tag in use elsewhere is not moved" 1 "$RC"
contains "and the refusal says where it is" 'already on snapshot 1' "$OUT"
contains "--move moves it" '"ok"' "$(cb tag set 1 pinned 2 --move)"
contains "to the new snapshot" '"snap_id":2' "$(cb tag list 1)"
cb tag remove 1 pinned > /dev/null

# Exclusions have to work from the CLI too, which is where anyone would
# actually type them.
mkdir -p "$WORK/exsrc/cache"
echo "keepme"  > "$WORK/exsrc/keep.txt"
echo "dropme"  > "$WORK/exsrc/cache/junk.bin"
echo "dropme2" > "$WORK/exsrc/big.iso"
contains "excluding backup completed" '"state":"completed"' \
    "$(cb backup 1 "$WORK/exsrc" --exclude cache --exclude '*.iso')"
contains "unexcluded file present" 'keep.txt' "$(cb search 1 keep)"
case "$(cb search 1 junk)" in
    *junk.bin*) bad "cli --exclude drops the directory" "absent" "present" ;;
    *)          ok  "cli --exclude drops the directory" ;;
esac
case "$(cb search 1 big)" in
    *big.iso*) bad "cli --exclude glob drops the file" "absent" "present" ;;
    *)         ok  "cli --exclude glob drops the file" ;;
esac

# B101: a tagged snapshot is refused unless forced, the refusal names the
# tag, a missing snapshot is an error, and every delete is journalled.
cb tag set 1 guard 1 > /dev/null
OUT=$(cb snapshot delete 1 1 2>&1); RC=$?
check    "a tagged snapshot is not deleted" 1 "$RC"
contains "and the refusal names the tag" 'guard' "$OUT"
contains "and says how to force it" '--force' "$OUT"
contains "the snapshot is still there" '"snap_id":1' "$(cb tag list 1)"
OUT=$(cb snapshot delete 1 1 --force 2>/dev/null); RC=$?
check "snapshot delete --force" 0 "$RC"
check "snapshot delete prints what it removed" \
    '{"removed":true,"id":1,"tags_removed":1}' "$OUT"
case "$(cb tag list 1)" in
    *guard*) bad "a forced delete removes the snapshot's tag" "absent" "present" ;;
    *)       ok  "a forced delete removes the snapshot's tag" ;;
esac
contains "the delete is journalled" 'removed snapshot 1 and its tag guard (forced)' \
    "$(cb journal 1)"
OUT=$(cb snapshot delete 1 1 2>&1); RC=$?
check    "deleting it again fails" 1 "$RC"
contains "as a missing snapshot" 'HTTP 404' "$OUT"
OUT=$(cb snapshot delete 1 999 2>&1); RC=$?
check    "deleting a snapshot that never existed fails" 1 "$RC"
contains "with one JSON error" '{"error":"no such snapshot"}' "$OUT"
# The id comes from the add. This disabled a hardcoded id 2 that did not
# exist, which passed only because a missing schedule answered success.
VSID=$(cb schedule add 1 verify 7200 | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
contains "schedule enable/disable" '"ok"' "$(cb schedule disable 1 "$VSID" || echo miss)"

# Calendar schedules. The CLI offered --cron, which built a trigger type the
# daemon rejects by design, and had no way to ask for the calendar triggers
# the daemon does support.
contains "weekly schedule from the CLI" '"id"' \
    "$(cb schedule add 1 gfs --every week --on sunday --at 03:00 --catchup skip --on-overlap skip)"
contains "monthly schedule from the CLI" '"id"' \
    "$(cb schedule add 1 verify --every month --on-day 31 --at 02:30)"
contains "calendar schedules listed" 'calendar' "$(cb schedule list 1)"
contains "a weekly schedule needs its day" 'needs --on WEEKDAY' \
    "$(cb schedule add 1 gc --every week --at 03:00 2>&1)"
contains "a non-numeric day is refused before sending" 'positive number' \
    "$(cb schedule add 1 gc --every month --on-day first --at 03:00 2>&1)"

# Content search. The pre-daemon CLI had this and the rewrite dropped it, so
# "which backup still has that line" had no answer short of restoring one.
echo "grep"
echo "the passphrase is hunter2" > "$WORK/src/secrets.txt"
cb backup 1 "$WORK/src" > /dev/null
GREP_OUT=$(cb grep 1 'passphrase is hunter2')
contains "grep finds the line"    'secrets.txt' "$GREP_OUT"
contains "grep reports the text"  'hunter2'     "$GREP_OUT"
contains "grep reports a line no" '"line"'      "$GREP_OUT"
# The hits alone: printing the run record first made two JSON documents,
# and nothing that reads JSON accepts that.
check "grep prints one document" "1" "$(printf '%s\n' "$GREP_OUT" | wc -l | tr -d ' ')"
contains "grep paths are absolute" "\"path\":\"$WORK/src/" "$GREP_OUT"
contains "grep regex syntax"      'secrets.txt' "$(cb grep 1 '^the pass[a-z]+ is')"
case "$(cb grep 1 'zzznotinanyfile')" in
    *'"hits"'*) ok "grep with no match still answers" ;;
    *)          bad "grep with no match still answers" '"hits"' "$(cb grep 1 zzznotinanyfile)" ;;
esac
contains "grep ignore case" 'secrets.txt' "$(cb grep 1 'HUNTER2' -i)"
case "$(cb grep 1 'hunter2' --path-prefix "$WORK/src/docs")" in
    *secrets.txt*) bad "grep path prefix scopes the search" "absent" "present" ;;
    *)             ok  "grep path prefix scopes the search" ;;
esac
OUT=$(cb grep 1 'unclosed[' 2>&1); RC=$?
check "grep rejects a bad pattern" 1 "$RC"
contains "grep --no-wait returns a job" '"job_id"' "$(cb grep 1 hunter2 --no-wait)"
# B066: a snapshot that does not exist completed with zero files, which
# reads as "not in that backup".
OUT=$(cb grep 1 hunter2 --snapshot 999 2>&1); RC=$?
check    "grep on a missing snapshot fails" 1 "$RC"
contains "and says so at submission" "no such snapshot" "$OUT"
check    "a malformed snapshot is a 400" 400 "$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    --unix-socket "$SOCK" -d '{"pattern":"x","snapshot":"latest"}' http://localhost/repos/1/grep)"
# B068: an unchanged file came back once per snapshot. Now one result per
# path and content, counting the snapshots, and --snapshot narrows to one.
SR=$(cb search 1 one.txt)
check "search collapses identical copies" 1 \
    "$(printf '%s' "$SR" | grep -o "\"path\":\"$WORK/src/one.txt\"" | wc -l | tr -d ' ')"
NSNAP=$(printf '%s' "$SR" | sed -n 's/.*"snapshots":\([0-9]*\).*/\1/p' | head -1)
if [ "${NSNAP:-0}" -ge 2 ]; then ok "and counts the snapshots it is in"; else bad "and counts the snapshots it is in" ">= 2" "${NSNAP:-none}"; fi
NEWEST=$(cb snapshots 1 --limit 1 | sed -n 's/.*"id":\([0-9]*\),.*/\1/p')
contains "--snapshot narrows the search" "\"snap_id\":$NEWEST," "$(cb search 1 one.txt --snapshot "$NEWEST")"
OUT=$(cb search 1 one.txt --snapshot 999 2>&1); RC=$?
check    "search in a missing snapshot fails" 1 "$RC"
# B067: the hit caps are request options, refused out of range rather than
# clamped, and the job result says whether anything was cut short.
contains "grep takes a per-file cap" 'secrets.txt' "$(cb grep 1 hunter2 --max-hits-per-file 1 --max-hits 5)"
OUT=$(cb grep 1 hunter2 --max-hits 0 2>&1); RC=$?
check    "a zero hit cap is refused" 1 "$RC"
contains "and names the range" "max_hits is out of range; it must be from 1 to 1000000" "$OUT"
GJ=$(curl -s -X POST --unix-socket "$SOCK" -d '{"pattern":"hunter2"}' http://localhost/repos/1/grep \
     | sed -n 's/.*"job_id":\([0-9]*\).*/\1/p')
for _ in $(seq 1 50); do
    case "$(curl -s --unix-socket "$SOCK" "http://localhost/jobs/$GJ")" in *'"state":"completed"'*) break ;; esac
    sleep 0.1
done
contains "the grep job reports its caps" '"files_capped":0' "$(curl -s --unix-socket "$SOCK" "http://localhost/jobs/$GJ")"
# NIT-2: a file capped per file was counted on the job and not on the run,
# so a run listed later could not say a file held more than it recorded.
printf 'capline\ncapline\ncapline\n' > "$WORK/src/capped.txt"
cb backup 1 "$WORK/src" > /dev/null
CJ=$(curl -s -X POST --unix-socket "$SOCK" -d '{"pattern":"capline","max_hits_per_file":1}' \
     http://localhost/repos/1/grep | sed -n 's/.*"job_id":\([0-9]*\).*/\1/p')
for _ in $(seq 1 50); do
    case "$(curl -s --unix-socket "$SOCK" "http://localhost/jobs/$CJ")" in *'"state":"completed"'*) break ;; esac
    sleep 0.1
done
contains "a capped grep job counts the capped file" '"files_capped":1' \
    "$(curl -s --unix-socket "$SOCK" "http://localhost/jobs/$CJ")"
contains "and so does its run record" '"pattern":"capline"' "$(cb grep runs 1 --limit 1)"
contains "the run record carries files_capped" '"files_capped":1' "$(cb grep runs 1 --limit 1)"

# B084: GET /repos/<id>/grep and /grep/<run-id> had no command, so a search
# run with --no-wait, or one whose output scrolled away, could not be read
# back. Every search above left a run behind.
RUNS=$(cb grep runs 1)
contains "grep runs lists searches" '"runs":[{"id":' "$RUNS"
contains "grep runs names the pattern" '"pattern":"hunter2"' "$RUNS"
check "grep runs --limit pages" 1 \
    "$(cb grep runs 1 --limit 1 | grep -o '"pattern":' | wc -l | tr -d ' ')"
FIRST=$(cb grep runs 1 --limit 1 | sed -n 's/.*"runs":\[{"id":\([0-9]*\).*/\1/p')
SECOND=$(cb grep runs 1 --limit 1 --offset 1 | sed -n 's/.*"runs":\[{"id":\([0-9]*\).*/\1/p')
check "grep runs --offset moves on" 1 "$([ -n "$SECOND" ] && [ "$SECOND" -lt "$FIRST" ] && echo 1 || echo 0)"
HIT_RUN=$(printf '%s' "$RUNS" | grep -o '"id":[0-9]*,"snapshot":[0-9]*,"pattern":"hunter2"' | head -1 \
          | sed 's/"id":\([0-9]*\).*/\1/')
contains "grep show reads a run's hits" 'secrets.txt' "$(cb grep show 1 "$HIT_RUN")"
contains "grep show pages" '"hits"' "$(cb grep show 1 "$HIT_RUN" --limit 1 --offset 0)"
check "grep show on a missing run fails" 1 "$(cb grep show 1 99999 >/dev/null 2>&1; echo $?)"
check "grep runs without a repo is usage" 1 "$(cb grep runs >/dev/null 2>&1; echo $?)"

# B064: schedule options are validated per action. Values outside the
# documented sets were stored and read back as something else; parameters an
# action never reads were stored as if they configured something; a grep
# schedule could not be given a pattern at all.
OUT=$(cb schedule add 1 gc 3600 --on-overlap bogus 2>&1); RC=$?
check    "an unknown on_overlap is refused" 1 "$RC"
contains "and the valid ones are named" "skip, queue or cancel_running" "$OUT"
OUT=$(cb schedule add 1 gc 3600 --catchup nope 2>&1); RC=$?
check    "an unknown catchup is refused" 1 "$RC"
OUT=$(cb schedule add 1 gc 3600 --dest /mnt/x 2>&1); RC=$?
check    "a parameter the action ignores is refused" 1 "$RC"
contains "and says whose it is" "dest applies only to a replicate" "$OUT"
OUT=$(cb schedule add 1 grep 3600 2>&1); RC=$?
check    "a grep schedule needs a pattern" 1 "$RC"
OUT=$(cb schedule add 1 grep 3600 --pattern 'unclosed[' 2>&1); RC=$?
check    "a grep schedule with a bad pattern is refused" 1 "$RC"
check    "--on-overlap queue is accepted" 0 \
    "$(cb schedule add 1 gc 3600 --on-overlap queue --catchup run_once >/dev/null 2>&1; echo $?)"
GSID=$(cb schedule add 1 grep 1 --pattern 'passphrase is' | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
contains "a grep schedule stores its pattern" '"params":{"pattern":"passphrase is"' "$(cb schedule list 1)"
FIRED=0
for _ in $(seq 1 50); do
    case "$(curl -s --unix-socket "$SOCK" 'http://localhost/repos/1/grep?limit=1')" in
        *'"pattern":"passphrase is"'*'"completed":true'*) FIRED=1; break ;;
    esac
    sleep 0.2
done
check "a scheduled grep runs with its pattern" 1 "$FIRED"
cb schedule remove 1 "$GSID" >/dev/null 2>&1
# NIT-1: ignore_case was stored as the number 1; every other flag is a
# boolean. A schedule made before the change, holding 1, still reads.
ISID=$(cb schedule add 1 grep 3600 --pattern 'case test' -i | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
contains "a grep schedule stores ignore_case as a boolean" '"ignore_case":true' "$(cb schedule list 1)"
cb schedule remove 1 "$ISID" >/dev/null 2>&1
OLD_IC=$(curl -s -X POST --unix-socket "$SOCK" \
         -d '{"action":"grep","trigger":{"type":"interval","seconds":3600},"pattern":"old spelling","ignore_case":1}' \
         http://localhost/repos/1/schedules)
OSID=$(printf '%s' "$OLD_IC" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
contains "ignore_case sent as 1 is still accepted" '"id"' "$OLD_IC"
contains "and stored as a boolean" '"pattern":"old spelling","ignore_case":true' "$(cb schedule list 1)"
[ -n "$OSID" ] && cb schedule remove 1 "$OSID" >/dev/null 2>&1
check "ignore_case of 2 is refused" 400 "$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    --unix-socket "$SOCK" \
    -d '{"action":"grep","trigger":{"type":"interval","seconds":3600},"pattern":"x","ignore_case":2}' \
    http://localhost/repos/1/schedules)"

# Encryption at creation, and the escrow path that keeps a system-managed key
# from existing only on the machine being backed up.
echo "encryption"
ENC_OUT=$(cb repo add "$WORK/encrepo" enc --encrypt system)
contains "encrypted repo created"     '"encryption":"system"' "$ENC_OUT"
contains "key returned once"          '"key":"'               "$ENC_OUT"
contains "key not yet escrowed"       '"key_escrowed":false'  "$ENC_OUT"
contains "creation warns about escrow" 'Save this key'        "$ENC_OUT"
ENC_ID=$(printf '%s' "$ENC_OUT" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
ENC_KEY=$(printf '%s' "$ENC_OUT" | sed -n 's/.*"key":"\([0-9a-f]*\)".*/\1/p')

# The key is on disk, private, and the marker names it without holding it.
test -f "$WORK/keys"/*.key && ok "keyfile written" || bad "keyfile written" "present" "absent"
KEYMODE=$(stat -c %a "$WORK"/keys/*.key)
check "keyfile is private" 600 "$KEYMODE"
contains "marker records encryption" '"mode":"system"' "$(cat "$WORK/encrepo/.cbackup")"
case "$(cat "$WORK/encrepo/.cbackup")" in
    *"$ENC_KEY"*) bad "marker does not contain the key" "absent" "present" ;;
    *)            ok  "marker does not contain the key" ;;
esac

contains "unescrowed repo counted" 'cbackup_repos_key_unescrowed 1' "$(cb metrics)"
EXPORTED=$(cb repo key export "$ENC_ID")
contains "key export matches creation" "$ENC_KEY" "$EXPORTED"
contains "key ack accepted" '"ok"' "$(cb repo key ack "$ENC_ID")"
contains "escrow gauge clears" 'cbackup_repos_key_unescrowed 0' "$(cb metrics)"

# Recovery: a host that lost its keyfile takes it back from escrow.
rm -f "$WORK"/keys/*.key
OUT=$(cb repo key export "$ENC_ID" 2>&1); RC=$?
check "export without a keyfile fails" 1 "$RC"
contains "key import accepted" '"ok"' "$(cb repo key import "$ENC_ID" "$ENC_KEY")"
contains "key readable again" "$ENC_KEY" "$(cb repo key export "$ENC_ID")"
OUT=$(cb repo key import "$ENC_ID" "$(printf '0%.0s' $(seq 1 64))" 2>&1); RC=$?
check "wrong key refused" 1 "$RC"

# B048, B050: the key commands on an unencrypted repository. Export sent the
# operator looking for a keyfile that never existed; ack answered ok.
OUT=$(cb repo key export 1 2>&1); RC=$?
check    "key export of an unencrypted repo fails" 1 "$RC"
contains "and says it is not encrypted" "repository is not encrypted" "$OUT"
contains "as a 400" "HTTP 400" "$OUT"
OUT=$(cb repo key ack 1 2>&1); RC=$?
check    "key ack of an unencrypted repo fails" 1 "$RC"
contains "and says it is not encrypted" "repository is not encrypted" "$OUT"

# Passphrase mode needs nothing escrowed: the wrapped key travels with the
# repository, so the passphrase alone recovers it.
PASS_OUT=$(cb repo add "$WORK/passrepo" pass --encrypt passphrase --passphrase 'correct horse')
contains "passphrase repo created"   '"encryption":"passphrase"' "$PASS_OUT"
contains "passphrase repo escrowed"  '"key_escrowed":true'       "$PASS_OUT"
contains "wrapped key in the marker" '"wrapped"' "$(cat "$WORK/passrepo/.cbackup")"
OUT=$(cb repo add "$WORK/nopass" np --encrypt passphrase 2>&1); RC=$?
check "passphrase mode requires a passphrase" 1 "$RC"
# B044: a one-character passphrase was accepted.
OUT=$(cb repo add "$WORK/shortpass" sp --passphrase 'a' 2>&1); RC=$?
check    "a short passphrase is refused" 1 "$RC"
contains "and says how long it must be" "at least 8" "$OUT"
check    "and creates nothing" 1 "$([ -e "$WORK/shortpass" ]; echo $?)"
# B011: a passphrase without --encrypt made an unencrypted repository and
# exited 0. It implies passphrase mode now, and a mode that cannot use one
# refuses it instead of dropping it.
IMPLIED=$(cb repo add "$WORK/implied" imp --passphrase 'correct horse')
contains "a passphrase alone implies passphrase mode" '"encryption":"passphrase"' "$IMPLIED"
contains "and the marker says so" '"mode":"passphrase"' "$(cat "$WORK/implied/.cbackup")"
OUT=$(cb repo add "$WORK/nonepass" np --encrypt none --passphrase 'x' 2>&1); RC=$?
check    "encrypt none with a passphrase is refused" 1 "$RC"
contains "and says why" "encrypt none" "$OUT"
OUT=$(cb repo add "$WORK/syspass" sp --encrypt system --passphrase 'x' 2>&1); RC=$?
check    "encrypt system with a passphrase is refused" 1 "$RC"
check    "and creates nothing" 1 "$([ -e "$WORK/syspass" ]; echo $?)"

# Removing a repository deregisters it and leaves its data where it was.
RM_ID=$(cb repo add "$WORK/gone" gone | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
cb backup "$RM_ID" "$WORK/src" >/dev/null
OUT=$(cb repo remove "$RM_ID"); RC=$?
check    "repo remove" 0 "$RC"
check    "repo remove prints what it removed" "{\"removed\":true,\"id\":$RM_ID}" "$OUT"
check    "repo remove keeps the directory" 0 "$([ -f "$WORK/gone/.cbackup" ]; echo $?)"
contains "repo remove deregisters" '"error"' "$(cb repo show "$RM_ID" 2>&1)"
# B046: the newest id was handed out again, and with it the removed
# repository's schedules and history.
NEXT_ID=$(cb repo add "$WORK/after-gone" after | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
if [ -n "$NEXT_ID" ] && [ "$NEXT_ID" -gt "$RM_ID" ]; then
    ok "a removed repository's id is not reused"
else
    bad "a removed repository's id is not reused" "greater than $RM_ID" "${NEXT_ID:-none}"
fi
cb repo remove "$NEXT_ID" >/dev/null 2>&1

# NIT-16: registering a repository that already holds snapshots listed none,
# and verify passed with "checked 0", until a manual reindex. Registration
# now queues the reindex itself, as origin "register".
OUT=$(cb repo add "$WORK/gone" back)
contains "re-adding a repository with snapshots counts them" '"snapshots_unindexed":1' "$OUT"
contains "and queues a reindex" '"reindex_job":' "$OUT"
BACK_ID=$(printf '%s' "$OUT" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
RJ=$(printf '%s' "$OUT" | sed -n 's/.*"reindex_job":\([0-9]*\).*/\1/p')
for _ in $(seq 1 100); do
    cb jobs "$RJ" | grep -q '"state":"completed"' && break
    sleep 0.1
done
contains "the reindex completes" '"state":"completed"' "$(cb jobs "$RJ")"
contains "and records who asked" '"origin":"register"' "$(cb jobs "$RJ")"
contains "the snapshot is listed without a manual reindex" '"id":1,' "$(cb snapshots "$BACK_ID")"
contains "the journal names the registration" '"source":"register"' "$(cb journal "$BACK_ID")"
OUT=$(cb repo add "$WORK/gone" back)
case "$OUT" in
    *reindex_job*) bad "re-adding an indexed repository queues nothing" "no reindex_job" "$OUT" ;;
    *) ok "re-adding an indexed repository queues nothing" ;;
esac
cb repo remove "$BACK_ID" >/dev/null 2>&1

# Data written into an encrypted repository has to actually be encrypted.
# Everything above this tests key management, which was all working while the
# key was never handed to the store and every byte went to disk in the clear.
cb backup "$ENC_ID" "$WORK/src" >/dev/null
ENC_DAT=$(find "$WORK/encrepo/packs" -name '*.dat' | head -1)
if [ -n "$ENC_DAT" ]; then
    ok "encrypted repo produced a pack"
    # Entry header: hash[32] type[1] compression[1] encryption[1] sizes[16].
    ENCBYTE=$(od -An -tu1 -j 46 -N 1 "$ENC_DAT" | tr -d ' ')
    check "pack entries are encrypted" 1 "$ENCBYTE"
else
    bad "encrypted repo produced a pack" "a pack file" "none"
fi

# A file that is stored raw would appear verbatim in the pack if the repo
# were not encrypted; grep is useless for compressed text, so use the
# incompressible fixture.
if contains_bytes "$WORK/src/random.bin" "$ENC_DAT" 2>/dev/null; then
    bad "no plaintext in the encrypted pack" "absent" "present"
else
    ok "no plaintext in the encrypted pack"
fi

# And it restores to exactly what went in.
cb restore "$ENC_ID" 1 "$WORK/encout" >/dev/null
if diff -r "$WORK/src" "$WORK/encout$WORK/src" >/dev/null 2>&1; then
    ok "encrypted round trip is byte-identical"
else
    bad "encrypted round trip is byte-identical" "identical" "differs"
fi

# B041: with the key gone, backup and restore of an encrypted repository
# failed deep inside the job ("refusing to pack", "1 entry could not be
# restored"). They are refused up front now, naming the repository and the
# command that fixes it.
ENC_UUID=$(sed -n 's/.*"uuid"[: ]*"\([^"]*\)".*/\1/p' "$WORK/encrepo/.cbackup" | head -1)
mv "$WORK/keys/$ENC_UUID.key" "$WORK/enc.key.aside"
OUT=$(cb backup "$ENC_ID" "$WORK/src" 2>&1); RC=$?
check    "backup without the key is refused" 1 "$RC"
contains "and names the repository" "no key loaded for repository $ENC_UUID" "$OUT"
contains "and the fix" "c-backup repo key import" "$OUT"
contains "as a 409" "HTTP 409" "$OUT"
OUT=$(cb restore "$ENC_ID" 1 "$WORK/nokeyout" 2>&1); RC=$?
check    "restore without the key is refused" 1 "$RC"
contains "and says why" "no key loaded for repository $ENC_UUID" "$OUT"
check    "and writes nothing" 1 "$([ -e "$WORK/nokeyout" ]; echo $?)"
OUT=$(cb reindex "$ENC_ID" 2>&1); RC=$?
check    "reindex without the key is refused" 1 "$RC"
contains "and says why" "no key loaded for repository $ENC_UUID" "$OUT"
contains "and the index is untouched" '"id":1,' "$(cb snapshots "$ENC_ID")"
mv "$WORK/enc.key.aside" "$WORK/keys/$ENC_UUID.key"

# B042: a passphrase repository recovered with nothing but its passphrase.
# The host loses its key cache and its registration; a wrong passphrase was
# accepted, and the right one wrote no key, so reindex found nothing and the
# restore after it found no snapshot.
PASS_ID=$(printf '%s' "$PASS_OUT" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
PASS_UUID=$(sed -n 's/.*"uuid"[: ]*"\([^"]*\)".*/\1/p' "$WORK/passrepo/.cbackup" | head -1)
cb backup "$PASS_ID" "$WORK/src" >/dev/null
# B052: tags lived only in state.db, so this recovery came back with every
# snapshot unprotected. The repository carries them now.
cb tag set "$PASS_ID" guard 1 >/dev/null
contains "a tag is written into the repository" '"name":"guard"' "$(cat "$WORK/passrepo/tags" 2>&1)"
rm -f "$WORK/keys/$PASS_UUID.key"
cb repo remove "$PASS_ID" >/dev/null
OUT=$(cb repo add "$WORK/passrepo" pass2 --passphrase 'wrong horse' 2>&1); RC=$?
check    "a wrong passphrase is refused" 1 "$RC"
contains "and says so" "wrong passphrase" "$OUT"
check    "and registers nothing" 0 "$(cb repo list | grep -c passrepo)"
OUT=$(cb repo add "$WORK/passrepo" pass2 2>&1); RC=$?
check    "no passphrase still registers" 0 "$RC"
contains "but says the key is missing" "no key loaded for repository $PASS_UUID" "$OUT"
PASS_ID=$(printf '%s' "$OUT" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
# B045: registering it again with the passphrase unlocks it.
OUT=$(cb repo add "$WORK/passrepo" pass2 --passphrase 'correct horse' 2>&1); RC=$?
check    "the right passphrase unlocks a registered repo" 0 "$RC"
contains "and says it did" '"key_unlocked":true' "$OUT"
check    "and caches the key" 0 "$([ -f "$WORK/keys/$PASS_UUID.key" ]; echo $?)"
# NIT-16: with the key loaded, the registration reindexes it unasked.
PRJ=$(printf '%s' "$OUT" | sed -n 's/.*"reindex_job":\([0-9]*\).*/\1/p')
for _ in $(seq 1 100); do
    cb jobs "$PRJ" | grep -q '"state":"completed"' && break
    sleep 0.1
done
contains "the unlocked repo reindexes" '"completed"' "$(cb jobs "$PRJ")"
contains "and the job says the tag came back" '"tags_restored":1' "$(cb jobs "$PRJ")"
contains "a manual reindex still runs" '"completed"' "$(cb reindex "$PASS_ID")"
contains "the tag survives losing its database rows" 'guard' "$(cb tag list "$PASS_ID")"
contains "and lists its snapshot" '"id":1,' "$(cb snapshots "$PASS_ID")"
# B047: a reindexed snapshot listed "size":"", which is null now that a
# missing value is served as one.
case "$(cb snapshots "$PASS_ID")" in
    *'"size":""'*|*'"size":null'*) bad "a reindexed snapshot keeps its size" "a size" "no size" ;;
    *)             ok  "a reindexed snapshot keeps its size" ;;
esac
cb restore "$PASS_ID" 1 "$WORK/passout" >/dev/null
if diff -r "$WORK/src" "$WORK/passout$WORK/src" >/dev/null 2>&1; then
    ok "a passphrase-recovered repo restores byte-identical"
else
    bad "a passphrase-recovered repo restores byte-identical" "identical" "differs"
fi

# Verify records runs and findings, and both were unreachable until now: the
# counters reached an operator only through /metrics and the findings not at
# all. A scrub whose answer you cannot read is a scrub you did not run.
VOUT=$(cb verify "$ENC_ID" --level deep)
contains "verify waits and reports its run" '"level":"deep"' "$VOUT"
contains "verify reports what it checked"   '"checked"'      "$VOUT"
check "verify prints one document"         "1"              "$(printf '%s\n' "$VOUT" | wc -l | tr -d ' ')"
contains "verify reports findings"          '"findings"'     "$VOUT"
contains "clean repo has no findings"       '"findings":[]'  "$VOUT"
contains "verify list shows past runs"      '"runs"'         "$(cb verify list "$ENC_ID" --limit 2)"

# Loss is a failure. A deep verify that found a pack overwritten with random
# bytes exited 0 and its job read "completed", so anything judging the run by
# its exit status was told the repository was fine. The run and findings
# still have to come out, on stdout, because they say what was lost.
mkdir -p "$WORK/losssrc"
for i in 1 2 3; do echo "loss fixture $i" > "$WORK/losssrc/f$i.txt"; done
LOSS_ID=$(cb repo add "$WORK/lossrepo" loss | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
cb backup "$LOSS_ID" "$WORK/losssrc" > /dev/null
LOSS_DAT=$(find "$WORK/lossrepo/packs" -name '*.dat' | head -1)
if [ -n "$LOSS_DAT" ]; then
    # Everything past the first 64 bytes: payloads and the parity that would
    # rebuild them, so nothing is left to recover from.
    LOSS_LEN=$(stat -c %s "$LOSS_DAT")
    head -c $((LOSS_LEN - 64)) /dev/urandom | \
        dd of="$LOSS_DAT" bs=1 seek=64 conv=notrunc status=none
    OUT=$(cb verify "$LOSS_ID" --level deep 2>"$WORK/loss.err"); RC=$?
    check    "verify that finds loss exits non-zero" 1 "$RC"
    contains "the failed job says why"  "data loss"              "$(cat "$WORK/loss.err")"
    contains "the failed job is failed" '"state":"failed"'       "$(cat "$WORK/loss.err")"
    contains "the run still prints"     '"level":"deep"'         "$OUT"
    contains "its findings still print" '"status":"unrecoverable"' "$OUT"
    check    "and as one document"      "1" "$(printf '%s\n' "$OUT" | wc -l | tr -d ' ')"

    # Nothing in that pack survived, trailer included, so the repairing run
    # salvaged it: every object checked against its own name, every one
    # failing, each carried into a new pack and reported as lost. What is
    # left is the true loss, not a pack nothing can walk.
    contains "the pack was salvaged" '"status":"salvaged"' "$OUT"
    if [ -e "$LOSS_DAT" ]; then bad "the salvaged pack is gone" "absent" "present"
    else ok "the salvaged pack is gone"; fi
    case "$OUT" in
        *'"status":"unwalkable pack"'*) bad "no unwalkable pack is left" "none" "$OUT" ;;
        *)                              ok  "no unwalkable pack is left" ;;
    esac
    OUT=$(cb verify "$LOSS_ID" --level deep --dry-run 2>/dev/null); RC=$?
    check    "a later check still exits non-zero" 1 "$RC"
    contains "and names the loss" '"status":"unrecoverable"' "$OUT"
    case "$OUT" in
        *'"unrecoverable":0,'*) bad "it counts the objects lost" "unrecoverable > 0" "$OUT" ;;
        *)                      ok  "it counts the objects lost" ;;
    esac
    case "$OUT" in
        *'"status":"unwalkable pack"'*) bad "with no unwalkable pack" "none" "$OUT" ;;
        *)                              ok  "with no unwalkable pack" ;;
    esac
    contains "verify show reads the failed run back" '"findings"' \
        "$(cb verify show "$LOSS_ID" "$(printf '%s' "$OUT" | sed -n 's/.*"runs":\[{"id":\([0-9]*\),.*/\1/p')")"
    # A failed run is still a completed walk, and the gauge reads those.
    GAUGE=$(cb metrics | sed -n 's/^cbackup_verify_unrecoverable_objects \([0-9]*\)$/\1/p')
    if [ "${GAUGE:-0}" -gt 0 ]; then ok "the loss gauge sees it"
    else bad "the loss gauge sees it" "> 0" "${GAUGE:-absent}"; fi
else
    bad "loss repo produced a pack" "a pack file" "none"
fi

# B106: verify --dry-run is a check that writes nothing to the repository,
# whatever else was asked for. One byte of damage inside a 1 MiB
# incompressible payload is within parity: the dry run has to report it
# recoverable and leave the byte where it is, and a repairing run fixes it.
mkdir -p "$WORK/drysrc"
head -c 1048576 /dev/urandom > "$WORK/drysrc/blob.bin"
DRY_ID=$(cb repo add "$WORK/dryrepo" dry | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
cb backup "$DRY_ID" "$WORK/drysrc" > /dev/null
DRY_OBJ=$(find "$WORK/dryrepo/packs" -type f -size +1000k 2>/dev/null | head -1)
if [ -n "$DRY_OBJ" ]; then
    DRY_GOOD=$(sha256sum < "$DRY_OBJ")
    DRY_OFF=$(( $(stat -c %s "$DRY_OBJ") / 2 ))
    DRY_BYTE=$(od -An -tu1 -j "$DRY_OFF" -N1 "$DRY_OBJ" | tr -d ' ')
    if [ "$DRY_BYTE" = 0 ]; then printf '\377'; else printf '\000'; fi | \
        dd of="$DRY_OBJ" bs=1 seek="$DRY_OFF" conv=notrunc status=none
    DRY_BAD=$(sha256sum < "$DRY_OBJ")
    OUT=$(cb verify "$DRY_ID" --dry-run 2>&1); RC=$?
    check    "a dry run over recoverable damage succeeds" 0 "$RC"
    contains "and reports nothing repaired" '"repaired":0,' "$OUT"
    contains "but the damage recoverable"   '"recoverable":1,' "$OUT"
    check    "and leaves the repository untouched" "$DRY_BAD" "$(sha256sum < "$DRY_OBJ")"
    # --dry-run wins over a request that would otherwise repair.
    OUT=$(cb verify "$DRY_ID" --level deep --dry-run 2>&1)
    contains "even with the level named" '"repaired":0,' "$OUT"
    OUT=$(cb verify "$DRY_ID" 2>&1)
    contains "a repairing run fixes it" '"repaired":1,' "$OUT"
    check    "and writes the right bytes back" "$DRY_GOOD" "$(sha256sum < "$DRY_OBJ")"
else
    bad "dry-run repo stored the blob" "a file over 1000k" "$(find "$WORK/dryrepo" -type f | head)"
fi

# B111 asked whether maintenance.auto_pack kept a sparse file's object out of
# a pack. There is no pack sweep left for it to switch: every object, a
# sparse file's included, is in a pack when the backup ends, and the key is
# refused as unknown. The sparse file is a chunk list with its hole as a gap,
# and comes back byte-identical with the hole still a hole.
mkdir -p "$WORK/apsrc"
for i in $(seq 1 16); do echo "auto-pack small $i" > "$WORK/apsrc/s$i.txt"; done
head -c 65536 /dev/urandom > "$WORK/apsrc/sparse.bin"
truncate -s 8M "$WORK/apsrc/sparse.bin"
AP_ID=$(cb repo add "$WORK/aprepo" ap | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
check "auto_pack is no longer a policy key" 1 \
    "$(cb policy set "$AP_ID" maintenance.auto_pack false > /dev/null 2>&1; echo $?)"
cb backup "$AP_ID" "$WORK/apsrc" > /dev/null
check "nothing is left staged after the backup" 0 \
    "$(cb objects "$AP_ID" --location staged --limit 100 | grep -o '"hash"' | wc -l | tr -d ' ')"
check "every object is in a pack, the sparse file's too" 17 \
    "$(cb objects "$AP_ID" --location packed --limit 100 | grep -o '"type":1,' | wc -l | tr -d ' ')"
cb restore "$AP_ID" 1 "$WORK/apout" > /dev/null
if cmp -s "$WORK/apsrc/sparse.bin" "$WORK/apout$WORK/apsrc/sparse.bin"; then
    ok "a sparse file restores byte-identical"
else
    bad "a sparse file restores byte-identical" "identical" "differs"
fi
AP_BLOCKS=$(stat -c %b "$WORK/apout$WORK/apsrc/sparse.bin")
if [ "$AP_BLOCKS" -lt 2048 ]; then
    ok "and its hole is still a hole"
else
    bad "and its hole is still a hole" "under 1 MiB allocated" "$AP_BLOCKS blocks"
fi

# Job commands wait by default and exit non-zero when the job fails.
OUT=$(cb gc "$ENC_ID"); RC=$?
check    "gc waits for its job"  0            "$RC"
contains "gc reports completion" '"completed"' "$OUT"
contains "--no-wait returns the id" '"job_id"'   "$(cb verify "$ENC_ID" --no-wait)"

# Search takes a filename, not an FTS expression. A dot used to be a syntax
# error, which made the most obvious query anyone would type a 400.
contains "search accepts a filename" 'one.txt' "$(cb search "$ENC_ID" 'one.txt' 2>&1 || true)"
OUT=$(cb search "$ENC_ID" "ab" 2>&1); RC=$?
check    "search rejects a too-short query" 1 "$RC"
contains "short query explains itself" "3 characters" "$OUT"

echo "replication"

# An encrypted repository replicated to a second path on this machine. The
# point of doing it with the encrypted one is that the replica must come out
# encrypted too: objects move in the form they are stored in, so the copy
# holds ciphertext under the source's key and the far end never needs it.
REPL="$WORK/replica"
OUT=$(cb replicate "$ENC_ID" "$REPL"); RC=$?
check    "replicate waits for its job"  0             "$RC"
contains "replicate reports completion" '"completed"' "$OUT"
# B070: a full copy, an incremental and a no-op all printed the same bare
# job; what was sent was only in the log.
contains "replicate reports what it sent" '"objects_sent":' "$OUT"
contains "including the snapshots" '"snapshots_sent":' "$OUT"
case "$OUT" in
    *'"objects_sent":0,'*) bad "a first replicate sends objects" ">0" "$OUT" ;;
    *) ok "a first replicate sends objects" ;;
esac
# B098: a second result document, written after this one to add
# "reencrypted", replaced it and dropped the walk's counts.
contains "replicate reports what it asked about" '"objects_asked":' "$OUT"
contains "and the subtrees it skipped" '"subtrees_skipped":' "$OUT"
contains "and whether it re-encrypted" '"reencrypted":false' "$OUT"

# The replica is the same repository, not a new one. A fresh uuid would mean
# nothing that reads a marker could tell the two are related.
SRC_UUID=$(sed -n 's/.*"uuid"[: ]*"\([^"]*\)".*/\1/p' "$WORK/encrepo/.cbackup" 2>/dev/null | head -1)
RPL_UUID=$(sed -n 's/.*"uuid"[: ]*"\([^"]*\)".*/\1/p' "$REPL/.cbackup" 2>/dev/null | head -1)
if [ -n "$SRC_UUID" ] && [ "$SRC_UUID" = "$RPL_UUID" ]; then
    ok "replica carries the source uuid"
else
    bad "replica carries the source uuid" "$SRC_UUID" "$RPL_UUID"
fi

# Objects arrive in a pack, not loose: they are already compressed, and every
# loose reader refuses a compressed loose object because only packing
# compresses. A replica written loose received every byte and could not be
# opened.
NPACK=$(find "$REPL/packs" -name '*.dat' 2>/dev/null | wc -l)
if [ "$NPACK" -gt 0 ]; then
    ok "replica holds a pack"
else
    bad "replica holds a pack" ">0" "$NPACK"
fi
if [ ! -e "$REPL/objects" ]; then
    ok "replica has no objects directory"
else
    bad "replica has no objects directory" "absent" "$(find "$REPL/objects" | head -3)"
fi
# The replica is a format 3 repository in its own right, high-water mark
# included, so snapshot ids allocated against it never repeat one it holds.
if [ -f "$REPL/snapshot-ids" ]; then ok "replica keeps a snapshot-id mark"
else bad "replica keeps a snapshot-id mark" "present" "absent"; fi

# And it is still ciphertext. The same fixture the pack test uses, for the
# same reason: compressed text is unsearchable either way, so look for the
# incompressible one.
REPL_HIT=0
while IFS= read -r -d '' f; do
    contains_bytes "$WORK/src/random.bin" "$f" && { REPL_HIT=1; break; }
done < <(find "$REPL" -type f -print0)
if [ "$REPL_HIT" = 1 ]; then
    bad "replica holds no plaintext" "absent" "present"
else
    ok "replica holds no plaintext"
fi

# Running it again moves nothing: the target already has the root tree, so
# one question ends the walk.
BEFORE=$(find "$REPL" -type f | wc -l)
OUT=$(cb replicate "$ENC_ID" "$REPL")
AFTER=$(find "$REPL" -type f | wc -l)
check "second replicate is a no-op" "$BEFORE" "$AFTER"
contains "and says it sent nothing" '"snapshots_sent":0' "$OUT"
contains "no objects either" '"objects_sent":0,' "$OUT"
contains "the journal says the target was current" 'target already up to date' \
    "$(cb journal "$ENC_ID")"

# B071: into the source's own directory it "completed" having copied
# nothing, since the target was the source. Inside or above it is refused
# too, and the comparison sees through a symlink.
OUT=$(cb replicate "$ENC_ID" "$WORK/encrepo" 2>&1); RC=$?
check    "replicate into itself fails" 1 "$RC"
contains "and says why" "the source repository itself" "$OUT"
check    "replicate inside itself fails" 1 "$(cb replicate "$ENC_ID" "$WORK/encrepo/sub" >/dev/null 2>&1; echo $?)"
check    "replicate above itself fails"  1 "$(cb replicate "$ENC_ID" "$WORK" >/dev/null 2>&1; echo $?)"
ln -s "$WORK/encrepo" "$WORK/enclink"
check    "replicate through a symlink to itself fails" 1 \
    "$(cb replicate "$ENC_ID" "$WORK/enclink" >/dev/null 2>&1; echo $?)"
check    "a schedule into itself is refused" 1 \
    "$(cb schedule add "$ENC_ID" replicate 3600 --dest "$WORK/encrepo" >/dev/null 2>&1; echo $?)"

# Replication can be scheduled, which is the point of an offsite copy: one
# that only runs when somebody remembers is not a backup strategy. The
# destination lives in schedules.params, a column that has existed since the
# first migration and was never written to until now.
OUT=$(cb schedule add "$ENC_ID" replicate 3600 --dest "$WORK/sched-replica" 2>&1); RC=$?
check    "replicate schedule accepted" 0 "$RC"
contains "schedule records its destination" "sched-replica" "$(cb schedule list "$ENC_ID")"

# And one without a destination is refused at creation rather than failing
# every night at two in the morning with only the log to say why.
OUT=$(cb schedule add "$ENC_ID" replicate 3600 2>&1); RC=$?
check    "replicate schedule without a dest is refused" 1 "$RC"
contains "and says what is missing" "dest is required" "$OUT"

# Creating a schedule is not running one. A one-second interval fires almost
# immediately, which proves the whole chain: the scheduler reads params, the
# dispatch hands them on, and the job finds a destination in them. Without
# this the tests above only prove a row was written.
cb schedule add "$ENC_ID" replicate 1 --dest "$WORK/fired-replica" >/dev/null 2>&1
# Wait for the destination to exist rather than for the job to appear in the
# list: a job is listed as soon as it is queued, so checking then races the
# work itself and fails about half the time.
FIRED=0
for _ in $(seq 1 60); do
    if find "$WORK/fired-replica/packs" -name '*.dat' 2>/dev/null | grep -q .; then
        FIRED=1; break
    fi
    sleep 0.25
done
check "a scheduled replicate fires and writes its pack" 1 "$FIRED"
contains "the fired job ran as a replicate job" '"action":"replicate"' "$(cb jobs)"
# B072: and its journal entry says a schedule asked, and which.
SCHED_J=""
for _ in $(seq 1 40); do
    SCHED_J=$(cb journal "$ENC_ID" | grep -o '"source":"schedule"[^}]*' | head -1)
    [ -n "$SCHED_J" ] && break
    sleep 0.25
done
contains "a scheduled job journals its schedule" '"message":"schedule ' "$SCHED_J"
# Stop it before it fires again under the rest of the suite. By its
# destination: the first id in the list is the hourly schedule above, and
# removing that one left this one firing every second to the end of the
# suite, which the idle check (B201) caught.
SID=$(cb schedule list "$ENC_ID" 2>/dev/null | sed 's/{"id":/\n{"id":/g' | grep 'fired-replica' | sed -n 's/^{"id":\([0-9]*\),.*/\1/p' | head -1)
check "the one-second replicate schedule is found" 1 "$([ -n "$SID" ] && echo 1 || echo 0)"
cb schedule remove "$ENC_ID" "$SID" >/dev/null 2>&1 || true
contains "and removed" "no such schedule" "$(cb schedule remove "$ENC_ID" "$SID" 2>&1)"

# A relative path would be resolved against the daemon's working directory,
# which is not where the operator was standing when they typed it.
OUT=$(cb schedule add "$ENC_ID" replicate 3600 --dest relative/path 2>&1); RC=$?
check    "relative dest is refused" 1 "$RC"

# The drill: lose the primary, recover from the replica.
#
# This is the only assertion that proves replication did its job. Everything
# above says bytes arrived; this says they can be turned back into files. It
# is also the one that caught the replica being written as loose objects -
# every other check passed while the copy was unopenable, because the
# transfer itself was fine and only a reader would ever notice.
#
# A second daemon with its own database, so nothing of the source's index is
# available to it. The system key lives on the host keyed by repo uuid, so it
# has to be carried across too: that is what "the key is stored on the daemon
# host" means, and a replica of an encrypted repository is useless without it.
SOCK2="$WORK/c2.sock"
mkdir -p "$WORK/keys2"
cp "$WORK/keys"/* "$WORK/keys2/" 2>/dev/null
cat > "$WORK/config2.yaml" <<EOF
db:
  path: $WORK/state2.db
http:
  socket: $SOCK2
  port: $PORT2
  bind: 127.0.0.1
log:
  level: warn
security:
  key_dir: $WORK/keys2
EOF
C_BACKUP_CONFIG_PATH="$WORK/config2.yaml" "$DAEMON" > "$WORK/daemon2.log" 2>&1 &
DAEMON2_PID=$!
for _ in $(seq 1 50); do [ -S "$SOCK2" ] && break; sleep 0.1; done
cb2() { "$CLI" --socket "$SOCK2" "$@"; }

if [ -S "$SOCK2" ]; then
    contains "second daemon adopts the replica" '"id":1' "$(cb2 repo add "$REPL" recovered)"
    contains "replica reindexes"        '"completed"' "$(cb2 reindex 1)"
    contains "replica lists a snapshot" '"snapshots"' "$(cb2 snapshots 1)"
    OUT=$(cb2 restore 1 1 "$WORK/recovered" 2>&1); RC=$?
    check    "restore from the replica succeeds" 0 "$RC"
    contains "restore from the replica completes" '"completed"' "$OUT"
    if diff -r "$WORK/src" "$WORK/recovered$WORK/src" >/dev/null 2>&1; then
        ok "recovered tree is byte-identical to the original"
    else
        bad "recovered tree is byte-identical to the original" "identical" "differs"
    fi
else
    bad "second daemon starts" "socket" "absent"
fi

# Registering a replica on the daemon that owns the source used to repoint
# the source at the copy and report success: re-link-by-uuid exists for a
# repository that moved, and until replication existed no two directories
# could hold the same uuid. Every backup after that wrote to the replica.
BEFORE_PATH=$(cb repo show "$ENC_ID" | sed -n 's/.*"path":"\([^"]*\)".*/\1/p')
OUT=$(cb repo add "$REPL" hijack 2>&1); RC=$?
check    "registering a replica beside its source is refused" 1 "$RC"
contains "and says which repo would have moved" "still registered as repo" "$OUT"
AFTER_PATH=$(cb repo show "$ENC_ID" | sed -n 's/.*"path":"\([^"]*\)".*/\1/p')
check    "the source keeps its path" "$BEFORE_PATH" "$AFTER_PATH"

# The case that rule is guarding must still work: a repository that actually
# moved re-links to its new path, because the old one is gone.
cb repo add "$WORK/driftrepo" drift >/dev/null
DRIFT_ID=$(cb repo list | tr '}' '\n' | grep driftrepo | sed -n 's/.*"id":\([0-9]*\),.*/\1/p')
mv "$WORK/driftrepo" "$WORK/driftmoved"
contains "a moved repository re-links" '"id"' "$(cb repo add "$WORK/driftmoved" drift 2>&1)"
contains "and now reports the new path" "driftmoved" "$(cb repo show "$DRIFT_ID")"

# A path already holding a different repository is refused rather than mixed.
cb repo add "$WORK/otherrepo" other >/dev/null 2>&1
OUT=$(cb replicate "$ENC_ID" "$WORK/otherrepo" 2>&1); RC=$?
check "replicate into a foreign repo fails" 1 "$RC"
contains "and points at --reencrypt" "--reencrypt" "$OUT"

# B069: the Manual said to change encryption by replicating into a fresh
# repository, and replication refused every repository but a replica of
# the source. --reencrypt copies into a repository of its own, under its
# own key, which then reindexes and restores like any other.
REENC_OUT=$(cb repo add "$WORK/reenc" reenc --encrypt system)
REENC_ID=$(printf '%s' "$REENC_OUT" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
REENC_UUID=$(sed -n 's/.*"uuid"[: ]*"\([^"]*\)".*/\1/p' "$WORK/reenc/.cbackup" | head -1)
OUT=$(cb replicate "$ENC_ID" "$WORK/reenc" --reencrypt 2>&1); RC=$?
check    "re-encrypting replication succeeds" 0 "$RC"
contains "and says it re-encrypted" '"reencrypted":true' "$OUT"
check    "the destination keeps its own identity" "$REENC_UUID" \
    "$(sed -n 's/.*"uuid"[: ]*"\([^"]*\)".*/\1/p' "$WORK/reenc/.cbackup" | head -1)"
if [ "$REENC_UUID" != "$SRC_UUID" ]; then ok "which is not the source's"
else bad "which is not the source's" "a different uuid" "$REENC_UUID"; fi
REENC_HIT=0
while IFS= read -r -d '' f; do
    contains_bytes "$WORK/src/random.bin" "$f" && { REENC_HIT=1; break; }
done < <(find "$WORK/reenc/packs" -type f -print0)
if [ "$REENC_HIT" = 1 ]; then
    bad "the re-encrypted copy holds no plaintext" "absent" "present"
else
    ok "the re-encrypted copy holds no plaintext"
fi
contains "the destination reindexes" '"completed"' "$(cb reindex "$REENC_ID")"
contains "and lists the snapshot" '"id":1,' "$(cb snapshots "$REENC_ID")"
cb restore "$REENC_ID" 1 "$WORK/reencout" >/dev/null
if diff -r "$WORK/encout" "$WORK/reencout" >/dev/null 2>&1; then
    ok "a re-encrypted copy restores what the source does"
else
    bad "a re-encrypted copy restores what the source does" "identical" "differs"
fi
OUT=$(cb replicate "$ENC_ID" "$WORK/reenc-missing" --reencrypt 2>&1); RC=$?
check    "re-encrypting into no repository is refused" 1 "$RC"
contains "and says to create it first" "repo add" "$OUT"
# Into an unencrypted repository the copy is plaintext, which takes
# --to-plaintext; without it nothing is written.
cb repo add "$WORK/reenc-plain" rp >/dev/null
OUT=$(cb replicate "$ENC_ID" "$WORK/reenc-plain" --reencrypt 2>&1); RC=$?
check    "re-encrypting into plaintext without --to-plaintext is refused" 1 "$RC"
contains "and names the flag" "--to-plaintext" "$OUT"
check    "and wrote no pack" 0 "$(find "$WORK/reenc-plain/packs" -type f | wc -l | tr -d ' ')"
OUT=$(cb replicate "$ENC_ID" "$WORK/reenc-plain" --to-plaintext 2>&1); RC=$?
check    "--to-plaintext without --reencrypt is refused locally" 1 "$RC"
contains "and says why" "--to-plaintext needs --reencrypt" "$OUT"
OUT=$(cb replicate "$ENC_ID" "$WORK/reenc-plain" --reencrypt --to-plaintext 2>&1); RC=$?
check    "with --to-plaintext it runs" 0 "$RC"
contains "and the journal says so" "into plaintext at $WORK/reenc-plain" "$(cb journal "$ENC_ID")"

echo "configured sources"

# Sources belong to the repository, not to each run. The CLI naming them is
# right for the CLI; a desktop button and a schedule have nowhere to get them
# from, which is why neither worked. Configure once, then back up with a bare
# command, a click, or a timer.
mkdir -p "$WORK/othersrc"
echo "other content" > "$WORK/othersrc/other.txt"

OUT=$(cb backup 2 2>&1); RC=$?
check    "a bare backup with nothing configured is refused" 1 "$RC"
contains "and says how to configure it" "sources" "$OUT"

cb sources set 1 "$WORK/src" --exclude '*.tmp' >/dev/null
OUT=$(cb sources list 1)
contains "sources round-trip"  "$WORK/src" "$OUT"
contains "excludes round-trip" '*.tmp'     "$OUT"

# The point of the whole feature: no paths on the command line.
BEFORE=$(cb snapshots 1 | grep -o '"id"' | wc -l)
cb backup 1 >/dev/null
AFTER=$(cb snapshots 1 | grep -o '"id"' | wc -l)
check "a bare backup uses the configured sources" "$((BEFORE + 1))" "$AFTER"

# And naming paths still overrides, so the explicit form is unchanged.
cb backup 1 "$WORK/othersrc" >/dev/null
# Newest first, so the snapshot just made is the head of the list.
LAST=$(cb snapshots 1 | tr '}' '\n' | grep -o '"id":[0-9]*' | head -1 | tr -dc '0-9')
cb restore 1 "$LAST" "$WORK/overrideout" >/dev/null 2>&1
if find "$WORK/overrideout" -name 'other.txt' | grep -q .; then
    ok "an explicit path still overrides the configured sources"
else
    bad "an explicit path still overrides the configured sources" "other.txt" "absent"
fi

# B107: a sources write during a running backup waited for the backup to
# end, 179-200 seconds, because the store phase of a backup of fewer than
# 2,000 files ran inside one transaction. It returns at once now, and the
# job already running keeps the sources it was submitted with.
mkdir -p "$WORK/slowsrc" "$WORK/nextsrc"
# 512 MiB to keep the backup running, in 32 files of 16 MiB.
for i in $(seq 1 32); do
    dd if=/dev/zero of="$WORK/slowsrc/slow$i.bin" bs=1M count=16 status=none
done
echo "next content" > "$WORK/nextsrc/next.txt"
SR=$(cb repo add "$WORK/srcrepo" sr | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
cb sources set "$SR" "$WORK/slowsrc" >/dev/null
SJ=$(cb backup "$SR" --no-wait | sed -n 's/.*"job_id":\([0-9]*\).*/\1/p')
for _ in $(seq 1 100); do
    cb jobs "$SJ" | grep -q '"phase":"store"' && break
    sleep 0.05
done
T0=$(date +%s%N)
cb sources set "$SR" "$WORK/nextsrc" >/dev/null; RC=$?
T1=$(date +%s%N)
STILL=$(cb jobs "$SJ")
check "a sources write during a backup succeeds" 0 "$RC"
case "$STILL" in
    *'"state":"running"'*)
        if [ $(( (T1 - T0) / 1000000 )) -lt 2000 ]; then
            ok "and returns while the backup is still running"
        else
            bad "and returns while the backup is still running" "< 2000 ms" \
                "$(( (T1 - T0) / 1000000 )) ms"
        fi ;;
    *) ok "and returns while the backup is still running (skipped: backup already done)" ;;
esac
for _ in $(seq 1 600); do
    cb jobs "$SJ" | grep -Eq '"state":"(completed|failed|cancelled)"' && break
    sleep 0.1
done
contains "the running backup kept the sources it started with" "slow32.bin" \
    "$(cb search "$SR" slow32.bin --snapshot 1)"
cb backup "$SR" >/dev/null
contains "and the next backup uses the new ones" "next.txt" \
    "$(cb search "$SR" next.txt --snapshot 2)"
rm -f "$WORK"/slowsrc/slow*.bin

echo "scheduled backups"

# A backup schedule was accepted, listed as enabled, and then failed on every
# tick with "no scheduled job for action 'backup'". The single most important
# thing a backup tool can schedule silently never ran, and nothing an
# operator could see said so. Two causes: backup was missing from the
# dispatch table, and schedule creation validated no action at all.
OUT=$(cb schedule add 1 nonsense 3600 2>&1); RC=$?
check    "an unrunnable action is refused" 1 "$RC"
contains "and lists what can be scheduled" "cannot be scheduled" "$OUT"

# Backup needs sources. Checked at creation, not at two in the morning.
OUT=$(cb schedule add 2 backup 3600 2>&1); RC=$?
check    "a backup schedule for a repo with no sources is refused" 1 "$RC"
contains "and says what is missing" "sources" "$OUT"

# With sources configured, a schedule needs nothing else.
OUT=$(cb schedule add 1 backup 3600 2>&1); RC=$?
check    "a backup schedule inherits the repo sources" 0 "$RC"
SID0=$(cb schedule list 1 | tr '}' '\n' | grep '"action":"backup"' | sed -n 's/.*"id":\([0-9]*\),.*/\1/p' | head -1)
# B065: another repository's schedule, or one that does not exist, is not
# this repository's to remove or disable; both reported success.
OUT=$(cb schedule remove 2 "$SID0" 2>&1); RC=$?
check    "removing another repo's schedule fails" 1 "$RC"
contains "and says there is no such schedule" "no such schedule" "$OUT"
OUT=$(cb schedule disable 2 "$SID0" 2>&1); RC=$?
check    "disabling another repo's schedule fails" 1 "$RC"
contains "the schedule survives" "\"id\":$SID0," "$(cb schedule list 1)"
[ -n "$SID0" ] && cb schedule remove 1 "$SID0" >/dev/null 2>&1
OUT=$(cb schedule remove 1 "$SID0" 2>&1); RC=$?
check    "removing it twice fails" 1 "$RC"

# The one that proves it: a one-second interval must produce real snapshots.
BEFORE=$(cb snapshots 1 | grep -o '"id"' | wc -l)
cb schedule add 1 backup 1 --source "$WORK/src" >/dev/null 2>&1
FIRED=0
for _ in $(seq 1 60); do
    AFTER=$(cb snapshots 1 | grep -o '"id"' | wc -l)
    if [ "$AFTER" -gt "$BEFORE" ]; then FIRED=1; break; fi
    sleep 0.25
done
check "a scheduled backup produces a snapshot" 1 "$FIRED"
contains "and ran as a backup job" '"action":"backup"' "$(cb jobs)"
SID=$(cb schedule list 1 | tr '}' '\n' | grep '"action":"backup"' | sed -n 's/.*"id":\([0-9]*\),.*/\1/p' | head -1)
[ -n "$SID" ] && cb schedule remove 1 "$SID" >/dev/null 2>&1

# B201: an idle daemon writes nothing. On FS2 an idle daemon's WAL grew about
# four frames a second, from the checkpointer's own restart row and warning
# line; nothing needs writing while nothing happens. Once the last job is
# done and logged, the WAL must not change for several seconds.
echo "idle"
for _ in $(seq 1 120); do
    case "$(cb jobs)" in *'"state":"running"'*|*'"state":"queued"'*) sleep 0.25 ;; *) break ;; esac
done
sleep 2
W0=$(stat -c '%s %y' "$WORK/state.db-wal" 2>/dev/null)
sleep 6
W1=$(stat -c '%s %y' "$WORK/state.db-wal" 2>/dev/null)
check "an idle daemon writes nothing to state.db for six seconds" "$W0" "$W1"

echo "auth"
contains "auth status before setup" '"configured":false' "$(cb auth status)"
# Setup is socket-only, so this is the one way the first admin password gets
# set. It reads the password rather than taking it as an argument, which
# would put it in shell history and /proc/<pid>/cmdline.
OUT=$(printf 'correcthorsebattery\n' | cb auth setup 2>&1); RC=$?
check "setup reads the password from stdin" 0 "$RC"
contains "and reports it configured" '"configured":true' "$OUT"
OUT=$(printf 'anotherpassword\n' | cb auth setup 2>&1); RC=$?
check "a second setup fails" 1 "$RC"
contains "and says to change it instead" "already configured" "$OUT"
# B055: the CLI only talks to the socket, which needs no credential, and
# `auth login` minted a 90-day token there that nothing stored. It refuses
# now, before asking for a password, and prints no token.
OUT=$(printf 'correcthorsebattery\n' | cb auth login 2>&1); RC=$?
check    "auth login fails" 1 "$RC"
contains "and says the socket needs no login" "needs no credential" "$OUT"
check    "and mints no token" "" "$(printf '%s' "$OUT" | grep -o '"token"')"
# A plain logout exited 0, printed nothing and revoked nothing.
OUT=$(cb auth logout 2>&1); RC=$?
check    "auth logout without --all fails" 1 "$RC"
contains "and points at --all" "auth logout --all" "$OUT"
tcp_login() {  # tcp_login <password>: the response body of a TCP login
    curl -s -X POST -d "{\"password\":\"$1\"}" "http://127.0.0.1:$MAIN_PORT/api/auth/login"
}
OUT=$(printf 'correcthorsebattery\nbrandnewpassword\n' | cb auth password 2>&1); RC=$?
check "password change reads both from stdin" 0 "$RC"
contains "old password no longer logs in" "invalid password" "$(tcp_login correcthorsebattery)"
# B053: from the host, logout --all ends every session a client holds.
LTOK=$(tcp_login brandnewpassword | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
check "a fresh token works over tcp" 200 "$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $LTOK" "http://127.0.0.1:$MAIN_PORT/repos")"
contains "logout --all revokes" '"revoked":1' "$(cb auth logout --all 2>&1)"
check "and the token is dead" 401 "$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $LTOK" "http://127.0.0.1:$MAIN_PORT/repos")"
OUT=$(cb auth setup < /dev/null 2>&1); RC=$?
check "no password on stdin fails" 1 "$RC"
contains "and says so" "no password read" "$OUT"

echo "listen address"

# http.bind has been a documented setting since the config was written,
# describing itself as the way to "force remote access through a reverse
# proxy", and it was never read: MHD was started without a socket address,
# which binds every interface. Setting it to 127.0.0.1 produced a port
# answering on the LAN and a config file saying it did not.
cat > "$WORK/bindcfg.yaml" <<EOF
db:
  path: $WORK/bind.db
http:
  socket: $WORK/bind.sock
  port: $BINDPORT
  bind: 127.0.0.1
log:
  level: warn
security:
  key_dir: $WORK/keys
EOF
C_BACKUP_CONFIG_PATH="$WORK/bindcfg.yaml" "$DAEMON" > "$WORK/bind.log" 2>&1 &
BIND_PID=$!
for _ in $(seq 1 50); do [ -S "$WORK/bind.sock" ] && break; sleep 0.1; done
LISTEN=$(ss -ltn 2>/dev/null | grep ":$BINDPORT" | awk '{print $4}' | head -1)
case "$LISTEN" in
    127.0.0.1:*) ok "bind 127.0.0.1 listens on loopback only" ;;
    "")          ok "bind 127.0.0.1 listens on loopback only (ss unavailable, skipped)" ;;
    *)           bad "bind 127.0.0.1 listens on loopback only" "127.0.0.1:$BINDPORT" "$LISTEN" ;;
esac
kill $BIND_PID 2>/dev/null; wait $BIND_PID 2>/dev/null

# A bind address that will not parse stops the daemon. Falling back would
# mean opening the port to the network because a config line had a typo in
# it, which is the one outcome worth refusing to reach by accident.
sed -i 's/  bind: 127.0.0.1/  bind: not-an-address/' "$WORK/bindcfg.yaml"
C_BACKUP_CONFIG_PATH="$WORK/bindcfg.yaml" "$DAEMON" > "$WORK/bind2.log" 2>&1
check    "an unparseable bind address stops the daemon" 1 "$?"
contains "and says which value was rejected" "not-an-address" "$(cat "$WORK/bind2.log")"
# B132: a port another daemon already holds fails at api_server_start, with
# the job manager, scheduler and routes all set up. Each startup failure
# unwinds what came before it; under ASan a leak report lands in the log, so
# its absence is the check (vacuous in a plain build).
sed -i "s/  bind: not-an-address/  bind: 127.0.0.1/; s/  port: $BINDPORT/  port: $MAIN_PORT/" \
    "$WORK/bindcfg.yaml"
C_BACKUP_CONFIG_PATH="$WORK/bindcfg.yaml" "$DAEMON" > "$WORK/bind3.log" 2>&1
check    "a port already taken stops the daemon" 1 "$?"
contains "and says it cannot start" "cannot start API server" "$(cat "$WORK/bind3.log")"
contains "naming the address" "failed to start HTTP server on 127.0.0.1:$MAIN_PORT" \
    "$(cat "$WORK/bind3.log")"
check    "and unwinds without a leak too" 0 \
    "$(grep -c 'LeakSanitizer\|AddressSanitizer' "$WORK/bind3.log")"
contains "and leaves the running daemon alone" '"repos"' "$(cb repo list)"
# NIT-19: a socket in a directory that does not exist said only "cannot
# start API server". The line now carries the path and the reason.
sed -i "s|  socket: $WORK/bind.sock|  socket: $WORK/no-such-dir/bind.sock|; s/  port: $MAIN_PORT/  port: 0/" \
    "$WORK/bindcfg.yaml"
C_BACKUP_CONFIG_PATH="$WORK/bindcfg.yaml" "$DAEMON" > "$WORK/bind4.log" 2>&1
check    "a socket in a missing directory stops the daemon" 1 "$?"
contains "and names the path and the reason" \
    "cannot start API server: cannot bind the unix socket at $WORK/no-such-dir/bind.sock (http.socket): No such file or directory" \
    "$(cat "$WORK/bind4.log")"

# sun_path is 108 bytes and the daemon used to strncpy into it, so a longer
# socket path was silently truncated: bind and listen succeeded against a
# name nobody configured, the log reported the untruncated one, and every
# client refused the configured path as too long and could not connect. A
# healthy-looking daemon that nothing can reach.
LONGDIR="$WORK/$(printf 'd%.0s' $(seq 1 100))"
mkdir -p "$LONGDIR"
cat > "$WORK/longcfg.yaml" <<EOF
db:
  path: $WORK/long.db
http:
  socket: $LONGDIR/c.sock
  port: 0
  bind: 127.0.0.1
log:
  level: warn
security:
  key_dir: $WORK/keys
EOF
C_BACKUP_CONFIG_PATH="$WORK/longcfg.yaml" "$DAEMON" > "$WORK/long.log" 2>&1
check    "an over-long socket path stops the daemon" 1 "$?"
contains "and says so rather than truncating" "socket path too long" "$(cat "$WORK/long.log")"
# B132: this is api_server_create failing, which returned with the scheduler
# open and the job manager's workers running; an ASan daemon reported the
# scheduler as leaked. Now it unwinds both.
check    "and unwinds its startup without a leak" 0 \
    "$(grep -c 'LeakSanitizer\|AddressSanitizer' "$WORK/long.log")"
if [ -n "$(find "$LONGDIR" -maxdepth 1 -name '*.sock' 2>/dev/null)" ]; then
    bad "leaves no socket behind" "no socket" "a socket was created"
else
    ok "leaves no socket behind"
fi

echo "key directory"

# B041: the container image kept keys at /etc/c-backupd/keys, inside the
# container's own filesystem, so replacing the container lost every key. It
# sets the key directory through the environment, which has to win over the
# config file for an operator's own mounted config not to undo it.
grep -q '^ENV C_BACKUP_SECURITY_KEY_DIR=/var/lib/c-backupd/keys$' "$(dirname "$0")/../Dockerfile" \
    && ok "the image keeps keys on the state volume" \
    || bad "the image keeps keys on the state volume" "ENV C_BACKUP_SECURITY_KEY_DIR" "absent"
# B205: a VOLUME line made every container start from the image, the
# helper's included, create an anonymous volume nothing removed.
grep -q '^VOLUME' "$(dirname "$0")/../Dockerfile" \
    && bad "the image declares no anonymous volume" "no VOLUME" "$(grep '^VOLUME' "$(dirname "$0")/../Dockerfile")" \
    || ok "the image declares no anonymous volume"
cat > "$WORK/envcfg.yaml" <<EOF
db:
  path: $WORK/env.db
http:
  socket: $WORK/env.sock
  port: 0
  bind: 127.0.0.1
log:
  level: warn
security:
  key_dir: $WORK/filekeys
EOF
C_BACKUP_SECURITY_KEY_DIR="$WORK/envkeys" C_BACKUP_CONFIG_PATH="$WORK/envcfg.yaml" \
    "$DAEMON" > "$WORK/env.log" 2>&1 &
ENV_PID=$!
for _ in $(seq 1 50); do [ -S "$WORK/env.sock" ] && break; sleep 0.1; done
"$CLI" --socket "$WORK/env.sock" repo add "$WORK/envrepo" e --encrypt system >/dev/null 2>&1
check "the environment's key directory wins over the file's" 1 \
    "$(find "$WORK/envkeys" -name '*.key' 2>/dev/null | wc -l | tr -d ' ')"
check "and the file's is not used" 0 \
    "$(find "$WORK/filekeys" -name '*.key' 2>/dev/null | wc -l | tr -d ' ')"
kill $ENV_PID 2>/dev/null; wait $ENV_PID 2>/dev/null

echo "damage beyond parity (OWN-31)"

# Overwrite 4 KiB of a file's stored bytes inside a pack, 8 KiB into it:
# eight times the burst parity can rebuild. The file is incompressible and
# the repository unencrypted, so its bytes sit in the pack as they are and
# can be found by content.
damage_beyond_parity() {  # damage_beyond_parity <sample> <pack>
    local pat hex at
    pat=$(head -c 32 "$1" | od -An -tx1 -v | tr -d ' \n')
    hex=$(od -An -tx1 -v "$2" | tr -d ' \n')
    at=$(printf '%s' "$hex" | grep -obF "$pat" | head -1 | cut -d: -f1)
    [ -n "$at" ] && [ $((at % 2)) -eq 0 ] || return 1
    head -c 4096 /dev/urandom | \
        dd of="$2" bs=1 seek=$((at / 2 + 8192)) conv=notrunc status=none
}

# The failure path is the job manager's: it marks the job failed and then
# composes the alert, and the line it logs for it is the evidence. That line
# follows the state the client returned on, so it is waited for, up to 10 s
# (the coverage build is slow enough to need it). With no
# Pushover credentials here it must say the alert was not sent (NIT-11).
alerted() {  # alerted <job-id>
    local _
    for _ in $(seq 1 100); do
        if grep -q "failure notification for job $1 not sent, Pushover is not configured: " \
                "$WORK/daemon.log"; then
            ! grep -q "sending failure notification for job $1:" "$WORK/daemon.log"
            return
        fi
        sleep 0.1
    done
    return 1
}
job_of() {  # job_of <failed job document>: its id, the first "id" in it
    printf '%s' "$1" | grep -o '"id":[0-9]*' | head -1 | cut -d: -f2
}

# Replication ships everything, records the snapshot, and fails the job
# naming the damaged object. It used to log the damage and report OK.
mkdir -p "$WORK/dmgsrc"
head -c 65536 /dev/urandom > "$WORK/dmgsrc/lost.bin"
echo "an intact neighbour" > "$WORK/dmgsrc/ok.txt"
DMG_ID=$(cb repo add "$WORK/dmgrepo" dmg | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
cb backup "$DMG_ID" "$WORK/dmgsrc" > /dev/null
# An object is named by SHA-256 over its type byte (1, a file) and then its
# payload, never over the payload alone.
LOST_HASH=$({ printf '\001'; cat "$WORK/dmgsrc/lost.bin"; } | sha256sum | cut -c1-64)
DMG_DAT=$(find "$WORK/dmgrepo/packs" -name '*.dat' | head -1)
if damage_beyond_parity "$WORK/dmgsrc/lost.bin" "$DMG_DAT"; then
    OUT=$(cb replicate "$DMG_ID" "$WORK/dmgrepl" 2>"$WORK/dmg.err"); RC=$?
    ERR=$(cat "$WORK/dmg.err")
    check    "replicating damage beyond parity exits non-zero" 1 "$RC"
    contains "the job is failed"               '"state":"failed"' "$ERR"
    contains "the failure says what happened" \
        "replicated with 1 object(s) damaged beyond parity; the replica holds them as damaged" "$ERR"
    contains "the result lists the object"     "$LOST_HASH" "$OUT$ERR"
    contains "and counts it"                   '"objects_damaged":1' "$OUT$ERR"
    contains "everything else was still sent"  '"snapshots_sent":1' "$OUT$ERR"
    DJ=$(job_of "$ERR")
    if [ -n "$DJ" ] && alerted "$DJ"; then ok "the failure alert was composed, and not claimed as sent"
    else bad "the failure alert was composed, and not claimed as sent" "a notification for job '$DJ'" "none"; fi
else
    bad "damage fixture" "the file's bytes in the pack" "not found"
fi

# gc rewriting a pack copies the damaged entry as it is, finishes, and fails.
mkdir -p "$WORK/gcdsrc"
head -c 65536 /dev/urandom > "$WORK/gcdsrc/keep.bin"
head -c 65536 /dev/urandom > "$WORK/gcdsrc/drop.bin"
GCD_ID=$(cb repo add "$WORK/gcdrepo" gcd | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
cb backup "$GCD_ID" "$WORK/gcdsrc" > /dev/null
rm -f "$WORK/gcdsrc/drop.bin"
cb backup "$GCD_ID" "$WORK/gcdsrc" > /dev/null
cb snapshot delete "$GCD_ID" 1 --force > /dev/null
KEEP_HASH=$({ printf '\001'; cat "$WORK/gcdsrc/keep.bin"; } | sha256sum | cut -c1-64)
GCD_DAT=$(find "$WORK/gcdrepo/packs" -name '*.dat' -size +100k | head -1)
if [ -n "$GCD_DAT" ] && damage_beyond_parity "$WORK/gcdsrc/keep.bin" "$GCD_DAT"; then
    OUT=$(cb gc "$GCD_ID" 2>"$WORK/gcd.err"); RC=$?
    ERR=$(cat "$WORK/gcd.err")
    check    "gc that copies damage beyond parity exits non-zero" 1 "$RC"
    contains "the gc job is failed"           '"state":"failed"' "$ERR"
    contains "the gc failure says what happened" \
        "1 pack entry damaged beyond parity was copied as damaged" "$ERR"
    contains "the gc result lists the entry"  "$KEEP_HASH" "$OUT$ERR"
    contains "and counts it"                  '"pack_entries_damaged":1' "$OUT$ERR"
    GJ=$(job_of "$ERR")
    if [ -n "$GJ" ] && alerted "$GJ"; then ok "the gc failure alert was composed, and not claimed as sent"
    else bad "the gc failure alert was composed, and not claimed as sent" "a notification for job '$GJ'" "none"; fi
else
    bad "gc damage fixture" "the file's bytes in the pack" "not found"
fi

# OWN-33: a label past the limit is refused before anything is sent, in
# the daemon's words; one with quotes and a backslash arrives intact.
N_BEFORE=$(cb snapshots 1 | grep -o '"id":[0-9]*' | wc -l | tr -d ' ')
OUT=$(cb backup 1 "$WORK/src" --label "$(head -c 1000 /dev/zero | tr '\0' x)" 2>&1); RC=$?
check    "a 1,000-byte label fails the backup" 1 "$RC"
contains "and says why" "label must be at most 256 bytes of printable text" "$OUT"
OUT=$(cb backup 1 "$WORK/src" --label "$(printf 'tab\there')" 2>&1); RC=$?
check    "a label with a tab fails too" 1 "$RC"
check    "and neither made a snapshot" "$N_BEFORE" \
    "$(cb snapshots 1 | grep -o '"id":[0-9]*' | wc -l | tr -d ' ')"
contains "a quoted label backs up" '"state":"completed"' \
    "$(cb backup 1 "$WORK/src" --label 'say "hi" C:\dir')"
contains "and is listed as given" '"label":"say \"hi\" C:\\dir"' "$(cb snapshots 1)"

echo "start-up reconcile"
# state.db runs synchronous=NORMAL, so a power cut can lose its newest
# commits; a backup's are its last, after the snapshot-id mark and the record
# are durable in the repository. That lost tail is made here by hand - the
# newest snapshot's rows deleted from a stopped daemon's database - and the
# next start has to put it back with no operator action: a reindex job of
# origin "startup", a WARN naming the repository and the count.
contains "the default state.db settings are applied" \
    "state database: synchronous=normal, cache 64 MiB, checkpoint every 16384 pages" \
    "$(cat "$WORK/daemon.log")"
contains "a start with nothing lost says the index is complete" "index complete" \
    "$(cat "$WORK/daemon.log")"
contains "a backup to lose" '"state":"completed"' "$(cb backup 1 "$WORK/src" --label tail)"
LOST=$(cb snapshots 1 | grep -o '"id":[0-9]*' | cut -d: -f2 | sort -n | tail -1)
kill -TERM "$DAEMON_PID"; wait "$DAEMON_PID" 2>/dev/null; DAEMON_PID=
if command -v sqlite3 > /dev/null 2>&1; then
    sqlite3 "$WORK/state.db" "DELETE FROM nodes WHERE repo_id = 1 AND snap_id = $LOST;
        DELETE FROM snapshots WHERE repo_id = 1 AND id = $LOST;
        UPDATE snapshot_id_seq SET last_id = $LOST - 1 WHERE repo_id = 1;"
    check "the lost snapshot is gone from the index" "0" \
        "$(sqlite3 "$WORK/state.db" "SELECT COUNT(*) FROM snapshots WHERE repo_id = 1 AND id = $LOST")"
    # The restart also changes every state.db setting, to show each one is
    # read from the file and applied (the line is logged after reading back).
    sed -i 's|^  path: \(.*state.db\)$|  path: \1\n  synchronous: full\n  cache_mb: 8\n  wal_autocheckpoint: 2000|' \
        "$WORK/config.yaml"
    : > "$WORK/daemon.log"
    C_BACKUP_CONFIG_PATH="$WORK/config.yaml" "$DAEMON" >> "$WORK/daemon.log" 2>&1 &
    DAEMON_PID=$!
    for _ in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
    contains "the configured state.db settings are applied" \
        "state database: synchronous=full, cache 8 MiB, checkpoint every 2000 pages" \
        "$(cat "$WORK/daemon.log")"
    contains "the start names the repository and the count" \
        "reconcile: repo 1 ($WORK/repo) has 1 snapshot(s) missing from the index" \
        "$(cat "$WORK/daemon.log")"
    STATE=""
    for _ in $(seq 1 300); do
        STATE=$(sqlite3 "$WORK/state.db" "SELECT state FROM jobs WHERE repo_id = 1
            AND action = 'reindex' AND origin = 'startup' ORDER BY id DESC LIMIT 1")
        [ "$STATE" = completed ] && break
        sleep 0.1
    done
    check "the reindex is a startup job and completes" "completed" "$STATE"
    contains "and its journal entry says startup" '"source":"startup"' "$(cb journal 1)"
    contains "the lost snapshot is listed again" "\"id\":$LOST," "$(cb snapshots 1)"
    contains "with its label" '"label":"tail"' "$(cb snapshots 1)"
    contains "and the next backup numbers past it" '"state":"completed"' "$(cb backup 1 "$WORK/src")"
    check "to the id after it" "$((LOST + 1))" \
        "$(cb snapshots 1 | grep -o '"id":[0-9]*' | cut -d: -f2 | sort -n | tail -1)"

    # B139 and V3-12: a tag set just before a power cut. By the time the API
    # answers, the repository's tag list holds it, 0600 (NIT-27), and the
    # database commit is synced. A lost commit is made by hand - the daemon
    # killed at once, the tag row deleted - and the next start puts the row
    # back from the repository's list.
    contains "a tag to lose" '"ok"' "$(cb tag set 1 pc-tag "$LOST")"
    kill -KILL "$DAEMON_PID"; wait "$DAEMON_PID" 2>/dev/null; DAEMON_PID=
    contains "the repository's tag list has it when the API answers" '"name":"pc-tag"' \
        "$(cat "$WORK/repo/tags" 2>/dev/null)"
    check "and the list is 0600" "600" "$(stat -c %a "$WORK/repo/tags" 2>/dev/null)"
    sqlite3 "$WORK/state.db" "DELETE FROM tags WHERE repo_id = 1 AND name = 'pc-tag'"
    : > "$WORK/daemon.log"
    C_BACKUP_CONFIG_PATH="$WORK/config.yaml" "$DAEMON" >> "$WORK/daemon.log" 2>&1 &
    DAEMON_PID=$!
    # The killed daemon left its socket behind, so wait for an answer, not
    # for the file.
    for _ in $(seq 1 100); do cb health 2>/dev/null | grep -q ready && break; sleep 0.1; done
    contains "the start restores the lost tag and says so" \
        "reconcile: repo 1 ($WORK/repo): 1 tag(s) missing from the index were restored" \
        "$(cat "$WORK/daemon.log")"
    contains "the tag is listed again" 'pc-tag' "$(cb tag list 1)"
else
    C_BACKUP_CONFIG_PATH="$WORK/config.yaml" "$DAEMON" >> "$WORK/daemon.log" 2>&1 &
    DAEMON_PID=$!
    for _ in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
    ok "start-up reconcile (skipped: no sqlite3)"
fi

echo "error handling"
OUT=$(cb repo show 999 2>&1); RC=$?
check    "unknown repo exits non-zero" 1 "$RC"
contains "unknown repo reports status" "HTTP 404" "$OUT"
OUT=$(cb backup 2>&1); RC=$?
check    "missing arguments exit non-zero" 1 "$RC"
contains "missing arguments show usage" "usage:" "$OUT"
# NIT-8: an argument a command does not take was ignored, so this listed
# repository 1's runs and exited 0.
OUT=$(cb grep runs 1 2 2>&1); RC=$?
check    "a stray positional exits non-zero" 1 "$RC"
contains "and names it" "unexpected argument '2'" "$OUT"
contains "with the usage" "usage: c-backup grep runs <repo-id>" "$OUT"
OUT=$(cb summary 1 --bogus 2>&1); RC=$?
check    "an unknown option exits non-zero" 1 "$RC"
contains "and names it too" "unknown option '--bogus'" "$OUT"
OUT=$(cb grep runs 1 --limit 1 2>&1); RC=$?
check    "options are still taken" 0 "$RC"

echo "argument parsing (B127)"
# `repo add PATH Main --encrypt` with the value forgotten registered an
# UNENCRYPTED repository and exited 0, and encryption is fixed at creation.
# Every refusal below must leave the registry as it was and create nothing.
REPOS_BEFORE=$(cb repo list)
b127_refused() {  # b127_refused <what> <expected message> <args...>
    local what="$1" msg="$2"; shift 2
    local out rc
    out=$(cb "$@" 2>&1 >/dev/null); rc=$?
    check    "$what exits 1" 1 "$rc"
    contains "$what: names it" "$msg" "$out"
    contains "$what: shows the usage, on stderr" "usage: c-backup repo add <path> [label]" "$out"
}
b127_refused "--encrypt with no value"      "option '--encrypt' needs a value" \
    repo add "$WORK/b127" Main --encrypt
b127_refused "--passphrase with no value"   "option '--passphrase' needs a value" \
    repo add "$WORK/b127" Main --passphrase
b127_refused "--encrypt with no label either" "option '--encrypt' needs a value" \
    repo add "$WORK/b127" --encrypt
b127_refused "--encrypt followed by an option" \
    "option '--encrypt' needs a value, and '--passphrase' looks like an option" \
    repo add "$WORK/b127" Main --encrypt --passphrase 'correct horse'
b127_refused "an empty --encrypt"           "needs a value, not an empty one" \
    repo add "$WORK/b127" Main --encrypt=
b127_refused "an empty --passphrase"        "needs a value, not an empty one" \
    repo add "$WORK/b127" Main --passphrase ''
b127_refused "an unknown flag"              "unknown option '--bogus'" \
    repo add "$WORK/b127" --bogus
b127_refused "an extra positional"          "unexpected argument 'extra'" \
    repo add "$WORK/b127" mylabel extra
b127_refused "--encrypt given twice"        "option '--encrypt' given more than once" \
    repo add "$WORK/b127" Main --encrypt system --encrypt none
check "none of them registered a repository" "$REPOS_BEFORE" "$(cb repo list)"
if [ -e "$WORK/b127" ]; then bad "and none created the directory" "absent" "present"
else ok "and none created the directory"; fi
# The usage line shows the label the Manual's examples pass.
contains "repo add usage shows the label" "repo add <path> [label]" "$(cb help repo add)"
# `--` ends the options, so a label may start with a dash.
contains "-- lets a label start with a dash" '"id"' \
    "$(cb repo add "$WORK/b127dash" --encrypt system -- -dashed 2>&1)"
contains "and it is the label" '"label":"-dashed"' "$(cb repo list)"
contains "and the options before it were taken" '"encryption":"system"' \
    "$(cb repo list | tr '}' '\n' | grep b127dash)"
# A value starting with a dash is passed in the = form.
OUT=$(cb backup 1 "$WORK/src" --label=-dash-label 2>&1); RC=$?
check    "--label=-x passes a dashed value" 0 "$RC"
contains "and it is the label" '"label":"-dash-label"' "$(cb snapshots 1)"
OUT=$("$CLI" --socket 2>&1); RC=$?
check    "--socket with no value exits 1" 1 "$RC"
contains "and names it" "option '--socket' needs a value" "$OUT"
# Variadic positionals still take any number.
contains "sources set takes several paths" '"sources"' \
    "$(cb sources set 1 "$WORK/src" "$WORK/src/one.txt" --exclude a --exclude b)"
contains "all of them" "one.txt" "$(cb sources list 1)"
cb sources set 1 "$WORK/src" >/dev/null

# Every command, in the longest form its spec takes, with an unknown flag
# and then with one positional too many. A row is "<command words>|<args>";
# a leading '*' marks a command whose last positional repeats, so it has no
# "too many". Each must exit 1 naming the argument, with that command's
# usage, and send nothing: against a socket where nothing listens a request
# would say "daemon not running", and against the live daemon the state
# must not move.
SWEEP=(
    "repo list|"
    "repo add|$WORK/sweep lbl"
    "repo show|1"
    "repo remove|1"
    "repo key export|1"
    "repo key import|1 00"
    "repo key ack|1"
    "sources list|1"
    "*sources set|1 $WORK/src"
    "*backup|1 $WORK/src"
    "restore|1 1 $WORK/sweepdst"
    "snapshots|1"
    "snapshot delete|1 1"
    "snapshot note|1 1 text"
    "snapshot tree|1 1 0"
    "ls|1 1 0"
    "search|1 q"
    "grep|1 pat"
    "grep runs|1"
    "grep show|1 1"
    "diff|1 1 2"
    "verify|1"
    "verify list|1"
    "verify show|1 1"
    "gc|1"
    "gfs|1"
    "replicate|1 $WORK/sweeprepl"
    "reindex|1"
    "stats|1"
    "summary|1"
    "objects|1"
    "object|1 abc refs"
    "packs|1 1"
    "fanout|1"
    "policy list|1"
    "policy set|1 retention.keep_snaps 1"
    "tag list|1"
    "tag set|1 sweep 1"
    "tag remove|1 sweep"
    "journal|1"
    "schedule list|1"
    "schedule add|1 backup 60"
    "schedule remove|1 1"
    "schedule enable|1 1"
    "schedule disable|1 1"
    "jobs|1"
    "jobs cancel|1"
    "jobs watch|1"
    "auth status|"
    "auth setup|pw"
    "auth password|a b"
    "auth logout|--all"
    "metrics|"
    "health|live"
    "version|"
    "help|repo add"
)
# Everything a stray request could change. The newest job on repository 1
# covers every job-starting command (a replicate schedule from an earlier
# section fires every second on repository 2); the rest cover direct writes.
b127_state() {
    cb repo list; cb snapshots 1; cb sources list 1; cb schedule list 1
    cb tag list 1; cb policy list 1; cb snapshot note 1 1; cb auth status
    cb jobs --limit 200 | grep -o '"id":[0-9]*,"repo_id":1,' | head -1
    cb grep runs 1 --limit 1 | grep -o '"id":[0-9]*' | head -1
    cb verify list 1 --limit 1 | grep -o '"id":[0-9]*' | head -1
}
STATE_BEFORE=$(b127_state 2>&1)
SWEEP_BAD=0
sweep_one() {  # sweep_one <socket> <command words> <expected message> <words...>
    local sock="$1" cmd="$2" msg="$3"; shift 3
    local out rc
    out=$("$CLI" --socket "$sock" "$@" 2>&1); rc=$?
    case "$rc:$out" in
        1:*"$msg"*"usage: c-backup $cmd"*)
            case "$out" in
                *"daemon not running"*|*HTTP*) SWEEP_BAD=$((SWEEP_BAD+1))
                    printf '       sent a request: %s\n' "$*" ;;
            esac ;;
        *) SWEEP_BAD=$((SWEEP_BAD+1))
           printf '       not refused (rc %s): %s\n       %s\n' "$rc" "$*" "$out" ;;
    esac
}
for row in "${SWEEP[@]}"; do
    variadic=0
    case "$row" in \**) variadic=1; row="${row#\*}" ;; esac
    CMD="${row%%|*}"
    read -ra W <<< "$CMD ${row#*|}"
    for sock in "$WORK/absent.sock" "$SOCK"; do
        sweep_one "$sock" "$CMD" "unknown option '--b127-bogus'" "${W[@]}" --b127-bogus
        sweep_one "$sock" "$CMD" "unknown option '--b127-bogus'" "${W[@]}" --b127-bogus=1
        [ "$variadic" = 1 ] ||
            sweep_one "$sock" "$CMD" "unexpected argument 'b127extra'" "${W[@]}" b127extra
    done
done
check "every command refuses an unknown flag and an extra positional, sending nothing" \
    0 "$SWEEP_BAD"
check "and the daemon's state did not move" "$STATE_BEFORE" "$(b127_state 2>&1)"

# A value option with its value missing, at the end of the line or before
# another option, for every command that takes one. Each used to be taken as
# a positional, or to take the next option as its value.
SWEEP_BAD=0
VALUE_SWEEP=(
    "backup|1 $WORK/src|--label"
    "backup|1 $WORK/src|--exclude"
    "sources set|1 $WORK/src|--exclude"
    "restore|1 1 $WORK/sweepdst|--subpath"
    "snapshots|1|--limit"
    "snapshots|1|--offset"
    "search|1 q|--snapshot"
    "grep|1 pat|--snapshot"
    "grep|1 pat|--path-prefix"
    "grep|1 pat|--max-hits"
    "grep|1 pat|--max-hits-per-file"
    "verify|1|--level"
    "stats|1|--scope"
    "objects|1|--location"
    "journal|1|--result"
    "schedule add|1 backup 60|--on-overlap"
    "schedule add|1 backup 60|--source"
    "schedule add|1 replicate|--every"
    "repo add|$WORK/sweep|--encrypt"
    "repo add|$WORK/sweep|--passphrase"
)
for row in "${VALUE_SWEEP[@]}"; do
    CMD="${row%%|*}"
    rest="${row#*|}"
    opt="${rest#*|}"
    read -ra W <<< "$CMD ${rest%|*}"
    for sock in "$WORK/absent.sock" "$SOCK"; do
        sweep_one "$sock" "$CMD" "option '$opt' needs a value" "${W[@]}" "$opt"
        sweep_one "$sock" "$CMD" "option '$opt' needs a value" "${W[@]}" "$opt" --no-wait
    done
done
check "every value option refuses a missing value, sending nothing" 0 "$SWEEP_BAD"
check "and the state still did not move" "$STATE_BEFORE" "$(b127_state 2>&1)"

# NIT-26: a value outside an option's fixed set, and a calendar missing what
# its period needs or carrying what it would drop. `verify --level bogus`,
# `objects --location bogus` and `--every day` without --at were sent and
# refused by the daemon, unlike `stats --scope bogus`.
SWEEP_BAD=0
CHOICE_SWEEP=(
    "verify|1 --level bogus|option '--level' takes index, structure or deep, not 'bogus'"
    "objects|1 --location bogus|option '--location' takes staged, packed or all, not 'bogus'"
    "stats|1 --scope bogus|option '--scope' takes staged, not 'bogus'"
    "journal|1 --result bogus|option '--result' takes success, failed, cancelled, warning or alert, not 'bogus'"
    "journal|1 --result completed|option '--result' takes success, failed, cancelled, warning or alert, not 'completed'"
    "repo add|$WORK/sweep --encrypt SYSTEM|option '--encrypt' takes system, passphrase or none"
    "schedule add|1 backup --every fortnight --at 01:00|option '--every' takes day, week, month or year"
    "schedule add|1 backup 60 --on-overlap later|option '--on-overlap' takes skip, queue or cancel_running"
    "schedule add|1 backup 60 --catchup always|option '--catchup' takes skip or run_once"
    "schedule add|1 backup --every day|--every day needs --at HH:MM"
    "schedule add|1 backup --every day --at 24:00|--at takes a 24-hour HH:MM, not '24:00'"
    "schedule add|1 backup --every day --at noon|--at takes a 24-hour HH:MM"
    "schedule add|1 backup --every week --at 01:00|--every week needs --on WEEKDAY"
    "schedule add|1 backup --every week --at 01:00 --on someday|--on takes a weekday, full or three letters"
    "schedule add|1 backup --every day --at 01:00 --on monday|--on names a weekday, which only --every week uses"
    "schedule add|1 backup --every month --at 01:00|--every month needs --on-day N"
    "schedule add|1 backup --every day --at 01:00 --on-day 3|--on-day is for --every month or year, not day"
    "schedule add|1 backup --every year --at 01:00 --on-day 3|--every year needs --on-month N"
    "schedule add|1 backup --every month --at 01:00 --on-day 3 --on-month 2|--on-month is for --every year, not month"
    "schedule add|1 backup 60 --at 01:00|they need --every"
)
for row in "${CHOICE_SWEEP[@]}"; do
    CMD="${row%%|*}"
    rest="${row#*|}"
    msg="${rest#*|}"
    read -ra W <<< "$CMD ${rest%%|*}"
    for sock in "$WORK/absent.sock" "$SOCK"; do
        sweep_one "$sock" "$CMD" "$msg" "${W[@]}"
    done
done
check "every bad option value and calendar shape is refused locally, sending nothing" \
    0 "$SWEEP_BAD"
check "and the state did not move for those either" "$STATE_BEFORE" "$(b127_state 2>&1)"
# Case is the daemon's to ignore for a weekday, so the CLI does too; and
# `all`, the daemon's default, is now in the CLI's list.
contains "a weekday in capitals is accepted" '"id"' \
    "$(cb schedule add 1 gfs --every week --on Sunday --at 03:00)"
# NIT-28: the three-letter form in any case, by the CLI and the daemon, and
# stored as the full lowercase name the list serves back.
contains "a three-letter weekday is accepted" '"id"' \
    "$(cb schedule add 1 verify --every week --at 03:00 --on FRI)"
contains "a mixed-case abbreviation is accepted" '"id"' \
    "$(cb schedule add 1 verify --every week --at 03:00 --on Tue)"
NIT28_LIST=$(cb schedule list 1)
contains "an abbreviation is stored as the full name" '"on":"friday"' "$NIT28_LIST"
contains "and in lowercase" '"on":"tuesday"' "$NIT28_LIST"
contains "four letters is not an abbreviation" 'three letters in any case' \
    "$(cb schedule add 1 verify --every week --at 03:00 --on tues 2>&1)"
contains "objects --location all answers" '"objects"' "$(cb objects 1 --location all --limit 1)"

# B154: packs and diff sent --limit and --offset and the daemon read
# neither; journal dropped --offset. Its own repository, three backups each
# changing every file, so there is more than one of everything to page.
echo "every collection pages (B154)"
mkdir -p "$WORK/pgsrc"
PG=$(cb repo add "$WORK/pgrepo" paging | sed -n 's/^{"id":\([0-9]*\).*/\1/p')
for gen in one two three; do
    for i in 1 2 3 4 5 6; do echo "$gen $i" > "$WORK/pgsrc/pgfile$i"; done
    cb backup "$PG" "$WORK/pgsrc" >/dev/null
done
PG_S=$(cb snapshots "$PG" | grep -oE '"id":[0-9]+' | tr -dc '0-9\n' | sort -n)
PG_S1=$(printf '%s\n' "$PG_S" | sed -n 1p)
PG_S2=$(printf '%s\n' "$PG_S" | sed -n 2p)
nth() { printf '%s' "$1" | grep -oE "\"$2\":(\"[^\"]*\"|[0-9]+)" | sed -n "${3}p"; }
cnt() { printf '%s' "$1" | grep -oE "\"$2\":" | grep -c .; }

PG_PK=$(cb packs "$PG" --limit 5000)
PG_PK1=$(cb packs "$PG" --limit 1 --offset 1)
check "packs --limit caps the page" 1 "$(cnt "$PG_PK1" filename)"
check "packs --offset skips a pack" "$(nth "$PG_PK" filename 2)" "$(nth "$PG_PK1" filename 1)"
OUT=$(cb packs "$PG" --limit 0 2>&1)
contains "packs --limit names the maximum" 'from 1 to 5000' "$OUT"

PG_DF=$(cb diff "$PG" "$PG_S1" "$PG_S2")
PG_DF1=$(cb diff "$PG" "$PG_S1" "$PG_S2" --limit 1 --offset 2)
check "diff --limit caps the page" 1 "$(cnt "$PG_DF1" change)"
check "diff --offset skips two changes" "$(nth "$PG_DF" path 3)" "$(nth "$PG_DF1" path 1)"
contains "diff counts every change on a page" '"modified":6' "$PG_DF1"
OUT=$(cb diff "$PG" "$PG_S1" "$PG_S2" --limit 0 2>&1)
contains "diff --limit names the maximum" 'from 1 to 5000' "$OUT"

PG_J=$(cb journal "$PG" --limit 100)
PG_J1=$(cb journal "$PG" --limit 2 --offset 1)
check "journal --limit caps the page" 2 "$(cnt "$PG_J1" operation)"
check "journal --offset skips the newest" "$(nth "$PG_J" id 2)" "$(nth "$PG_J1" id 1)"
OUT=$(cb journal "$PG" --limit 0 2>&1)
contains "journal --limit names the maximum" 'from 1 to 1000' "$OUT"
contains "journal --result success filters" '"result":"success"' \
    "$(cb journal "$PG" --result success --limit 1)"

PG_OBJ=$(printf '%s' "$PG_DF1" | sed -n 's/.*"old_hash":"\([a-f0-9]*\)".*/\1/p')
contains "object refs --offset skips" '"refs":[]' "$(cb object "$PG" "$PG_OBJ" refs --offset 1)"
check "object refs --limit caps" 1 "$(cnt "$(cb object "$PG" "$PG_OBJ" refs --limit 1)" snap_id)"
OUT=$(cb object "$PG" "$PG_OBJ" layout --limit 1 2>&1); RC=$?
check "--limit on an object view that is not a list fails" 1 "$RC"
contains "and says what it pages" 'page an object' "$OUT"
PG_SR=$(cb search "$PG" pgfile --snapshot "$PG_S1")
check "search finds the six files" 6 "$(cnt "$PG_SR" node_id)"
check "search --offset skips" "$(nth "$PG_SR" path 2)" \
    "$(nth "$(cb search "$PG" pgfile --snapshot "$PG_S1" --limit 1 --offset 1)" path 1)"
for n in a b c; do cb tag set "$PG" "pg-$n" "$PG_S1" >/dev/null; done
check "tag list --offset skips" '"name":"pg-b"' "$(nth "$(cb tag list "$PG" --limit 1 --offset 1)" name 1)"
cb schedule add "$PG" gc 3600 >/dev/null
cb schedule add "$PG" gc 7200 >/dev/null
PG_SC=$(cb schedule list "$PG")
check "schedule list --offset skips" "$(nth "$PG_SC" id 2)" \
    "$(nth "$(cb schedule list "$PG" --limit 1 --offset 1)" id 1)"
PG_RL=$(cb repo list)
check "repo list --offset skips" "$(nth "$PG_RL" id 2)" "$(nth "$(cb repo list --limit 1 --offset 1)" id 1)"
check "repo list --limit caps" 1 "$(cnt "$(cb repo list --limit 1)" uuid)"

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
