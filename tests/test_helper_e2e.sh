#!/bin/bash
#
# The helper split end to end: c-backup-helper serving the fake provider
# from its policy, c-backupd attached to it with units.helper_socket and
# no provider of its own, and the CLI backing a unit up and restoring it
# through both. Also: the helper refuses a policy others can write, checks
# one with --check, and a daemon whose helper went away fails units with
# the reason rather than hanging.

set -u

DAEMON="${1:-build/bins/c-backupd}"
# A write made outside the write lock aborts the daemon here rather than
# landing in another thread's transaction unnoticed (B157, db_write.h).
export C_BACKUP_TXN_GUARD=abort
CLI="${2:-build/bins/c-backup}"
HELPER="${3:-build/bins/c-backup-helper}"
TEST_ROOT="${C_BACKUP_TEST_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/c-backup-test}"
mkdir -p "$TEST_ROOT"
# As root (CI) the daemon runs as uid 990 (below), which has to reach its
# work tree: a checkout under a directory only root may enter (CI job 19985:
# the daemon could not even see its config.yaml). Search permission for
# others is added on the way down to the test directory, nothing else, as
# hf_unroot does for the C suites.
if [ "$(id -u)" = 0 ]; then
    d="$TEST_ROOT"
    while [ -n "$d" ] && [ "$d" != / ]; do
        [ -d "$d" ] && chmod o+x "$d"
        d="${d%/*}"
    done
fi
WORK="$(mktemp -d "$TEST_ROOT/c_backup_helper_e2e.XXXXXX")"
PORTBASE=$((20000 + ($$ % 15000) * 3))
port_busy() { ss -Htan "( sport = :$1 )" 2>/dev/null | grep -q .; }
while port_busy "$PORTBASE"; do
    PORTBASE=$((PORTBASE + 3)); [ "$PORTBASE" -gt 64990 ] && PORTBASE=20000
done
SOCK="$WORK/c.sock"
HSOCK="$WORK/h.sock"
PASS=0
FAIL=0

cleanup() {
    [ -n "${DAEMON_PID:-}" ] && kill "$DAEMON_PID" 2>/dev/null
    wait "${DAEMON_PID:-}" 2>/dev/null
    [ -n "${HELPER_PID:-}" ] && kill "$HELPER_PID" 2>/dev/null
    wait "${HELPER_PID:-}" 2>/dev/null
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
cb() { "$CLI" --socket "$SOCK" "$@"; }

mkdir -p "$WORK/snap" "$WORK/pool/app/a"
echo alpha > "$WORK/snap/a.txt"
# The tree root as read: a mode and a time no staging directory has (B168).
chmod 0751 "$WORK/snap"
touch -d @1500000000 "$WORK/snap"
cat > "$WORK/units.json" <<EOF
{"units":[
 {"id":"a","name":"cbk-uat-a","state":"running","caps":["quiesce","snapshot"],
  "blobs":[{"id":"domain.xml","path":".cbackup-unit/config/domain.xml","text":"<domain>a</domain>"}],
  "disks":[{"id":"vda","size":4194304,"runs":[{"off":0,"len":1048576,"kind":"data","seed":3}]}],
  "trees":[{"id":"vol","read_root":"$WORK/snap","path":"$WORK/pool/app/a","source":"$WORK/pool/app/a"}]},
 {"id":"k","name":"cbk-uat-k","state":"running","caps":["quiesce"],
  "disks":[{"id":"vda","size":8589934592,"runs":[{"off":0,"len":8589934592,"kind":"data","seed":9}]}]},
 {"id":"c","name":"cbk-uat-c","state":"running","caps":["quiesce"],
  "blobs":[{"id":"x","path":".cbackup-unit/config/x","text":"c"}]},
 {"id":"p","name":"prod-db","state":"running",
  "blobs":[{"id":"x","path":".cbackup-unit/config/x","text":"secret"}]}
]}
EOF
# The helper refuses daemon_uid 0 by design, and CI runs as root. There the
# daemon is started as a fixed non-root uid, which the policy names, and the
# work tree is that uid's; anywhere else both are the current user. (Started
# as that uid rather than dropping with security.run_as_uid, because the
# drop comes after the state database is opened, which would leave it
# root's.)
if [ "$(id -u)" = 0 ]; then
    DUID=990
    DGID=990
    AS_DAEMON=(setpriv --reuid="$DUID" --regid="$DUID" --clear-groups --)
else
    DUID=$(id -u)
    DGID=$(id -g)
    AS_DAEMON=()
fi
cat > "$WORK/policy.ini" <<EOF
[helper]
daemon_uid = $DUID
daemon_gid = $DGID
daemon_id = 0badf00d
max_pause_s = 30
capture_max_s = 3600
allow_restore = 1
allow_define = 1
state_dir = $WORK/hstate
socket = $HSOCK
fake_spec = $WORK/units.json

[units]
allow = fake:cbk-uat-*

[snapshot]
dir_domains = $WORK/pool
EOF

echo "policy"
chmod 0664 "$WORK/policy.ini"
OUT=$("$HELPER" --policy "$WORK/policy.ini" 2>&1); RC=$?
check    "a group-writable policy refuses the start" 1 "$RC"
contains "and says why" "writable by group" "$OUT"
chmod 0600 "$WORK/policy.ini"
OUT=$("$HELPER" --policy "$WORK/policy.ini" --check); RC=$?
check    "--check accepts the policy" 0 "$RC"
contains "and prints its digest" '"policy":"' "$OUT"

cat > "$WORK/config.yaml" <<EOF
db:
  path: $WORK/state.db
http:
  socket: $SOCK
  port: $PORTBASE
  bind: 127.0.0.1
log:
  level: info
security:
  key_dir: $WORK/keys
units:
  helper_socket: $HSOCK
EOF
# The policy stays root's: the helper refuses one another uid owns.
[ "$(id -u)" = 0 ] && chown -R "$DUID" "$WORK" && chown 0 "$WORK/policy.ini"
C_BACKUP_CONFIG_PATH="$WORK/config.yaml" "${AS_DAEMON[@]}" "$DAEMON" > "$WORK/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
if [ ! -S "$SOCK" ]; then echo "daemon did not start"; cat "$WORK/daemon.log"; exit 1; fi

echo "daemon first, helper not up yet"
OUT=$(cb unit providers)
contains "the daemon starts without its helper" '"state":"' "$OUT"
contains "and says the helper is not connected" '"connection":{"state":"disconnected"' "$OUT"
contains "naming the socket" "\"socket\":\"$HSOCK\"" "$OUT"
contains "repo add" '"id":1' "$(cb repo add "$WORK/repo" main)"
OUT=$(cb unit backup 1 --provider fake cbk-uat-a 2>&1); RC=$?
check    "a unit job with the helper away fails" 1 "$RC"
contains "and names the socket" "$HSOCK" "$OUT"
# A schedule firing meanwhile fails too, and alerts, never a quiet skip.
OUT=$(cb schedule add 1 units 1 --provider fake --all)
contains "a units schedule is accepted while the helper is away" '"id"' "$OUT"
SID=$(printf '%s' "$OUT" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
for _ in $(seq 1 150); do
    [ "$(grep -c "failure notification for job .*units of main" "$WORK/daemon.log")" -ge 2 ] && break
    sleep 0.1
done
ALERT=$(grep -A8 "failure notification for job .*units of main" "$WORK/daemon.log" | tr "\n" " ")
contains "the scheduled job's alert names the helper socket" "$HSOCK" "$ALERT"
N_ALERTS=$(grep -c "failure notification for job .*units of main" "$WORK/daemon.log")
if [ "$N_ALERTS" -ge 2 ]; then ok "the scheduled run alerted as well as the API one ($N_ALERTS)"; else bad "the scheduled run alerted" ">= 2 alerts" "$N_ALERTS"; fi
contains "the job came from the schedule" "\"origin\":\"schedule\"" "$(cb jobs --limit 5)"
contains "and the job failed" '"state":"failed"' "$(cb jobs --limit 5)"
cb schedule remove 1 "$SID" >/dev/null

echo "the helper arrives"
"$HELPER" --policy "$WORK/policy.ini" > "$WORK/helper.log" 2>&1 &
HELPER_PID=$!
for _ in $(seq 1 50); do [ -S "$HSOCK" ] && break; sleep 0.1; done
if [ ! -S "$HSOCK" ]; then echo "helper did not start"; cat "$WORK/helper.log"; exit 1; fi
check "the socket is 0660" 660 "$(stat -c %a "$HSOCK")"
check "its group is the daemon's" "$DGID" "$(stat -c %g "$HSOCK")"
contains "the helper logs the socket's owner, group and mode" "group $DGID mode 0660" "$(cat "$WORK/helper.log")"
if [ "$(id -u)" = 0 ]; then
    # daemon_gid changes the socket's group (root may give any group); a
    # directory the group cannot enter is warned about, loudly.
    mkdir -p "$WORK/shut"
    chmod 0700 "$WORK/shut"
    sed -e "s|^socket = .*|socket = $WORK/shut/h2.sock|" -e "s|^daemon_gid = .*|daemon_gid = 4242|" \
        -e "s|^state_dir = .*|state_dir = $WORK/hstate2|" "$WORK/policy.ini" > "$WORK/policy2.ini"
    chmod 0600 "$WORK/policy2.ini"
    "$HELPER" --policy "$WORK/policy2.ini" > "$WORK/helper2.log" 2>&1 &
    H2=$!
    for _ in $(seq 1 50); do [ -S "$WORK/shut/h2.sock" ] && break; sleep 0.1; done
    check "daemon_gid sets the socket's group" 4242 "$(stat -c %g "$WORK/shut/h2.sock" 2>/dev/null)"
    contains "an unreachable socket directory is warned about" "WARNING: no member of group 4242" "$(cat "$WORK/helper2.log")"
    kill "$H2"; wait "$H2" 2>/dev/null
fi
# The daemon reconnects on its own, within its backoff (at most 30 s).
for _ in $(seq 1 400); do
    cb unit providers | grep -q '"state":"connected"' && break
    sleep 0.1
done

echo "through the helper"
OUT=$(cb unit providers)
contains "the daemon connected without a restart" '"state":"connected"' "$OUT"
contains "the helper is shown" '"helper":{"socket"' "$OUT"
contains "with its version" '"version":1' "$OUT"
contains "with the daemon id it serves" '"daemon_id":"0badf00d"' "$OUT"
contains "fake is enabled, through it" '"enabled":true' "$OUT"
contains "and probes through the socket" "through the helper at" "$OUT"
OUT=$(cb unit list fake)
contains "enumeration crosses the helper" 'cbk-uat-a' "$OUT"
lacks    "and the helper's policy filters it" 'prod-db' "$OUT"
OUT=$(cb unit backup 1 --provider fake cbk-uat-a)
contains "a unit backup through the helper completes" '"state":"completed"' "$OUT"
contains "at the level the helper's session reached" '"consistency":"quiesced"' "$OUT"
OUT=$(cb unit restore 1 1 "$WORK/out")
contains "restore completes" '"state":"completed"' "$OUT"
check    "the disk came back full size" 4194304 "$(stat -c %s "$WORK/out/.cbackup-unit/disks/vda")"
check    "the tree from the helper's read root" alpha "$(cat "$WORK/out$WORK/pool/app/a/a.txt")"
check    "configuration as the helper read it" "<domain>a</domain>" "$(cat "$WORK/out/.cbackup-unit/config/domain.xml")"
check    "no holds are left in the helper's journal" "[]" "$(cat "$WORK/hstate/holds.json")"

echo "restore as a unit"
OUT=$(cb unit restore 1 1 --define 2>&1); RC=$?
check    "in place without --replace is refused" 1 "$RC"
contains "and says to pass replace" "pass replace" "$OUT"
OUT=$(cb unit restore 1 1 --define --name cbk-uat-a 2>&1); RC=$?
check    "its own name is in place, so without --replace it is refused" 1 "$RC"
contains "asking for replace, not a target root" "pass replace" "$OUT"
OUT=$(cb unit restore 1 1 --define --name cbk-uat-b 2>&1); RC=$?
check    "a new name without a target root is refused" 1 "$RC"
contains "and says so" "needs target_root" "$OUT"
OUT=$(cb unit restore 1 1 --define --name cbk-uat-b --target-root "$WORK/target")
contains "a new-name restore completes" '"state":"completed"' "$OUT"
contains "its result says where the data went" "restored_to" "$OUT"
check    "the data is under the target root" alpha "$(cat "$WORK/target$WORK/pool/app/a/a.txt")"
check    "the original's path is untouched" "" "$(ls "$WORK/pool/app/a")"
contains "the helper got the remapped paths" "$WORK/target$WORK/pool/app/a" "$(cat "$WORK/hstate/fake-define-a.json")"
contains "its warning is in the journal" "fake define" "$(cb journal 1)"
# B167: a define that fails after the swap puts the original back and
# starts the unit again; it used to leave the new data in and the unit down.
rm -f "$WORK/hstate/fake-define-a.json"
mkdir "$WORK/hstate/fake-define-a.json"
OUT=$(cb unit restore 1 1 --define --replace 2>&1); RC=$?
check    "an in-place restore whose define fails fails" 1 "$RC"
contains "and says it was rolled back" "rolled back" "$OUT"
check    "the original data is still in place" "" "$(ls "$WORK/pool/app/a")"
check    "nothing is left moved aside" "" "$(ls -d "$WORK/pool/app/a.cbackup-old-"* 2>/dev/null)"
contains "and the unit runs again" '"state":"running"' "$(cb unit show fake cbk-uat-a)"
contains "the error says where the restored copy was kept" "$WORK/pool/.cbackup-restore/" "$OUT"
check    "and it is there" alpha "$(cat "$WORK/pool/.cbackup-restore/"*/a.txt 2>/dev/null)"
rmdir "$WORK/hstate/fake-define-a.json"
rm -rf "$WORK/pool/.cbackup-restore"
OUT=$(cb unit restore 1 1 --define --replace)
contains "an in-place restore completes" '"state":"completed"' "$OUT"
check    "the data is back in place" alpha "$(cat "$WORK/pool/app/a/a.txt")"
check    "the mount root has the snapshot's mode, not the staging's" 751 "$(stat -c %a "$WORK/pool/app/a")"
check    "and its time" 1500000000 "$(stat -c %Y "$WORK/pool/app/a")"
# B187: the staging base is the daemon's bind in a deployment, so it stays;
# what the restore staged in it does not.
check    "the staging base stays, for the next restore" yes "$([ -d "$WORK/pool/.cbackup-restore" ] && echo yes || echo no)"
check    "and nothing is left in it" "" "$(ls -A "$WORK/pool/.cbackup-restore" 2>/dev/null)"

echo "a scheduled pause scope"
# pause_scope used to be dropped from a units schedule's parameters, so a
# scheduled job scope ran unit by unit, accepted and not honoured.
OUT=$(cb schedule add 1 units 1 --provider fake --unit cbk-uat-a --unit cbk-uat-c --pause-scope job)
SID2=$(printf '%s' "$OUT" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
contains "a units schedule keeps its pause scope" 'pause_scope' "$(cb schedule list 1)"
for _ in $(seq 1 100); do
    grep -q "2 unit(s) in one capture" "$WORK/daemon.log" && break; sleep 0.1
done
contains "and its run captures both units in one capture" "2 unit(s) in one capture" "$(cat "$WORK/daemon.log")"
cb schedule remove 1 "$SID2" >/dev/null
for _ in $(seq 1 100); do
    case "$(cb jobs --limit 3)" in *'"state":"running"'*|*'"state":"queued"'*) sleep 0.1 ;; *) break ;; esac
done
if command -v curl >/dev/null 2>&1; then
    OUT=$(curl -s --unix-socket "$SOCK" -X POST http://x/repos/1/schedules \
          -d '{"action":"units","trigger":{"type":"interval","seconds":3600},"provider":"fake","all":true,"pause_scop":"job"}')
    contains "an unknown key in a schedule is refused" "unknown key 'pause_scop'" "$OUT"
    OUT=$(curl -s --unix-socket "$SOCK" -X POST http://x/repos/1/units/backup \
          -d '{"provider":"fake","all":true,"pausescope":"job"}')
    contains "and in a units job" "unknown key 'pausescope'" "$OUT"
fi

echo "a daemon killed mid-backup"
# B169: the helper held the backup's export hold until its own restart or
# capture_max_s, and holds_open said 0 meanwhile. Now the daemon's anchor
# closes with it, and the helper ends the capture at once.
cb unit backup 1 --provider fake cbk-uat-k > /dev/null 2>&1 &
BG=$!
for _ in $(seq 1 100); do
    [ "$(cat "$WORK/hstate/holds.json" 2>/dev/null)" != "[]" ] && break; sleep 0.1
done
contains "the helper journals the running backup's hold" '"kind":"export"' "$(cat "$WORK/hstate/holds.json")"
OUT=$(cb unit providers)
lacks    "and holds_open counts it" '"holds_open":0' "$OUT"
contains "the helper's holds are shown" '"holds":' "$OUT"
kill -9 "$DAEMON_PID"; wait "$DAEMON_PID" 2>/dev/null; DAEMON_PID=
wait "$BG" 2>/dev/null
for _ in $(seq 1 50); do
    [ "$(cat "$WORK/hstate/holds.json")" = "[]" ] && break; sleep 0.1
done
check    "the helper releases it within seconds of the kill" "[]" "$(cat "$WORK/hstate/holds.json")"
contains "and says why" "session" "$(grep "ended (its connection closed)" "$WORK/helper.log")"
C_BACKUP_CONFIG_PATH="$WORK/config.yaml" "${AS_DAEMON[@]}" "$DAEMON" >> "$WORK/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 100); do
    cb unit providers 2>/dev/null | grep -q '"state":"connected"' && break; sleep 0.1
done
contains "the restarted daemon sees no hold open" '"holds_open":0' "$(cb unit providers)"

echo "helper gone"
kill "$HELPER_PID"; wait "$HELPER_PID" 2>/dev/null; HELPER_PID=
OUT=$(cb unit backup 1 --provider fake cbk-uat-a 2>&1); RC=$?
check    "a unit backup without its helper fails" 1 "$RC"
contains "and names the helper" "helper" "$OUT"

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
