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
CLI="${2:-build/bins/c-backup}"
HELPER="${3:-build/bins/c-backup-helper}"
TEST_ROOT="${C_BACKUP_TEST_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/c-backup-test}"
mkdir -p "$TEST_ROOT"
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
cat > "$WORK/units.json" <<EOF
{"units":[
 {"id":"a","name":"cbk-uat-a","state":"running","caps":["quiesce","snapshot"],
  "blobs":[{"id":"domain.xml","path":".cbackup-unit/config/domain.xml","text":"<domain>a</domain>"}],
  "disks":[{"id":"vda","size":4194304,"runs":[{"off":0,"len":1048576,"kind":"data","seed":3}]}],
  "trees":[{"id":"vol","read_root":"$WORK/snap","path":"$WORK/pool/app/a"}]},
 {"id":"p","name":"prod-db","state":"running",
  "blobs":[{"id":"x","path":".cbackup-unit/config/x","text":"secret"}]}
]}
EOF
cat > "$WORK/policy.ini" <<EOF
[helper]
daemon_uid = $(id -u)
daemon_id = 0badf00d
max_pause_s = 30
capture_max_s = 3600
allow_restore = 1
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
C_BACKUP_CONFIG_PATH="$WORK/config.yaml" "$DAEMON" > "$WORK/daemon.log" 2>&1 &
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

echo "helper gone"
kill "$HELPER_PID"; wait "$HELPER_PID" 2>/dev/null; HELPER_PID=
OUT=$(cb unit backup 1 --provider fake cbk-uat-a 2>&1); RC=$?
check    "a unit backup without its helper fails" 1 "$RC"
contains "and names the helper" "helper" "$OUT"

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
