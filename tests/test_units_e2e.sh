#!/bin/bash
#
# Units end to end: the CLI against a real daemon with the fake provider
# enabled by its test-only key (units.fake_spec). Covers the routes, the job,
# units.allow, the reserved names on the file-backup routes, restore of a
# disk as a sparse raw image, the NBD export of a disk (read by QEMU where
# installed, pinning, gc deferral, revoke, ttl, idle and restart), per-unit
# policy, the units schedule action and reindex rebuilding unit history.

set -u

DAEMON="${1:-build/bins/c-backupd}"
# A write made outside the write lock aborts the daemon here rather than
# landing in another thread's transaction unnoticed (B157, db_write.h).
export C_BACKUP_TXN_GUARD=abort
CLI="${2:-build/bins/c-backup}"
TEST_ROOT="${C_BACKUP_TEST_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/c-backup-test}"
mkdir -p "$TEST_ROOT"
WORK="$(mktemp -d "$TEST_ROOT/c_backup_units_e2e.XXXXXX")"
PORTBASE=$((20000 + ($$ % 15000) * 3))
port_busy() { ss -Htan "( sport = :$1 )" 2>/dev/null | grep -q .; }
while port_busy "$PORTBASE"; do
    PORTBASE=$((PORTBASE + 3)); [ "$PORTBASE" -gt 64990 ] && PORTBASE=20000
done
SOCK="$WORK/c.sock"
PASS=0
FAIL=0

cleanup() {
    [ -n "${DAEMON_PID:-}" ] && kill "$DAEMON_PID" 2>/dev/null
    wait "${DAEMON_PID:-}" 2>/dev/null
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

mkdir -p "$WORK/snap-a/sub"
echo "alpha" > "$WORK/snap-a/a.txt"
echo "bravo" > "$WORK/snap-a/sub/b.txt"

# Two allowed units and one the guard rail keeps out. Unit a's disk holds
# data, a 1 MiB run of zeroes the map reports as data, and a trailing hole.
write_spec() {
cat > "$WORK/units.json" <<EOF
{"units":[
 {"id":"a","name":"cbk-uat-a","state":"running","caps":["quiesce","snapshot"],
  "blobs":[{"id":"domain.xml","path":".cbackup-unit/config/domain.xml","text":"<domain>a</domain>"}],
  "disks":[{"id":"vda","size":8388608,"runs":[
     {"off":0,"len":1048576,"kind":"data","seed":7},
     {"off":1048576,"len":1048576,"kind":"zero_data"},
     {"off":2097152,"len":524288,"kind":"data","seed":8}]}],
  "trees":[{"id":"vol","read_root":"$WORK/snap-a","path":"$WORK/live/a"}]},
 {"id":"b","name":"cbk-uat-b","state":"stopped",
  "blobs":[{"id":"cfg","path":".cbackup-unit/config/lxc.config","text":"lxc.arch = x86_64"}]},
 {"id":"p","name":"prod-db","state":"running",
  "blobs":[{"id":"cfg","path":".cbackup-unit/config/x","text":"secret"}]}$1
]}
EOF
}
write_spec ""

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
  allow: cbk-uat-*
  fake_spec: $WORK/units.json
vm:
  export_dir: $WORK/ex
  export_idle_s: 1800
EOF

start_daemon() {
    C_BACKUP_CONFIG_PATH="$WORK/config.yaml" "$DAEMON" >> "$WORK/daemon.log" 2>&1 &
    DAEMON_PID=$!
    for _ in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
    if [ ! -S "$SOCK" ]; then echo "daemon did not start"; cat "$WORK/daemon.log"; exit 1; fi
}
start_daemon

echo "providers"
OUT=$(cb unit providers)
contains "the fake provider is listed" '"name":"fake"' "$OUT"
contains "and enabled" '"enabled":true' "$OUT"
contains "the allow list is shown" '"allow":"cbk-uat-*"' "$OUT"
OUT=$(cb unit list fake)
contains "enumeration lists an allowed unit" 'cbk-uat-a' "$OUT"
lacks    "and never one outside units.allow" 'prod-db' "$OUT"
contains "describe" '"id":"vda"' "$(cb unit show fake cbk-uat-a)"
OUT=$(cb unit show fake prod-db 2>&1); RC=$?
check    "describing a unit outside units.allow fails" 1 "$RC"
OUT=$(cb unit list nosuch 2>&1); RC=$?
check    "an unknown provider fails" 1 "$RC"
contains "and names what is enabled" 'enabled: fake' "$OUT"

contains "repo add" '"id":1' "$(cb repo add "$WORK/repo" main)"

echo "backup"
OUT=$(cb unit backup 1 --provider fake cbk-uat-a)
contains "a unit backup completes" '"state":"completed"' "$OUT"
contains "with the level it achieved" '"consistency":"quiesced"' "$OUT"
contains "and journals a clean success" '"operation":"units","result":"success"' \
    "$(cb journal 1 --limit 1)"
OUT=$(cb unit backup 1 --provider fake prod-db 2>&1); RC=$?
check    "a named unit outside units.allow fails" 1 "$RC"
contains "and says why" 'units.allow' "$OUT"
OUT=$(cb unit backup 1 --provider fake --all)
contains "--all completes" '"state":"completed"' "$OUT"
contains "and leaves out what units.allow does" '"filtered":1' "$OUT"
contains "with one snapshot per unit" '"ok":2' "$OUT"
OUT=$(cb unit backup 1 --provider fake cbk-uat-a --all 2>&1); RC=$?
check    "units and --all together are refused" 1 "$RC"

echo "c-backup's own containers (SOAK14-SELF)"
write_spec ',{"id":"s","name":"cbk-uat-self","state":"running","self":true,"blobs":[{"id":"cfg","path":".cbackup-unit/config/x","text":"s"}]}'
contains "the listing marks c-backup's own container" '"self":true' "$(cb unit list fake)"
# a and b excluded, so the later per-unit snapshot counts stand.
OUT=$(cb unit backup 1 --provider fake --all --exclude 'cbk-uat-[ab]')
contains "--all still completes" '"state":"completed"' "$OUT"
contains "and fails nothing" '"failed":0' "$OUT"
contains "it skips c-backup's own container" '"skipped":1' "$OUT"
contains "listing it as skipped" '"status":"skipped"' "$OUT"
contains "with the reason" "holds c-backup's own daemon or helper container; skipped" "$OUT"
OUT=$(cb unit backup 1 --provider fake cbk-uat-self 2>&1); RC=$?
check    "naming it is refused" 1 "$RC"
contains "and says why" "cannot back itself up" "$OUT"
write_spec ""

echo "history"
OUT=$(cb unit history 1)
contains "units with history" '"unit_key":"fake:a"' "$OUT"
contains "and their last level" '"consistency":"offline"' "$OUT"
OUT=$(cb unit history 1 --provider fake cbk-uat-a)
contains "a unit's snapshots" '"read_mode":"full"' "$OUT"
check    "two of them" 2 "$(printf '%s' "$OUT" | grep -o '"name":"cbk-uat-a"' | wc -l)"

# The in-process fake cannot capture units together: job scope is taken
# unit by unit, and the job says so rather than nothing.
OUT=$(cb unit backup 1 --provider fake cbk-uat-a cbk-uat-b --pause-scope job)
contains "a job scope the provider cannot honour completes" '"state":"completed"' "$OUT"
contains "and says each unit was its own capture" "each unit was its own capture" "$OUT"
# The warning outlives the job: the journal is the permanent record.
OUT=$(cb journal 1 --limit 1)
contains "and journals a warning, not a success" '"operation":"units","result":"warning"' "$OUT"
contains "with the warning in it" "each unit was its own capture" "$OUT"

echo "restore"
OUT=$(cb unit restore 1 1 "$WORK/out")
contains "files restore completes" '"state":"completed"' "$OUT"
# NIT-50: a unit restore journalled no message; it says what came back.
OUT=$(cb journal 1 --limit 1)
contains "its journal entry is a unit-restore" '"operation":"unit-restore","result":"success"' "$OUT"
contains "naming the unit, the snapshot and where it went" \
    "unit fake:a from snapshot 1: " "$OUT"
contains "and how much came back" "restored to $WORK/out" "$OUT"
check    "the disk is its full virtual size" 8388608 "$(stat -c %s "$WORK/out/.cbackup-unit/disks/vda")"
BLOCKS=$(stat -c %b "$WORK/out/.cbackup-unit/disks/vda")
if [ "$BLOCKS" -lt 8192 ]; then ok "and sparse ($BLOCKS blocks)"; else bad "and sparse" "< 8192 blocks" "$BLOCKS"; fi
check    "the tree lands at its recorded path" "alpha" "$(cat "$WORK/out$WORK/live/a/a.txt")"
check    "configuration is verbatim" "<domain>a</domain>" "$(cat "$WORK/out/.cbackup-unit/config/domain.xml")"
contains "unit.json is there" '"format":"cbackup-unit"' "$(cat "$WORK/out/.cbackup-unit/unit.json")"
OUT=$(cb unit restore 1 1 "$WORK/vda.raw" --item vda)
contains "an item restore completes" '"state":"completed"' "$OUT"
contains "an item restore journals the item and its size" \
    "unit fake:a from snapshot 1: item vda (8388608 bytes) restored to $WORK/vda.raw" \
    "$(cb journal 1 --limit 1)"
if cmp -s "$WORK/vda.raw" "$WORK/out/.cbackup-unit/disks/vda"; then ok "the raw image matches"; else bad "the raw image matches" "same bytes" "differs"; fi
check    "no partial file is left" "no" "$([ -e "$WORK/vda.raw.cbk-partial" ] && echo yes || echo no)"
OUT=$(cb unit restore 1 1 "$WORK/vda.raw" --item vda 2>&1); RC=$?
check    "an item never lands on an existing file" 1 "$RC"
# The directories above a tree item take their recorded modes, as in a
# whole restore; made on the way under the daemon's 0077 umask they were 0700.
OUT=$(cb unit restore 1 1 "$WORK/itemvol" --item vol)
contains "a tree item restore completes" '"state":"completed"' "$OUT"
check    "the tree item's data is there" alpha "$(cat "$WORK/itemvol$WORK/live/a/a.txt")"
check    "a directory above it has the mode a whole restore gives it" \
         "$(stat -c %a "$WORK/out$WORK/live")" "$(stat -c %a "$WORK/itemvol$WORK/live")"
lacks    "and not the umask's 0700" 700 "$(stat -c %a "$WORK/itemvol$WORK/live")"
OUT=$(cb unit restore 1 999 "$WORK/nope" 2>&1); RC=$?
check    "a snapshot that does not exist is refused" 1 "$RC"
contains "as missing, not as not a unit snapshot" "has no snapshot 999" "$OUT"
OUT=$(cb unit restore 1 1 "$WORK/vda2.raw" --item vda --overwrite 2>&1); RC=$?
check    "--overwrite with --item is refused by the CLI" 1 "$RC"
contains "saying why" "an item is restored to a new path" "$OUT"
if command -v curl >/dev/null 2>&1; then
    OUT=$(curl -s --unix-socket "$SOCK" -X POST http://x/repos/1/snapshots/1/unit/restore \
          -d "{\"mode\":\"item\",\"item\":\"vda\",\"dest\":\"$WORK/vda2.raw\",\"overwrite\":true}")
    contains "and by the API, before any job" "overwrite does not apply to mode item" "$OUT"
fi


echo "nbd export"
jget() { sed -n "s/.*\"$1\":\"\{0,1\}\([^\",}]*\).*/\1/p" | head -1; }
OUT=$(cb vm export 1 1 vda)
contains "an export opens" '"export_id"' "$OUT"
EID=$(printf '%s' "$OUT" | jget export_id)
ESOCK=$(printf '%s' "$OUT" | jget socket)
ENAME=$(printf '%s' "$OUT" | jget name)
check    "its name is 128 random bits" 32 "${#ENAME}"
check    "its socket is the owner's alone" 600 "$(stat -c %a "$ESOCK")"
check    "in a directory nobody else can enter" 700 "$(stat -c %a "$WORK/ex")"
if command -v qemu-img >/dev/null 2>&1; then
    qemu-img convert -q -f raw -O raw "nbd+unix:///$ENAME?socket=$ESOCK" "$WORK/vda.nbd" 2>"$WORK/qi.err"
    if cmp -s "$WORK/vda.nbd" "$WORK/vda.raw"; then ok "QEMU reads the disk back byte for byte"; else bad "QEMU reads the disk back" "same bytes" "$(cat "$WORK/qi.err")"; fi
    contains "and sees its holes" '"data": false' "$(qemu-img map --output=json "nbd+unix:///$ENAME?socket=$ESOCK" 2>&1)"
    OUT=$(qemu-img info "nbd+unix:///wrong?socket=$ESOCK" 2>&1); RC=$?
    check    "a wrong export name is refused" 1 "$RC"
else
    echo "  (qemu-img not installed: reads through QEMU not checked)"
fi
OUT=$(cb vm exports)
contains "it is listed" "\"id\":$EID" "$OUT"
lacks    "without its name" "$ENAME" "$OUT"
OUT=$(cb snapshot delete 1 1 --force 2>&1); RC=$?
check    "the exported snapshot cannot be deleted, even forced" 1 "$RC"
contains "and says why" "is exported" "$OUT"
# B255: a conflict with the export, not a server fault.
check    "the refusal is a 409" 409 \
         "$(curl -s -o "$WORK/del.out" -w '%{http_code}' --unix-socket "$SOCK" -X DELETE 'http://x/repos/1/snapshots/1?force=1')"
contains "naming the export" "(export $EID)" "$(cat "$WORK/del.out")"
GJ=$(cb gc 1 --no-wait | sed -n 's/.*"job_id":\([0-9]*\).*/\1/p')
sleep 1.5
OUT=$(cb jobs "$GJ")
contains "gc waits in the queue while it is open" '"state":"queued"' "$OUT"
contains "and says what for" 'waiting for an NBD export to close' "$OUT"
# B220: the CLI waited at most an hour and then failed while the job ran on.
# It now waits until the job ends; --timeout bounds the wait, with its own
# exit status, and the job keeps its place. The open export holds this gc in
# the queue for as long as the test needs.
T0=$(date +%s)
"$CLI" --socket "$SOCK" gc 1 --timeout 1 > "$WORK/to.out" 2> "$WORK/to.err"; RC=$?
T1=$(date +%s)
check    "a wait past --timeout exits 124" 124 "$RC"
check    "after about the timeout" "yes" "$([ $((T1 - T0)) -le 5 ] && echo yes || echo "no: $((T1 - T0)) s")"
contains "saying the job keeps running" "keeps running" "$(cat "$WORK/to.err")"
TJ=$(sed -n 's/.*job \([0-9]*\) is still running.*/\1/p' "$WORK/to.err")
check    "naming the job" "yes" "$([ -n "$TJ" ] && echo yes || echo "no: $(cat "$WORK/to.err")")"
check    "nothing on stdout" "" "$(cat "$WORK/to.out")"
contains "and the job is still queued" '"state":"queued"' "$(cb jobs "$TJ")"
OUT=$(cb vm export 1 1 domain.xml 2>&1); RC=$?
check    "only disks are exported" 1 "$RC"
OUT=$(cb vm export 1 1 vda --ttl 10s 2>&1); RC=$?
check    "a ttl under a minute is refused" 1 "$RC"
# B239 and NIT-52: a revoke takes no body. An unknown member is a 400 for an
# export that is open, and leaves it open; for one that is not, the URL
# names nothing and that 404 wins.
if command -v curl >/dev/null 2>&1; then
    check "a revoke with a member is refused" \
        "{\"error\":\"unknown key 'zz_b239'; this request takes no fields\"} 400" \
        "$(curl -s -w ' %{http_code}' --unix-socket "$SOCK" -X DELETE -d '{"zz_b239":1}' "http://x/exports/$EID")"
    contains "and the export is still open" "\"id\":$EID" "$(cb vm exports)"
    check "an unknown export with a member is a 404" \
        '{"error":"no open export with that id"} 404' \
        "$(curl -s -w ' %{http_code}' --unix-socket "$SOCK" -X DELETE -d '{"zz_b239":1}' http://x/exports/999999)"
fi
OUT=$(cb vm unexport "$EID"); RC=$?
check    "revoking it succeeds" 0 "$RC"
check    "and removes the socket" "no" "$([ -e "$ESOCK" ] && echo yes || echo no)"
contains "it is recorded as revoked" '"close_reason":"revoked"' "$(cb vm export-show "$EID")"
for _ in $(seq 1 50); do
    case "$(cb jobs "$GJ")" in *'"state":"completed"'*) break ;; esac; sleep 0.1
done
contains "and the waiting gc runs once it closes" '"state":"completed"' "$(cb jobs "$GJ")"
for _ in $(seq 1 50); do
    case "$(cb jobs "$TJ")" in *'"state":"completed"'*) break ;; esac; sleep 0.1
done
contains "as does the one the CLI stopped waiting for" '"state":"completed"' "$(cb jobs "$TJ")"
contains "gc runs again" '"state":"completed"' "$(cb gc 1)"
contains "the journal has it" "nbd-export" "$(cb journal 1)"
OUT=$(cb vm export 1 1 vda --ttl 2m)
EID2=$(printf '%s' "$OUT" | jget export_id)
SHOW=$(cb vm export-show "$EID2")
check    "--ttl sets its lifetime" 120 "$(( $(printf '%s' "$SHOW" | jget expires) - $(printf '%s' "$SHOW" | jget created_at) ))"
# The idle timeout is honoured, and an export ends with the daemon.
kill "$DAEMON_PID"; wait "$DAEMON_PID" 2>/dev/null
sed -i 's/^  export_idle_s: .*/  export_idle_s: 2/' "$WORK/config.yaml"
start_daemon
contains "an export ends with the daemon" '"close_reason":"daemon stopped"' "$(cb vm export-show "$EID2")"
EID3=$(cb vm export 1 1 vda | jget export_id)
sleep 4
contains "an export nobody uses closes when idle" '"close_reason":"idle"' "$(cb vm export-show "$EID3")"
# B257: a daemon killed outright never unlinks its sockets; the next start
# removes them, and only them.
EID5=$(cb vm export 1 1 vda | jget export_id)
check    "an export's socket is there" "yes" "$([ -S "$WORK/ex/$EID5.sock" ] && echo yes || echo no)"
kill -9 "$DAEMON_PID"; wait "$DAEMON_PID" 2>/dev/null
check    "a SIGKILL leaves it behind" "yes" "$([ -S "$WORK/ex/$EID5.sock" ] && echo yes || echo no)"
: > "$WORK/ex/998.sock"
# Its API socket is left too, and start_daemon waits for that path to be a
# socket: without this it would return before the new daemon is up.
rm -f "$SOCK"
start_daemon
check    "the next start removes the stale socket" "no" "$([ -e "$WORK/ex/$EID5.sock" ] && echo yes || echo no)"
check    "and leaves what is not a socket" "yes" "$([ -f "$WORK/ex/998.sock" ] && echo yes || echo no)"
contains "the export is closed as restarted" '"close_reason":"daemon restarted"' "$(cb vm export-show "$EID5")"
contains "and the start says what it swept" "removed 1 stale export socket" "$(cat "$WORK/daemon.log")"
rm -f "$WORK/ex/998.sock"

echo "reserved names"
OUT=$(cb backup 1 /.cbackup-unit 2>&1); RC=$?
check    "a file backup of /.cbackup-unit is refused" 1 "$RC"
contains "and says why" 'reserved' "$OUT"
OUT=$(cb backup 1 /.cbackup-captures/x 2>&1); RC=$?
check    "a file backup under /.cbackup-captures is refused" 1 "$RC"
OUT=$(cb sources set 1 "/.cbackup-unit/../.cbackup-captures" 2>&1); RC=$?
check    "configuring one as a source is refused" 1 "$RC"
OUT=$(cb schedule add 1 backup 3600 --source /.cbackup-unit 2>&1); RC=$?
check    "scheduling one is refused" 1 "$RC"

echo "policy and retention"
OUT=$(cb unit policy set 1 --provider fake cbk-uat-a retention.keep_daily 3)
contains "a unit's own retention" '"retention.keep_daily":"3"' "$OUT"
contains "shows what is in force" '"effective"' "$(cb unit policy show 1 --provider fake cbk-uat-a)"
OUT=$(cb unit policy set 1 --provider fake cbk-uat-a retention.bogus 3 2>&1); RC=$?
check    "an unknown key is refused" 1 "$RC"
contains "naming it" "unknown unit policy key 'retention.bogus'" "$OUT"
# A policy row no backup would read was accepted for any unit.
OUT=$(cb unit policy set 1 --provider fake prod-db retention.keep_daily 3 2>&1); RC=$?
check    "a policy for a unit outside units.allow is refused" 1 "$RC"
contains "saying why" "units.allow" "$OUT"
OUT=$(cb unit policy set 1 --provider fake nosuch retention.keep_daily 3 2>&1); RC=$?
check    "a policy for a unit that does not exist is refused" 1 "$RC"
contains "saying why" "lists no unit nosuch" "$OUT"
OUT=$(cb unit policy set 1 --provider bogus x retention.keep_daily 3 2>&1); RC=$?
check    "a policy for an unknown provider is refused" 1 "$RC"
contains "naming what is enabled" "enabled: fake" "$OUT"
contains "gfs runs over every group" '"state":"completed"' "$(cb gfs 1)"
check    "each unit keeps its head" 2 "$(cb unit history 1 | grep -o '"unit_key"' | wc -l)"

echo "incremental chain"
if command -v curl >/dev/null 2>&1; then
    OUT=$(curl -s --unix-socket "$SOCK" http://x/repos/1/units/fake/cbk-uat-a/checkpoints)
    contains "a unit's chain is shown" '"tokens":[]' "$OUT"
    contains "with whether incrementals are on" '"enabled":false' "$OUT"
    contains "reset-incremental answers for the unit" '"unit_key":"fake:a"' \
        "$(curl -s --unix-socket "$SOCK" -X POST http://x/repos/1/units/fake/cbk-uat-a/reset-incremental)"
    # NIT-41: a unit nothing knows of, on a provider that is not enabled,
    # answered both with success, the reset journalled as done.
    check "an unknown unit's chain and reset answer 404" "404 404" "$(curl -s -o /dev/null -w '%{http_code}' --unix-socket "$SOCK" http://x/repos/1/units/fake/nosuch/checkpoints) $(curl -s -o /dev/null -w '%{http_code}' --unix-socket "$SOCK" -X POST http://x/repos/1/units/fake/nosuch/reset-incremental)"
    # B203: the full key names the same unit, not <provider>:<provider>:<id>.
    contains "the chain by the unit's full key" '"unit_key":"fake:a"' \
        "$(curl -s --unix-socket "$SOCK" http://x/repos/1/units/fake/fake:a/checkpoints)"
    contains "and the reset by it" '"unit_key":"fake:a"' \
        "$(curl -s --unix-socket "$SOCK" -X POST http://x/repos/1/units/fake/fake:a/reset-incremental)"
    BEFORE=$(cb journal 1 | grep -o 'reset-incremental' | wc -l)
    check "an unknown full key answers 404" "404" "$(curl -s -o /dev/null -w '%{http_code}' --unix-socket "$SOCK" -X POST http://x/repos/1/units/fake/fake:nosuch/reset-incremental)"
    check "and journals nothing" "$BEFORE" "$(cb journal 1 | grep -o 'reset-incremental' | wc -l)"
fi
OUT=$(cb vm checkpoints 1 cbk-uat-a 2>&1); RC=$?
check    "vm checkpoints for a VM no provider has is refused" 1 "$RC"
contains "saying why" "libvirt" "$OUT"
OUT=$(cb vm reset-incremental 1 cbk-uat-a 2>&1); RC=$?
check    "and so is its reset-incremental" 1 "$RC"
OUT=$(cb vm checkpoints 1 2>&1); RC=$?
check    "checkpoints without a unit are refused" 1 "$RC"

echo "schedules"
contains "a units schedule with --all" '"id"' "$(cb schedule add 1 units 3600 --provider fake --all --exclude 'cbk-uat-b')"
contains "a units schedule naming a unit" '"id"' "$(cb schedule add 1 units 3600 --provider fake --unit cbk-uat-a)"
contains "is stored canonically" '"provider":"fake"' "$(cb schedule list 1)"
OUT=$(cb schedule add 1 units 3600 --provider libvirt --all 2>&1); RC=$?
check    "a provider that is not enabled is refused" 1 "$RC"
contains "naming it and what is enabled" "provider 'libvirt' is not enabled (enabled: fake)" "$OUT"
OUT=$(cb unit backup 1 --provider libvirt --all 2>&1); RC=$?
check    "a unit backup with it is refused the same way" 1 "$RC"
contains "in the same words" "provider 'libvirt' is not enabled (enabled: fake)" "$OUT"
if command -v curl >/dev/null 2>&1; then
    check "both answer 400" "400 400" "$(curl -s -o /dev/null -w '%{http_code}' --unix-socket "$SOCK" -X POST http://x/repos/1/units/backup -d '{"provider":"libvirt","all":true}') $(curl -s -o /dev/null -w '%{http_code}' --unix-socket "$SOCK" -X POST http://x/repos/1/schedules -d '{"action":"units","trigger":{"type":"interval","seconds":3600},"provider":"libvirt","all":true}')"
fi
OUT=$(cb schedule add 1 units 3600 --provider fake 2>&1); RC=$?
check    "a units schedule without units or --all is refused" 1 "$RC"
OUT=$(cb schedule add 1 gc 3600 --provider fake 2>&1); RC=$?
check    "unit options on another action are refused" 1 "$RC"

echo "enumerated when it runs"
sleep 1
write_spec ',{"id":"c","name":"cbk-uat-c","state":"stopped","blobs":[{"id":"x","path":".cbackup-unit/config/x","text":"c"}]}'
# B243: a policy set by name for a unit never backed up here is stored
# under its id, and show by name read <provider>:<name>: the policy just
# set was absent and the repository's values were shown as in force.
cb unit policy set 1 --provider fake cbk-uat-c retention.keep_daily 5 > /dev/null
OUT=$(cb unit policy show 1 --provider fake cbk-uat-c)
contains "a policy set by name reads back by name" '"unit":{"retention.keep_daily":"5"}' "$OUT"
contains "under the unit's id" '"unit_key":"fake:c"' "$OUT"
contains "and is in force" '"retention.keep_daily":5' "$OUT"
check    "by id it is the same policy" "$OUT" "$(cb unit policy show 1 --provider fake c)"
OUT=$(cb unit policy show 1 --provider fake nosuch 2>&1); RC=$?
check    "an unknown unit's policy is not shown as the repository's" 1 "$RC"
contains "saying why" "lists no unit nosuch" "$OUT"
cb unit policy set 1 --provider fake cbk-uat-c retention.keep_daily "" > /dev/null
OUT=$(cb unit backup 1 --provider fake --all)
contains "a unit added since is included" '"ok":3' "$OUT"

echo "reindex"
BEFORE=$(cb unit history 1)
contains "reindex completes" '"state":"completed"' "$(cb reindex 1)"
check    "unit history is rebuilt from the repository" "$BEFORE" "$(cb unit history 1)"

contains "no holds are left open" '"holds_open":0' "$(cb unit providers)"

echo "database detection"
# docs/db-detection.md 7.4, 8.1: detection on a unit without a backup, with
# the fake provider's units.probe: a PostgreSQL data directory its process
# makes live, a MariaDB image with nothing behind it (a hint), and the
# findings API and CLI over what was found.
mkdir -p "$WORK/snap-d/pg/global" "$WORK/snap-d/pg/base/1"
printf '16\n' > "$WORK/snap-d/pg/PG_VERSION"
: > "$WORK/snap-d/pg/global/pg_control"
head -c 8192 /dev/zero > "$WORK/snap-d/pg/base/1/2619"
UNIT_D=',{"id":"d","name":"cbk-uat-d","state":"running",
  "trees":[{"id":"vol","read_root":"'"$WORK"'/snap-d","path":"'"$WORK"'/live/d","source":"'"$WORK"'/snap-d"}],
  "probe":{"containers":[
    {"name":"pg","image":"postgres:16","running":true,"aliases":["db"],
     "processes":[{"comm":"postgres","args":"postgres -c fsync=on"}],"ports":[5432],
     "open_write":[{"path":"'"$WORK"'/live/d/pg/base/1/2619","comm":"postgres"}]},
    {"name":"client","image":"mariadb:11","running":true}]}}'
write_spec ",{\"id\":\"c\",\"name\":\"cbk-uat-c\",\"state\":\"stopped\",\"blobs\":[{\"id\":\"x\",\"path\":\".cbackup-unit/config/x\",\"text\":\"c\"}]}$UNIT_D"
OUT=$(cb db detect 1 cbk-uat-d --provider fake)
contains "a detection run completes" '"state":"completed"' "$OUT"
contains "and finds the data directory" "\"path\":\"$WORK/live/d/pg\"" "$OUT"
contains "live by its process and open file" '"confidence":"live"' "$OUT"
contains "with the open file as evidence" '"open_write":[{"path":' "$OUT"
contains "and its listening port (B252)" '"ports":[{"container":"pg","port":"5432"}]' "$OUT"
contains "the client image is a hint" '"engine":"mysql","path":"","confidence":"hint"' "$OUT"
contains "said as information" 'the image of client suggests MySQL/MariaDB' "$OUT"
contains "and the journal says the probe answered" 'with units.probe' "$(cb journal 1 --limit 5)"
DL=$(cb db list 1 --unit fake:d)
contains "the findings are listed under the unit" '"scope":"unit:fake:d"' "$DL"
contains "both of them" '"total":2' "$DL"
contains "no snapshot named, none was made" '"last_snap_id":null' "$DL"
contains "a hint has no coverage" '"coverage":null' "$(cb db list 1 --unit fake:d --coverage none)"
DID=$(printf '%s' "$DL" | grep -o '"id":[0-9]*,"scope":"unit:fake:d","engine":"postgres"' | sed 's/"id":\([0-9]*\).*/\1/')
contains "db show shows one" '"engine":"postgres"' "$(cb db show 1 "$DID")"
OUT=$(cb db list 1 --unit d 2>&1); RC=$?
check    "a unit key without its provider is refused" 1 "$RC"
OUT=$(cb db list 1 --unit fake:d --source /x 2>&1); RC=$?
check    "a unit and a source together are refused" 1 "$RC"
OUT=$(cb db detect 1 prod-db --provider fake 2>&1); RC=$?
check    "a unit outside units.allow is not detected" 1 "$RC"
contains "and says why" 'units.allow' "$OUT"
OUT=$(cb db detect 1 nosuch --provider fake 2>&1); RC=$?
check    "nor one that does not exist" 1 "$RC"
check    "detect is POST only" 405 "$(curl -s -o /dev/null -w '%{http_code}' --unix-socket "$SOCK" http://x/repos/1/units/fake/cbk-uat-d/detect)"
# Read live (the fake tree is on no snapshot domain), so allowed below crash.
OUT=$(cb unit backup 1 --provider fake cbk-uat-d --require inconsistent)
contains "a backup of it carries the same findings" '"engine":"postgres"' "$OUT"
contains "with the probe's liveness" '"confidence":"live"' "$OUT"
contains "and the finding now names its snapshot" '"last_snap_id":' "$(cb db show 1 "$DID")"
lacks    "no longer null" '"last_snap_id":null' "$(cb db show 1 "$DID")"
# B254: one finding carries its history, the journal entries about it,
# newest first; the list does not.
cb db dismiss 1 "$DID" --reason "test data" > /dev/null
H=$(cb db show 1 "$DID")
contains "B254: a finding shows its history" '"history":[{"journal_id":' "$H"
contains "B254: the dismissal is in it" "finding $DID (live PostgreSQL" "$H"
contains "B254: as journalled" '"operation":"db-dismiss"' "$H"
contains "B254: and whether it is all of it" '"history_truncated":false' "$H"
FIRST_OP=$(printf '%s' "$H" | sed -n 's/.*"history":\[{[^}]*"operation":"\([a-z-]*\)".*/\1/p')
check    "B254: the newest entry first" "db-dismiss" "$FIRST_OP"
lacks    "B254: another finding's entries are not" "\"message\":\"finding $((DID + 1)) (" "$H"
lacks    "B254: the list leaves history out" '"history"' "$(cb db list 1 --unit fake:d)"

echo "Nextcloud with its database in a unit not backed up here (B249)"
# docs/db-detection.md 3 and 14.12: Nextcloud's web server makes its finding
# live; its version is config.php's; its dbhost names a unit the host lists
# that nothing in this repository was recorded from, which still places the
# database in another capture, suggested group-units.
mkdir -p "$WORK/snap-n/config"
cat > "$WORK/snap-n/config/config.php" <<'PHP'
<?php
$CONFIG = array (
  'dbtype' => 'mysql',
  'dbhost' => 'cbk-uat-ncdb:3306',
  'version' => '30.0.2.2',
);
PHP
UNIT_N=',{"id":"n","name":"cbk-uat-nc","state":"running",
  "trees":[{"id":"html","read_root":"'"$WORK"'/snap-n","path":"'"$WORK"'/live/n","source":"'"$WORK"'/snap-n"}],
  "probe":{"containers":[{"name":"cbk-uat-nc","image":"nextcloud:30","running":true,
     "processes":[{"comm":"apache2","args":"apache2 -DFOREGROUND"}],"ports":[80]}]}},
 {"id":"m","name":"cbk-uat-ncdb","state":"running",
  "blobs":[{"id":"x","path":".cbackup-unit/config/x","text":"m"}]}'
write_spec ",{\"id\":\"c\",\"name\":\"cbk-uat-c\",\"state\":\"stopped\",\"blobs\":[{\"id\":\"x\",\"path\":\".cbackup-unit/config/x\",\"text\":\"c\"}]}$UNIT_D$UNIT_N"
OUT=$(cb db detect 1 cbk-uat-nc --provider fake)
contains "a detection run of it completes" '"state":"completed"' "$OUT"
contains "and finds Nextcloud" '"engine":"nextcloud"' "$OUT"
contains "at its own version, not its dbtype" '"version":"30.0.2.2"' "$OUT"
contains "live by its web server" '"confidence":"live"' "$OUT"
contains "with the process as evidence" '"processes":[{"container":"cbk-uat-nc","comm":"apache2"}]' "$OUT"
contains "its database placed in the listed unit" '"resolved":"unit:fake:m"' "$OUT"
contains "captured apart" '"consistent":false' "$OUT"
# The fake tree is read live (no snapshot domain, no pause): uncovered, which
# outranks split; test_db_findings covers split.
contains "read live, it stays uncovered" '"coverage":"uncovered"' "$OUT"
contains "and suggested to group them" '"suggestion":"group-units"' "$OUT"

echo "an export of a disk with unaligned holes (B250)"
# Holes that start and end mid-sector, one data run a single byte long, and
# a trailing hole. The export once reported them at byte offsets with a
# minimum block of 1, and qemu-img convert, working in sectors, zeroed
# every partial sector at a hole's edge while qemu-img compare called the
# copy identical. The unit restore of the same snapshot is the reference.
UNIT_E=',{"id":"e","name":"cbk-uat-e","state":"stopped",
  "disks":[{"id":"vde","size":4194304,"runs":[
     {"off":0,"len":1000,"kind":"data","seed":11},
     {"off":5000,"len":3000,"kind":"data","seed":12},
     {"off":70001,"len":131071,"kind":"data","seed":13},
     {"off":1048577,"len":1,"kind":"data","seed":14},
     {"off":2097000,"len":700,"kind":"data","seed":15},
     {"off":4193000,"len":100,"kind":"data","seed":16}]}]}'
write_spec ",{\"id\":\"c\",\"name\":\"cbk-uat-c\",\"state\":\"stopped\",\"blobs\":[{\"id\":\"x\",\"path\":\".cbackup-unit/config/x\",\"text\":\"c\"}]}$UNIT_D$UNIT_E"
OUT=$(cb unit backup 1 --provider fake cbk-uat-e)
contains "the unit backs up" '"state":"completed"' "$OUT"
SE=$(cb unit history 1 --provider fake cbk-uat-e --limit 1 | sed -n 's/.*"snapshots":\[{"id":\([0-9]*\).*/\1/p')
OUT=$(cb unit restore 1 "$SE" "$WORK/vde.raw" --item vde)
contains "its disk restores" '"state":"completed"' "$OUT"
EX=$(cb vm export 1 "$SE" vde)
EIDE=$(printf '%s' "$EX" | jget export_id)
ESOCKE=$(printf '%s' "$EX" | jget socket)
ENAMEE=$(printf '%s' "$EX" | jget name)
if command -v qemu-img >/dev/null 2>&1; then
    URI="nbd+unix:///$ENAMEE?socket=$ESOCKE"
    qemu-img convert -q -f raw -O raw "$URI" "$WORK/vde.nbd" 2>"$WORK/qe.err"
    check "QEMU's copy-out equals the unit restore" \
          "$(sha256sum < "$WORK/vde.raw")" "$(sha256sum < "$WORK/vde.nbd" 2>/dev/null)"
    MAP=$(qemu-img map --output=json -f raw "$URI" 2>&1)
    contains "QEMU sees holes in it" '"data": false' "$MAP"
    UNAL=$(printf '%s\n' "$MAP" | grep -o '"\(start\|length\)": [0-9]*' | awk '$2 % 512 != 0' | head -3)
    check "and every extent it sees is sector-aligned" "" "$UNAL"
else
    echo "  (qemu-img not installed: copy-out through QEMU not checked)"
fi
cb vm unexport "$EIDE" >/dev/null

echo "removing a repository with an export open"
# Retention has run since: unit a's newest snapshot, not snapshot 1.
SA=$(cb unit history 1 --provider fake cbk-uat-a --limit 1 | sed -n 's/.*"snapshots":\[{"id":\([0-9]*\).*/\1/p')
EID4=$(cb vm export 1 "$SA" vda | jget export_id)
ESOCK4="$WORK/ex/$EID4.sock"
check    "it is open" "yes" "$([ -S "$ESOCK4" ] && echo yes || echo no)"
OUT=$(cb repo remove 1); RC=$?
check    "the repository is removed" 0 "$RC"
check    "and its export is revoked first, socket gone" "no" "$([ -e "$ESOCK4" ] && echo yes || echo no)"
contains "it says why" '"close_reason":"repository removed"' "$(cb vm export-show "$EID4" 2>&1)"

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
