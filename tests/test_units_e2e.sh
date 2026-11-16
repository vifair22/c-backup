#!/bin/bash
#
# Units end to end: the CLI against a real daemon with the fake provider
# enabled by its test-only key (units.fake_spec). Covers the routes, the job,
# units.allow, the reserved names on the file-backup routes, restore of a
# disk as a sparse raw image, per-unit policy, the units schedule action and
# reindex rebuilding unit history.

set -u

DAEMON="${1:-build/bins/c-backupd}"
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
EOF

C_BACKUP_CONFIG_PATH="$WORK/config.yaml" "$DAEMON" > "$WORK/daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
if [ ! -S "$SOCK" ]; then echo "daemon did not start"; cat "$WORK/daemon.log"; exit 1; fi

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
OUT=$(cb unit backup 1 --provider fake prod-db 2>&1); RC=$?
check    "a named unit outside units.allow fails" 1 "$RC"
contains "and says why" 'units.allow' "$OUT"
OUT=$(cb unit backup 1 --provider fake --all)
contains "--all completes" '"state":"completed"' "$OUT"
contains "and leaves out what units.allow does" '"filtered":1' "$OUT"
contains "with one snapshot per unit" '"ok":2' "$OUT"
OUT=$(cb unit backup 1 --provider fake cbk-uat-a --all 2>&1); RC=$?
check    "units and --all together are refused" 1 "$RC"

echo "history"
OUT=$(cb unit history 1)
contains "units with history" '"unit_key":"fake:a"' "$OUT"
contains "and their last level" '"consistency":"offline"' "$OUT"
OUT=$(cb unit history 1 --provider fake cbk-uat-a)
contains "a unit's snapshots" '"read_mode":"full"' "$OUT"
check    "two of them" 2 "$(printf '%s' "$OUT" | grep -o '"name":"cbk-uat-a"' | wc -l)"

echo "restore"
OUT=$(cb unit restore 1 1 "$WORK/out")
contains "files restore completes" '"state":"completed"' "$OUT"
check    "the disk is its full virtual size" 8388608 "$(stat -c %s "$WORK/out/.cbackup-unit/disks/vda")"
BLOCKS=$(stat -c %b "$WORK/out/.cbackup-unit/disks/vda")
if [ "$BLOCKS" -lt 8192 ]; then ok "and sparse ($BLOCKS blocks)"; else bad "and sparse" "< 8192 blocks" "$BLOCKS"; fi
check    "the tree lands at its recorded path" "alpha" "$(cat "$WORK/out$WORK/live/a/a.txt")"
check    "configuration is verbatim" "<domain>a</domain>" "$(cat "$WORK/out/.cbackup-unit/config/domain.xml")"
contains "unit.json is there" '"format":"cbackup-unit"' "$(cat "$WORK/out/.cbackup-unit/unit.json")"
OUT=$(cb unit restore 1 1 "$WORK/vda.raw" --item vda)
contains "an item restore completes" '"state":"completed"' "$OUT"
if cmp -s "$WORK/vda.raw" "$WORK/out/.cbackup-unit/disks/vda"; then ok "the raw image matches"; else bad "the raw image matches" "same bytes" "differs"; fi
check    "no partial file is left" "no" "$([ -e "$WORK/vda.raw.cbk-partial" ] && echo yes || echo no)"
OUT=$(cb unit restore 1 1 "$WORK/vda.raw" --item vda 2>&1); RC=$?
check    "an item never lands on an existing file" 1 "$RC"

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
contains "gfs runs over every group" '"state":"completed"' "$(cb gfs 1)"
check    "each unit keeps its head" 2 "$(cb unit history 1 | grep -o '"unit_key"' | wc -l)"

echo "schedules"
contains "a units schedule with --all" '"id"' "$(cb schedule add 1 units 3600 --provider fake --all --exclude 'cbk-uat-b')"
contains "a units schedule naming a unit" '"id"' "$(cb schedule add 1 units 3600 --provider fake --unit cbk-uat-a)"
contains "is stored canonically" '"provider":"fake"' "$(cb schedule list 1)"
OUT=$(cb schedule add 1 units 3600 --provider libvirt --all 2>&1); RC=$?
check    "a provider that is not enabled is refused" 1 "$RC"
OUT=$(cb schedule add 1 units 3600 --provider fake 2>&1); RC=$?
check    "a units schedule without units or --all is refused" 1 "$RC"
OUT=$(cb schedule add 1 gc 3600 --provider fake 2>&1); RC=$?
check    "unit options on another action are refused" 1 "$RC"

echo "enumerated when it runs"
sleep 1
write_spec ',{"id":"c","name":"cbk-uat-c","state":"stopped","blobs":[{"id":"x","path":".cbackup-unit/config/x","text":"c"}]}'
OUT=$(cb unit backup 1 --provider fake --all)
contains "a unit added since is included" '"ok":3' "$OUT"

echo "reindex"
BEFORE=$(cb unit history 1)
contains "reindex completes" '"state":"completed"' "$(cb reindex 1)"
check    "unit history is rebuilt from the repository" "$BEFORE" "$(cb unit history 1)"

contains "no holds are left open" '"holds_open":0' "$(cb unit providers)"

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
