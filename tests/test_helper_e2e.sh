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
# A live PostgreSQL data directory for the database templates (D2): the
# signature (PG_VERSION, global/pg_control, base/) and postmaster.pid. And
# the engine's tools as the template scripts call them, on the helper's
# PATH: the fake provider has no container, so a template runs locally.
mkdir -p "$WORK/pgsnap/global" "$WORK/pgsnap/base/1" "$WORK/pool/app/d" "$WORK/bin"
echo 16 > "$WORK/pgsnap/PG_VERSION"
head -c 8192 /dev/zero > "$WORK/pgsnap/global/pg_control"
head -c 8192 /dev/zero > "$WORK/pgsnap/base/1/1259"
printf '42\n%s\n' "$WORK/pgsnap" > "$WORK/pgsnap/postmaster.pid"
printf '#!/bin/sh\necho "-- fake dump as $5 ($*)"\necho "dumped" >&2\n' > "$WORK/bin/pg_dumpall"
printf '#!/bin/sh\necho "PostgreSQL 16.4 (fake) $*"\n' > "$WORK/bin/psql"
chmod 0755 "$WORK/bin/pg_dumpall" "$WORK/bin/psql"
# B245's pgsplit: a live cluster whose pg_wal links to /wal, a bind the
# unit does not back up; the probe's mounts say so.
mkdir -p "$WORK/pgsplit/global" "$WORK/pgsplit/base/1" "$WORK/pool/app/s"
echo 16 > "$WORK/pgsplit/PG_VERSION"
head -c 8192 /dev/zero > "$WORK/pgsplit/global/pg_control"
printf '42\n/var/lib/postgresql/data\n' > "$WORK/pgsplit/postmaster.pid"
ln -s /wal "$WORK/pgsplit/pg_wal"
# A live Prometheus TSDB (wal/, chunks_head/, queries.active, lock) for an
# exec template, prometheus-snapshot, whose admin API call is a stand-in
# wget; its post run removes /prometheus/snapshots/*, absent here.
mkdir -p "$WORK/promsnap/wal" "$WORK/promsnap/chunks_head" "$WORK/pool/app/e"
head -c 4096 /dev/zero > "$WORK/promsnap/wal/00000000"
: > "$WORK/promsnap/queries.active"
: > "$WORK/promsnap/lock"
printf '#!/bin/sh\necho "{\\"status\\":\\"success\\"}"\n' > "$WORK/bin/wget"
chmod 0755 "$WORK/bin/wget"
# The tree root as read: a mode and a time no staging directory has (B168).
chmod 0751 "$WORK/snap"
touch -d @1500000000 "$WORK/snap"
cat > "$WORK/units.json" <<EOF
{"units":[
 {"id":"a","name":"cbk-uat-a","state":"running","caps":["quiesce","snapshot"],
  "blobs":[{"id":"domain.xml","path":".cbackup-unit/config/domain.xml","text":"<domain>a</domain>"}],
  "disks":[{"id":"vda","size":4194304,"runs":[{"off":0,"len":1048576,"kind":"data","seed":3}]}],
  "trees":[{"id":"vol","read_root":"$WORK/snap","path":"$WORK/pool/app/a","source":"$WORK/pool/app/a"}],
  "probe":{"containers":[{"name":"pg","image":"postgres:16","running":true,"aliases":["pg","db"],
           "processes":[{"comm":"postgres","args":"postgres -D /data"}],"ports":[5432],
           "open_write":[{"path":"$WORK/pool/app/a/base/1/2619","comm":"postgres"}]}]}},
 {"id":"k","name":"cbk-uat-k","state":"running","caps":["quiesce"],
  "disks":[{"id":"vda","size":8589934592,"runs":[{"off":0,"len":8589934592,"kind":"data","seed":9}]}]},
 {"id":"c","name":"cbk-uat-c","state":"running","caps":["quiesce"],
  "blobs":[{"id":"x","path":".cbackup-unit/config/x","text":"c"}],"probe_fail":true},
 {"id":"d","name":"cbk-uat-d","state":"running","caps":["quiesce","snapshot"],
  "trees":[{"id":"pg","read_root":"$WORK/pgsnap","path":"$WORK/pool/app/d","source":"$WORK/pool/app/d"}]},
 {"id":"s","name":"cbk-uat-s","state":"running","caps":["quiesce","snapshot"],
  "trees":[{"id":"pg","read_root":"$WORK/pgsplit","path":"$WORK/pool/app/s","source":"$WORK/pool/app/s"}],
  "probe":{"containers":[{"name":"pgsplit","image":"postgres:16","running":true,
           "processes":[{"comm":"postgres","args":"postgres"}],
           "mounts":[{"dest":"/var/lib/postgresql/data","recorded":"$WORK/pool/app/s"},
                     {"dest":"/wal","recorded":null}]}]}},
 {"id":"e","name":"cbk-uat-e","state":"running","caps":["quiesce","snapshot"],
  "trees":[{"id":"prom","read_root":"$WORK/promsnap","path":"$WORK/pool/app/e","source":"$WORK/pool/app/e"}]},
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

[db]
allow_templates = pg-dumpall, prometheus-snapshot
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

# B186: in a user and mount namespace of its own, the old helper layout (a
# bind under an rshared bind, as Docker makes it) doubles the stacked mounts
# of the state dir at every start; the helper refuses to start on that;
# the documented layout (rslave, no nested bind) leaves the table alone.
# Skipped where unprivileged namespaces are not allowed.
echo "B186 mount layout"
if unshare -Urm true 2>/dev/null; then
    B=$WORK/b186
    mkdir -p "$B"
    sed -e "s|^state_dir = .*|state_dir = $B/h/pool/state|" "$WORK/policy.ini" > "$B/policy.ini"
    chmod 0600 "$B/policy.ini"
    cat > "$B/sim.sh" <<'SIM'
#!/bin/bash
# Docker's steps for one container start, in a namespace that dies after:
# rootfs propagation, the pool bound at the container's path, then (old
# layout) the state dir bound under it.
B=$1 HELPER=$2
mount -t tmpfs h "$B/h" && mount --make-shared "$B/h"
mount -t tmpfs r "$B/r" && mount --make-shared "$B/r"
mkdir -p "$B/h/pool/state"
at() { awk -v p="$1" '$5==p' /proc/self/mountinfo | wc -l; }
start() {   # <layout> <n>
    mkdir -p "$B/r/c$2/pool"
    if [ "$1" = old ]; then
        unshare -m --propagation unchanged sh -c "mount --make-rshared / &&
            mount --rbind $B/h/pool $B/r/c$2/pool && mount --make-rshared $B/r/c$2/pool &&
            mount --bind $B/h/pool/state $B/r/c$2/pool/state"
    else
        unshare -m --propagation unchanged sh -c "mount --make-rslave / &&
            mount --rbind $B/h/pool $B/r/c$2/pool && mount --make-rslave $B/r/c$2/pool"
    fi
}
t0=$(wc -l </proc/self/mountinfo)
for i in 1 2 3 4 5; do start new "n$i"; done
echo "documented: state=$(at "$B/h/pool/state") table=$(( $(wc -l </proc/self/mountinfo) - t0 ))"
"$HELPER" --policy "$B/policy.ini" --check >/dev/null 2>&1; echo "documented-check: $?"
for i in 1 2 3; do start old "o$i"; done
echo "old: state=$(at "$B/h/pool/state")"
"$HELPER" --policy "$B/policy.ini" --check 2>&1 | tail -1
echo "old-after-refusal: state=$(at "$B/h/pool/state")"
SIM
    mkdir -p "$B/h" "$B/r"
    OUT=$(unshare -Urm --propagation unchanged bash "$B/sim.sh" "$B" "$HELPER" 2>&1)
    contains "B186: five starts of the documented layout stack nothing" "documented: state=0 table=0" "$OUT"
    contains "B186: and the helper accepts it" "documented-check: 0" "$OUT"
    contains "B186: three starts of the old layout leave 2^3-1 copies" "old: state=7" "$OUT"
    contains "B186: the helper refuses to start on them" "$B/h/pool/state is mounted 7 times" "$OUT"
    contains "B186: and adds none" "old-after-refusal: state=7" "$OUT"
else
    echo "  skip unprivileged user namespaces are not available here"
fi

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
PATH="$WORK/bin:$PATH" "$HELPER" --policy "$WORK/policy.ini" > "$WORK/helper.log" 2>&1 &
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
contains "a policy with only dir domains snapshots no path" '"path_snapshots":false' "$OUT"
lacks    "the helper's own paths backend is no provider" 'paths' "$(printf '%s' "$OUT" | grep -o '"providers":"[^"]*"')"
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

echo "units.probe over the socket"
# The daemon side of units.probe is not built yet, so the request goes
# straight to the helper's socket: hello, then the probe, one packet each.
# Perl's Socket is in perl-base, which every Debian image has.
hcall() {
    "${AS_DAEMON[@]}" perl -MSocket -e '
        socket(my $s, AF_UNIX, SOCK_SEQPACKET, 0) or die "socket: $!";
        connect($s, pack_sockaddr_un($ARGV[0])) or die "connect: $!";
        my $r;
        send($s, q({"v":1,"id":1,"op":"hello","args":{"client_version":1}}), 0);
        recv($s, $r, 65536, 0);
        send($s, $ARGV[1], 0);
        recv($s, $r, 65536, 0);
        print $r;' "$HSOCK" "$1"
}
OUT=$(hcall '{"v":1,"id":2,"op":"units.probe","args":{"unit":"fake:a"}}')
contains "the fake provider's spec probe comes back" '"ok":true' "$OUT"
contains "with its processes" '"comm":"postgres"' "$OUT"
contains "its listening ports" '"ports":[5432]' "$OUT"
contains "its files open for writing, as recorded" "\"path\":\"$WORK/pool/app/a/base/1/2619\"" "$OUT"
contains "and whether it was cut" '"truncated":false' "$OUT"
OUT=$(hcall '{"v":1,"id":3,"op":"units.probe","args":{"unit":"fake:c"}}')
contains "a probe that fails says so" '"code":"failed"' "$OUT"
OUT=$(hcall '{"v":1,"id":4,"op":"units.probe","args":{"unit":"fake:p"}}')
contains "a unit outside the policy is denied" '"code":"denied"' "$OUT"
OUT=$(hcall '{"v":1,"id":5,"op":"units.probe","args":{"unit":"fake:a","pid":1}}')
contains "an argument it does not take is invalid" '"code":"invalid"' "$OUT"

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

echo "database templates"
# D2 (db-detection.md 7.2 to 7.4): a unit's finding previewed through the
# helper's db.probe, enabled, and run by the next backup as a dump hook,
# stored at .cbackup-unit/hooks/<template>.dump; a template the helper's
# policy does not allow is refused with the line to add.
OUT=$(cb unit backup 1 --provider fake cbk-uat-d)
contains "a unit with a live PostgreSQL backs up" '"state":"completed"' "$OUT"
contains "and the finding is reported" '"engine":"postgres"' "$OUT"
contains "captured from a snapshot" '"coverage":"covered-quiesced"' "$OUT"
contains "the unit at the level its capture reached" '"consistency":"quiesced"' "$OUT"
FID=$(printf '%s' "$OUT" | sed -n 's/.*"findings":\[{"id":\([0-9]*\).*/\1/p')
check "with its id" yes "$([ -n "$FID" ] && echo yes || echo no)"
OUT=$(cb db preview 1 "$FID")
contains "preview names the template" '"template":"pg-dumpall"' "$OUT"
contains "the exact argv the helper builds" 'exec pg_dumpall -w --clean --if-exists -U' "$OUT"
contains "where its output goes" '"output":".cbackup-unit/hooks/pg-dumpall.dump"' "$OUT"
contains "the probe ran in the helper" 'PostgreSQL 16.4 (fake)' "$OUT"
contains "and verified it" '"verified":true' "$OUT"
contains "the policy allows it" '"allowed":true' "$OUT"
# B248: a probe refused for want of a password says how to give one.
mv "$WORK/bin/psql" "$WORK/bin/psql.ok"
printf '#!/bin/sh\necho "psql: error: connection to server failed: fe_sendauth: no password supplied" >&2\nexit 2\n' > "$WORK/bin/psql"
chmod 0755 "$WORK/bin/psql"
OUT=$(cb db preview 1 "$FID")
contains "B248: a probe without its password is not verified" '"verified":false' "$OUT"
contains "B248: and the preview says how to give the credential" '"credentials":"PostgreSQL wants a password' "$OUT"
mv "$WORK/bin/psql.ok" "$WORK/bin/psql"
OUT=$(cb db preview 1 "$FID" --template pg-dump --param user=postgres --param database=app)
contains "another template previews, though the policy would refuse it" '"allowed":false' "$OUT"
contains "with the line to add" '"policy_line":"[db] allow_templates = pg-dumpall, prometheus-snapshot, pg-dump"' "$OUT"
contains "and the argv from its params" '"argv":["pg_dump","-w","-Fc","-Z0","-U","postgres","app"]' "$OUT"
OUT=$(cb db enable 1 "$FID" --template pg-dump --param user=postgres --param database=app 2>&1); RC=$?
check    "enabling what the policy refuses fails" 1 "$RC"
contains "and returns the policy line instead" '[db] allow_templates = pg-dumpall, prometheus-snapshot, pg-dump' "$OUT"
OUT=$(cb db enable 1 "$FID" --template pg-dump --param user=Robert --param database=app 2>&1); RC=$?
check    "a parameter outside its grammar is refused" 1 "$RC"
contains "naming the grammar" '[a-z_][a-z0-9_]{0,62}' "$OUT"
OUT=$(cb db enable 1 "$FID" --template redis-rdb 2>&1); RC=$?
check    "a template for another engine is refused" 1 "$RC"
OUT=$(cb db enable 1 "$FID" --template pg-dumpall)
contains "enable records the template" '"state":"enabled"' "$OUT"
contains "and who chose it" '"decided_by":"local socket"' "$OUT"
OUT=$(cb unit backup 1 --provider fake cbk-uat-d)
contains "the next backup runs it" '"state":"completed"' "$OUT"
contains "and the finding is covered by it" '"coverage":"covered-application"' "$OUT"
contains "and the unit with it: its dump is the application taking part (B246)" \
    '"consistency":"application"' "$OUT"
SNAP=$(printf '%s' "$OUT" | sed -n 's/.*"snapshot_id":\([0-9]*\).*/\1/p')
OUT=$(cb unit restore 1 "$SNAP" "$WORK/outd")
contains "the unit snapshot restores" '"state":"completed"' "$OUT"
contains "the dump is in it" '-- fake dump as' "$(cat "$WORK/outd/.cbackup-unit/hooks/pg-dumpall.dump" 2>/dev/null)"
contains "as the template's own user" 'postgres' "$(cat "$WORK/outd/.cbackup-unit/hooks/pg-dumpall.dump" 2>/dev/null)"
contains "with its log" 'exit status 0' "$(cat "$WORK/outd/.cbackup-unit/hooks/pg-dumpall.log" 2>/dev/null)"
contains "unit.json says how the database was captured" '"template":{"name":"pg-dumpall","ok":true,"dump":".cbackup-unit/hooks/pg-dumpall.dump"}' \
    "$(cat "$WORK/outd/.cbackup-unit/unit.json" 2>/dev/null)"
contains "the choice is journalled" 'pg-dumpall enabled with params {} by local socket' "$(cb journal 1)"
# A dump that fails is left out, with a warning, and the finding keeps the
# coverage the capture itself achieved.
printf '#!/bin/sh\necho "half a dump"\necho "connection refused" >&2\nexit 2\n' > "$WORK/bin/pg_dumpall"
OUT=$(cb unit backup 1 --provider fake cbk-uat-d)
contains "a failed dump does not fail the unit" '"state":"completed"' "$OUT"
contains "it is named as a failed dump" 'dump(s) failed' "$OUT"
contains "and the coverage is the capture's" '"coverage":"covered-quiesced"' "$OUT"
contains "and so is the unit's level" '"consistency":"quiesced"' "$OUT"
# B248: a dump waiting on something that never comes (pg_basebackup on a
# password prompt) held the job until the dump's six-hour timeout, and a
# cancel did nothing. Now a cancel stops the dump's whole process group,
# SIGTERM then SIGKILL, and the job ends within seconds. This dump's child
# ignores SIGTERM, so the SIGKILL after the grace is what ends it.
cat > "$WORK/bin/pg_dumpall" <<SH
#!/bin/sh
echo \$\$ > "$WORK/dump.pids"
(trap '' TERM; exec sleep 300) &
echo \$! >> "$WORK/dump.pids"
wait
SH
chmod 0755 "$WORK/bin/pg_dumpall"
rm -f "$WORK/dump.pids"
JID=$(cb unit backup 1 --provider fake cbk-uat-d --no-wait | sed -n 's/.*"job_id":\([0-9]*\).*/\1/p')
for _ in $(seq 1 100); do [ "$(wc -l < "$WORK/dump.pids" 2>/dev/null)" = 2 ] && break; sleep 0.1; done
check    "B248: the hanging dump is running" 2 "$(wc -l < "$WORK/dump.pids" 2>/dev/null)"
T0=$(date +%s)
cb jobs cancel "$JID" > /dev/null
for _ in $(seq 1 300); do
    cb jobs "$JID" | grep -Eq '"state":"(cancelled|failed|completed)"' && break
    sleep 0.1
done
T1=$(date +%s)
contains "B248: the cancel ends the job" '"state":"cancelled"' "$(cb jobs "$JID")"
if [ $((T1 - T0)) -le 10 ]; then ok "B248: within seconds ($((T1 - T0)) s)"; else bad "B248: within seconds" "<= 10 s" "$((T1 - T0)) s"; fi
LEFT=""
for _ in $(seq 1 30); do
    LEFT=""
    for p in $(cat "$WORK/dump.pids"); do kill -0 "$p" 2>/dev/null && LEFT="$LEFT $p"; done
    [ -z "$LEFT" ] && break
    sleep 0.1
done
check    "B248: and no process of the dump is left" "" "$LEFT"
for p in $LEFT; do kill -9 "$p" 2>/dev/null; done
OUT=$(cb db dismiss 1 "$FID" --reason "test data")
contains "dismiss stops it" '"state":"dismissed"' "$OUT"
OUT=$(cb unit backup 1 --provider fake cbk-uat-d)
lacks    "a dismissed finding runs nothing" 'dump(s) failed' "$OUT"
# B245: a pg_wal on a bind the unit does not back up is split, not
# covered-crash, and the dump is suggested.
OUT=$(cb unit backup 1 --provider fake cbk-uat-s)
contains "a unit with a relocated pg_wal backs up" '"engine":"postgres"' "$OUT"
contains "and is split" '"coverage":"split"' "$OUT"
contains "the link judged, not left unchecked" '"found":"outside the capture"' "$OUT"
lacks    "never unchecked" '"found":"unchecked"' "$OUT"
contains "naming pg_wal" '"kind":"pg_wal","target":"/wal"' "$OUT"
contains "with the dump suggested" '"suggestion":"pg-dumpall"' "$OUT"
# An exec template (B246): prometheus-snapshot lifts the covered-quiesced
# finding, and the unit's level with it.
OUT=$(cb unit backup 1 --provider fake cbk-uat-e)
contains "a unit with a live Prometheus backs up" '"engine":"prometheus"' "$OUT"
contains "at its capture's level" '"consistency":"quiesced"' "$OUT"
PFID=$(printf '%s' "$OUT" | sed -n 's/.*"findings":\[{"id":\([0-9]*\).*/\1/p')
OUT=$(cb db enable 1 "$PFID" --template prometheus-snapshot)
contains "an exec template enables" '"state":"enabled"' "$OUT"
OUT=$(cb unit backup 1 --provider fake cbk-uat-e)
contains "the next backup runs it" '"state":"completed"' "$OUT"
contains "and it covers the finding" '"coverage":"covered-application"' "$OUT"
contains "and raises the unit to application" '"consistency":"application"' "$OUT"
printf '#!/bin/sh\necho "connection refused" >&2\nexit 4\n' > "$WORK/bin/wget"
OUT=$(cb unit backup 1 --provider fake cbk-uat-e)
contains "an exec template that fails leaves the finding as captured" '"coverage":"covered-quiesced"' "$OUT"
contains "and the unit too" '"consistency":"quiesced"' "$OUT"
cb db dismiss 1 "$PFID" --reason "test data" > /dev/null

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

# capture.begin_paths' backend in the real binary (db-detection.md 9.4):
# a helper whose policy lists a ZFS dataset, over the fake zfs, sweeps at
# its start a capture snapshot of ours that a helper which died left
# behind, with no unit provider configured, and leaves a foreign one.
echo "paths: the start-up sweep over a ZFS domain"
Z=$WORK/zfs
mkdir -p "$Z/state" "$Z/ds" "$Z/hstate"
printf 'cache/appdata\t%s\n' "$Z/ds" > "$Z/state/datasets"
FAKE_HOST_STATE="$Z/state" tests/fake_host/zfs snapshot -o com.cbackup:owner=0badf00d cache/appdata@cbk-0badf00d-77-0
FAKE_HOST_STATE="$Z/state" tests/fake_host/zfs snapshot -o com.cbackup:owner=feedface cache/appdata@cbk-feedface-5-0
cat > "$Z/policy.ini" <<EOF
[helper]
daemon_uid = $DUID
daemon_gid = $DGID
daemon_id = 0badf00d
state_dir = $Z/hstate
socket = $Z/h.sock

[snapshot]
zfs_domains = cache/appdata

[host]
nsenter = 0
host_root =
zfs = $PWD/tests/fake_host/zfs
mountinfo = /proc/self/mountinfo
EOF
chmod 0600 "$Z/policy.ini"
[ "$(id -u)" = 0 ] && chown 0 "$Z/policy.ini"
FAKE_HOST_STATE="$Z/state" "$HELPER" --policy "$Z/policy.ini" > "$Z/helper.log" 2>&1 &
ZPID=$!
for _ in $(seq 1 50); do [ -S "$Z/h.sock" ] && break; sleep 0.1; done
for _ in $(seq 1 50); do grep -q "cbk-0badf00d-77-0" "$Z/state/snaps" || break; sleep 0.1; done
lacks    "paths: our leftover capture snapshot is swept at start" "cbk-0badf00d-77-0" "$(cat "$Z/state/snaps")"
contains "paths: another daemon's is left alone" "cbk-feedface-5-0" "$(cat "$Z/state/snaps")"
kill "$ZPID"; wait "$ZPID" 2>/dev/null

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
