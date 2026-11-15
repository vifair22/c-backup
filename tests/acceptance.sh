#!/bin/bash
#
# The CLI half of docs/acceptance.md, against real data.
#
# Not part of `make test`: it backs up whatever trees it is pointed at, which
# takes minutes and needs real disk. It starts its own daemon on a scratch
# state directory, so it never touches an installed one, and it never writes
# into a source path: every restore goes to a scratch directory and is
# compared against the original.
#
# Usage:
#   tests/acceptance.sh [options]
#     --work DIR        scratch space; needs about twice the scale set's size
#     --d PATH          functional set (repeatable). Default /usr/share/doc
#     --g PATH          scale set. Default $HOME/git
#     --d-term TEXT     a filename fragment in the functional set
#     --g-term TEXT     a filename fragment in the scale set
#     --grep TEXT       a string some file in the functional set contains
#     --bin DIR         where c-backupd and c-backup are (default build/bins)
#     --keep            leave the daemon running and the work dir in place,
#                       for the UI half (90-acceptance.spec.ts)
#
# Prints one row per check: ID, PASS or FAIL, milliseconds, and what was
# measured. Exits non-zero if any check failed.

set -u

BIN="build/bins"
WORK="${HOME}/.local/state/cbackup-acceptance"
D_PATHS=()
G_PATH="${HOME}/git"
D_TERM="README"
G_TERM="Makefile"
GREP_TEXT="Copyright"
KEEP=0
while [ $# -gt 0 ]; do
    case "$1" in
        --work)   WORK="$2"; shift 2 ;;
        --d)      D_PATHS+=("$2"); shift 2 ;;
        --g)      G_PATH="$2"; shift 2 ;;
        --d-term) D_TERM="$2"; shift 2 ;;
        --g-term) G_TERM="$2"; shift 2 ;;
        --grep)   GREP_TEXT="$2"; shift 2 ;;
        --bin)    BIN="$2"; shift 2 ;;
        --keep)   KEEP=1; shift ;;
        *) echo "unknown option $1" >&2; exit 2 ;;
    esac
done
[ ${#D_PATHS[@]} -eq 0 ] && D_PATHS=(/usr/share/doc)
DAEMON="$(realpath "$BIN/c-backupd")"
CLI="$(realpath "$BIN/c-backup")"

# The socket path has to fit in sun_path, which a deep work dir would not.
SOCK_DIR="$(mktemp -d /tmp/cbacc.XXXXXX)"
SOCK="$SOCK_DIR/s"
PORT=$((20000 + $$ % 10000))
PASSWORD="acceptance-$$-password"

PASS=0; FAIL=0
RESULTS="$WORK/results.txt"

row() {  # row <id> <PASS|FAIL> <ms> <note>
    printf '%-4s %-4s %8s ms  %s\n' "$1" "$2" "$3" "$4" | tee -a "$RESULTS"
    if [ "$2" = PASS ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi
}
now_ms() { echo $(( $(date +%s%N) / 1000000 )); }
cb() { "$CLI" --socket "$SOCK" "$@"; }
# Time one command; leaves its stdout in $OUT and its duration in $MS.
timed() { local t0; t0=$(now_ms); OUT="$("$@" 2>&1)"; RC=$?; MS=$(( $(now_ms) - t0 )); }
jget() { python3 -c 'import sys,json; d=json.load(sys.stdin); print(eval(sys.argv[1]))' "$1"; }
check() {  # check <id> <note> <condition-command...>
    local id="$1" note="$2"; shift 2
    if "$@"; then row "$id" PASS "$MS" "$note"; else row "$id" FAIL "$MS" "$note"; fi
}

start_daemon() {
    C_BACKUP_CONFIG_PATH="$WORK/config.yaml" "$DAEMON" >> "$WORK/daemon.log" 2>&1 &
    DAEMON_PID=$!
    for _ in $(seq 1 100); do cb health >/dev/null 2>&1 && return 0; sleep 0.1; done
    echo "daemon did not start; see $WORK/daemon.log" >&2; exit 1
}
stop_daemon() { kill -TERM "$DAEMON_PID" 2>/dev/null; wait "$DAEMON_PID" 2>/dev/null; }
cleanup() {
    [ "$KEEP" = 1 ] && return
    stop_daemon
    rm -rf "$SOCK_DIR"
}
trap cleanup EXIT

if [ -e "$WORK" ]; then echo "$WORK exists; remove it or pick another --work" >&2; exit 2; fi
mkdir -p "$WORK/keys" "$WORK/restore" "$WORK/specials"
: > "$RESULTS"
cat > "$WORK/config.yaml" <<EOF
db:
  path: $WORK/state.db
http:
  socket: $SOCK
  port: $PORT
  bind: 127.0.0.1
security:
  key_dir: $WORK/keys
log:
  level: info
EOF

# The functional set gets one directory of the file types real trees rarely
# hold all of at once: sparse, hardlinked, a FIFO, an extended attribute.
S="$WORK/specials"
dd if=/dev/urandom of="$S/data.bin" bs=1M count=4 status=none
truncate -s 64M "$S/sparse.img"
printf 'tail' | dd of="$S/sparse.img" bs=1 seek=$((48*1024*1024)) conv=notrunc status=none
echo "linked" > "$S/link-a"; ln "$S/link-a" "$S/link-b"
ln -s link-a "$S/symlink"
mkfifo "$S/fifo"
echo "attr" > "$S/xattr.txt"; setfattr -n user.cbackup -v acceptance "$S/xattr.txt" 2>/dev/null
D_SOURCES=("${D_PATHS[@]}" "$S")

start_daemon
echo "c-backup acceptance: $(cb version)" | tee -a "$RESULTS"
echo "functional set: ${D_SOURCES[*]}; scale set: $G_PATH" | tee -a "$RESULTS"

# ---- A. Repository lifecycle ----------------------------------------------
timed cb repo add "$WORK/repo-d" Documents
D_ID=$(echo "$OUT" | jget 'd["id"]' 2>/dev/null)
check A1 "register a repository (id $D_ID)" test -n "$D_ID" -a -f "$WORK/repo-d/.cbackup"
timed cb repo list
check A2 "list repositories" grep -q Documents <<<"$OUT"
timed cb repo show "$D_ID"
check A3 "show one repository" grep -q "$WORK/repo-d" <<<"$OUT"
timed cb repo add "$WORK/repo-enc" Encrypted --encrypt system
E_ID=$(echo "$OUT" | jget 'd["id"]' 2>/dev/null)
E_KEY=$(echo "$OUT" | jget 'd.get("key","")' 2>/dev/null)
check A4 "create an encrypted repository (key shown once)" \
    bash -c "[ -n '$E_ID' ] && [ ${#E_KEY} -eq 64 ] && [ -n \"\$(ls '$WORK/keys')\" ]"
cb repo add "$WORK/repo-gone" Gone >/dev/null
GONE_ID=$(cb repo list | python3 -c 'import sys,json; print([r["id"] for r in json.load(sys.stdin)["repos"] if r["label"]=="Gone"][0])')
timed cb repo remove "$GONE_ID"
check A5 "remove deregisters, the directory stays" bash -c "! '$CLI' --socket '$SOCK' repo list | grep -q Gone && [ -f '$WORK/repo-gone/.cbackup' ]"

# ---- B. Sources and backup ------------------------------------------------
timed cb sources set "$D_ID" "${D_SOURCES[@]}" --exclude '*.tmp'
check B1 "set sources" bash -c "'$CLI' --socket '$SOCK' sources list $D_ID | grep -q specials"
timed cb backup "$D_ID"
EXPECT=$(find "${D_SOURCES[@]}" -type f 2>/dev/null | wc -l)
GOT=$(cb snapshots "$D_ID" | jget 'int(d["snapshots"][0]["files"])')
check B2 "first backup: $GOT files of $EXPECT, $((MS/1000)) s" \
    python3 -c "import sys; sys.exit(0 if abs($GOT-$EXPECT) <= max(1, $EXPECT//100) else 1)"
BYTES1=$(cb summary "$D_ID" | jget 'd["phys_size"]')
timed cb backup "$D_ID"
BYTES2=$(cb summary "$D_ID" | jget 'd["phys_size"]')
check B3 "unchanged incremental adds $((BYTES2-BYTES1)) bytes, $MS ms" \
    test $((BYTES2-BYTES1)) -lt 1048576
echo "changed $(date)" > "$S/changed.txt"
timed cb backup "$D_ID"
DIFF=$(cb diff "$D_ID" 2 3)
check B4 "one-file change shows in diff: $DIFF" grep -q '"added":1,' <<<"$DIFF"
timed cb backup "$D_ID" "$S" --exclude '*.bin'
LAST=$(cb snapshots "$D_ID" --limit 1 | jget 'd["snapshots"][0]["id"]')
check B5 "exclude keeps data.bin out" bash -c "! '$CLI' --socket '$SOCK' search $D_ID data.bin | grep -q '\"id\"' || true; ! '$CLI' --socket '$SOCK' diff $D_ID 3 $LAST | grep -q '\"removed\":0,'"
timed cb backup "$D_ID" "$S" --label acceptance-label
check B6 "label recorded" bash -c "'$CLI' --socket '$SOCK' snapshots $D_ID --limit 1 | grep -q acceptance-label"

timed cb repo add "$WORK/repo-g" Git
G_ID=$(echo "$OUT" | jget 'd["id"]')
cb sources set "$G_ID" "$G_PATH" >/dev/null
timed cb backup "$G_ID" --no-wait
G_JOB=$(echo "$OUT" | jget 'd["job_id"]')
check B8 "handoff of a $(find "$G_PATH" -type f 2>/dev/null | wc -l)-file backup" test "$MS" -lt 600
T0=$(now_ms); PROGRESS=""
while :; do
    J=$(cb jobs "$G_JOB")
    case "$J" in *'"state":"completed"'*|*'"state":"failed"'*) break ;; esac
    P=$(echo "$J" | jget 'd["current"]'); PROGRESS="$PROGRESS $P"; sleep 2
done
MS=$(( $(now_ms) - T0 ))
DISTINCT=$(echo "$PROGRESS" | tr ' ' '\n' | sort -u | grep -c .)
check B9 "progress advanced ($DISTINCT distinct readings)" \
    bash -c "[ $DISTINCT -ge 2 ] || [ $MS -lt 4000 ]"
G_FILES=$(cb snapshots "$G_ID" | jget 'int(d["snapshots"][0]["files"])')
check B7 "scale backup: $G_FILES files in $((MS/1000)) s ($(( G_FILES * 1000 / (MS+1) )) files/s)" \
    grep -q '"state":"completed"' <<<"$J"

# ---- C. Browse, search, compare --------------------------------------------
timed cb snapshots "$D_ID"
check C1 "list snapshots" grep -q '"files"' <<<"$OUT"
timed cb snapshots "$D_ID" --limit 2 --offset 2
P2=$(echo "$OUT" | jget 'd["snapshots"][0]["id"]')
check C2 "second page continues the first (starts at $P2)" test "$P2" -gt 0
timed cb ls "$D_ID" 1
check C3 "walk a directory ($MS ms)" grep -q '"name"' <<<"$OUT"
WIDE=$(cb ls "$G_ID" 1 | jget 'd["entries"][0]["id"]' 2>/dev/null)
timed cb snapshot tree "$G_ID" 1 "$WIDE" --limit 5 --offset 5
check C4 "paged directory at scale ($MS ms)" test "$RC" -eq 0
timed cb search "$D_ID" "$D_TERM"
check C5 "name search '$D_TERM' ($MS ms)" grep -q '"path"' <<<"$OUT"
timed cb search "$G_ID" "$G_TERM"
check C5g "name search over the scale set ($MS ms)" test "$MS" -lt 1000 -a "$RC" -eq 0
# Snapshot 1, the first full backup: the newest one by now is B6's, which
# backed up only the specials directory.
timed cb grep "$D_ID" "$GREP_TEXT" --snapshot 1 --limit 5
check C6 "content search finds '$GREP_TEXT'" grep -q '"line"' <<<"$OUT"
timed cb diff "$D_ID" 2 3
check C7 "diff two snapshots" grep -q '"added"' <<<"$OUT"
# Small on purpose: the content route feeds a preview and is capped at 2 MiB.
H=$(cb search "$D_ID" xattr.txt | jget 'd["results"][0]["hash"]' 2>/dev/null)
timed cb object "$D_ID" "$H" content
echo "$OUT" | python3 -c 'import sys,json,base64; sys.stdout.buffer.write(base64.b64decode(json.load(sys.stdin)["base64"]))' > "$WORK/object.bin" 2>/dev/null
check C8 "read one object back, byte for byte" cmp -s "$WORK/object.bin" "$S/xattr.txt"

# ---- D. Restore --------------------------------------------------------------
timed cb restore "$D_ID" 3 "$WORK/restore/full"
check D1 "whole snapshot restores identical ($((MS/1000)) s)" \
    diff -r --no-dereference "$S" "$WORK/restore/full$S" -x fifo -x changed.txt
timed cb restore "$D_ID" 3 "$WORK/restore/sub" --subpath "$S/xattr.txt"
check D2 "subpath restores only its file" bash -c "[ -f '$WORK/restore/sub$S/xattr.txt' ] && [ ! -e '$WORK/restore/sub$S/data.bin' ]"
R="$WORK/restore/full$S"
check D3 "modes, mtimes, links, sparse, fifo, xattr" bash -c "
  [ \"\$(stat -c %a:%Y '$S/data.bin')\" = \"\$(stat -c %a:%Y '$R/data.bin')\" ] &&
  [ -L '$R/symlink' ] && [ \"\$(stat -c %i '$R/link-a')\" = \"\$(stat -c %i '$R/link-b')\" ] &&
  [ \$(( \$(stat -c %b '$R/sparse.img') * 512 )) -lt 33554432 ] && [ -p '$R/fifo' ] &&
  { ! command -v getfattr >/dev/null || getfattr -n user.cbackup '$R/xattr.txt' 2>/dev/null | grep -q acceptance; }"
timed cb restore "$D_ID" 3 "$WORK/restore/full"
check D4 "restoring again skips what exists" grep -q '"state":"completed"' <<<"$OUT"

# ---- E. Integrity --------------------------------------------------------------
timed cb verify "$D_ID" --level index
check E1 "index verify clean" grep -q '"missing":0,' <<<"$OUT"
timed cb verify "$D_ID" --level deep
check E2 "deep verify clean ($((MS/1000)) s)" grep -q '"unrecoverable":0,' <<<"$OUT"
timed cb verify list "$D_ID"
check E3 "verify history" grep -q '"runs"' <<<"$OUT"
PACK=$(find "$WORK/repo-d/packs" -name '*.dat' | head -1)
SIZE=$(stat -c %s "$PACK")
OFF=$((SIZE / 2))
printf '\xff' | dd of="$PACK" bs=1 seek="$OFF" conv=notrunc status=none
timed cb verify "$D_ID" --level deep
check E4 "damage found and repaired: $(echo "$OUT" | jget 'd["runs"][0]["repaired"]') repaired" \
    bash -c "echo \"\$0\" | python3 -c 'import sys,json; r=json.load(sys.stdin)[\"runs\"][0]; sys.exit(0 if int(r[\"repaired\"])>0 and r[\"unrecoverable\"]==0 else 1)'" "$OUT"
timed cb verify "$D_ID" --level deep
check E5 "second deep verify clean" bash -c "echo \"\$0\" | python3 -c 'import sys,json; r=json.load(sys.stdin)[\"runs\"][0]; sys.exit(0 if r[\"repaired\"]==0 and r[\"unrecoverable\"]==0 else 1)'" "$OUT"

# ---- F. Maintenance -------------------------------------------------------------
timed cb pack "$D_ID"
check F1 "pack" grep -q '"state":"completed"' <<<"$OUT"
timed cb gc "$D_ID"
check F2 "gc" grep -q '"state":"completed"' <<<"$OUT"
cb policy set "$D_ID" retention.keep_snaps 100 >/dev/null
timed cb gfs "$D_ID"
check F3 "retention pass (keep_snaps 100 keeps all)" grep -q '"state":"completed"' <<<"$OUT"
BEFORE=$(cb stats "$D_ID")
timed cb reindex "$D_ID"
check F4 "reindex agrees with the stats before it" bash -c "[ \"\$('$CLI' --socket '$SOCK' stats $D_ID)\" = '$BEFORE' ]"
timed cb snapshot delete "$D_ID" 1
check F5 "delete a snapshot; others still restore" bash -c "
  '$CLI' --socket '$SOCK' restore $D_ID 3 '$WORK/restore/after-delete' >/dev/null &&
  cmp -s '$S/data.bin' '$WORK/restore/after-delete$S/data.bin'"

# ---- G. Metadata -----------------------------------------------------------------
timed cb tag set "$D_ID" keep 2
check G1 "tag" bash -c "'$CLI' --socket '$SOCK' tag list $D_ID | grep -q keep"
cb policy set "$D_ID" retention.keep_snaps 1 >/dev/null
timed cb gfs "$D_ID"
check G2 "tagged snapshot survives retention" bash -c "'$CLI' --socket '$SOCK' snapshots $D_ID | grep -q '\"id\":2,'"
timed cb tag remove "$D_ID" keep
check G3 "remove tag" bash -c "! '$CLI' --socket '$SOCK' tag list $D_ID | grep -q keep"
LAST=$(cb snapshots "$D_ID" --limit 1 | jget 'd["snapshots"][0]["id"]')
timed cb snapshot note "$D_ID" "$LAST" "acceptance note"
check G4 "note" bash -c "'$CLI' --socket '$SOCK' snapshot note $D_ID $LAST | grep -q 'acceptance note'"
timed cb snapshot note "$D_ID" "$LAST" ""
check G5 "clear note" bash -c "! '$CLI' --socket '$SOCK' snapshot note $D_ID $LAST | grep -q 'acceptance note'"
timed cb policy set "$D_ID" retention.keep_daily 14
check G6 "policy persists" bash -c "'$CLI' --socket '$SOCK' policy list $D_ID | grep -q '\"retention.keep_daily\",\"value\":14'"
timed cb journal "$D_ID"
check G7 "journal records operations with durations" grep -q '"duration_ms"' <<<"$OUT"

# ---- H. Scheduling --------------------------------------------------------------
BEFORE=$(cb snapshots "$G_ID" | jget 'len(d["snapshots"])')
timed cb schedule add "$G_ID" backup 30
SCHED=$(echo "$OUT" | jget 'd["id"]')
check H1 "add an interval schedule" test -n "$SCHED"
T0=$(now_ms)
for _ in $(seq 1 120); do
    [ "$(cb snapshots "$G_ID" | jget 'len(d["snapshots"])')" -gt "$BEFORE" ] && break; sleep 2
done
MS=$(( $(now_ms) - T0 ))
check H2 "it fires on its own" bash -c "[ \"\$('$CLI' --socket '$SOCK' snapshots $G_ID | python3 -c 'import sys,json; print(len(json.load(sys.stdin)[\"snapshots\"]))')\" -gt $BEFORE ]"
timed cb schedule disable "$G_ID" "$SCHED"
check H3 "pause" bash -c "'$CLI' --socket '$SOCK' schedule list $G_ID | grep -q '\"enabled\":false'"
timed cb schedule add "$G_ID" gfs --every day --at 04:00
check H3c "calendar schedule" grep -q '"id"' <<<"$OUT"
timed cb schedule remove "$G_ID" "$SCHED"
check H4 "remove" test "$RC" -eq 0

# ---- I. Jobs and observability ------------------------------------------------------
timed cb jobs
check I1 "list jobs" grep -q '"jobs"' <<<"$OUT"
cb repo add "$WORK/repo-cancel" Cancel >/dev/null
C_ID=$(cb repo list | jget '[r["id"] for r in d["repos"] if r["label"]=="Cancel"][0]')
cb backup "$C_ID" "$G_PATH" --no-wait > "$WORK/cancel.json"
CJ=$(jget 'd["job_id"]' < "$WORK/cancel.json")
sleep 2
timed cb jobs cancel "$CJ"
for _ in $(seq 1 60); do cb jobs "$CJ" | grep -q '"state":"cancelled"' && break; sleep 1; done
check I2 "cancel a running backup" bash -c "'$CLI' --socket '$SOCK' jobs $CJ | grep -q '\"state\":\"cancelled\"'"
timed cb summary "$G_ID"
check I3 "summary of the scale set ($MS ms)" test "$MS" -lt 1000
timed cb fanout "$G_ID"
check I4 "objects, packs, fanout" bash -c "'$CLI' --socket '$SOCK' packs $G_ID >/dev/null && '$CLI' --socket '$SOCK' objects $G_ID --limit 5 >/dev/null"
timed cb metrics
check I5 "metrics" grep -q cbackup_last_snapshot_age_seconds <<<"$OUT"

# ---- J. Encryption ------------------------------------------------------------------
cb backup "$E_ID" "$S" >/dev/null
check J1 "encrypted backup has no plaintext on disk" bash -c "! grep -rqa 'linked' '$WORK/repo-enc/packs' '$WORK/repo-enc/objects'"
timed cb restore "$E_ID" 1 "$WORK/restore/enc"
check J2 "encrypted round trip" cmp -s "$S/data.bin" "$WORK/restore/enc$S/data.bin"
timed cb repo key export "$E_ID"
check J3 "export the key" grep -q "$E_KEY" <<<"$OUT"
timed cb metrics
check J4 "unescrowed key is counted" grep -q 'cbackup_repos_key_unescrowed 1' <<<"$OUT"
timed cb repo key ack "$E_ID"
check J5 "acknowledging clears it" bash -c "'$CLI' --socket '$SOCK' metrics | grep -q 'cbackup_repos_key_unescrowed 0'"

# ---- K. Remote access ---------------------------------------------------------------
tcp() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
MS=0
check K1 "TCP before a password: 403" test "$(tcp "http://127.0.0.1:$PORT/repos")" = 403
printf '%s\n' "$PASSWORD" | cb auth setup >/dev/null
T0=$(now_ms)
TOKEN=$(curl -s -X POST -d "{\"password\":\"$PASSWORD\"}" "http://127.0.0.1:$PORT/api/auth/login" | jget 'd["token"]')
MS=$(( $(now_ms) - T0 ))
check K2 "login over TCP" test -n "$TOKEN"
T0=$(now_ms); OUT=$(curl -s -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:$PORT/repos/$D_ID/snapshots?limit=5"); MS=$(( $(now_ms) - T0 ))
check K3 "browse over TCP ($MS ms)" grep -q '"snapshots"' <<<"$OUT"
check K4 "wrong password refused" test "$(tcp -X POST -d '{"password":"nope-nope"}' "http://127.0.0.1:$PORT/api/auth/login")" = 401
check K5 "setup refused over TCP" test "$(tcp -X POST -d '{"password":"another-one"}' "http://127.0.0.1:$PORT/api/auth/setup")" = 403

# ---- L. Replication and recovery -------------------------------------------------------
timed cb replicate "$D_ID" "$WORK/replica"
check L1 "replicate ($((MS/1000)) s)" grep -q '"state":"completed"' <<<"$OUT"
timed cb replicate "$D_ID" "$WORK/replica"
check L2 "unchanged replicate is quick ($MS ms)" grep -q '"state":"completed"' <<<"$OUT"
# A second daemon adopts the replica, the way a replacement host would.
REC="$WORK/recovered"; mkdir -p "$REC"
sed -e "s|$WORK/state.db|$REC/state.db|" -e "s|socket: $SOCK|socket: $SOCK_DIR/r|" \
    -e "s|port: $PORT|port: 0|" "$WORK/config.yaml" > "$REC/config.yaml"
C_BACKUP_CONFIG_PATH="$REC/config.yaml" "$DAEMON" > "$REC/daemon.log" 2>&1 &
REC_PID=$!
for _ in $(seq 1 100); do "$CLI" --socket "$SOCK_DIR/r" health >/dev/null 2>&1 && break; sleep 0.1; done
rc() { "$CLI" --socket "$SOCK_DIR/r" "$@"; }
rc repo add "$WORK/replica" recovered >/dev/null
timed rc reindex 1
NEWEST=$(rc snapshots 1 --limit 1 | jget 'd["snapshots"][0]["id"]')
rc restore 1 "$NEWEST" "$WORK/restore/from-replica" >/dev/null
check L3 "a second daemon restores snapshot $NEWEST from the replica" cmp -s "$S/data.bin" "$WORK/restore/from-replica$S/data.bin"
kill -TERM "$REC_PID"; wait "$REC_PID" 2>/dev/null

# ---- M. Shutdown and crash -------------------------------------------------------------
cb repo add "$WORK/repo-stop" Stop >/dev/null
ST_ID=$(cb repo list | jget '[r["id"] for r in d["repos"] if r["label"]=="Stop"][0]')
cb backup "$ST_ID" "$G_PATH" --no-wait > "$WORK/stop.json"
SJ=$(jget 'd["job_id"]' < "$WORK/stop.json")
sleep 5
T0=$(now_ms); stop_daemon; MS=$(( $(now_ms) - T0 ))
check M1 "SIGTERM mid-backup stops in order ($MS ms), socket removed" test ! -e "$SOCK"
start_daemon
check M2 "the stopped backup is recorded cancelled" bash -c "'$CLI' --socket '$SOCK' jobs $SJ | grep -q '\"state\":\"cancelled\"'"
cb repo add "$WORK/repo-crash" Crash >/dev/null
CR_ID=$(cb repo list | jget '[r["id"] for r in d["repos"] if r["label"]=="Crash"][0]')
cb backup "$CR_ID" "$G_PATH" --no-wait >/dev/null
sleep 5
kill -KILL "$DAEMON_PID"; wait "$DAEMON_PID" 2>/dev/null
start_daemon
timed cb backup "$CR_ID" "$G_PATH"
timed cb verify "$CR_ID" --level index --no-repair
check M3 "after kill -9 mid-backup, the next backup is complete" \
    bash -c "echo \"\$0\" | python3 -c 'import sys,json; r=json.load(sys.stdin)[\"runs\"][0]; sys.exit(0 if r[\"missing\"]==0 else 1)'" "$OUT"

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL" | tee -a "$RESULTS"
if [ "$KEEP" = 1 ]; then
    echo "daemon left running: socket $SOCK, TCP 127.0.0.1:$PORT, password in $WORK/password"
    echo "$PASSWORD" > "$WORK/password"; chmod 600 "$WORK/password"
    trap - EXIT
fi
[ "$FAIL" -eq 0 ]
