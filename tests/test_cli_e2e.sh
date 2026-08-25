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
CLI="${2:-build/bins/c-backup}"
WORK="$(mktemp -d /tmp/c_backup_cli_e2e.XXXXXX)"
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

ok()  { PASS=$((PASS+1)); printf '  ok   %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; }

check()    { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "$2" "$3"; fi; }
contains() { case "$3" in *"$2"*) ok "$1" ;; *) bad "$1" "contains '$2'" "$3" ;; esac; }

cb() { "$CLI" --socket "$SOCK" "$@"; }

echo "no daemon"
# The most important case: a clear message, a non-zero exit, and no hang.
OUT=$("$CLI" --socket "$WORK/absent.sock" repo list 2>&1); RC=$?
check    "exits non-zero without a daemon" 1 "$RC"
contains "explains the daemon is not running" "daemon not running" "$OUT"
contains "suggests how to start it" "systemctl start c-backupd" "$OUT"

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

cat > "$WORK/config.yaml" <<EOF
db:
  path: $WORK/state.db
http:
  socket: $SOCK
  port: 18801
  bind: 127.0.0.1
log:
  level: warn
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
contains "repo list" '"path"' "$(cb repo list)"
contains "repo show" '"uuid"' "$(cb repo show 1)"

# Job commands wait for their job and report what happened, so there is
# nothing to poll and the assertion is on the outcome rather than on the
# daemon having accepted the request.
contains "backup waits and completes" '"state":"completed"' "$(cb backup 1 "$WORK/src")"
contains "jobs list"     '"action"'            "$(cb jobs)"
contains "snapshots"     '"id":"1"'            "$(cb snapshots 1)"
contains "ls"            '"name"'              "$(cb ls 1 1)"
contains "search"        'one.txt'             "$(cb search 1 txt)"

contains "tag set"    '"ok"'      "$(cb tag set 1 keeper 1)"
contains "tag list"   'keeper'    "$(cb tag list 1)"
check    "tag remove" 0           "$(cb tag remove 1 keeper > /dev/null; echo $?)"

contains "schedule add"    '"id"' "$(cb schedule add 1 gc 3600)"
contains "schedule list"   'gc'   "$(cb schedule list 1)"
check    "schedule remove" 0      "$(cb schedule remove 1 1 > /dev/null; echo $?)"

contains "gc"      '"state":"completed"' "$(cb gc 1)"
contains "reindex" '"state":"completed"' "$(cb reindex 1)"
contains "gfs"     '"state":"completed"' "$(cb gfs 1)"
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
contains "empty location filter keeps its key" '"objects"' \
    "$(cb objects 1 --location loose --limit 5)"
contains "packs"    '"'          "$(cb packs 1)"
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

contains "snapshot note set" '"ok"'   "$(cb snapshot note 1 1 'checked by hand')"
contains "snapshot note get" 'checked by hand' "$(cb snapshot note 1 1)"
contains "snapshot tree"     '"name"' "$(cb snapshot tree 1 1)"

contains "pack"   '"state":"completed"' "$(cb pack 1)"
contains "verify" '"level":"index"'     "$(cb verify 1 --level index)"
VERIFY_OUT=$(cb verify 1 --level deep)
contains "deep verify reports its run" '"level":"deep"' "$VERIFY_OUT"
contains "deep verify checked objects" '"checked"'      "$VERIFY_OUT"
contains "gc dry run" '"state":"completed"' "$(cb gc 1 --dry-run)"
contains "journal result filter" '"entries"' "$(cb journal 1 --result ok --limit 5)"

# A second snapshot so diff has two sides, and so snapshot delete has
# something to remove that is not the only copy.
echo "content three" > "$WORK/src/three.txt"
contains "second backup completed" '"state":"completed"' "$(cb backup 1 "$WORK/src")"
contains "diff" '"added"' "$(cb diff 1 1 2)"

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

check "snapshot delete" 0 "$(cb snapshot delete 1 1 > /dev/null 2>&1; echo $?)"
contains "schedule enable/disable" '"ok"' \
    "$(cb schedule add 1 verify 7200 > /dev/null; cb schedule list 1 | grep -q verify && cb schedule disable 1 2 || echo miss)"

# Content search. The pre-daemon CLI had this and the rewrite dropped it, so
# "which backup still has that line" had no answer short of restoring one.
echo "grep"
echo "the passphrase is hunter2" > "$WORK/src/secrets.txt"
cb backup 1 "$WORK/src" > /dev/null
GREP_OUT=$(cb grep 1 'passphrase is hunter2')
contains "grep finds the line"    'secrets.txt' "$GREP_OUT"
contains "grep reports the text"  'hunter2'     "$GREP_OUT"
contains "grep reports a line no" '"line"'      "$GREP_OUT"
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

# Passphrase mode needs nothing escrowed: the wrapped key travels with the
# repository, so the passphrase alone recovers it.
PASS_OUT=$(cb repo add "$WORK/passrepo" pass --encrypt passphrase --passphrase 'correct horse')
contains "passphrase repo created"   '"encryption":"passphrase"' "$PASS_OUT"
contains "passphrase repo escrowed"  '"key_escrowed":true'       "$PASS_OUT"
contains "wrapped key in the marker" '"wrapped"' "$(cat "$WORK/passrepo/.cbackup")"
OUT=$(cb repo add "$WORK/nopass" np --encrypt passphrase 2>&1); RC=$?
check "passphrase mode requires a passphrase" 1 "$RC"

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
if grep -qa "$(head -c 32 "$WORK/src/random.bin" 2>/dev/null || echo __nofixture__)" "$ENC_DAT" 2>/dev/null; then
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

# Verify records runs and findings, and both were unreachable until now: the
# counters reached an operator only through /metrics and the findings not at
# all. A scrub whose answer you cannot read is a scrub you did not run.
VOUT=$(cb verify "$ENC_ID" --level deep)
contains "verify waits and reports its run" '"level":"deep"' "$VOUT"
contains "verify reports what it checked"   '"checked"'      "$VOUT"
contains "verify reports findings"          '"findings"'     "$VOUT"
contains "clean repo has no findings"       '"findings":[]'  "$VOUT"
contains "verify list shows past runs"      '"runs"'         "$(cb verify list "$ENC_ID" --limit 2)"

# Job commands wait by default and exit non-zero when the job fails.
OUT=$(cb pack "$ENC_ID"); RC=$?
check    "pack waits for its job"  0            "$RC"
contains "pack reports completion" '"completed"' "$OUT"
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
if [ -z "$(find "$REPL/objects" -type f 2>/dev/null)" ]; then
    ok "replica writes no loose objects"
else
    bad "replica writes no loose objects" "none" "$(find "$REPL/objects" -type f | wc -l)"
fi

# And it is still ciphertext. The same fixture the pack test uses, for the
# same reason: compressed text is unsearchable either way, so look for the
# incompressible one.
NEEDLE=$(head -c 32 "$WORK/src/random.bin" 2>/dev/null || echo __nofixture__)
if grep -qra "$NEEDLE" "$REPL" 2>/dev/null; then
    bad "replica holds no plaintext" "absent" "present"
else
    ok "replica holds no plaintext"
fi

# Running it again moves nothing: the target already has the root tree, so
# one question ends the walk.
BEFORE=$(find "$REPL" -type f | wc -l)
cb replicate "$ENC_ID" "$REPL" >/dev/null
AFTER=$(find "$REPL" -type f | wc -l)
check "second replicate is a no-op" "$BEFORE" "$AFTER"

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
# Stop it before it fires again under the rest of the suite.
SID=$(cb schedule list "$ENC_ID" 2>/dev/null | tr ',' '\n' | grep -B0 -A0 '"id"' | head -1 | sed -n 's/.*"id":"\([0-9]*\)".*/\1/p')
cb schedule remove "$ENC_ID" "$SID" >/dev/null 2>&1 || true

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
  port: 18802
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
DRIFT_ID=$(cb repo list | tr '}' '\n' | grep driftrepo | sed -n 's/.*"id":"\([0-9]*\)".*/\1/p')
mv "$WORK/driftrepo" "$WORK/driftmoved"
contains "a moved repository re-links" '"id"' "$(cb repo add "$WORK/driftmoved" drift 2>&1)"
contains "and now reports the new path" "driftmoved" "$(cb repo show "$DRIFT_ID")"

# A path already holding a different repository is refused rather than mixed.
cb repo add "$WORK/otherrepo" other >/dev/null 2>&1
OUT=$(cb replicate "$ENC_ID" "$WORK/otherrepo" 2>&1); RC=$?
check "replicate into a foreign repo fails" 1 "$RC"

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
LAST=$(cb snapshots 1 | tr '}' '\n' | grep -o '"id":"[0-9]*"' | head -1 | tr -dc '0-9')
cb restore 1 "$LAST" "$WORK/overrideout" >/dev/null 2>&1
if find "$WORK/overrideout" -name 'other.txt' | grep -q .; then
    ok "an explicit path still overrides the configured sources"
else
    bad "an explicit path still overrides the configured sources" "other.txt" "absent"
fi

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
SID0=$(cb schedule list 1 | tr '}' '\n' | grep '"action":"backup"' | sed -n 's/.*"id":"\([0-9]*\)".*/\1/p' | head -1)
[ -n "$SID0" ] && cb schedule remove 1 "$SID0" >/dev/null 2>&1

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
SID=$(cb schedule list 1 | tr '}' '\n' | grep '"action":"backup"' | sed -n 's/.*"id":"\([0-9]*\)".*/\1/p' | head -1)
[ -n "$SID" ] && cb schedule remove 1 "$SID" >/dev/null 2>&1

echo "listen address"

# http.bind has been a documented setting since the config was written,
# describing itself as the way to "force remote access through a reverse
# proxy", and it was never read: MHD was started without a socket address,
# which binds every interface. Setting it to 127.0.0.1 produced a port
# answering on the LAN and a config file saying it did not.
BINDPORT=18809
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

echo "error handling"
OUT=$(cb repo show 999 2>&1); RC=$?
check    "unknown repo exits non-zero" 1 "$RC"
contains "unknown repo reports status" "HTTP 404" "$OUT"
OUT=$(cb backup 2>&1); RC=$?
check    "missing arguments exit non-zero" 1 "$RC"
contains "missing arguments show usage" "usage:" "$OUT"

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
