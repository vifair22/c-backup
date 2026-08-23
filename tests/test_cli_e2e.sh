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

contains "backup submits a job" '"job_id"' "$(cb backup 1 "$WORK/src")"
for _ in $(seq 1 60); do
    case "$(cb jobs 1)" in *'"state":"completed"'*|*'"state":"failed"'*) break ;; esac
    sleep 0.2
done
contains "job completed" '"state":"completed"' "$(cb jobs 1)"
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

contains "gc"      '"job_id"' "$(cb gc 1)"
contains "reindex" '"job_id"' "$(cb reindex 1)"
contains "gfs"     '"job_id"' "$(cb gfs 1)"
contains "journal" 'entries'  "$(cb journal 1)"

# Poll the restore's own job, not "is anything running". The maintenance jobs
# above may still be queued ahead of it, and a global check breaks out while
# the restore has not started.
RESTORE_OUT=$(cb restore 1 1 "$WORK/out")
contains "restore submits a job" '"job_id"' "$RESTORE_OUT"
RESTORE_JOB=$(printf '%s' "$RESTORE_OUT" | sed -n 's/.*"job_id":\([0-9]*\).*/\1/p')
# Three maintenance jobs were queued just above and there are two workers, so
# the restore can sit behind them. The old twenty-second budget ran out on a
# loaded machine and the loop fell through silently, leaving the tree
# comparison below to report a difference that was really "not finished yet".
RESTORE_DONE=0
for _ in $(seq 1 600); do
    case "$(cb jobs "$RESTORE_JOB")" in
        *'"state":"completed"'*|*'"state":"failed"'*|*'"state":"cancelled"'*)
            RESTORE_DONE=1; break ;;
    esac
    sleep 0.2
done
check "restore job reached a terminal state" 1 "$RESTORE_DONE"
# Completed, not merely terminal. Accepting "failed" here is what let a
# restore rejected with "repo is busy" masquerade as a tree difference.
contains "restore job completed" '"state":"completed"' "$(cb jobs "$RESTORE_JOB")"
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

contains "pack"   '"job_id"' "$(cb pack 1)"
contains "verify" '"job_id"' "$(cb verify 1 --level index)"
VERIFY_OUT=$(cb verify 1 --level deep)
VERIFY_JOB=$(printf '%s' "$VERIFY_OUT" | sed -n 's/.*"job_id":\([0-9]*\).*/\1/p')
for _ in $(seq 1 300); do
    case "$(cb jobs "$VERIFY_JOB")" in
        *'"state":"completed"'*|*'"state":"failed"'*) break ;;
    esac
    sleep 0.2
done
contains "deep verify completed" '"state":"completed"' "$(cb jobs "$VERIFY_JOB")"
contains "gc dry run"      '"job_id"' "$(cb gc 1 --dry-run)"
contains "journal result filter" '"entries"' "$(cb journal 1 --result ok --limit 5)"

# A second snapshot so diff has two sides, and so snapshot delete has
# something to remove that is not the only copy.
echo "content three" > "$WORK/src/three.txt"
B2=$(cb backup 1 "$WORK/src")
B2_JOB=$(printf '%s' "$B2" | sed -n 's/.*"job_id":\([0-9]*\).*/\1/p')
for _ in $(seq 1 300); do
    case "$(cb jobs "$B2_JOB")" in
        *'"state":"completed"'*|*'"state":"failed"'*) break ;;
    esac
    sleep 0.2
done
contains "second backup completed" '"state":"completed"' "$(cb jobs "$B2_JOB")"
contains "diff" '"added"' "$(cb diff 1 1 2)"

# Exclusions have to work from the CLI too, which is where anyone would
# actually type them.
mkdir -p "$WORK/exsrc/cache"
echo "keepme"  > "$WORK/exsrc/keep.txt"
echo "dropme"  > "$WORK/exsrc/cache/junk.bin"
echo "dropme2" > "$WORK/exsrc/big.iso"
EX=$(cb backup 1 "$WORK/exsrc" --exclude cache --exclude '*.iso')
EX_JOB=$(printf '%s' "$EX" | sed -n 's/.*"job_id":\([0-9]*\).*/\1/p')
for _ in $(seq 1 300); do
    case "$(cb jobs "$EX_JOB")" in
        *'"state":"completed"'*|*'"state":"failed"'*) break ;;
    esac
    sleep 0.2
done
contains "excluding backup completed" '"state":"completed"' "$(cb jobs "$EX_JOB")"
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
G1=$(cb backup 1 "$WORK/src")
G1_JOB=$(printf '%s' "$G1" | sed -n 's/.*"job_id":\([0-9]*\).*/\1/p')
for _ in $(seq 1 300); do
    case "$(cb jobs "$G1_JOB")" in
        *'"state":"completed"'*|*'"state":"failed"'*) break ;;
    esac
    sleep 0.2
done
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

echo "error handling"
OUT=$(cb repo show 999 2>&1); RC=$?
check    "unknown repo exits non-zero" 1 "$RC"
contains "unknown repo reports status" "HTTP 404" "$OUT"
OUT=$(cb backup 2>&1); RC=$?
check    "missing arguments exit non-zero" 1 "$RC"
contains "missing arguments show usage" "usage:" "$OUT"

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
