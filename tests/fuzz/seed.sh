#!/bin/bash
# Build seed corpora from a real repository.
#
# A fuzzer handed valid structures finds the interesting mutations; one
# starting from nothing spends its budget rediscovering four-byte magic
# numbers it was never going to guess. So make a repository, back something
# into it, and take the actual bytes.
#
# Run from the repo root: tests/fuzz/seed.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CORPUS="$ROOT/tests/fuzz/corpus"
W=$(mktemp -d)
trap 'rm -rf "$W"' EXIT

mkdir -p "$W/src/sub"
printf 'the quick brown fox\n%.0s' $(seq 1 200) > "$W/src/compressible.txt"
head -c 40000 /dev/urandom                      > "$W/src/incompressible.bin"
# Backup packs on completion, so the only objects still loose afterwards are
# the ones the prober marked incompressible and left alone - which by design
# means larger than the 16 MiB pack threshold. Without one of these there is
# nothing on disk in loose-object form to seed fuzz_object with.
head -c 17825792 /dev/urandom                   > "$W/src/big_incompressible.bin"
echo "small"                                    > "$W/src/sub/small.txt"
ln -s compressible.txt "$W/src/link"
# A sparse file, so a sparse object lands in the corpus.
dd if=/dev/zero of="$W/src/sparse.bin" bs=1 count=0 seek=1048576 2>/dev/null
printf 'data at the end' | dd of="$W/src/sparse.bin" bs=1 seek=1048000 conv=notrunc 2>/dev/null
setfattr -n user.seed -v "seedvalue" "$W/src/sub/small.txt" 2>/dev/null || true
setfacl -m u:"$(id -u)":rwx "$W/src/sub" 2>/dev/null || true

cat > "$W/config.yaml" <<YAML
db:
  path: $W/state.db
http:
  socket: $W/sock
  port: 19771
  bind: 127.0.0.1
security:
  key_dir: $W/keys
log:
  level: warn
YAML
mkdir -p "$W/keys"

"$ROOT/build/bins/c-backupd" >"$W/daemon.log" 2>&1 &
DPID=$!
trap 'kill $DPID 2>/dev/null || true; rm -rf "$W"' EXIT
export C_BACKUP_CONFIG_PATH="$W/config.yaml"
# The daemon reads its config from the environment, so restart it with one.
kill $DPID 2>/dev/null || true
C_BACKUP_CONFIG_PATH="$W/config.yaml" "$ROOT/build/bins/c-backupd" >"$W/daemon.log" 2>&1 &
DPID=$!
for _ in $(seq 1 60); do [ -S "$W/sock" ] && break; sleep 0.1; done
[ -S "$W/sock" ] || { echo "daemon did not start"; cat "$W/daemon.log"; exit 1; }

cb() { C_BACKUP_SOCKET="$W/sock" "$ROOT/build/bins/c-backup" "$@"; }

# One plain repository and one encrypted, so the corpus covers both the
# encrypted and unencrypted object layouts.
cb repo add "$W/plain" plain >/dev/null
cb repo add "$W/enc" enc --encrypt system >/dev/null
cb backup 1 "$W/src" >/dev/null
cb backup 2 "$W/src" >/dev/null

mkdir -p "$CORPUS/fuzz_object" "$CORPUS/fuzz_pack" "$CORPUS/fuzz_manifest"

# Loose objects. These are the incompressible ones the prober left alone, so
# they are large - too large to keep whole in a corpus, and libFuzzer would
# refuse them anyway. What matters is the header at the front and the parity
# trailer at the back, so each contributes both ends as separate seeds and
# the fuzzer explores outward from each.
n=0
for repo in "$W/plain" "$W/enc"; do
    while IFS= read -r f; do
        head -c 65536 "$f"        > "$CORPUS/fuzz_object/seed_${n}_head"
        tail -c 8192  "$f"        > "$CORPUS/fuzz_object/seed_${n}_tail"
        n=$((n + 1))
    done < <(find "$repo/objects" -type f 2>/dev/null | head -20)
done

# Pack files.
n=0
while IFS= read -r f; do
    cp "$f" "$CORPUS/fuzz_pack/seed_$n" && n=$((n + 1))
done < <(find "$W/plain/packs" "$W/enc/packs" -name '*.dat' 2>/dev/null)

# Manifest trees have to come out decoded: on disk they are wrapped in an
# object header and possibly compressed or encrypted, and fuzz_manifest
# parses the payload, not the container.
cb objects 1 --limit 200 2>/dev/null \
  | tr ',' '\n' | grep -o '"hash":"[0-9a-f]*"' | cut -d'"' -f4 > "$W/hashes"
n=0
while IFS= read -r h; do
    [ -n "$h" ] || continue
    body=$(cb object 1 "$h" content 2>/dev/null | grep -o '"base64":"[^"]*"' | cut -d'"' -f4 || true)
    [ -n "$body" ] || continue
    printf '%s' "$body" | base64 -d > "$W/decoded" 2>/dev/null || continue
    # "CBTR" little-endian is 43 42 54 52.
    if head -c 4 "$W/decoded" | od -An -tx1 | tr -d ' \n' | grep -qi '^43425452$'; then
        cp "$W/decoded" "$CORPUS/fuzz_manifest/seed_$n" && n=$((n + 1))
    fi
done < "$W/hashes"

kill $DPID 2>/dev/null || true

echo "seeded:"
for d in "$CORPUS"/*/; do
    printf '  %-16s %s file(s)\n' "$(basename "$d")" "$(find "$d" -type f | wc -l)"
done
