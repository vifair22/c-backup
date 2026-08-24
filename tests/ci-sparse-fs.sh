#!/bin/bash
# Find or make somewhere the sparse suites can write a fixture with holes.
#
# Source this, do not run it: it exports the two variables the suites read.
#
#   . tests/ci-sparse-fs.sh
#
# The sparse tests and fuzz_sparse_write both skip when no directory reports
# holes, which is correct - a fixture stored dense tests the ordinary
# streaming path while claiming to test the sparse one - but skipping is not
# coverage. On this runner nothing the container mounts keeps three 4 KiB
# islands in a 128 KiB file sparse, so seven tests and one fuzz target do
# nothing there.
#
# Two ways out, cheapest first:
#
#   1. Some path in the container may already support holes even when the
#      build directory does not. /dev/shm and /var/tmp are on different
#      mounts than /builds and cost nothing to try.
#
#   2. A loopback ext4 image, which works wherever it can be mounted and
#      needs CAP_SYS_ADMIN - so it depends on the runner being privileged.
#
# Whichever fires is printed. If neither does, that is printed too, and the
# suites skip exactly as they do now: this script never fails a job, because
# a missing filesystem is a property of the runner and not a broken build.

# Does this directory keep the shape the tests actually use? Three 4 KiB
# islands in 128 KiB, asking for the gap between the first two - the same
# question object_store_file asks before it decides a file is sparse.
_sparse_probe() {
    local dir="$1"
    [ -d "$dir" ] || return 1
    local img="$dir/.ci_sparse_probe.$$"
    python3 - "$img" <<'PY' 2>/dev/null
import os, sys
path = sys.argv[1]
try:
    fd = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o644)
except OSError:
    sys.exit(1)
try:
    os.ftruncate(fd, 131072)
    for off in (0, 32768, 98304):
        os.lseek(fd, off, os.SEEK_SET)
        os.write(fd, b"P" * 4096)
    hole = os.lseek(fd, 4096, os.SEEK_HOLE)
    sys.exit(0 if 4096 <= hole < 32768 else 1)
except OSError:
    sys.exit(1)
finally:
    os.close(fd)
    try: os.unlink(path)
    except OSError: pass
PY
}

_sparse_adopt() {
    export CBACKUP_TEST_SPARSE_DIR="$1"
    export CBACKUP_FUZZ_SPARSE_DIR="$1"
    echo "sparse fixtures: $1 ($2)"
}

_sparse_setup() {
    local d
    for d in "$PWD/build" /var/tmp /dev/shm /tmp; do
        if _sparse_probe "$d"; then
            _sparse_adopt "$d" "already reports holes"
            return 0
        fi
    done

    # Nothing native. Build a filesystem we control.
    local img=/tmp/cbackup-sparse.img
    local mnt=/mnt/cbackup-sparse
    if ! command -v mkfs.ext4 >/dev/null 2>&1; then
        echo "sparse fixtures: none - no path reports holes and mkfs.ext4 is absent" >&2
        return 1
    fi
    mkdir -p "$mnt" 2>/dev/null || true
    if ! dd if=/dev/zero of="$img" bs=1M count=64 status=none 2>/dev/null; then
        echo "sparse fixtures: none - could not create $img" >&2
        return 1
    fi
    if ! mkfs.ext4 -q -F "$img" >/dev/null 2>&1; then
        echo "sparse fixtures: none - mkfs.ext4 failed" >&2
        rm -f "$img"
        return 1
    fi
    # Report what mount actually said. Loopback needs CAP_SYS_ADMIN, so an
    # unprivileged runner is the likely cause - but "likely" is not a
    # diagnosis, and a message that names the wrong cause sends whoever
    # reads it after the wrong fix.
    local err
    if ! err=$(mount -o loop "$img" "$mnt" 2>&1); then
        echo "sparse fixtures: none - loopback mount failed: ${err:-no error text}" >&2
        echo "  (this needs CAP_SYS_ADMIN; check whether the runner is privileged)" >&2
        rm -f "$img"
        return 1
    fi
    chmod 1777 "$mnt" 2>/dev/null || true
    if _sparse_probe "$mnt"; then
        _sparse_adopt "$mnt" "loopback ext4"
        return 0
    fi
    echo "sparse fixtures: none - loopback mounted but still reports no holes" >&2
    umount "$mnt" 2>/dev/null || true
    rm -f "$img"
    return 1
}

# Report what every candidate did, so a run that ends up skipping says why
# rather than going quiet.
echo "--- sparse filesystem probe ---"
for _d in "$PWD/build" /var/tmp /dev/shm /tmp; do
    if _sparse_probe "$_d"; then _r="holes"; else _r="no holes"; fi
    printf '  %-24s %-9s %s\n' "$_d" "$_r" "$(df -T "$_d" 2>/dev/null | awk 'NR==2{print $2}')"
done
unset _d _r

_sparse_setup || true
