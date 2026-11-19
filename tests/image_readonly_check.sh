#!/bin/bash
#
# B244: the image, started the hardened way Manual 22.2.1 documents - a
# read-only root and no /tmp, nine capabilities, dropping itself to uid
# 990 - must start, on fresh state and on a database an M9 daemon left at
# migration 008. With the root read-only SQLite has no temporary directory
# of its own, and M10's first start failed in migration 009 on both.
#
# Run by the image jobs in CI against the image just built, on the host's
# Docker daemon: bind mounts would name host paths, so state goes into
# named volumes, filled through the image itself. Needs sqlite3 here to load
# the fixture.
#
#   tests/image_readonly_check.sh <image> <name-suffix>

set -u

IMAGE="$1"
SFX="$2"
FIXTURE="$(dirname "$0")/fixtures/state-at-008.sql"
# The newest migration in this checkout, which the image was built from.
LAST_MIG=$(basename "$(ls "$(dirname "$0")"/../migrations/*.sql | tail -n 1)")
FAIL=0
NAMES=()
VOLS=()

cleanup() {
    for n in "${NAMES[@]}"; do docker rm -f "$n" >/dev/null 2>&1; done
    for v in "${VOLS[@]}"; do docker volume rm "$v" >/dev/null 2>&1; done
    rm -f "state-at-008.$SFX.db"
}
trap cleanup EXIT

ok()  { printf '  ok   %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  FAIL %s\n' "$1"; }

# A state volume owned by 990, holding the given database if any.
make_volume() {
    local vol="$1" db="${2:-}"
    docker volume create "$vol" >/dev/null || return 1
    VOLS+=("$vol")
    if [ -n "$db" ]; then
        docker run --rm -i -v "$vol:/s" --entrypoint sh "$IMAGE" \
            -c 'cat > /s/state.db && chown -R 990:990 /s' < "$db"
    else
        docker run --rm -v "$vol:/s" --entrypoint chown "$IMAGE" -R 990:990 /s
    fi
}

# The daemon as 22.2.1 runs it, less the host mounts a check has no use for.
run_hardened() {
    local name="$1" vol="$2"
    NAMES+=("$name")
    docker run -d --name "$name" \
        --cap-drop ALL \
        --cap-add DAC_READ_SEARCH --cap-add DAC_OVERRIDE --cap-add CHOWN \
        --cap-add FOWNER --cap-add FSETID --cap-add MKNOD \
        --cap-add SETUID --cap-add SETGID --cap-add SETPCAP \
        --read-only \
        --tmpfs /run/c-backupd:uid=990,gid=990,mode=0755 \
        -e C_BACKUP_SECURITY_RUN_AS_UID=990 \
        -v "$vol:/var/lib/c-backupd" \
        "$IMAGE" >/dev/null
}

# Running after a few seconds, the migrations it should have applied in the
# log, none failed, and SQLite's temporaries pointed at the state volume.
check_started() {
    local name="$1" label="$2" log
    shift 2
    sleep 5
    log=$(docker logs "$name" 2>&1)
    if [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" = true ]; then
        ok "$label: running"
    else
        bad "$label: running"
        printf '%s\n' "$log"
        return
    fi
    case "$log" in *"migrations failed"*) bad "$label: a migration failed" ;;
                   *) ok "$label: no migration failed" ;; esac
    case "$log" in *"sqlite temporary files: /var/lib/c-backupd"*)
                       ok "$label: temporaries in the state volume" ;;
                   *) bad "$label: temporaries in the state volume" ;; esac
    case "$log" in *"running as uid 990"*) ok "$label: dropped to 990" ;;
                   *) bad "$label: dropped to 990" ;; esac
    local m
    for m in "$@"; do
        case "$log" in *"$m"*) ok "$label: $m" ;; *) bad "$label: $m" ;; esac
    done
}

echo "read-only root, fresh state"
make_volume "cbk-ro-fresh-$SFX" || exit 1
run_hardened "cbk-ro-fresh-$SFX" "cbk-ro-fresh-$SFX" || exit 1
check_started "cbk-ro-fresh-$SFX" "fresh" "applying 009_journal_autoincrement.sql"

echo "read-only root, an M9 database at migration 008"
sqlite3 "state-at-008.$SFX.db" < "$FIXTURE" || exit 1
make_volume "cbk-ro-008-$SFX" "state-at-008.$SFX.db" || exit 1
run_hardened "cbk-ro-008-$SFX" "cbk-ro-008-$SFX" || exit 1
check_started "cbk-ro-008-$SFX" "from 008" "applying 009_journal_autoincrement.sql" \
              "applying $LAST_MIG"
case "$(docker logs "cbk-ro-008-$SFX" 2>&1)" in
    *"applying 008_id_reserve.sql"*) bad "from 008: 008 not applied again" ;;
    *) ok "from 008: 008 not applied again" ;;
esac

[ "$FAIL" -eq 0 ] && echo "image read-only check: passed" || echo "image read-only check: $FAIL failed"
[ "$FAIL" -eq 0 ]
