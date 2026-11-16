#!/bin/bash
#
# Run one test suite under a scratch budget.
#
#   tests/scratch-guard.sh <label> <command> [args...]
#
# Every suite writes its scratch under C_BACKUP_TEST_DIR (see the Makefile
# and tests/test_tmp.h). This runs the suite, polls that directory's
# allocated size every half second, and stops the suite and fails if it goes
# more than C_BACKUP_SCRATCH_BUDGET past where it started. CI runners share a
# disk across concurrent jobs: one test that wrote a 129 GiB sparse fixture
# out densely filled it for every job on the box, and nothing in the failing
# job's log said which test had done it. This names the suite.
#
# du counts allocated blocks, so a sparse fixture costs what it really
# occupies, which is exactly the property that went wrong. Suites run one at
# a time, so the directory's growth is this suite's alone.
#
# Also printed: the suite's name and start time before it runs, its peak
# scratch afterwards, and anything it left behind. Leftovers are removed and
# fail the suite, because a suite that does not clean up on the way out
# leaves its bytes to the next one and to the next job on the runner.
#
# Environment:
#   C_BACKUP_TEST_DIR        the scratch directory (required)
#   C_BACKUP_SCRATCH_BUDGET  bytes, or with a K/M/G suffix; default 10G
#   C_BACKUP_SCRATCH_LOG     optional file to append "<label> <peak bytes>"
#   C_BACKUP_SCRATCH_QUIET   1 to discard the suite's own output, keeping
#                            this script's lines (make coverage)
#
# Exit status: the suite's, or 99 when it went over budget, so a caller that
# tolerates failing suites (make coverage) can still fail on that.

set -u

if [ $# -lt 2 ]; then
    echo "usage: $0 <label> <command> [args...]" >&2
    exit 2
fi
label=$1; shift

dir=${C_BACKUP_TEST_DIR:-}
if [ -z "$dir" ]; then
    echo "scratch-guard: C_BACKUP_TEST_DIR is not set" >&2
    exit 2
fi

to_bytes() {
    local v=$1 n
    case $v in
        *[Kk]) n=${v%?}; echo $((n * 1024)) ;;
        *[Mm]) n=${v%?}; echo $((n * 1024 * 1024)) ;;
        *[Gg]) n=${v%?}; echo $((n * 1024 * 1024 * 1024)) ;;
        *)     echo $((v)) ;;
    esac
}
budget=$(to_bytes "${C_BACKUP_SCRATCH_BUDGET:-10G}") || exit 2
if [ "$budget" -le 0 ]; then
    echo "scratch-guard: bad budget '${C_BACKUP_SCRATCH_BUDGET:-}'" >&2
    exit 2
fi

human() {
    awk -v b="$1" 'BEGIN {
        split("B KiB MiB GiB TiB", u, " "); i = 1
        while (b >= 1024 && i < 5) { b /= 1024; i++ }
        printf (i == 1 ? "%d %s" : "%.1f %s"), b, u[i]
    }'
}

# Files vanish mid-walk while a suite runs; du complains and still totals
# what it found, which is the number wanted.
usage() { du -s -B1 "$dir" 2>/dev/null | cut -f1; }

mkdir -p "$dir" || exit 2
base=$(usage); base=${base:-0}
before=$(mktemp) || exit 2
ls -A "$dir" > "$before"

printf '=== [%s] %s ===\n' "$(date +%H:%M:%S)" "$label"

# Its own session, so stopping it reaches everything it started: the shell
# suites run daemons, and a daemon left writing is the problem itself.
if [ "${C_BACKUP_SCRATCH_QUIET:-0}" = 1 ]; then
    setsid "$@" >/dev/null 2>&1 &
else
    setsid "$@" &
fi
pid=$!

stop() {
    kill -TERM -- "-$pid" 2>/dev/null
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.5
    done
    kill -KILL -- "-$pid" 2>/dev/null
}
trap 'stop; rm -f "$before"; exit 130' INT
trap 'stop; rm -f "$before"; exit 143' TERM

peak=0
over=0
while kill -0 "$pid" 2>/dev/null; do
    u=$(usage); u=${u:-0}
    d=$((u - base))
    [ "$d" -gt "$peak" ] && peak=$d
    if [ "$d" -gt "$budget" ]; then
        over=1
        stop
        break
    fi
    sleep 0.5
done
wait "$pid"
rc=$?
# Anything it started and left running would go on writing unwatched.
kill -TERM -- "-$pid" 2>/dev/null

# What the suite created and did not remove. Only names that were not there
# before it started: the directory may be shared with something else, and
# this must never delete what it did not see appear.
left=$(ls -A "$dir" | grep -vxF -f "$before")
rm -f "$before"
lbytes=0
if [ -n "$left" ]; then
    lbytes=$(cd "$dir" && printf '%s\n' "$left" | tr '\n' '\0' |
             xargs -0 du -s -B1 -c 2>/dev/null | tail -1 | cut -f1)
    (cd "$dir" && printf '%s\n' "$left" | tr '\n' '\0' | xargs -0 rm -rf --one-file-system)
fi

printf 'scratch: %s peak %s\n' "$label" "$(human "$peak")"
[ -n "${C_BACKUP_SCRATCH_LOG:-}" ] && echo "$label $peak" >> "$C_BACKUP_SCRATCH_LOG"

if [ "$over" -eq 1 ]; then
    echo "scratch-guard: $label used more than its $(human "$budget") scratch budget in $dir; stopped" >&2
    exit 99
fi
if [ -n "$left" ]; then
    echo "scratch-guard: $label left $(human "${lbytes:-0}") behind in $dir (removed):" >&2
    printf '  %s\n' $left >&2
    [ "$rc" -eq 0 ] && rc=1
fi
exit "$rc"
