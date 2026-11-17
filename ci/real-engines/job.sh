#!/bin/bash
#
# The real-engines job's driver: runs in the CI job (image docker:cli) and
# talks to the runner VM's engine through the bound socket. It starts ONE
# privileged sibling from the fixed image in this directory, which runs
# run.sh and nothing else, and makes sure the sibling goes away however the
# job ends.
#
# Three layers of reaping, because the job itself can die without running
# any of its own cleanup (a cancel, the job timeout, the runner restarting):
#
#   1. at start, any labelled sibling older than CBK_REAP_AFTER_MIN is
#      removed: it outlived every job that could own it;
#   2. a trap removes this job's sibling on every exit path;
#   3. the sibling runs under timeout(1) for CBK_LIFETIME_S and is created
#      with --rm, so an orphan stops and removes itself.
#
# Locally: CI_PIPELINE_ID and CI_JOB_ID default to "local", and the same
# script runs against the local engine.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

PIPELINE="${CI_PIPELINE_ID:-local}"
JOB="${CI_JOB_ID:-local-$$}"
LIFETIME="${CBK_LIFETIME_S:-2400}"          # the sibling's own hard limit
REAP_AFTER_MIN="${CBK_REAP_AFTER_MIN:-50}"  # older than this is an orphan
CPUS="${CBK_SIBLING_CPUS:-1}"
MEMORY="${CBK_SIBLING_MEMORY:-2560m}"
CUTILS_DIR="${CUTILS_DIR:-/tmp/c-utils}"
# Local runs only: a desktop's dockerd puts containers at the default CPU
# weight whatever slice the client is in; 2 shares is cgroup v2 weight 1.
CPU_SHARES="${CBK_SIBLING_CPU_SHARES:-}"
NAME="cbk-real-engines-$JOB"

log() { printf '[real-engines] %s\n' "$*"; }

# ---- 1. reap stale siblings -----------------------------------------------
# The creation time is in a label rather than parsed from docker's own
# timestamp, so the comparison is integer seconds with no date parsing.
now=$(date +%s)
docker ps -a --filter label=cbk.ci=real-engines --format '{{.ID}} {{.Label "cbk.ci.started"}} {{.Names}}' |
while read -r id started name; do
    case "$started" in ''|*[!0-9]*) started=0 ;; esac
    age_min=$(( (now - started) / 60 ))
    if [ "$age_min" -ge "$REAP_AFTER_MIN" ]; then
        log "reaping stale sibling $name ($id), ${age_min} min old"
        docker rm -f -v "$id" >/dev/null || true
    else
        log "leaving sibling $name ($id), ${age_min} min old: younger than ${REAP_AFTER_MIN} min"
    fi
done

# ---- the fixed image --------------------------------------------------------
# Tagged by the content of this directory, so an unchanged Dockerfile and
# run.sh is a cache hit on the runner; the tags it replaces are removed
# below.
TAG=$(cd "$HERE" && cat Dockerfile run.sh | sha256sum | cut -c1-16)
IMAGE="cbk-real-engines:$TAG"
log "image $IMAGE"
# Retried: BuildKit's session stream has been seen to drop mid-build ("error
# reading from server: EOF") with nothing wrong in the Dockerfile, and a
# rebuild then hits the cache.
built=0
for attempt in 1 2 3; do
    if docker build -q --label cbk.ci=real-engines -t "$IMAGE" "$HERE" >/dev/null; then built=1; break; fi
    log "image build attempt $attempt failed"
    sleep 5
done
[ "$built" = 1 ] || { log "cannot build $IMAGE"; exit 1; }
# Superseded tags keep their names, so they are never dangling and a prune
# alone would keep every one: remove each other cbk-real-engines tag. One in
# use by a live sibling refuses removal, which is the right answer.
docker images cbk-real-engines --format '{{.Repository}}:{{.Tag}}' | grep -vxF "$IMAGE" |
    while read -r old; do docker rmi "$old" >/dev/null 2>&1 && log "removed superseded image $old"; done || true
docker image prune -f --filter label=cbk.ci=real-engines >/dev/null || true

# ---- c-utils at the pinned version -----------------------------------------
if [ ! -d "$CUTILS_DIR/.git" ]; then
    git clone -q https://git.airies.net/vifair22/c-utils.git "$CUTILS_DIR"
fi
git -C "$CUTILS_DIR" fetch -q origin 2>/dev/null || true
git -C "$CUTILS_DIR" checkout -q "$(tr -d '[:space:]' < "$REPO_ROOT/cutils_version")"

# ---- 2. the sibling, removed on every exit path ----------------------------
cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    [ -n "${ATTACH_PID:-}" ] && kill "$ATTACH_PID" 2>/dev/null
    if docker inspect "$NAME" >/dev/null 2>&1; then
        log "removing sibling $NAME"
        docker rm -f -v "$NAME" >/dev/null 2>&1 || true
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

# --privileged: loop devices, mount, a nested dockerd, LXC and /dev/kvm.
# --network none: nothing in it needs the network; the registry and every
# engine socket are its own. --init: a real PID 1 that reaps and forwards
# signals, so timeout(1) and --stop-timeout behave. --rm (3. above).
# CPU and memory are capped well inside one runner slot so a run never
# starves the other concurrent jobs.
docker create --rm --init --privileged --network none \
    --cpus "$CPUS" --memory "$MEMORY" --memory-swap "$MEMORY" --pids-limit 4096 \
    --stop-timeout 30 \
    ${CPU_SHARES:+--cpu-shares "$CPU_SHARES"} \
    --label cbk.ci=real-engines \
    --label "cbk.ci.pipeline=$PIPELINE" \
    --label "cbk.ci.job=$JOB" \
    --label "cbk.ci.started=$(date +%s)" \
    -e "CBK_LIFETIME_S=$LIFETIME" \
    ${CBK_POOL_MB:+-e "CBK_POOL_MB=$CBK_POOL_MB"} \
    --name "$NAME" "$IMAGE" >/dev/null

# The tree as it is checked out (the merged result in an MR pipeline),
# without build output or git metadata: the sibling builds from scratch.
tar -C "$REPO_ROOT" --exclude=./build --exclude=./.git --exclude=./ui/node_modules \
    --exclude=./.claude -cf - . | docker cp - "$NAME:/src"
tar -C "$CUTILS_DIR" --exclude=./.git --exclude=./build -cf - . | docker cp - "$NAME:/c-utils"

log "starting $NAME (cpus $CPUS, memory $MEMORY, lifetime ${LIFETIME}s)"
# In the background and waited for: bash runs a trap only once the
# foreground command returns, so a cancel arriving during a foreground
# `docker start -a` would wait for the whole run before removing anything.
# `wait` returns as soon as a trapped signal arrives.
set +e
docker start -a "$NAME" &
ATTACH_PID=$!
wait "$ATTACH_PID"
rc=$?
ATTACH_PID=
set -e
log "sibling exited $rc"
exit "$rc"
