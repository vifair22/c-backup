# Two stages so the runtime image carries the binaries and their shared
# libraries, not a toolchain. Debian rather than Alpine because the daemon
# links against glibc, sqlite, openssl and microhttpd, and matching the
# deployment libc avoids a class of "works on my machine" that is tedious to
# debug in a container.
FROM debian:trixie AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential ca-certificates git pkg-config \
        libssl-dev liblz4-dev libacl1-dev libsqlite3-dev \
        libmicrohttpd-dev libcurl4-openssl-dev libcmocka-dev \
        libvirt-dev libxml2-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# c-utils is a sibling checkout in development. Here it is cloned to a known
# path and CUTILS_DIR points at it, so the same Makefile serves both. The ref
# comes from cutils_version rather than master: an image that silently picks
# up a different library than CI tested against is not the artefact CI
# approved.
COPY cutils_version /build/cutils_version
RUN git clone -q https://git.airies.net/vifair22/c-utils.git /c-utils \
    && git -C /c-utils checkout -q "$(tr -d '[:space:]' < /build/cutils_version)" \
    && make -C /c-utils

COPY . /build
# The image runs on whatever the deployment host is, which is not this
# builder, so it is built to the portable floor rather than the Makefile's
# znver3 default. See the MARCH comment in the Makefile.
#
# VARIANT=asan builds with AddressSanitizer (libasan linked statically, so the
# runtime image needs nothing extra) for testing on real data: slower, more
# memory, and a memory error stops the daemon with a report in its log
# rather than corrupting something quietly. The binaries keep their normal
# names in the image either way.
#
# VARIANT=tsan builds with ThreadSanitizer instead (it cannot be combined
# with ASan): data races and lock-order inversions are reported in the log
# and the daemon keeps running, so a race on real data leaves a report with
# both stacks rather than a corruption nobody can trace. Expect it to be
# several times slower and to use far more memory than release.
ARG VARIANT=release
RUN if [ "$VARIANT" = asan ] || [ "$VARIANT" = tsan ]; then \
        make CUTILS_DIR=/c-utils MARCH=x86-64-v2 MTUNE=generic "$VARIANT" \
        && mv "build/bins/c-backupd-$VARIANT" build/bins/c-backupd \
        && mv "build/bins/c-backup-$VARIANT" build/bins/c-backup \
        && mv "build/bins/c-backup-helper-$VARIANT" build/bins/c-backup-helper; \
    elif [ "$VARIANT" != release ]; then \
        echo "unknown VARIANT=$VARIANT (release, asan or tsan)" >&2; exit 1; \
    else \
        make CUTILS_DIR=/c-utils MARCH=x86-64-v2 MTUNE=generic; \
    fi

FROM debian:trixie-slim

# tzdata because calendar schedules fire in local time, and without the zone
# database TZ=America/Chicago silently means UTC: "--at 03:00" fired at 03:00
# UTC, 21:00 the previous evening on the host. The container still defaults
# to UTC; run it with -e TZ=<zone> to change that.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3 liblz4-1 libacl1 libsqlite3-0 \
        libmicrohttpd12 libcurl4 tzdata libvirt0 libxml2 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /build/build/bins/c-backupd /usr/local/bin/c-backupd
COPY --from=build /build/build/bins/c-backup  /usr/local/bin/c-backup
# The host helper ships in the same image and runs as a second container
# with its own entrypoint (docs/helper.md): one artefact to version, and the
# daemon and its helper always speak the same protocol.
COPY --from=build /build/build/bins/c-backup-helper /usr/local/bin/c-backup-helper
COPY packaging/config.yaml /etc/c-backupd/config.yaml
# 0600 in the image itself, the mode the daemon requires, so a read-only root
# filesystem never needs a chmod at start.
RUN chmod 0600 /etc/c-backupd/config.yaml
# Inside a container, loopback is the container's own: a published port
# reaches nothing bound there. The image binds every interface instead, and
# what is actually exposed is decided by -p at run time. Until an admin
# password is set (docker exec <container> c-backup auth setup) TCP serves
# only the health probes.
RUN sed -i 's/^  bind: 127.0.0.1$/  bind: 0.0.0.0/' /etc/c-backupd/config.yaml \
    && grep -q '^  bind: 0.0.0.0$' /etc/c-backupd/config.yaml

# The socket directory has to exist before the daemon binds. systemd creates
# it outside a container; here it is the image's job.
RUN mkdir -p /run/c-backupd /var/lib/c-backupd/keys \
    && chmod 0700 /var/lib/c-backupd /var/lib/c-backupd/keys

ENV C_BACKUP_CONFIG_PATH=/etc/c-backupd/config.yaml
# Repository keys live on the state volume, not at the systemd default of
# /etc/c-backupd/keys. That path is in the image's own filesystem, so
# replacing the container - an image upgrade, a recreate - threw away every
# key, and every encrypted repository behind it stopped backing up and
# restoring. An environment variable rather than a line in config.yaml
# because the environment wins over the file: an operator who mounts their
# own config.yaml without a security section still keeps keys on the volume.
ENV C_BACKUP_SECURITY_KEY_DIR=/var/lib/c-backupd/keys
# Read only by an asan build; harmless otherwise. Leak checks run at exit,
# which a SIGTERM stop now reaches through the daemon's own shutdown.
ENV ASAN_OPTIONS=detect_leaks=1:print_summary=1
# Read only by a tsan build. second_deadlock_stack gives both stacks of a
# lock-order inversion; history_size=4 keeps the other access of a race in
# the report on a long-running daemon instead of "failed to restore stack".
ENV TSAN_OPTIONS=second_deadlock_stack=1:history_size=4
EXPOSE 8765
# No VOLUME: the helper runs from this image too, and a VOLUME line made
# Docker create an anonymous volume at every helper start, which nothing
# removed (B205). The daemon's state is a named volume or a bind the run
# names (-v cbackup-state:/var/lib/c-backupd, Manual 2.3); an anonymous one
# never survived replacing the container anyway.

# No HEALTHCHECK curl: the slim image has no curl and adding one to run a
# health check is more surface than the check is worth. Orchestrators can
# probe /healthz themselves.
ENTRYPOINT ["/usr/local/bin/c-backupd"]
