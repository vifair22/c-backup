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
ARG VARIANT=release
RUN if [ "$VARIANT" = asan ]; then \
        make CUTILS_DIR=/c-utils MARCH=x86-64-v2 MTUNE=generic asan \
        && mv build/bins/c-backupd-asan build/bins/c-backupd \
        && mv build/bins/c-backup-asan build/bins/c-backup; \
    else \
        make CUTILS_DIR=/c-utils MARCH=x86-64-v2 MTUNE=generic; \
    fi

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3 liblz4-1 libacl1 libsqlite3-0 \
        libmicrohttpd12 libcurl4 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /build/build/bins/c-backupd /usr/local/bin/c-backupd
COPY --from=build /build/build/bins/c-backup  /usr/local/bin/c-backup
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
RUN mkdir -p /run/c-backupd /var/lib/c-backupd && chmod 0700 /var/lib/c-backupd

ENV C_BACKUP_CONFIG_PATH=/etc/c-backupd/config.yaml
# Read only by an asan build; harmless otherwise. Leak checks run at exit,
# which a SIGTERM stop now reaches through the daemon's own shutdown.
ENV ASAN_OPTIONS=detect_leaks=1:print_summary=1
EXPOSE 8765
VOLUME ["/var/lib/c-backupd"]

# No HEALTHCHECK curl: the slim image has no curl and adding one to run a
# health check is more surface than the check is worth. Orchestrators can
# probe /healthz themselves.
ENTRYPOINT ["/usr/local/bin/c-backupd"]
