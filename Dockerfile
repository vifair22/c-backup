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

# c-utils is a sibling checkout in development. In CI it is cloned to a known
# path and CUTILS_DIR points at it, so the same Makefile serves both.
ARG CUTILS_REF=master
RUN git clone --depth 1 --branch "${CUTILS_REF}" \
        https://git.airies.net/vifair22/c-utils.git /c-utils \
    && make -C /c-utils

COPY . /build
RUN make CUTILS_DIR=/c-utils

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3 liblz4-1 libacl1 libsqlite3-0 \
        libmicrohttpd12 libcurl4 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /build/build/bins/c-backupd /usr/local/bin/c-backupd
COPY --from=build /build/build/bins/c-backup  /usr/local/bin/c-backup
COPY packaging/config.yaml /etc/c-backupd/config.yaml

# The socket directory has to exist before the daemon binds. systemd creates
# it outside a container; here it is the image's job.
RUN mkdir -p /run/c-backupd /var/lib/c-backupd && chmod 0700 /var/lib/c-backupd

ENV C_BACKUP_CONFIG_PATH=/etc/c-backupd/config.yaml
EXPOSE 8765
VOLUME ["/var/lib/c-backupd"]

# No HEALTHCHECK curl: the slim image has no curl and adding one to run a
# health check is more surface than the check is worth. Orchestrators can
# probe /healthz themselves.
ENTRYPOINT ["/usr/local/bin/c-backupd"]
