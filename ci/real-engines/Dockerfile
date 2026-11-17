# The real-engines sibling (see run.sh and the real-engines job in
# .gitlab-ci.yml).
#
# A fixed image: the build dependencies plus every engine the unit providers
# drive, and nothing of the source. The job copies the tree in and the
# container runs only run.sh, so this file changes when the toolchain or an
# engine does, and the runner's cache serves it otherwise.
#
# Everything comes from Debian rather than upstream repositories so one apt
# mirror is the only thing the build reaches, and the sibling itself runs
# with no network at all.
FROM debian:trixie

LABEL cbk.ci=real-engines

ENV DEBIAN_FRONTEND=noninteractive

# Build: the same set as the CI test job and the release Dockerfile.
# Engines: docker.io is the Docker engine the dind daemon runs, docker-cli
# its CLI (a separate package in trixie, and only a Recommends of docker.io);
# docker-compose is the compose plugin unit.define calls for a project;
# docker-registry is a local registry, so a test image has a digest that a
# define can pull back with no network. busybox-static makes that image from
# nothing, and is the LXC template's busybox too. libvirt-daemon-driver-qemu
# is libvirtd's QEMU driver (Debian 13 has no separate virtqemud); ovmf is
# not needed (the test VMs boot SeaBIOS into nothing).
RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends \
        build-essential ca-certificates git pkg-config \
        libssl-dev liblz4-dev libacl1-dev libsqlite3-dev \
        libmicrohttpd-dev libcurl4-openssl-dev libcmocka-dev \
        libvirt-dev libxml2-dev \
        docker.io docker-cli docker-compose docker-registry \
        btrfs-progs e2fsprogs util-linux procps iproute2 kmod \
        lxc lxc-templates busybox-static \
        libvirt-daemon libvirt-daemon-driver-qemu libvirt-clients \
        qemu-system-x86 qemu-utils seabios \
        curl jq sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Built without a uid of its own: the daemon starts as root and drops to
# 990 itself, as in production (docs/helper.md), so the drop is exercised too.
RUN groupadd -g 990 cbackup && useradd -u 990 -g 990 -M -d /nonexistent -s /usr/sbin/nologin cbackup

# The job copies the tree and c-utils into these with docker cp.
RUN mkdir -p /src /c-utils

COPY run.sh /usr/local/bin/cbk-real-engines
RUN chmod 0755 /usr/local/bin/cbk-real-engines

ENTRYPOINT ["/usr/local/bin/cbk-real-engines"]
