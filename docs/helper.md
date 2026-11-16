# c-backup-helper: the host helper

`c-backup-helper` is the privileged half of unit backup (option B of
`source-providers.md` section 11, decided 2026-11-16). It holds what is
root-equivalent on the host (sockets, devices, capabilities, the host's own
`zfs`, `lxc-*` and libvirt) and offers the daemon a fixed list of typed
operations over a unix socket. The daemon, which faces the network, holds
none of it.

What this build has: the protocol, the policy, the leases and watchdogs, the
peer check, fd passing for dump hooks, restore staging and swap, and two
backends: the fake provider (tests only) and `dir_domains` (a directory
"snapshot domain" for tests and for `capture.begin_paths`). libvirt, Docker
and LXC operations are validated and held to the policy and then answered
`unsupported` until those providers exist.

## Deployment (Docker, Unraid)

One image carries both binaries. Two containers run from it.

```sh
# The helper: privileged, no network, the host's pid namespace for nsenter.
docker run -d --name c-backup-helper --restart unless-stopped \
  --privileged --pid=host --network=none \
  -v /boot/config/c-backup-helper.ini:/etc/c-backup-helper.ini:ro \
  -v /mnt/cache/system/c-backup-helper:/mnt/cache/system/c-backup-helper \
  -v /run/c-backup-helper:/run/c-backup-helper \
  --entrypoint /usr/local/bin/c-backup-helper \
  c-backup:latest --policy /etc/c-backup-helper.ini

# The daemon: starts as root with only the capabilities it needs, drops to
# uid 990 itself, and sees the helper's socket and nothing of the host's.
docker run -d --name c-backupd --restart unless-stopped \
  --cap-drop ALL \
  --cap-add DAC_READ_SEARCH --cap-add DAC_OVERRIDE --cap-add CHOWN \
  --cap-add FOWNER --cap-add FSETID --cap-add MKNOD \
  --cap-add SETUID --cap-add SETGID --cap-add SETPCAP \
  -e C_BACKUP_SECURITY_RUN_AS_UID=990 \
  -e C_BACKUP_UNITS_HELPER_SOCKET=/run/c-backup-helper/helper.sock \
  -v /run/c-backup-helper:/run/c-backup-helper \
  -v /mnt/user/appdata:/mnt/user/appdata:ro,rslave \
  -v /mnt/disk3/backups:/repos \
  -v /mnt/cache/appdata/c-backupd:/var/lib/c-backupd \
  -p 8765:8765 \
  c-backup:latest
```

Compose, equivalently:

```yaml
services:
  c-backup-helper:
    image: c-backup:latest
    entrypoint: ["/usr/local/bin/c-backup-helper", "--policy", "/etc/c-backup-helper.ini"]
    privileged: true
    pid: host
    network_mode: none
    restart: unless-stopped
    volumes:
      - /boot/config/c-backup-helper.ini:/etc/c-backup-helper.ini:ro
      - /mnt/cache/system/c-backup-helper:/mnt/cache/system/c-backup-helper
      - /run/c-backup-helper:/run/c-backup-helper
  c-backupd:
    image: c-backup:latest
    depends_on: [c-backup-helper]
    restart: unless-stopped
    cap_drop: [ALL]
    cap_add: [DAC_READ_SEARCH, DAC_OVERRIDE, CHOWN, FOWNER, FSETID, MKNOD, SETUID, SETGID, SETPCAP]
    environment:
      C_BACKUP_SECURITY_RUN_AS_UID: "990"
      C_BACKUP_UNITS_HELPER_SOCKET: /run/c-backup-helper/helper.sock
    volumes:
      - /run/c-backup-helper:/run/c-backup-helper
      - /mnt/user/appdata:/mnt/user/appdata:ro,rslave
      - /mnt/disk3/backups:/repos
      - /mnt/cache/appdata/c-backupd:/var/lib/c-backupd
    ports: ["8765:8765"]
```

Notes that matter:

- **The policy lives on the flash** (`/boot/config/`), root-owned, mode 0600
  or 0644. The helper refuses to start on a file anybody but its owner can
  write, an unknown section or key, or a value out of range. Unraid's root
  filesystem is RAM, so nothing installed on the host survives a reboot; the
  sidecar is the install, autostarted by Docker.
- **The helper's state directory is on a pool**, never on the RAM rootfs:
  `holds.json` records every hold it has taken, and its next start releases
  what a crash left held.
- **The socket directory** `/run/c-backup-helper` should be `0750
  root:990`; the helper creates the socket `0660`. On every connection the
  helper reads `SO_PEERCRED` and refuses any uid but the policy's
  `daemon_uid`, so the socket is safe to bind-mount into the daemon and
  nowhere else.
- **Why SETUID, SETGID and SETPCAP on the daemon.** Docker gives a non-root
  `--user` none of the capabilities added with `--cap-add`, so the daemon
  starts as root and drops itself (`security.run_as_uid`). The drop needs
  those three for a moment: SETPCAP to empty the bounding set of everything
  but the six, SETGID and SETUID to change ids. Once it returns, none of the
  three is in any set, `no_new_privs` is on, and only
  `CAP_DAC_READ_SEARCH` is effective; restores raise the other five on their
  own thread and drop them after (`src/common/privs.h`). This is a
  correction to section 11.6, which listed the six alone: without
  SETUID/SETGID the drop cannot happen at all.
- **Ownership.** The state volume and repositories must be owned by 990:
  AppGuard opens the config and `state.db` before the drop, so a database
  created as root would be unwritable by the daemon after it.
- **Start order** does not matter, and neither does a helper restart. The
  daemon's link to the helper retries with a bounded backoff (0.5 s
  doubling to 30 s) while the helper is away, says hello again when it
  answers (logging a policy whose digest changed), and enables its
  providers then, with no daemon restart. While connected it checks every
  15 s. A request that finds the helper gone marks the link down at once.
- **A unit job while the helper is away fails and alerts**, scheduled or
  not, with the socket path and the reason in the error, the journal and
  the push alert; it is never skipped quietly. A job in flight when the
  helper restarts fails cleanly (no snapshot, the helper's own start-up
  sweep releases what the old process held), and the next one reconnects.
  `GET /providers` shows the link (`helper.state`: `connected`,
  `disconnected` or `never-connected`, `since`, `last_error`,
  `reconnects`) and, per provider behind it, `connection.state`.

## The policy

A line starting with `#` or `;` is a comment, and so is `#` after
whitespace at the end of a value, as in the example; an `argv.N` value is
taken verbatim, `#` included.

```ini
[helper]
daemon_uid = 990              # the only peer uid accepted (non-root)
daemon_id = 7f3a09c1          # the daemon id artefacts are named with
max_pause_s = 30              # watchdog bound on freezes and pauses
max_captures_per_hour = 60    # rate limit, captures of either kind
capture_max_s = 86400         # lease: a capture nobody ends is destroyed
allow_restore = 1             # restore.stage, restore.swap, unit.stop/start
allow_define = 0
allow_privileged_define = 0
state_dir = /mnt/cache/system/c-backup-helper
socket = /run/c-backup-helper/helper.sock
# fake_spec = /path/units.json   (tests only: enables the fake provider)

[units]
allow = docker:compose:immich, docker:cbk-uat-*, libvirt:cbk-uat-*, lxc:cbk-uat-*

[snapshot]
zfs_domains = cache/appdata, cache/Photos
btrfs_domains =
dir_domains =                  # directory domains (tests, begin_paths)

[hook immich-pg_dumpall]
unit = docker:compose:immich
kind = dump                    # dump: stdout comes back as a pipe fd; run: discarded
phase = pre
container = immich_postgres
argv.0 = pg_dumpall
argv.1 = --clean
timeout_s = 900
on_fail = continue
```

Allow globs match `<provider>:<id>` and `<provider>:<name>`, the name from
the helper's own inspect. `c-backup-helper --policy PATH --check` validates a
policy and prints its SHA-256, the digest the helper's `hello` reports and
`GET /providers` shows.

## The protocol

`AF_UNIX`, `SOCK_SEQPACKET`, one request and one response per packet of at
most 64 KiB, each one UTF-8 JSON object (`source-providers.md` 11.2):

```json
{"v":1,"id":17,"op":"capture.begin","args":{"units":["fake:u1"],"pause":true}}
{"id":17,"ok":true,"result":{"capture":"<32 hex>","members":[...],"snapshot_ms":3,"expires":...}}
{"id":17,"ok":false,"error":{"code":"denied","msg":"unit fake:prod is not in the policy"}}
```

The first request on a connection must be `hello`. Error codes are the
closed set `denied`, `invalid`, `busy`, `timeout`, `failed`, `unsupported`.
An operation outside the list is `denied`; a request with an unknown or
missing key, a wrong type, an out-of-range value, a malformed unit key or an
unclean path is `invalid` before any operation sees it. The operations are
the 19 of section 11.3 (`hello`, `units.list`, `units.describe`,
`capture.begin`, `capture.begin_paths`, `capture.end`, `hook.run`,
`hook.wait`, `vm.backup_begin`, `vm.backup_end`, `vm.checkpoint_redefine`,
`vm.install_state`, `restore.stage`, `restore.swap`, `unit.stop`,
`unit.start`, `unit.define`, `lease.release`, `sweep`).

The daemon (`src/providers/remote_provider.c`) opens one connection per
request, says hello, sends it, and closes, so concurrent jobs never queue
behind each other and a helper restart costs nothing. Data never crosses the
socket: the daemon reads trees through the read roots the helper returns,
and disks through its own mounts (for the fake, its run map).

## What a compromised daemon gets

Exactly section 11.5's table. `tests/test_helper.c` asserts it: with every
operation spoken correctly, only allowlisted operations on units and domains
the policy allows succeed; tokens it did not get from the helper are
`invalid`; a sweep it asks for never removes an artefact the helper cannot
prove is its own.

## Known limits in this build

- A BLOB is returned in `units.describe` as hex inside the 64 KiB packet, so
  one larger than about 30 KiB is `failed`. UEFI NVRAM (128 KiB to 4 MiB)
  needs the daemon to read the file through its own mount, or an fd.
- `capture.begin_paths` and `restore.stage` over `dir_domains` take no real
  snapshot; ZFS and btrfs domains come with the container provider.
- `pause: false` is passed to the backend as the wanted level; the fake
  pauses whenever the unit can.
