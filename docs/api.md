# HTTP API

Two transports, one API.

- **Unix socket** (`http.socket`) bypasses authentication entirely. Filesystem
  permissions on the socket are the access control: if you can open it, you
  are authorised. This is what the CLI uses.
- **TCP** (`http.port`) is for remote clients, chiefly the desktop app on
  another machine. It binds loopback unless `http.bind` says otherwise, and
  requires `Authorization: Bearer <token>` (the `Bearer ` prefix is optional)
  or an `auth` cookie, except for the public routes below. The transport is
  plain HTTP; put a reverse proxy or a tunnel in front on an untrusted network.

The first admin password is set over the socket, on the daemon's host:
`c-backup auth setup`. Until one exists, TCP serves only the public routes
and answers everything else with `403` and a message saying so. Setting it
over TCP is refused outright: a port that accepted it would belong to
whoever reached it first.

There are no CORS headers. No client of this API is a browser page, and a
wildcard origin would let any page open in a browser on the host read it.

## Conventions

Every collection route pages with `offset` and `limit`, and there is a
server-side maximum. This is not stylistic: returning a 10,000-entry directory
row by row measured 2304 us against 175 us for the pre-rewrite in-memory scan,
while the first page through the index costs 93 us and beats it. The
regression only exists if the API offers a way to ask for everything at once,
so it does not. **No route returns a whole snapshot.**

`limit` and `offset` must be plain decimal counts: `limit` 1 or more,
`offset` 0 or more. Anything else (`-1`, `0` for a limit, `+5`, `lots`) is a
400 naming the parameter; they used to be replaced by the defaults, which
answered a request the server had not understood as if it had. A `limit`
above a route's maximum is clamped to it. The same goes for filters with a
fixed set of values: `objects?location=` takes `loose`, `pack` (or
`packed`) or `all`, and anything else is a 400 rather than an unfiltered
listing.

Directory listings are ordered by name because the index carries `name` as its
trailing column, making the query a range scan rather than a sort of every
child.

Long operations return `202` with a job id rather than blocking. A backup can
run for hours, and a request that lives that long dies to any timeout between
client and daemon.

## Public

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | Liveness: the process answers |
| GET | `/readyz` | Readiness: the database is open |
| GET | `/metrics` | Prometheus text exposition |
| GET | `/api/auth/setup` | Is an admin password configured? |

Health and metrics are public deliberately: an endpoint that needs a
credential cannot report a daemon too unhealthy to authenticate, and a client
asks `/readyz` before it has logged in. Metrics carry counts and ages, never
paths or file content.

## Auth

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/auth/setup` | `{password}`: set the first one. Socket only (`403` over TCP), `409` once set |
| POST | `/api/auth/login` | `{password}` returns `{token, expires_in}` and sets the `auth` cookie. `403` before setup |
| POST | `/api/auth/logout` | Revoke the presented token; `204` |
| POST | `/api/auth/logout-all` | Revoke every session, the caller's included; `{revoked}` |
| POST | `/api/auth/password` | `{current, new}`: always needs the current password; returns `{changed, sessions_revoked}` |

Passwords are at least 8 characters. Sessions last 90 days and slide forward
on every authenticated request.

A password change revokes every session except the one that made it (over
the socket there is none, so all of them). It used to revoke nothing, which
left whoever the change was meant to shut out holding a token that kept
working - and kept sliding forward - for 90 days. `logout-all` is the same
revocation without a password change, for a lost or untrusted client; from
the host it is `c-backup auth logout --all`.

### Login throttle

Failed logins over TCP are throttled; before this, 100 wrong passwords took
2.66 s and the right one still worked straight afterwards.

| Rule | Value |
|---|---|
| Free failures per client address | 5 |
| Lockout after each further failure | 1 s, doubling each time (1, 2, 4, ...), at most 300 s |
| Address forgotten after | 900 s without a failure, or at once on a successful login |
| Global cap, all addresses together | a bucket of 60 failures draining one every 2 s; while full, every TCP login is refused |
| Addresses remembered | 256; when full, the one that failed longest ago is dropped |

A refused attempt is `429 {"error":"too many failed logins; retry in N
seconds"}` with a `Retry-After` header. It is refused before the password is
hashed, so it reveals nothing and costs nothing, and the correct password is
refused too while the lockout lasts - a throttle that let the right answer
through would not slow guessing at all. A refused attempt does not extend
the lockout.

The address is the TCP peer. Behind a reverse proxy every client shares the
proxy's address and so one budget; `X-Forwarded-For` is not trusted, because
it is whatever the client sent. The global cap can lock the owner out during
an attack as well as the attacker. The unix socket is never throttled, so the
host always has a way in.

## Repositories

| Method | Path | Purpose |
|---|---|---|
| GET | `/repos` | List |
| POST | `/repos` | Register: `{path, label}` |
| GET | `/repos/<id>` | One repository , including `encryption` and `key_escrowed` |
| DELETE | `/repos/<id>` | Deregister; the data directory is left alone |

`POST /repos` normalises the path (no `//`, `.`, `..` or trailing slash) and
stores that form. It answers:

| Status | Body | When |
|---|---|---|
| 201 | `{id}`, plus `encryption` and key fields for an encrypted repository | a new registration: a repository created, or an existing one adopted |
| 200 | `{id, already_registered: true, label_updated, encryption}` | the repository was already registered, at this path or, if it moved, at its old one |
| 400 | `{error}` | the path is relative, cannot be created, is inside or contains a registered repository, is a non-empty directory with no marker, or asks for an encryption mode the existing repository does not have |

Re-registering is a success rather than a 409 because it is how a moved
repository re-links. A non-empty `label` that differs from the stored one is
applied (`label_updated: true`); an absent or empty one leaves it alone.

## Snapshots and browsing

| Method | Path | Purpose |
|---|---|---|
| GET | `/repos/<id>/snapshots` | List, paged |
| GET | `/repos/<id>/snapshots/<sid>/tree?parent=<node>` | One directory level |
| GET | `/repos/<id>/search?q=<text>[&snapshot=<sid>]` | Substring filename search |
| DELETE | `/repos/<id>/snapshots/<sid>` | Prune |

Search goes through an FTS5 trigram index. A B-tree cannot serve
`LIKE '%needle%'`, and scanning the node table loses to the pre-rewrite
implementation by 2.7x; the trigram index wins by 50.9x.

`tree` answers 404 for a snapshot that does not exist and for a `parent`
that is not a node of that snapshot; so do `packs/<pid>/entries` for a
missing pack, `verify/<run-id>` and `grep/<run-id>` for a missing run. All
of them answered an empty list, which is what an empty directory, an empty
pack and a clean scrub look like.

Without `snapshot`, every snapshot is searched and identical copies are
collapsed: one result per (path, content hash), carrying the newest
snapshot it was seen in (`snap_id`, `node_id`) and how many snapshots hold
it (`snapshots`). A file unchanged across thirty snapshots used to come back
thirty times. A file whose content changed is a different hash and a
separate result, so the versions of a file stay visible. To keep a page
cheap the search reads at most eight matching rows per result asked for
(8,000 at most) and does not sort; when it stopped there, `partial` is
`true` and the counts cover what was read. With `snapshot=<sid>` only that
snapshot is searched (`snapshots` is 1); one that does not exist is a 404.
The CLI is `search <id> <text> [--snapshot N]`.

## Encryption

A repository is encrypted or not, decided at creation and never afterwards.
Converting in place would mean rewriting every object with both forms live;
replicating into a fresh repository is the supported path instead.

`POST /repos` takes `encrypt` (`none`, `system`, `passphrase`) and, for the
last, a `passphrase`. A non-empty `passphrase` with no `encrypt` means
`passphrase` mode. A non-empty `passphrase` with `encrypt` `none` or `system`
is refused with 400 rather than ignored: ignoring it used to create an
unencrypted repository for somebody who had just typed a passphrase. An empty
`passphrase` is treated as absent.

| Mode | Key lives | Recovery needs | Escrow |
|---|---|---|---|
| `system` (recommended default) | host keyfile only | that key | the API returns it once |
| `passphrase` | wrapped in the repository, cached in a host keyfile | the repository and the passphrase | nothing to do |

The daemon holds the key on the host either way, because a scheduler that
stops for a prompt is not a scheduler.

**A system-managed key exists in exactly one place: a file on the machine
being backed up.** Lose that machine and every offsite copy of its repository
is unreadable, which is the disaster the offsite copy existed for. So the key
is returned once at creation and the repository is counted by
`cbackup_repos_key_unescrowed` until someone confirms they have stored it
elsewhere, with a warning on every backup and a journal entry to match. This
is deliberately a persistent nag rather than a message shown once.

| Method | Path | Purpose |
|---|---|---|
| GET | `/repos/<id>/key` | Export the key for escrow |
| POST | `/repos/<id>/key/import` | Install a key on a host that lacks one: the recovery path after losing the original machine. Refused unless it matches the fingerprint in the marker |
| POST | `/repos/<id>/key/ack` | Confirm the key is stored elsewhere; clears the warning |

The marker records the mode, cipher and a one-way fingerprint of the key, and
for passphrase mode the Argon2id parameters and wrapped key. It never contains
the key itself. Markers are version 2 for every repository, encrypted or
not, so a build predating encryption refuses them outright rather than
writing plaintext objects into an encrypted one.

Keys are ChaCha20-Poly1305, chosen over AES-GCM because ARM64 is a deploy
target and it does not depend on AES-NI. `security.key_dir` sets where host
keyfiles live (default `/etc/c-backupd/keys`, one file per repository UUID,
mode 0600). It is file-backed configuration rather than runtime-settable: a
key directory changeable through the API would let an attacker with API access
point the daemon at keys they control.

What this protects is the repository wherever it travels. What it does not
protect is the machine running the daemon, which holds the key and a
plaintext index; that is the operating system's job, through full-disk
encryption and ordinary access control.

## The CLI covers all of this

`c-backup` is a thin client over these endpoints and deliberately exposes
every one of them. A capability reachable through the API but not the CLI
means an operator has to hand-write a `curl --unix-socket` call, or open the
desktop app, to run their own backup tool - which is the friction the daemon
rewrite existed to remove. When an endpoint is added, a command goes with it,
and `tests/test_cli_e2e.sh` exercises the whole table.

The desktop app is the second client and is held to the same rule. Its
requests are one closed union in `ui/src/shared/api.ts`, translated to method
and path in exactly one place (`ui/src/main/client.ts`), and
`ui/tests/contract.test.ts` starts a real daemon and asks it whether every
request the app can build names a route that exists.

That suite exists because the app spent four months posting backups to
`/repos/<id>/run`, which the daemon has never served. The path was built by
interpolating an action string, so nothing at build time could object, and
nothing at run time was looking. When an endpoint is added, a variant goes in
the union and a sample goes in the contract suite - the union is typed as a
`Record` over every kind, so omitting the sample does not compile.

Replication is deliberately absent from the app. The engine exists and the
CLI drives it; exposing a capability that writes a repository to an arbitrary
path is a decision to take on purpose rather than by completing a table.

`c-backup help` lists the commands. Flags may appear anywhere in a command,
values are JSON-escaped and query parameters percent-encoded, so a path
containing a quote, a space or a `#` is sent correctly rather than producing
a malformed request.

Every collection response carries its key even when it is empty, so a client
can iterate the array unconditionally. The array builder creates the key
lazily on first append, which used to leave an empty result serialising as
`{}` and made the response shape depend on whether data happened to exist.

## Operations

| Method | Path | Purpose |
|---|---|---|
| POST | `/repos/<id>/backup` | `{paths:[...], label, exclude:[...], one_filesystem}`; with no `paths`, the repository's configured sources are used |
| GET/PUT | `/repos/<id>/sources` | What this repository backs up: `{sources:[...], exclude:[...]}` |
| POST | `/repos/<id>/snapshots/<sid>/restore` | `{dest, overwrite, subpath}` |
| POST | `/repos/<id>/gc` | Mark and sweep; `?dry_run=1` to report only |
| POST | `/repos/<id>/gfs` | Apply retention |
| POST | `/repos/<id>/reindex` | Rebuild the index from the repository |
| POST | `/repos/<id>/pack` | Coalesce loose objects into pack files |
| POST | `/repos/<id>/grep` | Content search as a job: `{pattern, snapshot, path_prefix, ignore_case, max_files}` |
| GET | `/repos/<id>/grep` | Recent searches, newest first |
| GET | `/repos/<id>/grep/<run-id>` | The hits from one search, paginated |
| POST | `/repos/<id>/verify` | Scrub: prove stored objects still read back. `?level=index\|structure\|deep` (default `deep`), `?repair=0` to check without writing |
| GET | `/repos/<id>/verify` | Recent verify runs, newest first; `?limit=N` (default 20) |
| GET | `/repos/<id>/verify/<run-id>` | The findings from one run, paginated |
| POST | `/repos/<id>/replicate` | Copy this repository into another one: `{dest}` |

`subpath` restores one path out of the snapshot instead of all of it, so
pulling a single file back does not cost a full restore. It accepts the path
either as it appears in the browser or without its leading slash, and matches
on path boundaries, so `/src/doc` does not also pull in `/src/documents`.
It is normalised first: repeated slashes collapse and `.` and `..` resolve,
so `/src//doc/./` names the same directory as `/src/doc`.

A restore request is refused before any job is created when it cannot do
what was asked:

| Response | When |
|---|---|
| `400 dest must be an absolute path` | `dest` is relative; it would resolve against the daemon's working directory |
| `400 dest is inside the repository at <path>` | `dest`, compared as a canonical path so a symlink or `..` cannot route around it, is a registered repository or lies beneath one |
| `404 subpath <p> matches nothing in snapshot <n>` | `subpath` names nothing in the snapshot; the index answers this with one lookup per path component |

A snapshot whose nodes are not resident cannot be checked up front, so for
that one a subpath that matches nothing fails the job with the same message
instead of reporting a restore that wrote nothing.

`exclude` takes patterns, and how one is matched follows from what it
contains rather than from a flag:

| Pattern | Matches |
|---|---|
| `/var/cache` | that directory and its subtree, on component boundaries, so not `/var/cache-manager` |
| `/home/*/Downloads` | absolute with wildcards, `*` stopping at `/` |
| `build/tmp` | any component-aligned tail of a path, at any depth |
| `node_modules`, `*.iso` | the basename, at any depth |
| anything containing `**` | as above, but wildcards may cross `/` |

A trailing slash is accepted and ignored, so `node_modules/` and
`node_modules` mean the same thing. An absolute pattern is normalised
lexically before matching: repeated slashes collapse and `.` and `..`
resolve, so `/a/../a/b` and `/a//b` exclude what `/a/b` does. This is done
on the text, not the filesystem, so it does not follow symlinks. Directories are tested before the walk
descends, so excluding one prunes its subtree rather than filtering entries
on the way out.

A backup in which every source is excluded, or none could be read, fails
with "nothing to back up" and writes no snapshot. It used to succeed with a
snapshot holding only the directories above each source, which looks like a
backup and restores nothing.

Before this, the matcher accepted absolute prefixes only and silently
skipped everything else, and the route did not read the field at all: a
request could carry any exclusions it liked and back up the whole tree
regardless. `one_filesystem` was in the same position, which is the
difference between backing up a home directory and backing up every network
mount under it.

`grep` searches file *contents*, which is what `/search` does not do: that
one matches names. It runs as a job because reading a repository through a
regular expression takes minutes, and it runs in the daemon because the
daemon is the only side that can resolve an object - and once repositories
are encrypted, the only side that can read one.

Matching is POSIX extended regular expressions, line by line, over every
distinct object in the snapshot. Distinct, not every path: a file present
under several names is one object, so it is read once and reported once
rather than re-read per copy. Objects are visited in pack order for the same
reason verify is. A file with a NUL byte in its first 8 KiB is a binary: it
gets one hit, excerpt `(binary file matches)`, and is not read further, the
way `grep(1)` reports one. Three limits bound the rest:

| Field | Default | Range | Effect |
|---|---|---|---|
| `max_files` | 0 (none) | 0-1000000 | stop after this many matching files |
| `max_hits_per_file` | 100 | 1-10000 | stop reading a file once it has this many hits |
| `max_hits` | 10000 | 1-1000000 | stop the search once this many hits are recorded |

A value out of range is a 400, not clamped. These exist because hits were
counted without limit: one 10 MiB binary produced 40,776 hits and one
20 MiB log 806,597, every one of them read and matched for nothing.
`hits` counts what was recorded, so it never exceeds `max_hits`.

The run's `truncated` is 1 when `max_files` or `max_hits` stopped the search,
so a caller can tell "no more matches" from "stopped looking". The job's
`result` carries `run_id`, `snapshot`, `files_scanned`, `files_matched`,
`files_capped` (files that reached `max_hits_per_file` and may hold more),
`hits` and `truncated`. The CLI takes `--limit`, `--max-hits-per-file` and
`--max-hits`.

`snapshot` defaults to the newest. One that does not exist is `404 no such
snapshot` when the search is submitted, and a value that is not an id is a
400; the first used to complete with zero files scanned, which reads as "not
in that backup", and the second silently searched the newest instead.

The hits outlive the job, so they are fetched separately: `GET
/repos/<id>/grep` lists runs newest-first and `GET /repos/<id>/grep/<run-id>`
returns the hits. `c-backup grep` does all three steps and prints the
matches, so it feels like `grep(1)` despite the work happening elsewhere.

Verify is the answer to "is the backup still good", which is a different
question from "did the backup run". Three levels, because the cost spread is
enormous: `index` confirms every registered object still resolves and that
every pack file the index refers to exists, and reads no payloads;
`structure` additionally parses each object header; `deep` reads every
payload and checks its SHA-256, which is the only level that can prove the
bytes are right. Every object in a pack file that has gone is `missing` at
every level, never "damaged": parity lives in the same file as the payload,
so there is nothing for a repair to work from.

By default a deep verify repairs what it can. Every object carries Reed-Solomon
parity, and the read that proves damage exists is the expensive part, so
writing the correction while already there costs almost nothing extra. Pass
`repair=0` (or `dry_run=1`) for a scrub that touches nothing; it still reports
damage, and for small objects it still distinguishes recoverable damage from
loss, because it can apply parity in memory without writing.

A checking-only verify takes the repository read lock, so a backup can run
underneath it. With repair on it takes the write lock like any other writer.

**A verify that finds loss fails.** If the walk finishes with any
`unrecoverable` or `missing` object, the job ends `failed` with an error
naming the run and the counts, and `c-backup verify` exits 1. The run record
and its findings are written first and read back exactly as for a clean run;
the run is still `completed: 1`, since the walk did finish. Repaired and
recoverable damage is not loss and does not fail the job.

A run (`GET /repos/<id>/verify`) carries these counts:

| Field | Meaning |
|---|---|
| `checked` | Objects examined |
| `ok` | Intact on disk |
| `repaired` | Damaged, corrected from parity, and the correction written to disk. Always 0 with `repair=0` |
| `recoverable` | Damaged, readable only through parity, and still wrong on disk: repair was off, or the write-back failed |
| `unrecoverable` | Damaged beyond what parity can rebuild. Data loss |
| `missing` | Indexed but absent: no loose file, no pack entry, or a pack file that has gone. Data loss |
| `affected_files` | Distinct file paths referencing a lost object |
| `affected_snapshots` | Distinct snapshots holding any of those files |

Findings (`GET /repos/<id>/verify/<run-id>`) name files, not hashes, so the
report says what was lost and what is still clean. Every object that is not
`ok` gets one, with `status` one of `missing`, `unrecoverable`, `unreadable
header`, `uncorrectable parity`, `damaged (run with repair to attempt
recovery)` (a large streamed object with repair off, whose recoverability
cannot be known without writing), `recoverable` or `repaired`:

| Field | Meaning |
|---|---|
| `affected_files` | Distinct paths referencing the object |
| `affected_versions` | Node rows referencing it: one per path per snapshot |
| `sample_paths` | Up to three full paths, `, `-separated, sorted, as restore would recreate them |
| `affected_snapshots` | Up to ten snapshot ids, comma-separated, ascending |
| `affected_snapshot_count` | Every snapshot referencing it |
| `snapshots_truncated` | `1` when `affected_snapshots` lists fewer than `affected_snapshot_count` |

An object no file references - a manifest tree, or one from an evicted
snapshot - reports zero files; that is not a claim the loss is harmless.

`/metrics` exposes `cbackup_last_verify_age_seconds` alongside
`cbackup_last_snapshot_age_seconds` - between them, an alert can tell that
backups are running *and* that what they wrote is still readable - plus the
newest completed run's `cbackup_verify_unrecoverable_objects` (unrecoverable
plus missing), `cbackup_verify_repaired_objects`,
`cbackup_verify_recoverable_objects` and `cbackup_verify_affected_files`.

Packing runs automatically at the end of every backup, because compression
only happens at pack time: a backup that stopped at the snapshot would have
written more bytes than it read and left one file per object behind. Set the
policy key `maintenance.auto_pack` to `false` to turn it off, and use this
endpoint or a schedule to pack on your own terms instead.

### Sources belong to the repository

A backup used to carry its paths in every request. That is right for the CLI,
where `backup 1 /home/me` reads exactly what it says, and wrong for
everything else: a desktop button had nowhere to get them and no way to ask,
and a schedule had the same problem. Both were broken because of it.

`PUT /repos/<id>/sources` stores them once. After that a backup with no
`paths` uses them, so the same configuration serves an ad-hoc run, a button
and a timer. A request that does name paths still overrides, because the
CLI's explicitness is worth keeping - and excludes are only inherited when
the request supplies none, since a request that listed some has said what it
wants excluded.

### Replication

`dest` is a path, so any target this machine can mount is valid: a second
disk, a NAS share, a USB dock. What is copied is objects, not files - packs
get rewritten by GC and by coalescing, and a file-level copy would re-send a
whole pack because one object inside it was collected. The target stores what
it receives and packs it on its own schedule, so the two sides may disagree
about physical layout while holding provably the same data.

Objects move in the form they are stored in, still compressed and still
encrypted. A replica of an encrypted repository is therefore encrypted with
the source's key and the target never needs it, which is what lets a replica
live somewhere less trusted than the source. The replica carries the source's
uuid and key fingerprint rather than an identity of its own: it is the same
repository in another place, and replicating into a path that already holds a
different repository is refused rather than merged.

Only what is missing is sent. A directory's tree hash covers everything
beneath it, so a subtree the target already holds is dismissed by a single
comparison and an unchanged repository replicates in one question.

There is no delete. Retention at the target belongs to the target, which is
what stops a compromised source from taking the offsite copy with it.

Replication is schedulable, which is the point of an offsite copy: one that
runs only when somebody remembers is not a strategy. The destination goes in
the schedule, since it belongs to the target rather than to the timing:

```json
{"action":"replicate","dest":"/mnt/nas/replica",
 "trigger":{"type":"calendar","every":"day","at":"03:00"}}
```

A `backup` schedule carries its sources the same way:

```json
{"action":"backup","paths":["/home/me"],
 "trigger":{"type":"calendar","every":"day","at":"02:00"}}
```

Backup was missing from the scheduler's dispatch table until now, and
schedule creation validated no action at all - so the schedule was accepted,
listed as enabled, and failed on every tick with nothing an operator could
see. The action is now checked against the dispatcher's own list, so a
schedule that cannot run cannot be created.

`dest` is required for a `replicate` schedule and must be absolute, both
checked when the schedule is created rather than when it first fires - a
schedule that turns out to have nowhere to send anything otherwise fails
every night with only the log to say why. It is stored in `schedules.params`
and returned by `GET /repos/<id>/schedules`.

## Jobs

| Method | Path | Purpose |
|---|---|---|
| GET | `/jobs` | Recent jobs |
| GET | `/jobs/<id>` | One job with progress |
| GET | `/jobs/stream/<id>` | Server-sent progress events |
| POST | `/jobs/<id>/cancel` | Request cancellation |

Cancellation is cooperative: the flag is set and the pipeline checks it
between files, so a cancelled backup stops within one file rather than at the
end of the run.

The stream path puts the id last because the router matches a trailing
wildcard, and `/jobs/<id>/stream` would be indistinguishable from the buffered
job endpoint.

Finished jobs are kept for 30 days and then deleted, so `/jobs/<id>` answers
404 for a job older than that. The jobs table is operational state; the
permanent record of what ran is each repository's journal
(`GET /repos/<id>/journal`). A job left running or queued by a daemon that
stopped is marked failed with "interrupted by daemon restart" when the next
one starts, since nothing resumes it.

## Introspection

What is actually inside a repository, as opposed to what it can do.

| Method | Path | Purpose |
|---|---|---|
| GET | `/repos/<id>/summary` | Dashboard aggregate in one request |
| GET | `/repos/<id>/stats` | Object counts and sizes by type; `?scope=loose` narrows |

Size fields on the object endpoints are `orig_size`, the logical bytes that
went in, and `phys_size`, the bytes the repository spends on them: payload
after compression, plus the 56-byte object header, plus the Reed-Solomon
parity trailer. `phys_size` exceeding `orig_size` is normal and expected for
incompressible input, where the payload is stored raw and the parity trailer
is pure overhead. `pack_objects` reports `comp_size` instead, which there is
genuinely the compressed entry payload.

| GET | `/repos/<id>/objects` | Paginated; `?location=loose` or `?location=pack` |
| GET | `/repos/<id>/objects/<hash>` | Where one object lives |
| GET | `/repos/<id>/objects/<hash>/refs` | Which snapshots and nodes reference it |
| GET | `/repos/<id>/objects/<hash>/layout` | Header fields and on-disk footprint |
| GET | `/repos/<id>/objects/<hash>/content` | Base64, capped at 2 MiB |
| GET | `/repos/<id>/packs` | Pack listing with entry counts |
| GET | `/repos/<id>/packs/<pid>/entries` | Contents of one pack, paginated |
| GET | `/repos/<id>/fanout` | Hash-prefix histogram |
| GET | `/repos/<id>/diff?from=<sid>&to=<sid>` | Compare two snapshots |

`/objects` replaces three separate actions that each read a different part of
the old `.idx` sidecar. Pack membership is a column now, so one query serves
all of them.

`/fanout` replaces the sidecar's 256-entry fanout table. That table existed
because a sorted on-disk index needs somewhere to start a binary search; the
same shape is a `GROUP BY` over hash prefixes, and it is a visualisation now
rather than a lookup structure.

`/content` is capped deliberately. It feeds a preview pane, and a viewer that
tries to render a multi-gigabyte object helps nobody; restore is how you get a
whole file.

`/diff` walks manifest trees rather than comparing node lists. Two directories
with the same hash have identical contents by construction, so an unchanged
subtree is pruned in a single comparison instead of being read. That is the
same property that makes incremental backups cheap, applied to reading.

## Metadata

| Method | Path | Purpose |
|---|---|---|
| GET/POST | `/repos/<id>/tags` | List, or set `{name, snap_id, move}` |
| DELETE | `/repos/<id>/tags/<name>` | Remove; 404 if there is no such tag |
| GET/PUT | `/repos/<id>/snapshots/<sid>/note` | Read or write `{body}`; 404 for a snapshot that does not exist |
| GET/PUT | `/repos/<id>/policy` | Read, or set `{key, value}` |
| GET | `/repos/<id>/journal` | Operation history |

Setting a tag or a note on a snapshot that does not exist is a 404, and a tag
with no name is a 400. Removing a tag that is not there is a 404 `no such
tag`, so a typo does not read as success.

A tag names one snapshot. Setting a name that is already on a different
snapshot is refused with a 409 that names the snapshot it is on, unless the
body carries `"move":true` (`tag set ... --move` from the CLI). Moving a tag
takes its retention protection off the snapshot it was on, so it has to be
asked for; it used to happen silently, and the next retention pass could
then delete a snapshot the operator believed was protected. Setting a name on
the snapshot it is already on succeeds, so a retried request is harmless.

Tag names are 1 to 64 characters of `A-Z a-z 0-9 . _ -`, starting with a
letter or digit; anything else is a 400 at set time. A name is the last
segment of the `DELETE` path, and one that cannot be a path segment (a space,
a slash, a `?`) could be set and then never removed. Clients should still
percent-encode the segment, as the CLI and the app do, so that a tag stored
before this rule can be removed: the server decodes the path before routing.

`GET /repos/<id>/policy` returns `policy`, what the repository has set, and
`defaults`, what applies wherever the repository has set nothing: the
daemon's configured `retention.*` values from `config.yaml`, and
`maintenance.auto_pack` (`true`). A client cannot read the daemon's config
file, so without them it could only guess at what is in force.

`PUT /repos/<id>/policy` accepts only the keys something in the daemon reads,
and refuses anything else with a 400 that lists them:

| Key | Value |
|---|---|
| `retention.keep_snaps` | whole number, 0 or more |
| `retention.keep_daily` | whole number, 0 or more |
| `retention.keep_weekly` | whole number, 0 or more |
| `retention.keep_monthly` | whole number, 0 or more |
| `retention.keep_yearly` | whole number, 0 or more |
| `maintenance.auto_pack` | `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`, any case); stored as `true` or `false` |

A count is decimal digits only: `-3`, `+3` and `banana` are refused. These
were all stored once, and a retention pass read `banana` as 0, which for a
tier means "keep all". A value stored before the route validated is ignored
when read, with a warning in the daemon log, and the default applies.

Retention keeps a snapshot that is among the newest `retention.keep_snaps`, or
holds a tier (daily, weekly, monthly, yearly) and is within that tier's count,
or is tagged. Everything else is pruned. A count of 0 keeps all of a tier;
`keep_snaps` below 1 still keeps the newest snapshot. Retention runs when
`/repos/<id>/gfs` is called or a `gfs` schedule fires, never as a side effect
of a backup.

A retention job's `result` says what it did, and on a dry run
(`?dry_run=1`) what it would do:

```json
{"dry_run":true,"would_remove":[1,2],"count":2,
 "retention":{"keep_snaps":2,"keep_daily":7,"keep_weekly":4,"keep_monthly":12,"keep_yearly":3}}
```

A real pass names `removed` instead of `would_remove`, listing only the
snapshots it actually deleted. `retention` is what the pass applied: the
repository's policy where it has one, the daemon defaults elsewhere. The
journal entry for the job carries the same in a line, such as `removed 2
snapshots: 1, 2`, naming at most 20 ids and counting the rest.

Any tag protects a snapshot from retention pruning. Tags are how a user says
"keep this one", and deleting a tagged snapshot because a window rolled past
would be the worst kind of surprise.

## Schedules

| Method | Path | Purpose |
|---|---|---|
| GET/POST | `/repos/<id>/schedules` | List, or create |
| PUT | `/repos/<id>/schedules/<sid>` | `{enabled}`; 404 if this repository has no such schedule |
| DELETE | `/repos/<id>/schedules/<sid>` | Remove; 404 if this repository has no such schedule |

Both used to answer success for an id that did not exist, or that belonged
to another repository, so a typo read as done while the schedule meant kept
firing.

The list carries the daemon's zone and each schedule's trigger and
parameters as objects:

```json
{"timezone":"America/Chicago",
 "schedules":[{"id":"2","action":"replicate",
   "trigger":{"type":"calendar","every":"day","at":"03:00"},
   "overlap":"skip","catchup":"skip","enabled":"1",
   "next_due":"1794733200","next_due_iso":"2026-11-16T03:00:00-06:00",
   "params":{"dest":"/mnt/nas/replica"}}]}
```

`trigger` and `params` were strings holding JSON, so every client parsed
JSON out of JSON. `next_due` stays an epoch for scripts; `next_due_iso` is
the same instant in the daemon's local time with its UTC offset. `timezone`
is the `TZ` value, else the zone `/etc/localtime` names, else the
abbreviation the C library reports (`UTC`). It is there because a container
with no `TZ` runs in UTC, and a schedule meant for 03:00 local fired at 03:00
UTC with nothing in the API to say so. `params` is absent for a schedule
with none.

The trigger is a nested object, validated at creation rather than at fire
time:

```json
{"action":"backup","trigger":{"type":"interval","seconds":14400}}
{"action":"gc","trigger":{"type":"calendar","every":"day","at":"02:00"}}
{"action":"gfs","trigger":{"type":"calendar","every":"week","on":"sunday","at":"03:00"}}
{"action":"verify","trigger":{"type":"calendar","every":"month","on_day":1,"at":"04:00"}}
{"action":"gc","trigger":{"type":"calendar","every":"year","on_month":1,"on_day":1,"at":"05:00"}}
```

`every` is `day`, `week` (with `on`, a weekday name), `month` (with
`on_day`, 1-31) or `year` (with `on_month` and `on_day`, a day that month
has: 30 February and 31 April are refused, 29 February is accepted). There
is no cron syntax. The CLI builds the same with `--every`, `--at`, `--on`, `--on-day`
and `--on-month`, or takes a number of seconds for an interval.

Interval triggers anchor to the last successful fire, not to wall-clock
multiples: a four-hour backup that runs long should not be due the moment it
finishes.

Calendar triggers use local time, which is what someone means by "02:00".
That choice creates the DST cases, all handled: a time that does not exist on
a spring-forward day resolves to a real instant, a time that occurs twice on a
fall-back day fires once, "the 31st" clamps to the end of short months, and
29 February fires every year rather than every fourth.

`on_overlap` decides what happens when a schedule comes due while its previous
run is still going: `skip` (default), `queue`, or `cancel_running`. `catchup`
decides what a missed window means after downtime: `skip` moves to the next
future fire, `run_once` fires immediately and then resumes. That includes a
schedule that has never fired: if its first due time passed while the daemon
was down, `run_once` fires it when the daemon starts. Any other value of
either is a 400; both used to be stored as sent and then read as something
else.

Parameters belong to one action each, and naming one on a schedule for a
different action is a 400 rather than a value stored and never read:

| Action | Parameters |
|---|---|
| `backup` | `paths` (optional; the repository's sources otherwise) |
| `replicate` | `dest` (required, absolute) |
| `grep` | `pattern` (required, a valid extended regular expression), `path_prefix`, `ignore_case`, `max_files` |
| others | none |

A `grep` schedule searches the newest snapshot as of each fire, so it takes
no `snapshot`. From the CLI: `schedule add 1 grep --every day --at 04:00
--pattern 'BEGIN RSA' [--path-prefix PATH] [-i]`.
