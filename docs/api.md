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
400 naming the parameter, and for `limit` the route's range: `limit must be
a whole number from 1 to 1000 (a larger one is reduced to 1000)`. They used
to be replaced by the defaults, which answered a request the server had not
understood as if it had. A `limit` above a route's maximum is clamped to it,
however many digits it has; an `offset` past the last row is an empty page. The same goes for filters with a
fixed set of values: `objects?location=` takes `loose`, `pack` (or
`packed`) or `all`, and anything else is a 400 rather than an unfiltered
listing.

Directory listings are ordered by name because the index carries `name` as its
trailing column, making the query a range scan rather than a sort of every
child.

A path the API serves, asked with a method it does not answer, is a `405`
with an `Allow` header listing the methods that work there (`PATCH
/repos/1` gives `405`, `Allow: GET, DELETE`). Any method other than GET,
HEAD, POST, PUT and DELETE is one of those; it used to be served as a GET.
A path that names nothing, under any method, is a `404`, and so is anything
under `/repos/<id>` for a repository that is not registered. The 405 comes
after authentication, so over TCP it needs a token like everything else.

Every success carries a JSON body, removes included: a `DELETE` answers
`200` with `{"removed":true,"id":N}` (or `"name"` for a tag) naming what went.
They used to answer `204` with no body, and `c-backup repo remove` printed
nothing on success. `POST /api/auth/logout` over TCP is the one `204` left.

Long operations return `202` with a job id rather than blocking. A backup can
run for hours, and a request that lives that long dies to any timeout between
client and daemon.

### Value types

Every response is typed. Until 1.0 most routes copied database rows out as
text, so ids, counts and sizes arrived as strings (`"id":"3"`) and every
client had to know which strings were numbers; they no longer do.

| Kind | JSON type | Examples |
|---|---|---|
| Ids, counts, byte sizes, offsets | number | `id`, `snap_id`, `files`, `size`, `phys_size`, `refcount`, `offset` |
| Times | number, epoch seconds | `ts`, `added_at`, `started_at`, `finished_at`, `next_due`, `mtime` |
| Type codes, modes, bitfields | number | `type`, `mode`, `gfs_flags`, pack `version` |
| Flags | `true`/`false` | `enabled`, `completed`, `truncated`, `materialized`, `has_children`, `key_escrowed` |
| Paths, names, labels, hashes, enums | string | `path`, `name`, `label`, `hash` (hex), `state`, `location`, `level` |
| A value the row does not have | `null` | `label` never set, `finished_at` of a running scrub, `hash` of a directory |

`null` replaces the empty string or `0` that stood in for "no value": a
NULL in the index is `null` in the response, whatever the field's type.

Integers are exact up to 10^15 (the serialiser prints 15 significant
digits). Every id, count, epoch second and byte size stays far below that;
10^15 bytes is a petabyte. The one value that would not, a file's
modification time in nanoseconds, is served as two numbers: `mtime` in
seconds and `mtime_nsec` within the second.

Requests take the same types. Where a field used to be sent as text or as
0/1 - a policy `value`, a schedule's `enabled` - the old form is still
accepted.

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
paths or file content. The only repository they name is the stalest one, by id
and label, in `cbackup_stalest_snapshot_repo_info` and
`cbackup_stalest_verify_repo_info`: one series each, value 1, so an alert on
the matching age can say which repository it means without a series per
repository. Label values are escaped per the text exposition format.

## Auth

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/auth/setup` | `{password}`: set the first one. Socket only (`403` over TCP), `409` once set |
| POST | `/api/auth/login` | `{password}` returns `{token, expires_in}` and sets the `auth` cookie. TCP only (`400` over the socket), `403` before setup |
| POST | `/api/auth/logout` | Revoke the presented token; `204`. Over the socket, `400` unless a token is presented |
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

Sessions are for TCP clients. The socket is authorised by its file
permissions and carries no session, so over it a login is a `400` (it used to
mint a 90-day token that nothing held, and `c-backup auth login` printed it),
and a logout is a `400` unless the request names a token in `Authorization:
Bearer`, which revokes that one session: the host's way to end a single known
token without ending the rest. It used to answer `204` having revoked
nothing. The CLI has no `auth login`, and its `auth logout` needs `--all`.

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
| GET | `/repos/<id>` | One repository, the same object as each entry of the list |
| DELETE | `/repos/<id>` | Deregister; the data directory is left alone. Index rows are deleted in the background. 409 while a job runs against it. `{"removed":true,"id"}` |

`POST /repos` normalises the path (no `//`, `.`, `..` or trailing slash) and
stores that form. It answers:

| Status | Body | When |
|---|---|---|
| 201 | `{id}`, plus `encryption` and key fields for an encrypted repository | a new registration: a repository created, or an existing one adopted |
| 200 | `{id, already_registered: true, label_updated, encryption}` | the repository was already registered, at this path or, if it moved, at its old one |
| 400 | `{error}` | the path is relative, cannot be created, is inside or contains a registered repository, is a non-empty directory with no marker, asks for an encryption mode the existing repository does not have, or gives a passphrase that does not unlock it (`wrong passphrase`) |

Registering an existing passphrase repository with its `passphrase`, whether
or not it is already registered, unwraps the key from the marker and caches
it in `security.key_dir`; the answer carries `key_unlocked: true`. Registering
an existing encrypted repository whose key is not on this host still
succeeds, and the answer carries `key_loaded: false` and a `warning` saying
`no key loaded for repository <uuid>` and how to supply it.

Repository ids are never reused. Removing the newest repository used to
make its id the next one handed out, and the new repository inherited the
old one's schedules, history and index rows, which deregistering does not
delete. Migration 019 rebuilt `repos` with `AUTOINCREMENT`, starting above
every id any table still refers to.

No other id the API hands out is reused either: job ids, schedule ids,
search and verify run ids, and snapshot ids within a repository. Job,
schedule and run ids came round again when the newest rows were deleted
(by the purge after `DELETE /repos/<id>`, the 30-day job cleanup, or
deleting a schedule), so job 312 of a removed repository was followed by a
job 312 on another one, and a client still holding the old id read an
unrelated job (B113). Migration 023 rebuilt those tables with
`AUTOINCREMENT`. Snapshot ids were one more than the highest present, so
deleting the newest snapshot gave its id to the next backup; the daemon
now keeps a per-repository high-water mark. Ids reused before 023 stay
reused. The snapshot mark lives in the database, so after a database loss
a reindex resumes above the highest snapshot record left in the repository
(see `docs/repo-format.md`).

A repository is `{id, uuid, path, label, added_at, encryption,
key_escrowed}`: `label` is `null` when none was given, `encryption` is
`none`, `system` or `passphrase`, and `key_escrowed` is a boolean that is
`false` while a system-managed key exists only on this host.

Re-registering is a success rather than a 409 because it is how a moved
repository re-links. A non-empty `label` that differs from the stored one is
applied (`label_updated: true`); an absent or empty one leaves it alone.

## Snapshots and browsing

| Method | Path | Purpose |
|---|---|---|
| GET | `/repos/<id>/snapshots` | List, paged |
| GET | `/repos/<id>/snapshots/<sid>/tree?parent=<node>` | One directory level |
| GET | `/repos/<id>/search?q=<text>[&snapshot=<sid>]` | Substring filename search |
| DELETE | `/repos/<id>/snapshots/<sid>[?force=1]` | Delete one snapshot, synchronously; `{"removed":true,"id","tags_removed"}` |

Deleting a snapshot is not a job: it is a few row deletes and one unlink,
answered in the response. GC reclaims the space later. A tagged snapshot is
refused with `409` and an error naming its tags (`snapshot 3 is tagged
(guard)...`) unless `force=1`, which deletes it and its tags, from the
database and from the repository's own tag file. `tags_removed` counts
them. A snapshot that does not exist, including one already deleted, is
`404 {"error":"no such snapshot"}`; `force` other than `1`/`true`/`0`/`false`
is `400`; `503` while a job holds the repository. Each delete that runs is
journalled with `source` `api` and `operation` `snapshot-delete`, its
message naming the snapshot and any tags a forced delete took with it
(`removed snapshot 3 and its tag guard (forced)`); a refusal or 404 changes
nothing and is not journalled.

A snapshot is `{id, ts, label, files, size, gfs_flags, materialized}`.
`gfs_flags` is a bitfield of the retention tiers holding it: 1 daily, 2
weekly, 4 monthly, 8 yearly. Only a retention pass (not a dry run) assigns
them, recomputing every snapshot's flags each time, by the rules under
Retention below; the newest snapshot holds all four, 15, as does the last
snapshot of any year. A snapshot no tier elected shows 0, tagged or not.
`materialized` is `false` for a cold snapshot whose rows must be faulted
back in before it can be browsed.

A tree entry is `{id, name, type, mode, size, mtime, mtime_nsec, hash,
has_children}`. `id` is the node id to pass back as `parent`. `hash` is
the content's hex hash, `null` for a directory or a file with no content
object. `mode` is the file's full `st_mode` as a decimal integer, file type
bits included: `33188` is `0100644`, a regular file with `rw-r--r--`, and
`mode & 0o7777` is the permission bits. `type` is the node type:

| `type` | Node |
|---|---|
| 1 | Regular file |
| 2 | Directory |
| 3 | Symbolic link |
| 4 | Reserved for hard links; none is written, and a hard-linked file is type 1 |
| 5 | FIFO |
| 6 | Character device |
| 7 | Block device |

The same codes are `type` in search results. Object `type` (in `stats`,
`objects` and an object's locate, layout and content) is a different set:
1 file content, 2 extended attributes, 3 ACL, 4 sparse file, 5 manifest
tree.

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
replicating into a fresh repository with `reencrypt` (see Replication) is the
supported path instead.

`POST /repos` takes `encrypt` (`none`, `system`, `passphrase`) and, for the
last, a `passphrase`. A non-empty `passphrase` with no `encrypt` means
`passphrase` mode. A non-empty `passphrase` with `encrypt` `none` or `system`
is refused with 400 rather than ignored: ignoring it used to create an
unencrypted repository for somebody who had just typed a passphrase. An empty
`passphrase` is treated as absent. Creating a repository needs a passphrase
of at least 8 characters (400 otherwise); unlocking an existing one accepts
whatever it was created with.

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

All three answer 400 `repository is not encrypted` for an unencrypted
repository, which has no key to export, import or escrow.

An encrypted repository whose key is not on this host answers 409 `no key
loaded for repository <uuid>; ...` to backup, restore, reindex, grep, pack,
gc and replicate, naming the command that supplies the key. Verify and gfs
run without it: parity covers ciphertext, and retention only deletes
records. A job that reaches the repository without the key (a scheduled
one, or a key removed after submission) fails with the same message.

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
and `tests/test_cli_e2e.sh` exercises the whole table. The search and verify
run lists, a search's hits, `GET /api/auth/setup` (`auth status`), the jobs
list's paging and `stats?scope=loose` were found missing in an audit of this
table and added; every route and query parameter here now has a command or
a flag.

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
| POST | `/repos/<id>/reindex` | Rebuild the index from the repository; the job fails if any snapshot or pack cannot be read, and what could not be read keeps its previous rows |
| POST | `/repos/<id>/pack` | Coalesce loose objects into pack files |
| POST | `/repos/<id>/grep` | Content search as a job: `{pattern, snapshot, path_prefix, ignore_case, max_files}` |
| GET | `/repos/<id>/grep` | Recent searches, newest first; `limit` and `offset` |
| GET | `/repos/<id>/grep/<run-id>` | The hits from one search, paginated |
| POST | `/repos/<id>/verify` | Scrub: prove stored objects still read back. `?level=index\|structure\|deep` (default `deep`), `?repair=0` to check without writing, `?dry_run=1` (or `true`) the same and overriding any `repair` |
| GET | `/repos/<id>/verify` | Recent verify runs, newest first; `?limit=N` (default 20) and `offset` |
| GET | `/repos/<id>/verify/<run-id>` | The findings from one run, paginated |
| POST | `/repos/<id>/replicate` | Copy this repository somewhere else: `{dest, reencrypt}` |

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

A backup in which every source is excluded fails with "nothing to back up"
and writes no snapshot. It used to succeed with a snapshot holding only the
directories above each source, which looks like a backup and restores
nothing.

A source that does not exist, or cannot be opened, fails the job before
anything is written, with an error naming it (`source /data/x cannot be read
(No such file or directory); no snapshot written`), whatever the other
sources hold. It used to be skipped with a log line, so a snapshot silently
lacking a whole source looked like a backup of everything asked for. A file
*inside* a source that cannot be read is still skipped with a warning and the
backup goes on.

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

The run's `truncated` is `true` when `max_files` or `max_hits` stopped the
search, so a caller can tell "no more matches" from "stopped looking". A file
that reached `max_hits_per_file` does not set it; it is counted in
`files_capped` (files that may hold more than was recorded), which both the
run record in `GET /repos/<id>/grep` and the job's `result` carry. The
`result` also has `run_id`, `snapshot`, `files_scanned`, `files_matched`,
`hits` and `truncated`. `ignore_case` is a boolean; `0` and `1` are still
accepted. The CLI takes `--limit`, `--max-hits-per-file` and
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

`dry_run=1` is `repair=0` and nothing else: it overrides `repair`, and it
does not lower the level, so with no `level` it is a full deep read like a
plain verify, with the same counts on a healthy repository. The job's
`result` says which kind of run it was, since the run record does not:

```json
{"run_id":12,"level":"deep","repair":false,"dry_run":true}
```

`run_id` is null when the walk did not finish (cancelled or aborted); the
partial run is still in `GET /repos/<id>/verify`, with `completed` false.

A checking-only verify takes the repository read lock, so a backup can run
underneath it. With repair on it takes the write lock like any other writer.

**A verify that finds loss fails.** If the walk finishes with any
`unrecoverable` or `missing` object, the job ends `failed` with an error
naming the run and the counts, and `c-backup verify` exits 1. The run record
and its findings are written first and read back exactly as for a clean run;
the run is still `completed: true`, since the walk did finish. Repaired and
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
| `sample_paths` | Up to three full paths, `, `-separated, sorted, as restore would recreate them; `null` if none |
| `affected_snapshots` | Up to ten snapshot ids as a comma-separated string, ascending; `null` if none |
| `affected_snapshot_count` | Every snapshot referencing it |
| `snapshots_truncated` | `true` when `affected_snapshots` lists fewer than `affected_snapshot_count` |

An object no file references - a manifest tree, or one from an evicted
snapshot - reports zero files; that is not a claim the loss is harmless.

`/metrics` exposes `cbackup_last_verify_age_seconds` alongside
`cbackup_last_snapshot_age_seconds` - between them, an alert can tell that
backups are running *and* that what they wrote is still readable - plus the
newest completed run's `cbackup_verify_unrecoverable_objects` (unrecoverable
plus missing), `cbackup_verify_repaired_objects`,
`cbackup_verify_recoverable_objects` and `cbackup_verify_affected_files`.

A backup writes every object of 16 MiB or less straight into a pack, and
ends with a pack sweep for what it could not: a sparse file's object and an
object over 16 MiB are written loose and uncompressed, and the sweep packs
them and coalesces small packs, as this endpoint does. The policy key
`maintenance.auto_pack` set to `false` skips that sweep and nothing else;
small objects are packed during the backup either way, so a backup of small
files leaves nothing loose and a pack after it reports `loose` 0. With the
key off, use this endpoint or a schedule for the sweep.

### Sources belong to the repository

A backup used to carry its paths in every request. That is right for the CLI,
where `backup 1 /home/me` reads exactly what it says, and wrong for
everything else: a desktop button had nowhere to get them and no way to ask,
and a schedule had the same problem. Both were broken because of it.

`PUT /repos/<id>/sources` replaces the list and answers `200
{"sources":N,"exclude":M,"warnings":[...]}`. `sources` is required and must
be a list of at most 256 absolute paths; `exclude`, if present, a list of at
most 256 non-empty patterns. Anything else - a missing or non-list
`sources`, a non-string, an empty or relative path - is a 400 that leaves the
stored configuration untouched. It used to skip what it could not read, and
since the route replaces wholesale, a malformed request answered 204 having
emptied the list. Paths are stored normalised (`/a//b/./` is `/a/b`). A path
that does not exist on the daemon's host is accepted with a warning rather
than refused, because it may be a share that is not mounted yet, and the
warning says that a backup fails while it is missing: a backup does not skip
a missing source, it fails the job (see above).

A `PUT` during a running backup returns promptly; it does not wait for the
backup (it did, for the whole run, before B107). The new list applies to
backups submitted after it: a backup reads the sources when it is submitted,
so one already running or queued keeps the list it was submitted with.

Stored once, they serve every run. After that a backup with no
`paths` uses them, so the same configuration serves an ad-hoc run, a button
and a timer. A request that does name paths still overrides, because the
CLI's explicitness is worth keeping - and excludes are only inherited when
the request supplies none, since a request that listed some has said what it
wants excluded.

### Replication

`dest` is a path, so any target this machine can mount is valid: a second
disk, a NAS share, a USB dock. It must be absolute and must not be the
source repository's own directory, inside it, or above it, compared as
canonical paths so a symlink or `..` cannot route around the check; each is
a 400 when the job is submitted or the schedule created, and a scheduled
fire checks again. Into its own directory a replication used to "complete":
the target was the source, already held every object, and nothing was
copied anywhere. What is copied is objects, not files - packs
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

`"reencrypt": true` is the exception, and the way to change a repository's
encryption. `dest` must already be a repository with an identity of its
own (create it with `POST /repos` and the encryption wanted); each object is
decrypted with the source key and encrypted under the destination's, or
written plaintext into an unencrypted destination. Hashes are of the
plaintext, so they and the trees carry over unchanged, as do snapshot ids;
a snapshot id the destination already uses for another tree fails the run
instead of being overwritten. The job fails, saying which, when `dest` is
not a repository, is a replica of the source, or is encrypted and its key is
not on this host. The job's result reports `snapshots_sent`,
`snapshots_skipped`, `objects_asked`, `objects_sent`, `bytes_sent`,
`subtrees_skipped` and `reencrypted`. Re-encrypting is a manual operation: the
schedule route does not store `reencrypt`.

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
| GET | `/jobs` | Recent jobs, newest first. `limit` (default 50, at most 1000) and `offset`; a malformed one is a 400 |
| GET | `/jobs/<id>` | One job with progress |
| GET | `/jobs/stream/<id>` | Server-sent progress events |
| POST | `/jobs/<id>/cancel` | Request cancellation |

Cancellation is cooperative: the flag is set and the job stops at its next
check. Those sit between files and also between chunks inside one file -
storing it, packing it, restoring it - so a cancel lands within one read
(at most 8 MiB of I/O), not at the end of a large file. A cancelled backup
discards its open pack, the index rows staged into it and its staging files.

`POST /jobs/<id>/cancel` answers 200 with the job record, as `/jobs/<id>`
returns it, plus `"cancel_requested":true`. A queued job is taken out of the
queue and never runs: its `state` is already `cancelled`, and the journal
records it as cancelled with the message "cancelled before it started"; its
`started` is null. Shutting the daemon down does the same to every queued
job. A running job's `state` is usually still `running`: it reports `cancelled` once
it has stopped. A job that is not queued or running is a 404.

A job record carries `origin`, who asked for it: `api` for anything submitted
over the API (the CLI and the app both are), `schedule` for the scheduler, with
`schedule_id` naming the schedule (null otherwise).

`/jobs/stream/<id>` sends one `data:` frame at once with the job's state as it
is, then a frame whenever it changes, and the same frame again every 5 seconds
while it does not, so a watcher can tell a busy job from a dead stream. The
stream ends after the frame with a terminal state. A frame is
`{"id","state","phase","current","total","bytes_done","bytes_total","error"}`.
`bytes_done` advances inside a file as backup and restore read and write it,
not only between files, so a job spending minutes on one large file still
sends a changed frame a few times a second.

The stream path puts the id last because the router matches a trailing
wildcard, and `/jobs/<id>/stream` would be indistinguishable from the buffered
job endpoint.

Finished jobs are kept for 30 days and then deleted, so `/jobs/<id>` answers
404 for a job older than that. The jobs table is operational state; the
permanent record of what ran is each repository's journal
(`GET /repos/<id>/journal`). A job left running or queued by a daemon that
stopped is marked failed with "interrupted by daemon restart" when the next
one starts, since nothing resumes it, and journalled then with that message:
a daemon that was killed had no chance to journal it itself.

A journal entry's `source` is the job's origin: `api`, or `schedule` with the
message starting `schedule <id>`.

What some jobs report in `result`, beyond the backup and restore counts:

| Job | `result` |
|---|---|
| gc | `dry_run`, `reachable`, `objects_swept` and `bytes_freed` (what was or would be swept), `pack_entries_kept`, `pack_entries_dropped`, `packs_rewritten`, `packs_deleted`, `tmp_files_removed`, `tmp_bytes_removed` |
| replicate | `snapshots_sent`, `snapshots_skipped`, `objects_asked`, `objects_sent`, `bytes_sent` (as stored), `subtrees_skipped`, `reencrypted` |
| pack | `loose` (loose objects found), `packed`, `skip_marked` (already marked incompressible, left loose), `incompressible` (marked by this run's probe, left loose), `reason` |

A backup's `result` has its counts (`snapshot_id`, `files`, `dirs`,
`files_unchanged`, `bytes_logical`, `objects_new`, `bytes_stored`,
`trees_new`, `trees_reused`, `store_workers`) and what it wrote into the
repository, in bytes:

| Field | What |
|---|---|
| `bytes_written` | The sum of the categories below: everything the backup wrote into the repository apart from small metadata (the snapshot record, skip marks) |
| `bytes_written_loose` | Loose objects installed, header and payload: files over 16 MiB and sparse files, always uncompressed |
| `bytes_written_loose_parity` | The parity trailers of those loose objects |
| `bytes_written_loose_discarded` | Loose objects written to `tmp/` in full and then dropped because the repository already had them, such as the second of two identical large files |
| `bytes_written_pack` | Sealed pack files, without their parity trailers: the backup's own packs and any the pack pass wrote |
| `bytes_written_pack_parity` | The parity trailers of those packs |
| `bytes_written_parity_spill` | Reed-Solomon parity over its memory cap, written to `tmp/` and then copied into its trailer, so these bytes are written twice |
| `bytes_written_pack_pass` | Of `bytes_written`, the share of the pack pass the job ends with (`maintenance.auto_pack`) |
| `pack_pass_packed` | Objects that pass packed; `0` when it is off or found nothing |

`bytes_written / bytes_logical` is the backup's write amplification: on
storage whose write bandwidth is the limit, it is roughly the backup's time
over a plain copy's. A loose object the pack pass compresses is counted
twice, once loose and once in its pack, because it was written twice; its
loose copy is deleted afterwards. The state database is not counted. The
same figures are logged after the backup's phase profile, which also
covers the pack pass (`pack-sweep`).

A pack's `reason` is `null` when it packed something, and otherwise says why
it did not: `no loose objects`, `all loose objects are skip-marked as
incompressible`, `every remaining loose object is incompressible`, or `no
loose object could be packed`. It had no result at all before, so a run that
packed nothing read as a bare `completed`.

All three also put a one-line summary in their journal entry, such as `swept
12 objects (40960 bytes); 1 pack rewritten, 0 deleted; 0 stale temp files
removed`, `target already up to date (3 snapshots present)` or `nothing
packed: no loose objects`. A dry-run gc removes nothing, temp files
included; its pack figures are zero because compaction does not run.

A finished job's `phase`, `current` and `total` are its last progress report.
Progress reaches the database a few times a second, and the final report is
written when the job ends, so a phase that finished inside one interval no
longer stays at its first count. Each phase ends with `current` equal to
`total`. A restore counts the entries in scope: the whole snapshot, or with
a `subpath` only that subtree, so a one-file restore ends at 1 of 1.

## Introspection

What is actually inside a repository, as opposed to what it can do.

| Method | Path | Purpose |
|---|---|---|
| GET | `/repos/<id>/summary` | Dashboard aggregate in one request |
| GET | `/repos/<id>/stats` | Object counts and sizes by type; `?scope=loose` narrows |

Size fields on the object endpoints are `orig_size`, the logical bytes that
went in, and `phys_size`, the bytes the object itself is charged for. What
that covers depends on where the object lives, by design:

- **Loose:** the whole object file. The 64-byte `object_header_t`, the
  payload after compression, and the Reed-Solomon parity trailer. This
  matches `/layout`'s `physical_size`. `phys_size` exceeding `orig_size` is
  normal for incompressible input, where the payload is stored raw and the
  parity trailer is pure overhead.
- **Packed:** the stored payload alone, after compression (and encryption,
  nonce and tag included). The 51-byte pack entry header and the entry's
  parity block in the pack's trailer are charged to the pack (`packs.size`),
  not to the object. So for a packed object `phys_size` against `orig_size`
  is its compression ratio, and the disk a repository uses is the sum of
  `packs.size` plus `phys_size` over loose objects, with nothing counted
  twice. `pack_objects` reports the same figure as `comp_size`.

`/layout` reads the object on disk rather than the index. `header_size` is
64 or 51, whichever header the object sits behind (the sizes are
compile-time checked in `src/common/types.h` and `src/store/pack.h`).
`physical_size` is the record as it sits in the file: for a loose object,
the whole file (equal to `phys_size`); for a packed one, entry header plus
payload, which is `phys_size + 51`, with the parity block in the pack
trailer in neither figure. `payload_size` is `physical_size - header_size`.
For a packed object that equals `phys_size`; for a loose one it also
includes the parity trailer. A packed object that reports `phys_size`
5242880 and `physical_size` 5242931 is consistent: 5242931 - 51 = 5242880.

| GET | `/repos/<id>/objects` | Paginated; `?location=loose` or `?location=pack` |
| GET | `/repos/<id>/objects/<hash>` | Where one object lives |
| GET | `/repos/<id>/objects/<hash>/refs` | Which snapshots and nodes reference it |
| GET | `/repos/<id>/objects/<hash>/layout` | Header fields and on-disk footprint |
| GET | `/repos/<id>/objects/<hash>/content` | Base64, capped at 2 MiB |
| GET | `/repos/<id>/packs` | Pack listing with entry counts |
| GET | `/repos/<id>/packs/<pid>/entries` | Contents of one pack, paginated |
| GET | `/repos/<id>/fanout` | Hash-prefix histogram |
| GET | `/repos/<id>/diff?from=<sid>&to=<sid>` | Compare two snapshots |

An object's `location` is `loose`, `staged` or `packed`, and `pack_id` is
the pack's number or `null`. The index stores the pack number in place of
the location, and serving it as stored made `location` a word for some
objects and `"3"` for others. `/objects/<hash>` also gives `offset` and
`entry_index` within the pack, `null` for an object not in one. In
`/summary`, `last_snapshot_ts` is `null` for a repository with no snapshots.
In `/diff`, the counts are numbers and `truncated` a boolean; a change's
`old_hash` or `new_hash` is `null` for the side that does not exist, whose
size is 0.

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
| DELETE | `/repos/<id>/tags/<name>` | Remove; `{"removed":true,"name"}`, 404 if there is no such tag |
| GET/PUT | `/repos/<id>/snapshots/<sid>/note` | Read or write `{body}`; 404 for a snapshot that does not exist |
| GET/PUT | `/repos/<id>/policy` | Read, or set `{key, value}` |
| GET | `/repos/<id>/journal` | Operation history: `{id, ts, source, operation, result, duration_ms, message}`, `duration_ms` and `message` `null` where none was recorded |

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
`maintenance.auto_pack` (`true`). Each is `{key, value}` with the value
typed by its key: a number for a count, a boolean for `auto_pack`. A value
stored before the route validated, which the readers ignore, is `null`. A client cannot read the daemon's config
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

The value may be sent as the GET serves it (`6`, `true`) or as text (`"6"`,
`"true"`). A count is decimal digits only: `-3`, `+3` and `banana` are refused. These
were all stored once, and a retention pass read `banana` as 0, which for a
tier means "keep all". A value stored before the route validated is ignored
when read, with a warning in the daemon log, and the default applies.

Retention keeps a snapshot that is among the newest `retention.keep_snaps`, or
holds a tier (daily, weekly, monthly, yearly) and is within that tier's count,
or is tagged. Everything else is pruned. A count of 0 keeps all of a tier;
`keep_snaps` below 1 still keeps the newest snapshot. Retention runs when
`/repos/<id>/gfs` is called or a `gfs` schedule fires, never as a side effect
of a backup.

Tiers, exactly. Periods are local calendar days in the daemon's timezone.
Within each period the latest snapshot (by timestamp, then id) is elected:

- daily: the last snapshot of each day;
- weekly: the last daily of each Monday-to-Sunday week;
- monthly: the last weekly of each month;
- yearly: the last monthly of each year.

A period still in progress elects its latest snapshot so far, so the newest
snapshot always holds all four tiers. A later snapshot in the same period
takes the tier over on the next pass, and the earlier one loses it: every
pass clears and recomputes the flags of every snapshot. A snapshot counts
against the count of its highest tier only. Tier `keep_N` keeps the newest
N snapshots whose highest tier is that tier; so with every count at 1 a pass
keeps the newest snapshot (yearly), the newest whose highest tier is daily,
the newest whose highest is weekly, and the newest whose highest is
monthly, plus the `keep_snaps` window and tagged snapshots.

Tags play no part in electing tiers. A tagged snapshot holds whatever tiers
it was elected for and counts against them like any other; a tagged
snapshot no period elected holds none and shows `gfs_flags` 0. Either way
the tag keeps it.

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
| PUT | `/repos/<id>/schedules/<sid>` | `{enabled}`, `true`/`false` (or `1`/`0`); 404 if this repository has no such schedule; 409 enabling one that breaks a creation rule |
| DELETE | `/repos/<id>/schedules/<sid>` | Remove; `{"removed":true,"id"}`, 404 if this repository has no such schedule |

Both used to answer success for an id that did not exist, or that belonged
to another repository, so a typo read as done while the schedule meant kept
firing.

The list carries the daemon's zone and each schedule's trigger and
parameters as objects:

```json
{"timezone":"America/Chicago",
 "schedules":[{"id":2,"action":"replicate",
   "trigger":{"type":"calendar","every":"day","at":"03:00"},
   "overlap":"skip","catchup":"skip","enabled":true,
   "next_due":1794733200,"next_due_iso":"2026-11-16T03:00:00-06:00",
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

`on_overlap` decides what happens when a schedule comes due while a job for
the same repository and action is queued or running: `skip` (default) does
nothing; `queue` submits one run that waits for the repository, unless one of
the schedule's runs is already queued, which the fire is folded into;
`cancel_running` cancels the running job as `POST /jobs/<id>/cancel` does and
submits one replacement, which starts once the cancelled job has stopped. A
`cancel_running` fire that finds its replacement still queued adds nothing and
never cancels a queued job. Manual section 10 has the details. `catchup`
decides what a missed window means after downtime: `skip` moves to the next
future fire, `run_once` fires immediately and then resumes. That includes a
schedule that has never fired: if its first due time passed while the daemon
was down, `run_once` fires it when the daemon starts. Any other value of
either is a 400; both used to be stored as sent and then read as something
else.

A schedule stored before one of these rules existed survives an upgrade, so
at every start the daemon checks each enabled schedule's action, trigger,
`on_overlap` and `catchup` against them, and disables one that fails rather
than reinterpreting it. The list then carries `"enabled":false` and a
`disabled_reason`, present only when the daemon turned the schedule off:

```json
{"id":4,"action":"gc","trigger":{"type":"interval","seconds":3600},
 "overlap":"skip","catchup":"always","enabled":false,
 "disabled_reason":"catchup 'always' is not skip or run_once", ...}
```

Enabling such a schedule is a 409 naming the same problem; delete it and
create it again. Any `PUT` that succeeds clears the reason. Parameters that
depend on the host, such as a replication destination, are not part of the
check: they can be right again tomorrow, and fail at fire time with the job
saying why.

Parameters belong to one action each, and naming one on a schedule for a
different action is a 400 rather than a value stored and never read:

| Action | Parameters |
|---|---|
| `backup` | `paths` (optional; the repository's sources otherwise) |
| `replicate` | `dest` (required, absolute) |
| `grep` | `pattern` (required, a valid extended regular expression), `path_prefix`, `ignore_case` (a boolean, stored as one; `0`/`1` accepted), `max_files` |
| others | none |

A `grep` schedule searches the newest snapshot as of each fire, so it takes
no `snapshot`. From the CLI: `schedule add 1 grep --every day --at 04:00
--pattern 'BEGIN RSA' [--path-prefix PATH] [-i]`.
