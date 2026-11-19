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

`limit` and `offset` must be whole numbers: `limit` 1 or more, `offset` 0
or more. Something that is not a whole number (`+5`, `lots`, `1.5`) is a
400 `<param> must be a whole number`; one below the minimum (`-1`, `0` for
a limit) is a 400 naming the range, which for `limit` is the route's:
`limit is out of range; it must be from 1 to 1000 (a larger one, up to
9007199254740992, is reduced to 1000)`. They used
to be replaced by the defaults, which answered a request the server had not
understood as if it had. A `limit` above a route's maximum is clamped to it,
however many digits it has; an `offset` past the last row is an empty page,
up to 2^53 (9007199254740992); a number past that cannot be represented
exactly (`99999999999999999999`, `1e999`) and is out of range, as it is
everywhere else. The same goes for filters with a
fixed set of values: `objects?location=` takes `staged`, `pack` (or
`packed`) or `all`, and anything else (`loose` included, which format 3 no
longer has) is a 400 rather than an unfiltered listing.

Directory listings are ordered by name because the index carries `name` as its
trailing column, making the query a range scan rather than a sort of every
child.

Every list route, with its page size and order. Each order ends in a unique
key, so a page is the same page on every request and paging from offset 0
to the end visits every row once (while nothing is added or removed).

| Route | Default | Maximum | Order |
|---|---|---|---|
| `/repos` | 1000 | 1000 | id |
| `/repos/<id>/snapshots` | 200 | 1000 | id, newest first |
| `/repos/<id>/snapshots/<sid>/tree` | 200 | 1000 | name |
| `/repos/<id>/search` | 200 | 1000 | see Snapshots and browsing |
| `/repos/<id>/grep`, `/repos/<id>/verify` | 20 | 1000 | id, newest first |
| `/repos/<id>/grep/<run>`, `/repos/<id>/verify/<run>` | 200 | 1000 | id |
| `/repos/<id>/objects` | 500 | 5000 | hash |
| `/repos/<id>/objects/<hash>/refs` | 500 | 5000 | snapshot, node |
| `/repos/<id>/packs` | 500 | 5000 | pack number |
| `/repos/<id>/packs/<pid>/entries` | 500 | 5000 | offset in the pack |
| `/repos/<id>/diff` | 2000 | 5000 | tree walk (see Introspection) |
| `/repos/<id>/tags` | 1000 | 1000 | name |
| `/repos/<id>/schedules` | 1000 | 1000 | id |
| `/repos/<id>/journal` | 100 | 1000 | time then id, newest first |
| `/repos/<id>/units`, `/repos/<id>/units/<key>` | 100 | 1000 | unit name; snapshot id, newest first |
| `/repos/<id>/databases` | 100 | 1000 | scope, path, engine, id |
| `/providers/<p>/units` | 100 | 1000 | the provider's enumeration |
| `/jobs` | 50 | 1000 | id, newest first |

`packs`, `diff`, `objects/<hash>/refs`, `search`, `tags`, `schedules`
and `/repos` read no `offset` (the first two not even a `limit`), and the
journal no `offset`, before B154: the CLI sent them and the daemon answered
the whole list or the first page again. `/repos`, `tags` and `schedules`
default to their maximum so a client that does not page sees every row any
real deployment has. Lists that are bounded by construction do not page:
`/fanout` (at most 256 buckets), `/stats` (one row per object type),
`/policy` (the fixed key set), `/sources` (what the operator configured)
and `/providers`.

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
| Flags | `true`/`false` | `enabled`, `completed`, `truncated`, `materialized`, `has_children`, `may_be_torn`, `key_escrowed` |
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

### Request errors

Numeric query parameters (`limit`, `offset`, `snapshot` on search,
`parent` on a tree listing, `from` and `to` on a diff) read numbers the
same way and answer in the same words: something that is not a whole
number is `<param> must be a whole number`, and a negative, oversized or
unrepresentable one (`1e999`) is `<param> is out of range; it must be from
<min> to <max>`. A valid id that names nothing is still a 404. An
overflowing id used to be clamped and answered 404, as though it were
merely unknown.

An id in the path (`/repos/<id>`, `/jobs/<id>`, `/repos/<id>/snapshots/<sid>`,
a schedule, grep or verify run id, a pack number) is decimal digits from 1
(0 for a pack) to 2^53. Anything else in that place - `-1`, `0`, `1e999`,
`1.5`, `abc`, `99999999999999999999` - is a 400 `invalid <kind> id`
(`invalid repo id`, `invalid job id`, `invalid snapshot id`, `invalid
schedule id`, `invalid grep run id`, `invalid verify run id`, `invalid pack
id`). These were routed rather than parsed, so the same mistake was a 400,
a 404 or a 405 depending on where it was made.

A body that is not JSON is a 400 `invalid JSON` on every route. A member
name that appears twice in one object is refused too, naming it and where
it is: `invalid JSON: duplicate key 'seconds' in object 'trigger'`, or `at
the top level`. `POST /repos` used to say `body is not valid JSON` and no
route named the key.

A request body field is either absent or of its documented type. A field
that is present with the wrong type is a 400 naming it and the type it
must be, and nothing is changed:

| Problem | Error |
|---|---|
| A required field is absent | `<field> is required` |
| A string field holds anything else | `<field> must be a string` |
| A boolean field holds anything else | `<field> must be true or false` |
| A number field holds anything else | `<field> must be a number` |
| A whole-number field holds a fraction | `<field> must be a whole number` |
| A number outside the field's range, negative where it cannot be, or too large to represent (`1e999`) | `<field> is out of range; it must be from <min> to <max>` |
| A list of strings is not a list, or an entry is not a string | `<field> must be a list of strings` (with the entry's position) |
| A string holds a NUL (`\u0000`), in the body or as `%00` in the query | `<field> must not contain a NUL character`; a `label` gets the label rule, `label must be at most 256 bytes of printable text` |

`null` is a value of the wrong type, not a way of leaving a field out: omit
a field to leave it unset. Flags that also accepted `0`/`1` (`ignore_case`,
`one_filesystem`, `overwrite`, a schedule's `enabled`) still do. A nested
field is named by its path, as in `interval trigger: trigger.seconds must
be a number`. Integers are exact only up to 2^53, so a field whose type
allows more is limited to that, and the range in the message says so.

A NUL used to cut the string short without a word: the daemon is C, and
`"label":"nul\u0000here"` stored the label `nul` and answered 202. No
field in this API can hold one, so the request is refused instead.

Each of these used to read as absent: an array `label` registered a
repository with no label and answered 201, `"one_filesystem":true` was
ignored because only `1` was read, and `"snap_id":1e999` was told
`snap_id is required`.

The request is checked before the state it acts on. A body that is wrong
in itself is reported first, and only a sound one goes on to errors about
the repository: missing configured sources (400), a key that is not loaded
(409), a busy repository (503). A malformed schedule trigger on a
repository with no sources is reported as the trigger error. A path or id
in the URL that names nothing is still a 404 before the body is read.

### When a write's answer comes back

Every request that changes something answers once its change is committed
to `state.db`. Some also wait until the change is on disk, so that a power
cut the moment the answer arrives cannot take it back. Which ones is fixed
per route, and every route that is not a `GET` has one of the two classes;
the daemon refuses to start with a write route that has neither, and
`tests/test_route_classes.c` holds each route below to its class.

- **Durable**: the answer waits for a sync of the database's write-ahead
  log (an `fdatasync`). Tags, registering and removing a repository, every
  `DELETE`, and the few other writes that revoke or discard something.
  If that sync fails (a full or failing disk), the answer is a `500` whose
  error says the change is committed but could not be made durable: it is
  in the database, and may not survive a power cut.
- **Commit-only**: the answer comes as soon as the change is committed. The
  daemon's once-a-second flush puts it on disk, so a power cut within about
  a second of the answer can lose that last edit. It cannot corrupt
  anything: the database opens and the repository and its index agree
  after the next start, with the edit either wholly there or wholly not.
  An edit a cut takes back never gives its id to anything else: a job,
  schedule, search or verify run, export, journal entry or repository id
  that was in an
  answer is not issued again after the restart, though the row itself may
  be gone. After an unclean stop the next ids skip ahead (at most 256 per
  table) to guarantee it. About one answer in 128 that creates a row also
  waits for one sync, which keeps that promise (B217).

The sync is the only cost the durable class adds, and it depends on the
disk under `state.db` and what else is writing to it: about 25 ms at the
median on an idle NVMe drive, and up to 2.7 s (p99) on a saturated local
disk (Manual section 6.4 has the numbers measured during a backup).

| Method | Route | Class | Why |
|---|---|---|---|
| POST | `/api/auth/setup` | commit-only | Lost to a cut, setup is simply run again |
| POST | `/api/auth/login` | commit-only | Lost to a cut, the client logs in again |
| POST | `/api/auth/logout`, `/api/auth/logout-all` | durable | Revokes credentials: a lost revocation would hand a session back |
| POST | `/api/auth/password` | durable | Same: the old password must not come back |
| POST | `/repos` | durable | A client told a repository exists acts on it |
| DELETE | `/repos/<id>` | durable | Every delete |
| POST | `/repos/<id>/tags` | durable | A tag is retention protection |
| DELETE | `/repos/<id>/tags/<name>` | durable | Every delete |
| DELETE | `/repos/<id>/snapshots/<sid>` | durable | Every delete |
| DELETE | `/repos/<id>/schedules/<sid>` | durable | Every delete |
| DELETE | `/exports/<id>` | durable | Every delete |
| PUT | `/repos/<id>/sources`, `/repos/<id>/policy` | commit-only | DB-only edit; repeat it if a cut takes it |
| POST, PUT | `/repos/<id>/schedules`, `/repos/<id>/schedules/<sid>` | commit-only | DB-only edit |
| PUT | `/repos/<id>/snapshots/<sid>/note` | commit-only | DB-only edit |
| POST | `/repos/<id>/key/import`, `/repos/<id>/key/ack` | commit-only | The key file is written by its own path; the escrow flag is DB-only |
| PUT | `/repos/<id>/units/<p>/<u>/policy` | commit-only | DB-only edit |
| POST | `/repos/<id>/units/<p>/<u>/reset-incremental` | commit-only | DB-only edit; the next session reads in full either way |
| POST | `/jobs/<id>` (cancel) | commit-only | A cut interrupts the job anyway |
| POST | `/repos/<id>/backup`, `grep`, `verify`, `gc`, `gfs`, `reindex`, `replicate`, `repair`, `snapshots/<sid>/restore`, `units/backup`, `snapshots/<sid>/unit/restore`, `snapshots/<sid>/unit/export` | commit-only | Job submissions: a job a cut catches is reported interrupted, or never started. `gc` and `gfs` delete in their jobs, which make each commit durable before removing what it describes; `repair` makes the index rows it deletes durable before it completes |

## Public

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | Liveness: the process answers; `storage_sync` is `ok` or `failed`, `repos_sync_failed` a count |
| GET | `/readyz` | Readiness: the database is open and its syncs have not failed (503 if they have); `"status":"degraded"` with `repos_sync_failed` listed when only repositories' have, and with `"state_db_checkpoint":"failing"`, `state_db_checkpoint_error` and `state_db_checkpoint_failing_since` (unix time) while `state.db`'s WAL is not being checkpointed |
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

**After a failed sync.** Once a sync has failed, the daemon stops promising
durability for what it covered (Manual section 17.7): Linux may already have
dropped the bytes, and a later sync can succeed over the loss.

- `state.db` or the key directory: the whole daemon, until it restarts.
  `/healthz` still answers 200 and adds `"storage_sync":"failed"`,
  `storage_sync_error` (the file and the error) and `storage_sync_failed_at`
  (unix time); `/readyz` answers 503 with the same reason;
  `cbackup_storage_sync_failed` is 1 and
  `cbackup_storage_sync_failed_timestamp_seconds` the time. Every
  durable-class write is answered `500` with `durable write refused: a
  previous sync failed (<file>: <error>); restart the daemon to re-read from
  disk`, and its handler does not run, so nothing is committed.
- One repository's own file: that repository only. Its `state` is
  `sync_failed`, with `sync_failed` (the file and the error) and
  `sync_failed_at` in `GET /repos` and `GET /repos/<id>`; its durable-class
  writes get the same `500`, naming the repository; its backup, gc, prune
  and replication jobs fail; `/readyz` stays 200 with `"status":"degraded"`
  and `repos_sync_failed: [{"id","path","error","at"}]`;
  `cbackup_repos_sync_failed` counts such repositories. Other repositories
  are unaffected. The latch is recorded in `state.db` and survives a
  restart. `POST /repos/<id>/verify?clear_sync=1` (CLI `repo repair <id>`)
  is the only way back: a job running a deep verify
  that writes nothing, after dropping the repository's pages from the cache;
  it clears the latch only if the verify finds nothing, and fails saying
  what it found otherwise. `409` when the repository has no failed sync.

Reads and commit-only writes still work in both cases.

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
| GET | `/repos` | List, by id; `limit` (default and maximum 1000) and `offset` |
| POST | `/repos` | Register: `{path, label}`; `label` follows the snapshot label rule (at most 256 bytes of printable UTF-8) |
| GET | `/repos/<id>` | One repository, the same object as each entry of the list. `state` is `ok`, `degraded` or `unavailable`. `degraded` is null, or why the repository refuses backups (too many packs missing at once); `unavailable` is null, or why the repository is not there to work on (no marker, `packs/` unreadable or replaced, every pack missing), checked when asked. Manual section 17.6 |
| POST | `/repos/<id>/repair` | `{"accept_missing":true}`: accept a degraded repository's missing packs as lost, marking their objects absent so the next backup stores them again. Without `accept_missing` a 400. Otherwise a job, answered 202 `{"job_id":N}` and queued behind any job holding the repository like every other repo-scoped job; the job fails, naming why, when the repository is unavailable (no marker, or `packs/` unreadable). Every pack missing does not stop the repair, and neither does a replaced `packs/` with packs missing from it when the root's marker is the registered one: its new device:inode is recorded (Manual section 17.6). The job's `result` is `{packs_checked, packs_missing, packs_unknown, packs_dropped, objects_absent, packs_returned, objects_restored, degraded}`, where `packs_returned` counts dropped packs that were back and indexed again |
| DELETE | `/repos/<id>` | Deregister; the data directory is left alone. Index rows are deleted in the background. 409 while a job runs against it. `{"removed":true,"id"}` |

`POST /repos` normalises the path (no `//`, `.`, `..` or trailing slash) and
stores that form. It answers:

| Status | Body | When |
|---|---|---|
| 201 | `{id}`, plus `encryption` and key fields for an encrypted repository | a new registration: a repository created, or an existing one adopted |
| 200 | `{id, already_registered: true, label_updated, encryption}` | the repository was already registered, at this path or, if it moved, at its old one |
| 400 | `{error}` | the path is relative, cannot be created, is inside or contains a registered repository, is a non-empty directory with no marker, holds a repository of another format, asks for an encryption mode the existing repository does not have, or gives a passphrase that does not unlock it (`wrong passphrase`) |

This build reads and writes repository format 3 only. Registering an
existing repository of another format is refused with `<path> is repository
format 2; this build reads only format 3 and has no reader for older
formats. Restore from it with the build that wrote it and back up again into
a new repository`, or for a newer one `<path> is repository format 4,
written by a newer c-backup; this build reads only format 3`. The same check
runs wherever a repository is opened, so a job against a registered
repository whose marker no longer passes fails with the same message, and so
does a replication whose source or target fails it. There is no migration.

Registering an existing passphrase repository with its `passphrase`, whether
or not it is already registered, unwraps the key from the marker and caches
it in `security.key_dir`; the answer carries `key_unlocked: true`. Registering
an existing encrypted repository whose key is not on this host still
succeeds, and the answer carries `key_loaded: false` and a `warning` saying
`no key loaded for repository <uuid>` and how to supply it.

When the repository holds snapshot records (`<repo>/snapshots/<id>`) that
the index has no row for - an adopted repository, a replica, one removed
and added back, or any after `state.db` was lost - either answer carries
`snapshots_unindexed: N` and, unless the key is missing, `reindex_job: <id>`
for the reindex the registration queued. That job's `origin` is `register`,
as is its journal entry's `source`. Until it completes the snapshot list is
short and a verify checks less than the repository holds.

Repository ids are never reused. Removing the newest repository used to
make its id the next one handed out, and the new repository inherited the
old one's schedules, history and index rows, which deregistering does not
delete. `repos` uses `AUTOINCREMENT`, so a new id is above every id ever
handed out.

No other id the API hands out is reused either: job ids, schedule ids,
search and verify run ids, and snapshot ids within a repository. Job,
schedule and run ids came round again when the newest rows were deleted
(by the purge after `DELETE /repos/<id>`, the 30-day job cleanup, or
deleting a schedule), so job 312 of a removed repository was followed by a
job 312 on another one, and a client still holding the old id read an
unrelated job (B113). Those tables use
`AUTOINCREMENT`. Snapshot ids were one more than the highest present, so
deleting the newest snapshot gave its id to the next backup; the daemon
now keeps a per-repository high-water mark.
The mark is kept in the repository too, in `<repo>/snapshot-ids`,
raised before each snapshot record is written, so after a database loss a
reindex restores it from there rather than resuming above whatever record
happens to be highest, which would hand an id pruned from the top to the
next backup. The reindex job's result says what it found
(`snapshot_mark`, `snapshot_ids_repaired`); see `docs/repo-format.md`.
Pack numbers have the same guarantee through `<repo>/pack-ids`, and the
reindex result reports it as `pack_mark` and `pack_ids_repaired`.

A repository is `{id, uuid, path, label, added_at, encryption,
key_escrowed, state, degraded, unavailable}`: `label` is `null` when none
was given, `encryption` is `none`, `system` or `passphrase`, and
`key_escrowed` is a boolean that is `false` while a system-managed key
exists only on this host. `state` is `ok`, `degraded` or `unavailable`
(unavailable winning), and `degraded` and `unavailable` are null or the
reason (Manual section 17.6).

Re-registering is a success rather than a 409 because it is how a moved
repository re-links. A non-empty `label` that differs from the stored one is
applied (`label_updated: true`); an absent or empty one leaves it alone.

## Snapshots and browsing

| Method | Path | Purpose |
|---|---|---|
| GET | `/repos/<id>/snapshots` | List, paged |
| GET | `/repos/<id>/snapshots/<sid>/tree?parent=<node>[&gen=<n>]` | One directory level; `index_gen` with the entries |
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
has_children, may_be_torn}`. `id` is the node id to pass back as `parent`,
with the page's `index_gen` as `gen` (see Node ids and index generations
below).
`may_be_torn` is `true` for a file that was still changing on the backup's
second read and was stored as that read found it (Manual section 6.3): its
bytes may mix two versions of the file. It is `false` for everything else,
and a search result carries it too. `hash` is
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
1 file content (a whole file or one chunk of one), 2 extended attributes,
3 ACL, 5 manifest tree (a directory or one fragment of one), 6 chunk list.
4 was the sparse-file type of format 2 and is never reused.

### Node ids and index generations

A node id is a handle into the daemon's index, not a name for a file. It is
stable only within one index generation of its snapshot: a reindex of the
snapshot (`POST /repos/<id>/reindex`), a gap filled at start, or a cold
snapshot faulted back in rebuilds the snapshot's rows and numbers them
afresh, so after one an id a client kept can name nothing or, worse, a
different file of the same snapshot. Each snapshot's rows therefore carry a
generation, an integer that changes on every rebuild and is never reused
for that snapshot (nor any other: one sequence serves every repository, and
a rebuild that fails burns its value rather than handing it out again).
Deleting the snapshot removes it. A stable node id is a 1.1 candidate.

Every response that carries node ids carries the generation they belong to:

| Route | Node ids | Generation |
|---|---|---|
| `GET /repos/<id>/snapshots/<sid>/tree` | `entries[].id` | top-level `index_gen` |
| `GET /repos/<id>/search` | `results[].node_id` | `results[].index_gen`, per result, since results span snapshots |
| `GET /repos/<id>/objects/<hash>/refs` | `refs[].node_id` | `refs[].index_gen` |
| `GET /repos/<id>/grep/<run-id>` | `hits[].node_id` | top-level `index_gen`, recorded when the run started; `null` for a run older than the field |

The one route that takes a node id, `tree?parent=`, also takes the
generation back as `gen`. When it is passed and is not the snapshot's
current generation the answer is `409 {"error":"index rebuilt; re-resolve by
path"}`, whatever the parent (the root, `parent=0`, included): the client
should resolve the path it was showing again from the root, one component
at a time, and continue with the ids and generation that gives it. Without
`gen` the route behaves exactly as before, so an old client is not broken
by this, only unprotected. `gen` other than a non-negative integer is a
400. A snapshot never indexed reports 0.

The generation and the rows of one response are read so that they agree:
a page whose rows changed under it is read again, and after repeated
rebuilds mid-read the route answers `503` rather than label ids with the
wrong generation. A request made while a rebuild of that snapshot is
running can still see the directory partly filled or missing (a 404 for a
parent the rebuild has not reached yet), as it always could; its ids are
labelled with the new generation and stay valid once the rebuild ends.

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
snapshot it was seen in (`snap_id`, `node_id`, and that snapshot's
`index_gen`) and how many snapshots hold
it (`snapshots`). A file unchanged across thirty snapshots used to come back
thirty times. A file whose content changed is a different hash and a
separate result, so the versions of a file stay visible. To keep a page
cheap the search reads at most eight matching rows per result asked for
(8,000 at most) and does not sort; when it stopped there, `partial` is
`true` and the counts cover what was read. `offset` skips results: with
`snapshot` it skips matching rows, and without one it skips collapsed
results, reading from the first row enough for `offset + limit` of them
under the same 8,000-row bound. Results come in first-seen order, so result
N is the same result on every page; an offset past what the bound can
reach is an empty page with `partial` true. With `snapshot=<sid>` only that
snapshot is searched (`snapshots` is 1); one that does not exist is a 404.
The CLI is `search <id> <text> [--snapshot N]`.

## Encryption

A repository is encrypted or not, decided at creation and never afterwards.
Converting in place would mean rewriting every object with both forms live;
replicating into a fresh repository with `reencrypt` (see Replication) is the
supported path instead.

`POST /repos` takes `encrypt` (`none`, `system`, `passphrase`) and, for the
last, a `passphrase`. A `passphrase` with no `encrypt` means `passphrase`
mode. A `passphrase` with `encrypt` `none` or `system` is refused with 400
rather than ignored: ignoring it used to create an unencrypted repository for
somebody who had just typed a passphrase. Leaving a field out means the
default (no encryption); a field that is present must carry a valid value.
An empty or unknown `encrypt` (`""`, `"SYSTEM"`), an empty `passphrase`, and
either one of the wrong JSON type are each a 400 naming the field, and
nothing is created: both empty forms used to register an unencrypted
repository (B127). On an existing repository, an `encrypt` other than its
own mode is refused, `none` included. Creating a repository needs a passphrase
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
| POST | `/repos/<id>/key/import` | Install a key on a host that lacks one: the recovery path after losing the original machine. `{key}` is exactly 64 hex characters (whitespace around and between them is ignored); anything longer, shorter or not hex is a 400. Refused unless it matches the fingerprint in the marker |
| POST | `/repos/<id>/key/ack` | Confirm the key is stored elsewhere; clears the warning |

All three answer 400 `repository is not encrypted` for an unencrypted
repository, which has no key to export, import or escrow.

An encrypted repository whose key is not on this host answers 409 `no key
loaded for repository <uuid>; ...` to backup, restore, reindex, grep,
gc and replicate, naming the command that supplies the key. Verify and gfs
run without it: parity covers ciphertext, and retention only deletes
records. A job that reaches the repository without the key (a scheduled
one, or a key removed after submission) fails with the same message.

The marker records the mode, cipher and a one-way fingerprint of the key, and
for passphrase mode the Argon2id parameters and wrapped key. It never contains
the key itself. Markers are format 3 for every repository, encrypted or
not, and a build of another format refuses them outright, so nothing ever
writes plaintext objects into an encrypted repository by not knowing it is
one.

Objects are encrypted one at a time with ChaCha20-Poly1305, chosen over
AES-GCM because ARM64 is a deploy target and it does not depend on AES-NI.
The cipher key is derived from the repository key with HKDF, each object
gets a fresh random nonce, and the object's type and name are the
associated data, so stored bytes moved under another name fail to
authenticate. The content-defined chunker's boundaries are keyed by the
repository key too, so the sizes of stored chunks do not reveal known
content. `security.key_dir` sets where host
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
list's paging and `stats?scope=` were found missing in an audit of this
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
| POST | `/repos/<id>/backup` | `{paths:[...], label, exclude:[...], one_filesystem}`; with no `paths`, the repository's configured sources are used. `label` is at most 256 bytes of well-formed UTF-8 with no control characters (C0, DEL or C1); anything else is `400 {"error":"label must be at most 256 bytes of printable text"}` |
| GET/PUT | `/repos/<id>/sources` | What this repository backs up: `{sources:[...], exclude:[...], snapshot:[{path, mode}]}` |
| POST | `/repos/<id>/snapshots/<sid>/restore` | `{dest, overwrite, subpath}` |
| POST | `/repos/<id>/gc` | Mark and sweep; `?dry_run=1` to report only. On gc, gfs and verify, `dry_run` and `repair` take `1`, `true`, `0` or `false`; any other value, empty included, is a 400 rather than read as off |
| POST | `/repos/<id>/gfs` | Apply retention |
| POST | `/repos/<id>/reindex` | Rebuild the index from the repository; the job fails if any snapshot or pack cannot be read, and what could not be read keeps its previous rows |
| POST | `/repos/<id>/grep` | Content search as a job: `{pattern, snapshot, path_prefix, ignore_case, max_files}` |
| GET | `/repos/<id>/grep` | Recent searches, newest first; `limit` and `offset` |
| GET | `/repos/<id>/grep/<run-id>` | The hits from one search, paginated |
| POST | `/repos/<id>/verify` | Scrub: prove stored objects still read back. `?level=index\|structure\|deep` (default `deep`), `?repair=0` to check without writing, `?dry_run=1` (or `true`) the same and overriding any `repair`. `?clear_sync=1`: clear the repository's failed sync if a deep verify that writes nothing finds nothing (409 when there is none; see Public) |
| GET | `/repos/<id>/verify` | Recent verify runs, newest first; `?limit=N` (default 20) and `offset` |
| GET | `/repos/<id>/verify/<run-id>` | The findings from one run, paginated |
| POST | `/repos/<id>/replicate` | Copy this repository somewhere else: `{dest, reencrypt, allow_plaintext}` |

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

The body may also carry `"prefer_engine_copy":true` (`db-detection.md`
9.5): a database with an engine copy in the snapshot, under
`/.cbackup-captures/`, is restored from the copy at its original path, and
its `-wal`, `-shm` and `-journal` are left out, since the old WAL beside a
complete copy would corrupt it.

A restore job's `result` is `files`, `dirs`, `symlinks`, `hardlinks`,
`specials`, `bytes`, `skipped`, `kept` and `failed`, and
`engine_copies` (databases restored that have an engine copy in the
snapshot), `engine_copies_used` (restored from it) and
`engine_siblings_left_out`. Without `overwrite`, a
regular file already at its path with the snapshot's size and an mtime
within two seconds of the snapshot's counts as `skipped` (an earlier run
restored it); anything else there is left alone and counted as `kept`.
Neither fails the job; any `failed` entry does, with the error
`restore incomplete: N entries could not be restored (M written); see the
log for each; first: <path>: <reason>`. Every failed entry is logged as
`restore: <path> not restored: <reason>`. Each file is written under a
temporary `.c-backup-partial.*` name and renamed into place only once its
object has read back verified, so a file at its final path is always
complete (Manual section 8).

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
under several names is one object, so it is read once rather than re-read
per copy. A match in it is still reported under every name, in the searched
snapshot and inside `path_prefix` when one is given, with the same hits:
the other names come from the index, not from reading the content again.
Objects are visited in pack order for the same reason verify is. A file with a NUL byte in its first 8 KiB is a binary: it
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

`files_scanned`, `files_matched`, `files_skipped` and `files_capped` count
files, meaning paths, not objects: content held under three names is read
once, counts as three files, and its match is recorded three times. The
caps treat each name as a matching file of its own. `max_files` 2 against
those three names records two and stops, `truncated`; a name that would
take the hit count past `max_hits` gets the hits that are left, and the
search stops there, `truncated`. A name over its `max_hits_per_file` is in
`files_capped`, as the first one is. `files_matched` used to count objects
and the hits named only the first path, so three identical logs holding a
match showed one file.

The run's `truncated` is `true` when `max_files` or `max_hits` stopped the
search, so a caller can tell "no more matches" from "stopped looking". A file
that reached `max_hits_per_file` does not set it; it is counted in
`files_capped` (files that may hold more than was recorded), which both the
run record in `GET /repos/<id>/grep` and the job's `result` carry. The
`result` also has `run_id`, `snapshot`, `files_scanned`, `files_matched`,
`files_skipped`, `hits` and `truncated`.

A file's matches are recorded only once its content has read back whole and
authentic: every object is checked against its name, and in an encrypted
repository authenticated, before any of it is searched, and a file of many
chunks is read to its end even after its cap or a binary match. A file that does not read
back (damaged beyond parity, failing its tag, or an object that is not file
content) records nothing and is counted in `files_skipped`; the run record
carries `files_skipped` and `skip_reason`, the first such file as `<path>:
<reason>` (`null` when there was none). `ignore_case` is a boolean; `0` and `1` are still
accepted. The CLI takes `--limit`, `--max-hits-per-file` and
`--max-hits`.

`snapshot` defaults to the newest. One that does not exist is `404 no such
snapshot` when the search is submitted, and a value that is not an id is a
400; the first used to complete with zero files scanned, which reads as "not
in that backup", and the second silently searched the newest instead.

The hits outlive the job, so they are fetched separately: `GET
/repos/<id>/grep` lists runs newest-first and `GET /repos/<id>/grep/<run-id>`
returns the hits, `{path, line, excerpt, node_id}` each, with the
`index_gen` their node ids belong to (see Node ids and index generations).
`c-backup grep` does all three steps and prints the
matches, so it feels like `grep(1)` despite the work happening elsewhere.

Verify is the answer to "is the backup still good", which is a different
question from "did the backup run". Three levels, because the cost spread is
enormous: `index` confirms every registered object still resolves and that
every pack file the index refers to exists, and reads no payloads;
`structure` additionally parses each entry header; `deep` reads every
payload and checks it against its name (the SHA-256 of its type and
payload), which is the only level that can prove the bytes are right. Every object in a pack file that has gone is `missing` at
every level, never "damaged": parity lives in the same file as the payload,
so there is nothing for a repair to work from.

By default a deep verify repairs what it can. Every object carries Reed-Solomon
parity, and the read that proves damage exists is the expensive part, so
writing the correction while already there costs almost nothing extra. Pass
`repair=0` (or `dry_run=1`) for a scrub that touches nothing; it still reports
damage, and it still distinguishes recoverable damage from loss, because it
applies parity in memory without writing.

`dry_run=1` is `repair=0` and nothing else: it overrides `repair`, and it
does not lower the level, so with no `level` it is a full deep read like a
plain verify, with the same counts on a healthy repository. The job's
`result` says which kind of run it was, since the run record does not:

```json
{"run_id":12,"level":"deep","repair":false,"dry_run":true,"checked":52,
 "ok":52,"repaired":0,"recoverable":0,"unrecoverable":0,"missing":0,
 "failed":false,"unsettled_files":0,
 "pack_structures_repaired":0,"pack_structures_recoverable":0,
 "packs_unwalkable":0,"packs_restored":0,"packs_salvaged":0,
 "unreferenced_dropped_bytes":0,"unreferenced_dropped_ranges":0,"unreferenced_dropped":[]}
```

The counts are the run's (table below), and `failed` is true exactly when
`unrecoverable` or `missing` is non-zero, which is when the job fails.

`unsettled_files` is how many file entries the run found flagged as
possibly torn (below); a notice, never a failure.

`pack_structures_recoverable` and `pack_structures_repaired` count packs
whose own header, tail copies or CRC table disagreed with their redundancy
(within parity, so not loss): found by every level, each a `pack-NNNNNNNNNN`
finding with status `recoverable`, or `repaired` when a repairing run
rewrote them. Each such pack is also one item in the run's totals: it adds
one to `checked` and one to `recoverable` or `repaired`, so the counts on
the run record (`GET /repos/<id>/verify`, what `c-backup verify` prints and
the verify metrics read) agree with its findings. These two fields say how
much of those totals is pack structure rather than objects.

`packs_unwalkable` counts packs whose entry headers cannot be walked even
with parity (the packs gc leaves as they are); each is a finding named
`pack-NNNNNNNNNN` with status `unwalkable pack`, at every level, and fails
the run. `packs_restored` counts those a repairing run made walkable again
by rebuilding their headers from the index; each is a `repaired` finding,
and counted like one: one in `checked` and one in `repaired`.
`packs_salvaged` counts packs that could not be rebuilt (a damaged header
had no index row) and were salvaged instead: every indexed entry copied into
a new pack, a `salvaged` finding, and the bytes no row covers dropped and
reported as `unreferenced_dropped_bytes`, `unreferenced_dropped_ranges` and
`unreferenced_dropped` (first 16, `{"pack","offset","length"}` in the old
pack). An indexed entry damaged beyond parity is carried as it is and stays
an `unrecoverable` finding.

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
| `checked` | Items examined: every indexed object, plus every regular-file entry that names no content, plus each `pack-NNNNNNNNNN` finding counted as `recoverable` or `repaired` (a pack whose header, tails or CRC table were damaged, or whose headers were restored). Always `ok + repaired + recoverable + unrecoverable + missing` |
| `ok` | Intact on disk. Never counts a failure: a run with anything missing or unrecoverable has `ok` below `checked` |
| `repaired` | Damaged, corrected from parity, and the correction written to disk; or an object whose indexed copy is beyond parity, moved by the run to another copy proven to be it (below). Always 0 with `repair=0` |
| `recoverable` | Damaged, readable only through parity, and still wrong on disk: repair was off, or the write-back failed. Also an object whose indexed copy is beyond parity while another copy of it is proven intact, with repair off |
| `unrecoverable` | Damaged beyond what parity can rebuild. Data loss |
| `missing` | Indexed but absent: no pack entry, or a pack file that has gone. Also each index row of a regular file that names no file content: no object, or one that is neither `FILE` nor `CHUNKLIST` (see below). Data loss |
| `affected_files` | Distinct file paths referencing a lost object |
| `affected_snapshots` | Distinct snapshots holding any of those files |

Findings (`GET /repos/<id>/verify/<run-id>`) name files, not hashes, so the
report says what was lost and what is still clean. Every object that is not
`ok` gets one, with `status` one of `missing`, `unrecoverable`, `unreadable
header`, `uncorrectable parity`, `recoverable` or `repaired`. Every object
is read whole with parity applied in memory, so `unrecoverable` means the
same with repair off as with it on. (Format 2 had two more, `unrecoverable:
too large for 32-bit parity fields; repair disabled pending format fix` and
its `recoverable, not written back` form, for packs whose trailer sizes
might have wrapped. Format 3 bounds every pack so that cannot happen, and
they are gone.) Each finding also carries:

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

An object whose indexed copy cannot be read even with parity is not loss
if the repository holds another copy of it: two backups can store one
object before either commits it, and gc keeps the other copy and moves the
index to it once proven (`index_relocated`). Verify runs the same search
before it calls anything lost: every pack's entry headers are walked and a
copy that is not the indexed one is read and checked against its name, as
gc proves one. With such a copy the object is `recoverable` (repair off;
the log names the pack and offset of the good copy) or `repaired` (the run
moved the index to it and read the object back through it). Without one it
is lost as before. The search runs only when the walk found something
unreadable, so a clean run pays nothing for it. An encrypted copy cannot be
proven without the key.

Every level also checks the index for regular files that name no content
object: a file a snapshot lists and no restore can produce. Backups by a
c-backup older than 1.0.0 recorded a file that was skipped during the store
phase that way. These are one finding with `hash` `""` and `status` `missing: file
entry has no content`, its `affected_*` fields and `sample_paths` naming
the files and snapshots, and each such row counts once in `checked` and
once in `missing`, so the run fails and `ok` stays below `checked`. A backup no longer writes such an entry (see the backup
`result` below), and the tree writer refuses one.

Every level also lists the file entries flagged as possibly torn - stored
while still changing, `backup.unsettled: store` - as one finding with
`hash` `""` and `status` `notice: stored while still changing; may not be
internally consistent`, naming the files and snapshots. A status starting
`notice: ` is not damage: it is never counted in `missing` or
`unrecoverable`, never in the blast radius, and never fails the run.

`/metrics` exposes `cbackup_last_verify_age_seconds` alongside
`cbackup_last_snapshot_age_seconds` - between them, an alert can tell that
backups are running *and* that what they wrote is still readable - plus the
newest completed run's `cbackup_verify_unrecoverable_objects` (unrecoverable
plus missing), `cbackup_verify_repaired_objects`,
`cbackup_verify_recoverable_objects` and `cbackup_verify_affected_files`.

A backup writes every object exactly once, straight into a pack; there are
no loose objects, so there is no pack job, no `/pack` route and no
`maintenance.auto_pack` policy. Small packs are merged by gc.

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

`snapshot` is the snapshot mode of each source (`db-detection.md` 9.4,
Manual 6.6): a list of `{"path", "mode"}`, `path` one of `sources` and
`mode` `"strict"` (the default when left out) or `"warn"`; a source not
listed is read live as always. `GET` always carries the list, `[]` when no
source is in snapshot mode. On `PUT`, a `snapshot` given replaces the modes;
left out, the modes of the sources that stay are kept, so a client that
knows nothing of it cannot turn it off by saving its source list. A path
that is not one of the sources, a mode other than those two, a source
listed twice or a `snapshot` that is not a list is a 400 that changes
nothing. The answer counts them (`"snapshot":N`) and warns when no helper
is configured, since only the helper can take a snapshot. A backup reads
the modes for its paths when it runs, whether its paths came from this
list or from the request.

A strict source that cannot be read from a snapshot fails the backup before
anything is read: the job fails with `N source(s) in strict snapshot mode
cannot be read from a snapshot, so nothing was backed up: <path>: <reason>;
...`, and no snapshot is written.

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
whole pack because one object inside it was collected. The target writes
what it receives into packs of its own, with fresh parity, so the two sides
may disagree about physical layout while holding provably the same data.
Both ends must be format 3; a source or target of another format fails the
job with the refusal described under `POST /repos`.

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
written plaintext into an unencrypted destination. That last case, an
encrypted source into an unencrypted destination, is a 400 (`this
repository is encrypted and the target is not, so the copy would hold every
file in plaintext; ...`) unless the request also sets `"allow_plaintext":
true`, a JSON boolean (anything else is `allow_plaintext must be true or
false`; with no `reencrypt` it is `allow_plaintext applies only with
reencrypt`). The check is repeated when the job runs. An allowed run logs a
warning and journals `replicated encrypted repo N into plaintext at <path>`
on the source. Unencrypted into unencrypted needs no flag. Names are of the
plaintext, so they and the trees carry over unchanged, as do snapshot ids;
a snapshot id the destination already uses for another tree fails the run
instead of being overwritten. The job fails, saying which, when `dest` is
not a repository, is a replica of the source, or is encrypted and its key is
not on this host. The job's result reports `snapshots_sent`,
`snapshots_skipped`, `objects_asked`, `objects_sent`, `bytes_sent`,
`subtrees_skipped` and `reencrypted`. An object damaged beyond parity is
sent and the run finishes, then the job fails with `replicated with N
object(s) damaged beyond parity; the replica holds them as damaged`, and the
result adds `objects_damaged`, `damaged` (the first 100, each
`{"hash","snapshot"}`) and `damaged_listed`. Re-encrypting is a manual
operation: the schedule route does not store `reencrypt`. Each object is
decrypted and authenticated whole under the source key before it is
encrypted for the destination, with the same associated data, so nothing
unauthenticated reaches the destination at all. An object that does not
authenticate fails the run.

Replication also carries the snapshot-id mark: the target's `snapshot-ids`
is raised to each snapshot's id before that snapshot's record is written,
and at the end of a successful run to at least the source's mark, so a
replica never numbers below its source.

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
check. Those sit between files and also inside one file - between reads
while storing it and between objects while restoring it - so a cancel lands
within one object (at most 16 MiB of I/O), not at the end of a large file. A cancelled backup
discards its open pack, the index rows staged into it and its staging files.

`POST /jobs/<id>/cancel` answers 200 with the job record, as `/jobs/<id>`
returns it, plus `"cancel_requested":true`. A queued job is taken out of the
queue and never runs: its `state` is already `cancelled`, and the journal
records it as cancelled with the message "cancelled before it started"; its
`started` is null. Shutting the daemon down does the same to every queued
job. A running job's `state` is usually still `running`: it reports `cancelled` once
it has stopped. A job that is not queued or running is a 404.

Jobs for one repository start one at a time, in submission order. A job
whose turn comes while a synchronous request holds its repository (`DELETE
/repos/<id>/snapshots/<sid>`, `DELETE /repos/<id>`) is not failed: it stays
`queued` with `phase` `"waiting for repository"`, holds no worker, and is
retried with a backoff of at most 500 ms until the request lets go. It can be
cancelled meanwhile like any queued job. The `phase` is cleared when it
starts. A replicate whose `dest` is a repository registered here holds both
repositories: it starts only when both are free, takes both or neither, and
counts as queued on each, so a job submitted later on either one starts after
it. The requests themselves are unchanged: they still answer 503 (or 409
for a removal) while a job holds the repository.

A job record carries `origin`, who asked for it: `api` for anything submitted
over the API (the CLI and the app both are), `schedule` for the scheduler, with
`schedule_id` naming the schedule (null otherwise), `startup` for a reindex
the daemon queued itself at start because the index had lost snapshots the
repository holds (Manual section 17.5), or `register` for the reindex a
repository registration queues (`POST /repos`).

`/jobs/stream/<id>` sends one `data:` frame at once with the job's state as it
is, then a frame whenever it changes, and the same frame again every 5 seconds
while it does not, so a watcher can tell a busy job from a dead stream. The
stream ends after the frame with a terminal state. A frame is
`{"id","state","phase","current","total","bytes_done","bytes_total","error"}`,
and the last frame of a job that completed with warnings adds `"warning"`,
the same line as its `result`'s (below).
`bytes_done` advances inside a file as backup and restore read and write it,
not only between files, so a job spending minutes on one large file still
sends a changed frame a few times a second.

The stream path puts the id last because the router matches a trailing
wildcard, and `/jobs/<id>/stream` would be indistinguishable from the buffered
job endpoint.

Finished jobs are kept for 30 days and then deleted, so `/jobs/<id>` answers
404 for a job older than that. The jobs table is operational state; the
permanent record of what ran is each repository's journal
(`GET /repos/<id>/journal`). A job that completed with warnings is
journalled with `result` `warning` rather than `success`, its message the
job's summary followed by the warnings (Manual section 12), so they outlive
the job row. While the row lasts its `result` carries them too, in
`/jobs/<id>` and `/jobs` alike (B211): `"warning"`, that same line (the
text itself for one, `"N warnings: a; b"` for more, with how many did not
fit), and `"warnings"`, each one shown in it. A job that set no result has
one with just those two; a result that names either key itself keeps its
own. A job left running or queued by a daemon that
stopped is marked failed with "interrupted by daemon restart" when the next
one starts, since nothing resumes it, and journalled then with that message:
a daemon that was killed had no chance to journal it itself. A backup whose
snapshot record is in the repository is the exception (B238): the backup
records which snapshot it is writing, durably, before the record exists, and
the next start marks it `completed` with `result`
`{"snapshot_id":N,"recovered":true}` and journals it as a success. When the
repository cannot be read at that start its error is `interrupted by daemon
restart; snapshot N may have been written, and the repository could not be
read at start: <reason>`.

A job's end is made durable before it is shown: from the moment its end is
written until the WAL holding it is synced, `/jobs`, `/jobs/<id>` and the
stream show the job as `running`, with no `ended`, `error` or `result`. So a
client that has read a job's end never reads a different one after a power
cut.

A job whose end cannot be written, because `state.db`'s filesystem is full,
say, is not left reading `running`. Its end (`state`, `error`, `ended`, its
`result` and final progress) is kept in the daemon's memory, and `/jobs`,
`/jobs/<id>` and the stream show it from there, with `"end_unrecorded":true`
on the record while the table does not have it yet. It is written, with its
journal entry, once the database takes writes again: retried with a backoff
of 1 to 30 seconds, and straight away whenever another job's end is
written. A daemon that stops before then records the job as interrupted at
its next start, as above, and logs the end it could not write.

A journal entry's `source` is the job's origin: `api`, `startup`, `register`, or
`schedule` with the message starting `schedule <id>`.

What some jobs report in `result`, beyond the backup and restore counts:

| Job | `result` |
|---|---|
| gc | `dry_run`, `reachable`, `objects_swept` (objects removed from the index, or that would be), `bytes_freed` (stored bytes compaction removed from packs, only what it finished when it fails part way; on a dry run, what sweeping would make reclaimable), `pack_entries_kept`, `pack_entries_dropped`, `packs_rewritten`, `packs_deleted`, `packs_coalesced` (small packs merged by the coalescing pass) and `packs_removed_by_coalesce` (how many fewer pack files that left: 20 merged into 3 is 20 and 17), `tmp_files_removed`, `tmp_bytes_removed`, `content_missing` (file content a tree names that the repository does not hold, a chunk list included; nothing beneath it can be kept alive), `snapshots_indexed` (snapshot records the index lacked, indexed before marking; B200), `dups_dropped`, `dups_kept` and `index_relocated` (OWN-22 duplicate copies dropped behind a confirmed indexed copy, kept because the indexed copy could not be confirmed, and index rows moved off such a copy onto a proven one), `packs_unwalkable`, `unwalkable` (first 16, `{"pack","reason"}`), `pack_entries_damaged`, `damaged` (first 100 hashes), `damaged_listed`; the job fails when `pack_entries_damaged` or `packs_unwalkable` is non-zero |
| replicate | `snapshots_sent`, `snapshots_skipped`, `objects_asked`, `objects_sent`, `bytes_sent` (as stored), `subtrees_skipped`, `reencrypted`, `objects_damaged`, `damaged` (first 100, with snapshot), `damaged_listed`; the job fails when `objects_damaged` is non-zero |
| reindex | `snapshots`, `snapshots_failed`, `files`, `objects`, `packs_unreadable`, `tags_restored`, `tags_skipped`, `snapshot_mark` (the snapshot-id high-water mark the index now holds), `snapshot_ids_repaired` (`true` when `<repo>/snapshot-ids` was missing, damaged, from another repository or behind, and was rewritten), `pack_mark` (the lowest pack number never issued, as `<repo>/pack-ids` now holds it), `pack_ids_repaired` (`true` when that file was missing, damaged or behind the packs on disk or the index, and was rewritten) |

A backup's `result` has its counts (`snapshot_id`, `files`, `dirs`,
`files_unchanged`, `bytes_logical`, `objects_new`, `bytes_stored`,
`trees_new`, `trees_reused`, `store_workers`), the files it left out, and
what it wrote into the repository, in bytes. `store_workers` is the number
of chunk workers the backup used for files over 16 MiB (1 when every chunk
was stored on the backup thread). A failed backup has no `result`, except
one that wrote its snapshot record and then could not commit the index
rows: `{"snapshot_id":12,"indexed":false}`, with an error naming the
snapshot and how it gets indexed (Manual section 6.3, step 5).

A regular file the scan found and the store could not read is left out of
the snapshot entirely: not in its trees, not in the index, not in `files`.
That is a file that vanished before it was read, one the daemon may not
open, one replaced by a directory or other non-file, one whose read
failed partway, or one still changing on a second read. Every store path
applies the same rule: the backup thread, the file workers and the
small-file workers. The job still completes (Manual section 6.3), and says so:

| Field | What |
|---|---|
| `files_skipped` | Every file left out. Always present; `0` on a clean run |
| `skipped` | The first 100 of them, in scan order, each `{"path","reason"}`; `[]` when none |
| `skipped_listed` | How many `skipped` holds; less than `files_skipped` when more than 100 were left out |
| `files_changed` | Files whose size, times or inode (a file replaced by rename) had changed by the time they were read (since the scan, or during a read that was then repeated) and that were stored as read: one whole version, its size and mtime in the snapshot. Always present |
| `changed` | The first 100 of them, as paths; `[]` when none |
| `changed_listed` | How many `changed` holds |
| `changed_unsettled` | Files still changing on the second read and stored as it found them, flagged as possibly torn (`backup.unsettled: store`, the default). Not in `files_changed`. Always present |
| `unsettled` | The first 100 of them, each `{"path"}`, with `"why"` when the file is a detected database: what it is, and whether a consistent engine copy is in the snapshot (`db-detection.md` 9.5); `[]` when none |
| `unsettled_listed` | How many `unsettled` holds |
| `databases` | What database detection found in what the backup read (`db-detection.md`): `{"findings":[...],"listed":N,"total":N,"sqlite_small":N}`. Present on every completed backup while `db_detect.enabled` is 1. Each finding has `id` (its row in `db_findings`), `engine`, `version` (when known), `path` (`""` for a hint), `confidence` (`live`, `dormant`, `hint`), and for a live one `coverage` with `coverage_why`, `method` (`live-read` for a file source, `engine-copy` once its engine copy was made, which makes the coverage `covered-application`), `suggestion` and `optional` (template or action names, section 6 of the design), `unsettled` (files under it that changed during their read) and `evidence` (an object: the marker files, links, journal mode). At most 200 are listed; `sqlite_small` counts dormant SQLite files under `db_detect.sqlite_min_kb`, which are not listed. When the backup made engine copies, `captures` lists each: `path`, `engine`, `method` (`sqlite-online-backup`), `finding`, `time`, `outcome` (`ok`, `refused`, `abandoned`, `failed`), `copy` (its path in the snapshot, or `null` with a `reason`), `size`, `journal_mode` and `settle` (`settled` or `unsettled`, how the raw file read) |
| `source_snapshots` | Present when any source is in snapshot mode (`db-detection.md` 9.4, Manual 6.6): each source read from a snapshot, with `"zfs"` or `"btrfs"` |
| `live_sources` | With it: each source in `warn` snapshot mode that was read live, `{"path","reason"}`; `[]` when none. The job's warning and the journal name the same paths and reasons |

A file is skipped for still changing on its second read only under
`backup.unsettled: skip`; by default it is stored and counted in
`changed_unsettled` instead.

`reason` reads `cannot open: it no longer exists`, `cannot open:
Permission denied` (or another errno text), `no longer a regular file (now
a symlink)` (or `a directory`, `a FIFO`, `a socket`, `a character device`,
`a block device`; a symlink is never followed), `a directory on its path is
now a symlink; not followed`, `its path now crosses a mount point or leaves
the source; not followed` (a one-filesystem backup; Manual section 6.3),
`changed while it was being
read, and again on a second read` (a file still changing when read a second
time; Manual section 6.3), or `cannot read: ` followed by the store's
error. Each is also a `WARN` line in the daemon log, `backup:
skipping <path>, it is not in this snapshot: <reason>`, followed by one
summary line, and the job's journal entry reads `snapshot N: F files, S
skipped (not in the snapshot); first: <path>: <reason>`. A file stored
although it never settled adds `U stored while still changing (may be
torn); first: <path>` to the same entry (after a `; ` when both apply). A
clean backup journals no message, as before. A backup that skipped
anything, or kept anything that never settled, also sends one warning
alert naming the count and first path of each (Manual section 16.3); the
job stays `completed`.

What it wrote:

| Field | What |
|---|---|
| `bytes_written` | The sum of the categories below: everything the backup wrote into the repository apart from small metadata (the snapshot record, `snapshot-ids`) |
| `bytes_written_pack` | Sealed pack files, without their parity trailers |
| `bytes_written_pack_parity` | The parity trailers of those packs |

`bytes_written / bytes_logical` is the backup's write amplification: on
storage whose write bandwidth is the limit, it is roughly the backup's time
over a plain copy's. Every object is written once, into a pack, so nothing
is counted twice. The state database is not counted. The same figures are
logged after the backup's phase profile.

Gc and replicate also put a one-line summary in their journal entry, such
as `swept 12 objects, 40960 bytes freed; 1 pack rewritten, 0 deleted; 0 stale
temp files removed` or `target already up to date (3 snapshots present)`.
A verify that passes journals its run's totals, such as `deep verify (check
only), run 12: 53 checked, 52 ok, 0 repaired, 1 recoverable (1 of them pack
structure)`; one that fails journals its error, which carries them. A
dry-run gc removes nothing, temp files included; its pack figures are zero
because compaction does not run.

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
| GET | `/repos/<id>/stats` | Object counts and sizes by type; `?scope=staged` narrows to objects in a pack a running backup has not sealed yet |

`/summary` carries `snapshots`, `objects`, `phys_size`, `orig_size`,
`packs`, `staged` (objects in packs not yet sealed: 0 unless a backup is
running) and `tags`, plus `last_snapshot_ts`.

Size fields on the object endpoints are `orig_size`, the logical bytes that
went in, and `phys_size`, the bytes the object itself is charged for: the
stored payload alone, after compression (and encryption, nonce and tag
included). Every object lives in a pack, and the 51-byte pack entry header
and the entry's parity block in the pack's trailer are charged to the pack
(`packs.size`), not to the object. So `phys_size` against `orig_size` is an
object's compression ratio, and the disk a repository uses is the sum of
`packs.size`, with nothing counted twice. `pack_objects` reports the same
figure as `comp_size`.

`/layout` reads the object on disk rather than the index. `header_size` is
always 51, the pack entry header (compile-time checked in
`src/store/pack.h`). `physical_size` is the entry as it sits in the pack:
entry header plus payload, which is `phys_size + 51`, with the parity block
in the pack trailer not counted. `payload_size` is `physical_size -
header_size`, which equals `phys_size`. An object that reports `phys_size`
5242880 and `physical_size` 5242931 is consistent: 5242931 - 51 = 5242880.

| GET | `/repos/<id>/objects` | Paginated; `?location=staged`, `?location=pack` (or `packed`), or `?location=all` |
| GET | `/repos/<id>/objects/<hash>` | Where one object lives |
| GET | `/repos/<id>/objects/<hash>/refs` | Which snapshots and nodes reference it, `{snap_id, node_id, name, path, index_gen}` each (`index_gen` as under Node ids and index generations); `limit` (default 500, at most 5000) and `offset` |
| GET | `/repos/<id>/objects/<hash>/layout` | Header fields and on-disk footprint |
| GET | `/repos/<id>/objects/<hash>/content` | Base64, capped at 2 MiB |
| GET | `/repos/<id>/packs` | Pack listing with entry counts, by pack number; `limit` (default 500, at most 5000) and `offset` |
| GET | `/repos/<id>/packs/<pid>/entries` | Contents of one pack, paginated |
| GET | `/repos/<id>/fanout` | Hash-prefix histogram |
| GET | `/repos/<id>/diff?from=<sid>&to=<sid>` | Compare two snapshots; `limit` (default 2000, at most 5000) and `offset` page the changes |

An object's `refcount` is the number of references to it as the last `gc`
or `reindex` counted them, not a live count: a backup records the objects it
writes with `refcount` 0 and does not count them, and deleting a snapshot
does not take its references away. So right after a first backup every
object reads 0, which does not mean unreferenced, and after a prune a count
can be too high until the next `gc`. GC does not trust the column either -
it zeroes it and re-marks from the snapshots' trees on every run, which is
what makes the count exact afterwards - and `/objects/<hash>/refs` is the
live answer for one object. Keeping it exact through every backup and
prune would mean adjusting it on every path that adds or drops a
reference, which is the drift mark-and-sweep exists to avoid (`gc.h`).

An object's `location` is `packed`, `staged` (in a pack a running backup
has not sealed yet) or `absent` (named by a snapshot's trees but held by no
pack, as a reindex found it; verify reports it `missing`), and `pack_id` is
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
whole file. For a chunk list (type 6) it returns the start of the file the
list describes rather than the list's own bytes, holes as zeroes:
`{"type":6,"chunklist":true,"size":<logical size>,"returned":<bytes>,
"truncated":<bool>,"base64":"..."}`.

`/diff` walks manifest trees rather than comparing node lists. Two directories
with the same hash have identical contents by construction, so an unchanged
subtree is pruned in a single comparison instead of being read. That is the
same property that makes incremental backups cheap, applied to reading.

`/diff` pages its `changes`. The walk is deterministic (both trees merged
in name order, depth first), so the Nth change is the same change on every
request for the same two snapshots. A page is cut from the walk as it runs:
changes before `offset` are counted and dropped, and changes past `offset +
limit` are counted and set `truncated`. So `added`, `removed` and
`modified` always cover the whole comparison, `truncated` is `true` exactly
when more changes follow this page, and neither the response nor the walk
holds more than a page of changes and one tree per directory level. Each
page walks the changed part of the trees again; the unchanged part is
pruned as always. The default page is 2000, the cap the route had before it
paged, so a client that does not page sees what it always did.

## Metadata

| Method | Path | Purpose |
|---|---|---|
| GET/POST | `/repos/<id>/tags` | List, or set `{name, snap_id, move}` |
| DELETE | `/repos/<id>/tags/<name>` | Remove; `{"removed":true,"name"}`, 404 if there is no such tag |
| GET/PUT | `/repos/<id>/snapshots/<sid>/note` | Read or write `{body}`; 404 for a snapshot that does not exist |
| GET/PUT | `/repos/<id>/policy` | Read, or set `{key, value}` |
| GET | `/repos/<id>/journal` | Operation history, newest first: `{id, ts, source, operation, result, duration_ms, message}`, `duration_ms` and `message` `null` where none was recorded. `id` is unique across every repository's journal and never issued twice, so a repository's ids have gaps. `limit` (default 100, at most 1000), `offset`, and `result` |

`result` filters the journal on one of the values anything journals:
`success`, `failed` and `cancelled` for how a job ended, `warning` and
`alert` for advisories. Anything else is a 400 `result must be one of:
success, failed, cancelled, warning, alert`; it used to answer an empty
list, which reads as a clean history (NIT-32).

`/tags` pages by name and `/schedules` by id, each `limit` (default and
maximum 1000) and `offset`.

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
daemon's configured `retention.*` values from `config.yaml`. Each is
`{key, value}` with the value typed by its key, a number for a count. A value
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

The value may be sent as the GET serves it (`6`) or as text (`"6"`).
`maintenance.auto_pack` is refused like any other unknown key: format 3 has
no pack pass for it to control. A count is decimal digits only: `-3`, `+3` and `banana` are refused. These
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

`every` is `day`, `week` (with `on`, a weekday: the full name or its first
three letters, in any case, stored and served as the full lowercase name),
`month` (with
`on_day`, 1-31) or `year` (with `on_month` and `on_day`, a day that month
has: 30 February and 31 April are refused, 29 February is accepted). There
is no cron syntax. The CLI builds the same with `--every`, `--at`, `--on`, `--on-day`
and `--on-month`, or takes a number of seconds for an interval.

Interval triggers anchor to the last time the schedule fired, or until its
first fire to when it was created, not to wall-clock multiples: a four-hour
backup that runs long should not be due the moment it finishes. A daemon
restart does not move the anchor.

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
| `backup` | `paths` (optional; the repository's sources otherwise), `label` (optional; the same rule as a backup request, checked at creation) |
| `replicate` | `dest` (required, absolute) |
| `grep` | `pattern` (required, a valid extended regular expression), `path_prefix`, `ignore_case` (a boolean, stored as one; `0`/`1` accepted), `max_files` |
| `units` | `provider` (required, enabled now), and `all` with optional `exclude` globs, or `units` (names or ids); optional `require`. The body of `POST /repos/<id>/units/backup`, stored canonically |
| others | none |

A `units` schedule with `all` enumerates the provider's units each time it
fires, so a unit added since is included. Its overlap is keyed by repository,
action and unit set: two `units` schedules naming different units are not
each other's overlap. From the CLI: `schedule add 1 units --every day --at
01:00 --provider fake --all [--exclude GLOB]...` or `--unit NAME`.

## Database findings

What database detection found (`db-detection.md`, Manual 23): the current
view per repository, kept in `db_findings` by every backup and detection
run. Read-only here; enabling and dismissing a suggestion are their own
routes.

| Method | Path | Purpose |
|---|---|---|
| GET | `/repos/<id>/databases` | Findings, paginated (`limit`, `offset`; 100, at most 1000), ordered by scope, path, engine and id, filtered exactly by `scope` (`unit:<unit key>` or `source:<path>`), `engine` (the vocabulary below), `coverage` (the coverage names, or `none` for not assessed) and `state` (`new`, `suggested`, `enabled`, `dismissed`). A value outside its set is a 400. Answers `{"findings":[...],"total":N}`, `total` counting every row the filters match |
| GET | `/repos/<id>/databases/<f>` | One finding, the same fields. 404 for an id that is not this repository's |

Each finding:

| Field | Meaning |
|---|---|
| `id` | its row, never reused |
| `scope` | `unit:<unit key>` or `source:<source path>` |
| `engine` | `postgres`, `mysql`, `sqlite`, `redis`, `prometheus`, `influxdb1`, `influxdb2`, `mongodb`, `elasticsearch`, `leveldb`, `rocksdb`, `lmdb`, `bbolt`, `clickhouse`, `nextcloud` |
| `version` | when the files say (`16`, `rdb 11`, Nextcloud's own `30.0.2.2` from `config.php`), else null |
| `path` | the recorded path of the data directory or file; `""` for a hint |
| `confidence` | `live`, `dormant` or `hint` |
| `coverage`, `coverage_why` | for a live finding, how consistent the capture was, and why in a sentence; null otherwise |
| `method` | how it was captured: `snapshot`, `paused-read`, `live-read`, or `engine-copy` for a file source's SQLite whose engine copy was made (its coverage is then `covered-application`); null for a hint |
| `suggestion`, `optional` | a template or action name, or null (Manual 23.3) |
| `state` | `new`, `suggested`, `enabled`, `dismissed` |
| `evidence` | an object: `markers`, `journal_mode`, `flavour`, `unsettled`, `links`, `also`; from units.probe, `processes` (`container`, `comm`, and for java the `args_match` that made it Elasticsearch's or OpenSearch's) and `open_write` (at most 8 each), `ports` (`container`, `port`: the engine's listening ports; none under `--network none` says nothing), `unit_names` (a PostgreSQL's or MySQL's unit, for resolving a Nextcloud) and Nextcloud's `link` (`dbtype`, `dbhost`, `resolved`, `databases` (its database's data directories in the same unit), `host`, `consistent`, `note`); for a hint, `images`, `ports`, `processes` and a `note` |
| `first_seen`, `last_seen` | epoch seconds |
| `last_snap_id` | the snapshot it was last seen in; a detection run leaves it as it was, null if only detection has seen it |
| `alerted_at` | the one alert of its uncovered spell, or null |
| `template`, `params`, `decided_by`, `decided_at`, `reason` | what enabling chose (`params` an object) or why it was dismissed, and who and when; null until then |

## Units

Virtual machines and containers backed up through a source provider
(`docs/source-providers.md`). One snapshot per unit per run, with the unit's
metadata under the reserved `.cbackup-unit/` directory at its top, disks as
sparse regular files and container data at its real paths. Every unit a
request names is held to `units.allow` (a config key, so no session can widen
it); a unit outside it is not listed and cannot be described or backed up.

| Method | Path | Purpose |
|---|---|---|
| GET | `/providers` | Every compiled-in provider, enabled or not, its probe, the daemon id, `units.allow` and how many holds are journalled |
| GET | `/providers/<p>/units` | Live enumeration, by name, paginated (`limit`, `offset`), with `total` |
| GET | `/providers/<p>/units/<u>` | Describe one unit (by name or id): items, kinds, sizes, capabilities |
| GET | `/repos/<id>/units` | Units with history here: last snapshot, its level, how many, paginated by name |
| GET | `/repos/<id>/units/<p>/<u>/snapshots` | That unit's snapshots, newest first, each with its level and items (`read_mode`, `reason`, `bytes_read`) |
| GET/PUT | `/repos/<id>/units/<p>/<u>/policy` | Per-unit policy: `retention.keep_*` keys over the repository's; `capture.exclude` (patterns on recorded paths, one per line, applied only when set); `capture.group` (units captured together under `pause_scope: group`); `null` or `""` drops one; GET shows the unit's own and what is in force. PUT takes a unit with snapshots in the repository (under their key), or one its provider lists now and `units.allow` takes (keyed by its id); otherwise 404, or 403 outside `units.allow`, or 409 when its provider cannot be asked |
| GET | `/repos/<id>/units/<p>/<u>/checkpoints` | The unit's incremental chain (vm-backup.md 5.8): `incremental` (`enabled`, `disabled_reason` and `disabled_at` after a failed audit, `last_audit_at`, `audit_days`, `delta_full_pct`) and `tokens`, one per disk: `item`, `checkpoint`, the `snapshot` it describes, `usable`, `identity`, `created_at`. A unit this repository has no snapshot or chain of must be one its provider lists now, as for a policy write: 404 otherwise (an unknown or disabled provider, a unit it does not list), 403 outside `units.allow` |
| POST | `/repos/<id>/units/<p>/<u>/reset-incremental` | After a failed bitmap audit: incrementals back on for the unit, every token made unusable so the next backup is a full read under a new checkpoint (the old one is retired when it commits). Journalled. Answers `unit_key` and `was_disabled`, the reason they were off. Refused like `checkpoints` for a unit nothing knows of |
| POST | `/repos/<id>/units/backup` | `{"provider":"fake","all":true,"exclude":["x*"]}` or `{"provider":"fake","units":["a"]}`, optional `"require":"crash"` (the default) and `"pause_scope":"unit"` (the default; a `units` schedule stores and honours it too, `--pause-scope` from the CLI; any other key is refused), `"group"` (units sharing a `capture.group` captured together) or `"job"` (every unit at one instant); 202 with a job. The job fails when any unit fails, and its result lists each unit's outcome and snapshot, `freeze_ms` when it was frozen, and its `warning` (the first, or the one that outranks the rest) with `warnings`, every one of them; the job's own warning names the first and how many more there are |
| POST | `/repos/<id>/snapshots/<sid>/unit/restore` | `{"mode":"files","dest":"/abs/dir"}`, the whole snapshot with each disk as a sparse raw file; or `{"mode":"item","item":"vda","dest":"/abs/file","format":"raw"}`, one item, written through `<dest>.cbk-partial` and never over an existing path; or `{"mode":"unit","replace":true}` (in place: stopped, staged, swapped, the old data moved aside) or `{"mode":"unit","name":"N","target_root":"/abs"}` (a new unit with its data under the target root), with optional `pin_digest`, `start`, `keep_macs`; the definition is re-created through the helper. 202 with a job |

A retention pass (`POST /repos/<id>/gfs`) runs once per retention group: the
file snapshots, then each unit, each with its own newest snapshot, which is
never pruned. A snapshot whose `.cbackup-unit/unit.json` cannot be read is its
own `unknown:<id>` group and is never pruned.

**NBD exports** (vm-backup.md 7.3) serve one disk of a unit snapshot
read-only over NBD on a unix socket, so a filesystem in it can be mounted
without restoring the image:

| Method | Path | Purpose |
|---|---|---|
| POST | `/repos/<id>/snapshots/<sid>/unit/export` | `{"item":"vda","ttl_s":14400}` (`ttl_s` 60 to 604800, default `vm.export_ttl_s`). 201 with `export_id`, `socket`, `name`, `size`, `expires` (wall clock), `idle_timeout_s` and an `attach` command line. The `name` (128 random bits) is returned here only; 400 for an item that is not a disk, 404 for no such snapshot or item, 503 while gc or another writer holds the repository |
| GET | `/exports` | Open exports: id, repo, snapshot, item, socket, size, caller, created and expiry times, live `connections`, `bytes_served` and `cache_bytes` (read budget its connections hold); never the name |
| GET | `/exports/<id>` | One, open or closed; a closed one has `closed_at` and `close_reason` (`revoked`, `expired`, `idle`, `repository removed`, `daemon stopped`, `daemon restarted`) |
| DELETE | `/exports/<id>` | Revoke: disconnect every client, remove the socket, close. 204 |

An open export pins its snapshot: deleting it is refused even with
`force` (`ERR_BUSY`, the message names the export) and retention keeps it
as if tagged. A gc on its repository waits in the queue, phase "waiting for
an NBD export to close", and runs once the export closes; the export's ttl
(at most 7 days) bounds that wait. Removing the repository revokes its
exports first. Each connection holds at most one chunk of the read budget
while it reads and none once it idles for a second; `vm.export_max_conns`
(default 4, 0 turns exports off) caps connections across every export, and
the daemon refuses to start when `jobs.read_budget_mb` cannot hold that
many 16 MiB chunks beside a job's 48 MiB. An export closes at its expiry,
after `vm.export_idle_s` with no connection, when revoked, when its
repository is removed, and when the daemon stops; the ids are `AUTOINCREMENT` and opening and closing are journalled
with the caller. The server speaks fixed newstyle with structured replies,
`NBD_OPT_GO`/`INFO` (minimum block 512, preferred 4096), `base:allocation`
(the snapshot's holes are holes to the client, in whole 512-byte sectors;
a sector holding any data is data), `READ`, `BLOCK_STATUS` and `DISC`; writes are refused with
`EPERM`, `NBD_OPT_LIST` with a policy error, a wrong name at `NBD_OPT_GO`.

| POST | `/repos/<id>/units/<p>/<u>/detect` | Database detection on the unit now, without a backup (`db-detection.md` 7.4, Manual 23.9): units.probe, and a scan of each included mount where a live read would read it, reading only file headers. No fields: no body or `{}`, and any key is a 400 naming it. 202 with `{"job_id":N}`; the job's result is `{"databases":...}`, the object a backup's result carries, judged against the capture the next backup would make, and its findings are recorded under `unit:<key>`. It never alerts; new live findings and coverage changes are journalled. 409 while `db_detect.enabled` is 0 or the provider's helper cannot be reached, 404 for an unknown provider; a unit its provider does not list, or outside `units.allow`, fails the job |

File backups refuse a source at or under `/.cbackup-unit` or
`/.cbackup-captures`, on `POST /backup`, `PUT /sources` and backup schedules
alike: those names are the daemon's in every snapshot root.

A `grep` schedule searches the newest snapshot as of each fire, so it takes
no `snapshot`. From the CLI: `schedule add 1 grep --every day --at 04:00
--pattern 'BEGIN RSA' [--path-prefix PATH] [-i]`.

## Databases

Acting on what database detection found (`docs/db-detection.md` 7.3, 7.4;
Manual 23.7). A finding is the `id` in a job result's `databases`. Each
action is an explicit call; nothing else enables a template.

| Method | Path | Purpose |
|---|---|---|
| POST | `/repos/<id>/databases/<f>/preview` | Body optional: `{"template":"pg-dump","params":{...}}`; without a template, the finding's suggestion, what is enabled, or the engine's default dump. For a unit's finding: `template`, `kind` (`dump` or `exec`), `summary`, `params` (canonical, sqlite-backup's `db` filled with the finding's path), `container` and `user` it runs as, `argv` and for an exec template with a post run `post_argv` (as the helper builds them), `output` (`.cbackup-unit/hooks/<name>.dump`, dumps) and `log`, `needs_quiesce` when set, `allowed` by the helper's policy and `policy_line` when not, `probe` (`argv`, `exit`, `timed_out`, `output` at most 64 KiB, `truncated`) and `verified` (the probe exited 0). For a file source's: `kind` `engine-copy`, `output` under `.cbackup-captures/`, `verified` and `probe` null. 409 when the unit's provider is not behind the helper, 502 when the helper cannot be asked |
| POST | `/repos/<id>/databases/<f>/enable` | `{"template":"pg-dumpall","params":{}}`, template required; any other key is refused. The template must be in the catalog and for the finding's engine, and its params must check (400 naming the rule). A file source takes only `sqlite-backup`, with `{"allow_rollback_lock":true}` its one optional parameter. For a unit, a template the helper's policy does not allow is 409 with `error` and `policy_line`, the line to add; 503 when the helper has not answered since the daemon started. Records state `enabled`, `template`, `params`, `decided_by` (the caller), `decided_at`; journalled. Answers the finding |
| POST | `/repos/<id>/databases/<f>/dismiss` | `{"reason":"..."}`, 1 to 512 bytes, no control characters. State `dismissed`, the template cleared, `reason`, `decided_by`, `decided_at`; journalled. Answers the finding |

The finding these answer: `id`, `scope`, `engine`, `path`, `version` (when
known), `confidence`, `coverage`, `suggestion` (when any), `state`,
`template` and `params` (when enabled), `decided_by`, `decided_at` and
`reason` (when set). 404 for a finding the repository does not have.
