# HTTP API

Two transports, one API.

- **Unix socket** (`http.socket`) bypasses authentication entirely. Filesystem
  permissions on the socket are the access control: if you can open it, you
  are authorised. This is what the CLI uses.
- **TCP** (`http.port`) requires `Authorization: Bearer <token>` or an `auth`
  cookie, except for the public routes below.

Until an admin password is set, TCP is open so the daemon can be bootstrapped.
It locks down the moment `POST /api/auth/setup` succeeds.

## Conventions

Every collection route pages with `offset` and `limit`, and there is a
server-side maximum. This is not stylistic: returning a 10,000-entry directory
row by row measured 2304 us against 175 us for the pre-rewrite in-memory scan,
while the first page through the index costs 93 us and beats it. The
regression only exists if the API offers a way to ask for everything at once,
so it does not. **No route returns a whole snapshot.**

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
| POST | `/api/auth/setup` | Set it, first run only |
| POST | `/api/auth/login` | `{password}` returns `{token}` |

Health and metrics are public deliberately: an endpoint that needs a
credential cannot report a daemon too unhealthy to authenticate.

## Repositories

| Method | Path | Purpose |
|---|---|---|
| GET | `/repos` | List |
| POST | `/repos` | Register: `{path, label}` |
| GET | `/repos/<id>` | One repository |
| DELETE | `/repos/<id>` | Deregister; the data directory is left alone |

## Snapshots and browsing

| Method | Path | Purpose |
|---|---|---|
| GET | `/repos/<id>/snapshots` | List, paged |
| GET | `/repos/<id>/snapshots/<sid>/tree?parent=<node>` | One directory level |
| GET | `/repos/<id>/search?q=<text>` | Substring filename search |
| DELETE | `/repos/<id>/snapshots/<sid>` | Prune |

Search goes through an FTS5 trigram index. A B-tree cannot serve
`LIKE '%needle%'`, and scanning the node table loses to the pre-rewrite
implementation by 2.7x; the trigram index wins by 50.9x.

## Operations

| Method | Path | Purpose |
|---|---|---|
| POST | `/repos/<id>/backup` | `{paths:[...], label}` |
| POST | `/repos/<id>/snapshots/<sid>/restore` | `{dest, overwrite}` |
| POST | `/repos/<id>/gc` | Mark and sweep; `?dry_run=1` to report only |
| POST | `/repos/<id>/gfs` | Apply retention |
| POST | `/repos/<id>/reindex` | Rebuild the index from the repository |

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
| GET/POST | `/repos/<id>/tags` | List, or set `{name, snap_id}` |
| DELETE | `/repos/<id>/tags/<name>` | Remove |
| GET/PUT | `/repos/<id>/snapshots/<sid>/note` | Read or write `{body}` |
| GET/PUT | `/repos/<id>/policy` | Read, or set `{key, value}` |
| GET | `/repos/<id>/journal` | Operation history |

Any tag protects a snapshot from retention pruning. Tags are how a user says
"keep this one", and deleting a tagged snapshot because a window rolled past
would be the worst kind of surprise.

## Schedules

| Method | Path | Purpose |
|---|---|---|
| GET/POST | `/repos/<id>/schedules` | List, or create |
| PUT | `/repos/<id>/schedules/<sid>` | `{enabled}` |
| DELETE | `/repos/<id>/schedules/<sid>` | Remove |

The trigger is a nested object, validated at creation rather than at fire
time:

```json
{"action":"backup","trigger":{"type":"interval","seconds":14400}}
{"action":"gc","trigger":{"type":"calendar","every":"day","at":"02:00"}}
{"action":"gfs","trigger":{"type":"calendar","every":"week","on":"sunday","at":"03:00"}}
```

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
future fire, `run_once` fires immediately and then resumes.
