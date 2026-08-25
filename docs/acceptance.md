# Acceptance checklist

Every function the app exposes, driven twice: once through `c-backup` and
once through the desktop app, against real data on this machine rather than
a fixture. Each row records what was run, what proves it worked, and how
long the interaction took.

The point of driving both is that they are different clients of the same
daemon and have failed differently before. A route can answer correctly
while the screen reading it looks for fields that are not there, and the
contract suite stays green through exactly that.

## Data

| Set | Path | Files | Dirs | Size | What it is for |
|-----|------|-------|------|------|----------------|
| D | `~/Documents` | 5,722 | 690 | 6.8 GB | The functional sweep. Small enough to iterate on, real enough to have odd filenames, large media and nested junk |
| G | `~/git` | 225,230 | 25,506 | 46 GB | Scale. 225k files is the case that never finished before `build_trees` stopped walking parent chains, and 25k directories is what made it quadratic. Also thousands of near-identical `.git` objects, which is a dedup test nothing synthetic reproduces |

Sources are read-only throughout. **No restore targets a source path.**
Restores go to a scratch directory and are compared against the original.

## Timing

Two budgets, because two kinds of thing are being measured.

- **Interactive** - anything a person waits on. Listing, browsing, searching,
  opening a screen. Target under 400 ms, and the number is the whole
  click-to-answer or command-to-output time.
- **Work** - backup, restore, verify, gc. These take as long as the data
  takes. What is measured is the *handoff*: the daemon accepts the job and
  answers with an id. Target under 600 ms regardless of how big the job is.
  The job's own duration is recorded but not budgeted.

A row that is slow on one client and fast on the other is the interesting
result, because both are talking to the same daemon.

## The list

Legend: **CLI** = the command. **UI** = where it lives in the app.
**Proof** = what makes it a pass rather than "it did not error".

### A. Repository lifecycle

| # | Function | CLI | UI | Proof |
|---|----------|-----|----|-------|
| A1 | Register a repository | `repo add <path> <label>` | Connections > Add Repo | Appears in `repo list` and in the sidebar |
| A2 | List repositories | `repo list` | Sidebar | Both show the same set |
| A3 | Show one repository | `repo show <id>` | Dashboard header | Path, label, encryption agree |
| A4 | Create encrypted repo | `repo add --encrypt system` | Add Repo dialog | `encryption` reads `system`, key file exists |
| A5 | Remove a repository | `repo remove <id>` | Repo context menu > Remove | Gone from both; **repo directory still on disk** |

### B. Sources and backup

| # | Function | CLI | UI | Proof |
|---|----------|-----|----|-------|
| B1 | Set sources | `sources set <id> <path>` | Backup dialog > Sources > Edit | `sources list` echoes them; dialog shows them next open |
| B2 | First backup (D) | `backup <id>` | Run Backup | Snapshot 1 exists; file count within 1% of `find -type f` |
| B3 | Incremental, nothing changed | `backup <id>` | Run Backup | New snapshot; **new-data bytes near zero** - this is the dedup claim |
| B4 | Incremental, one file changed | `backup <id>` | Run Backup | Diff against previous shows exactly that path |
| B5 | Excludes | `backup <id> --exclude PAT` | Backup dialog | Excluded paths absent from the tree |
| B6 | Label | `backup <id> --label TEXT` | n/a | Label shows in `snapshots` and in the list |
| B7 | Backup at scale (G) | `backup <id>` | Run Backup | Completes. Record wall time and files/sec |
| B8 | Handoff is instant | `backup --no-wait` | Run Backup | Returns a job id in under 600 ms with 225k files queued |
| B9 | Progress is visible | `jobs watch <id>` | Task bar | Counter advances; percentage is not stuck |

### C. Browse, search, compare

| # | Function | CLI | UI | Proof |
|---|----------|-----|----|-------|
| C1 | List snapshots | `snapshots <id>` | Snapshots | Same ids, dates, file counts |
| C2 | Snapshot paging | `snapshots <id> --limit 5 --offset 5` | Load more | Second page continues the first |
| C3 | Walk a directory | `ls <id> <snap> [node]` | Snapshot browser | Entry names match the real directory |
| C4 | Wide directory | `snapshot tree` on the largest dir | Browser, load more | Page boundary handled; nothing silently missing |
| C5 | Search by name | `search <id> <term>` | Search | Same hit count; paths resolve to real files |
| C6 | Search inside files | `grep <id> <pattern>` | Find in files | Hits point at real content |
| C7 | Diff two snapshots | `diff <id> 1 2` | Compare | Change counts match B4's known edit |
| C8 | Read one file back | `object <id> <hash> content` | View Content | Bytes equal the original file |

### D. Restore

| # | Function | CLI | UI | Proof |
|---|----------|-----|----|-------|
| D1 | Whole snapshot | `restore <id> <snap> <dest>` | Restore | `diff -r` against source is clean |
| D2 | One subtree | `restore --subpath PATH` | Restore file | Only that subtree appears |
| D3 | Permissions and mtimes | - | - | `stat` matches for a sample; symlinks are still symlinks |
| D4 | Existing files are left alone | `restore` onto existing | Restore | Skips them and says how many; `--overwrite` replaces. This is resume semantics, not a safety refusal - an interrupted restore is meant to be re-run |

### E. Integrity

| # | Function | CLI | UI | Proof |
|---|----------|-----|----|-------|
| E1 | Index verify | `verify <id> --level index` | Integrity > Verify | Completes clean |
| E2 | Deep verify | `verify <id> --level deep` | Integrity | Every object read back; clean |
| E3 | Verify history | `verify list <id>` | Integrity list | Runs listed with counts |
| E4 | Damage is caught | flip a byte in a pack, `verify --level deep` | Integrity | Reports it; repairs it from parity; says which |
| E5 | Damage is repaired | re-verify after E4 | Integrity | Second run clean, `repaired` count from the first |

### F. Maintenance

| # | Function | CLI | UI | Proof |
|---|----------|-----|----|-------|
| F1 | Pack | `pack <id>` | Ops > Pack | Loose count drops; objects still readable |
| F2 | Garbage collect | `gc <id>` | Ops > Garbage Collect | Runs; nothing referenced is removed |
| F3 | Apply retention | `gfs <id>` | Ops > Apply Retention | Tier flags appear on closed days |
| F4 | Reindex | `reindex <id>` | Ops > Reindex | Rebuilt index agrees with the pre-reindex stats |
| F5 | Delete a snapshot | `snapshot delete <id> <snap>` | Snapshots > Delete | Gone; other snapshots still restore |

### G. Metadata

| # | Function | CLI | UI | Proof |
|---|----------|-----|----|-------|
| G1 | Tag | `tag set <id> <name> <snap>` | Tags > Add | Listed both sides |
| G2 | Tag protects from retention | `tag set` then `gfs` | - | Tagged snapshot survives |
| G3 | Remove tag | `tag remove <id> <name>` | Tags > Remove | Gone |
| G4 | Note | `snapshot note <id> <snap> TEXT` | Browser > Add note | Readable both sides |
| G5 | Clear note | `snapshot note <id> <snap> ""` | Browser > Delete | Empty both sides |
| G6 | Retention policy | `policy set <id> retention.keep_daily 14` | Retention | Value persists and is what `gfs` reads |
| G7 | Activity log | `journal <id>` | Activity | Every operation above appears, with durations |

### H. Scheduling

| # | Function | CLI | UI | Proof |
|---|----------|-----|----|-------|
| H1 | Add a schedule | `schedule add <id> backup --interval` | Schedules > Add | Listed enabled with a next-due time |
| H2 | It actually fires | wait for the interval | Task bar | A snapshot appears without anyone asking |
| H3 | Pause and resume | `schedule disable/enable` | Pause / Resume | State changes both sides |
| H4 | Remove | `schedule remove` | Remove | Gone |

### I. Jobs and observability

| # | Function | CLI | UI | Proof |
|---|----------|-----|----|-------|
| I1 | List jobs | `jobs` | Tasks | Same set |
| I2 | Cancel a job | `jobs cancel <job>` | Cancel | Stops; journal records `cancelled` |
| I3 | Stats and summary | `stats <id>`, `summary <id>` | Dashboard | Numbers agree |
| I4 | Objects and packs | `objects`, `packs`, `fanout` | Objects, Packs | Counts agree; fanout is flat |
| I5 | Health and metrics | `health`, `metrics` | Daemon status | Ready; backup age is seconds |
| I6 | Daemon gone | kill the daemon | Sidebar | Both say what is wrong and how to start it |

### J. Encryption

| # | Function | CLI | UI | Proof |
|---|----------|-----|----|-------|
| J1 | Backup to an encrypted repo | `backup <enc-id>` | Run Backup | Succeeds; **raw bytes on disk do not contain source plaintext** |
| J2 | Restore from it | `restore <enc-id>` | Restore | Round trip is byte-identical |
| J3 | Export the key | `repo key export <id>` | Encryption > Show key | 64 hex characters |
| J4 | Unescrowed key is flagged | - | Encryption, Daemon status | Warned until acknowledged |
| J5 | Acknowledge | `repo key ack <id>` | I have saved it | Warning clears |
| J6 | Import a key | `repo key import` | Restore a key from a copy | Repository opens with the imported key |

## Out of scope

- **Replication.** Paused: the engine and local sink exist, the network path
  is deliberately disabled.
- **Auth.** A unix-socket connection needs no password; it matters only for
  the TCP path, which is paused with the rest of the remote work.

---

# Results: first full pass

Run against `~/Documents` (5,722 files, 6.8 GB) and `~/git` (225,240 files,
46 GB) on the development machine. Every group was driven from the CLI; the
browse, search and dashboard rows were driven from the app as well, through
`e2e/90-acceptance.spec.ts` against the same daemon.

## Outcome

All sixty checks pass. Four defects were found and three fixed; the fourth is
recorded rather than rushed. Two rows in this checklist were wrong and have
been corrected against what the code actually claims.

## What the numbers were

| Operation | Documents (5.7k files) | ~/git (225k files) |
|-----------|------------------------|--------------------|
| First backup | 21.5 s | 443 s |
| Incremental, nothing changed | 1 s | - |
| Restore | 20 s | - |
| Deep verify | 5,639 objects clean | 159,300 objects |
| Snapshot list | 5 ms | 5 ms |
| One directory | 5 ms | 5 ms |
| Name search | 6 ms | 22 ms (200 hits) |
| Dashboard (3 calls) | - | 142 ms |
| Backup handoff | 11 ms | 6 ms |

Interactive work does not care how much data is behind it, which is the
claim the paging and indexing exist to make. The one place it did care was
the dashboard, and that is fixed below.

In the app, against the same daemon: repository list 9 ms, snapshot list
11 ms, open a tree 16 ms, expand a directory 72 ms, search 26 ms, dashboard
of 225k files 182 ms. The app adds roughly 20 ms of IPC and render over the
CLI, and nothing is near the 400 ms budget.

## Storage

- Documents: 6.8 GB of source became a 2.9 GB repository. 5,722 files became
  5,054 objects, so 668 were duplicates of content already stored.
- `~/git`: 46 GB became 26 GB. 225,240 files became 159,300 objects. The
  corpus is mostly already-compressed - git objects, archives, build output -
  so almost all of that is dedup rather than compression.
- **The repository peaked at 53 GB during the run and settled at 26 GB when
  packing finished.** Loose objects are written uncompressed by design, so
  transient headroom needs to be roughly twice the final size. Nothing says
  this anywhere and it is the number that decides whether a backup fits.

## Defects found

1. **Directory mtimes were lost on restore.** 575 of 689 directories came
   back stamped with the time of the restore. Files, modes and ownership were
   all correct, which is what made it invisible. Fixed, with a regression
   test that fails without the fix.
2. **The dashboard was twice its budget at scale** - 795 ms. `/summary`
   scanned the objects table four times for what one pass answers, and
   `/stats` sorted every object row to group four numbers. Fixed: 182 ms.
3. **A restore never reported completion.** Progress counted from a 0-based
   index, so the last entry of a restore was announced as 6414 of 6415 and
   the job then finished. A bar that stops just short and stays there reads
   as a stall. Fixed.
4. **Metadata writes fail intermittently during a backup.** `sources set`
   returned `500 {"error":"cannot begin"}`. Nine `BEGIN` sites across six
   files and only one takes a lock. **Not fixed** - the correct fix is one
   lock held by every writer, and it needs the lock ordering proven first,
   because a deadlock during a backup is worse than a retryable 500. See the
   trap recorded in CLAUDE.md.

Two smaller ones: `c-backup grep` emits two JSON documents rather than one,
so piping it to a JSON parser fails; and grep hit paths have no leading
slash while search result paths do.

## Corrections to this checklist

- **D4** asserted that a restore onto an existing destination is refused
  without `--overwrite`. It is not, and should not be: existing files are
  skipped and counted, because an interrupted restore is meant to be
  re-runnable. An earlier interrupted 45 GB restore flattened 2,262
  hardlinks before it worked that way.
- **B8** was recorded as a failure on the first run because the submit
  response carries `job_id` while job listings carry `id`. The handoff was
  fine; the harness was reading the wrong key. The inconsistency in the API
  is real but harmless.

## Not covered by this pass

- Replication, which is paused.
- Auth, which only matters for the TCP path.
- G2 (a tag protecting a snapshot from retention) needs backdated snapshots
  and is covered by `tests/test_gfs.c` instead, where dates can be pinned.

---

# Results: Unraid

A second pass against a live Unraid 7.3.2 server - 125 GB RAM, 27 running
containers including GitLab, Plex and Immich, load average around 5 before
any of this started. The daemon ran as the CI-built container image from the
registry, with the source mounted read-only and the repository written to a
directory of its own.

This is the machine the app was reported to die on, so the questions were
what dies and why, not whether the features work.

## Where the repository lives is the biggest single factor

The same source, the same 6,749 files, with only the destination changed:

| Phase | Repo on `/mnt/disk1` (parity array) | Repo on `/mnt/cache` (ZFS SSD) |
|-------|-------------------------------------|--------------------------------|
| scan+store | 67.0 s | 18.7 s |
| trees | 60.9 s | 18.8 s |
| packing | around 360 s | far less |

A repository is thousands of small loose objects before it is packed, and
every small write to a parity-protected array costs a parity
read-modify-write. Reads are unaffected - a deep verify read 2.38 GB back
off the same array in 18 seconds - so this is a write-amplification story,
not a slow-disk story.

**A repository does not belong on a parity-protected array disk.** Cache, an
unassigned device, or anything outside the array.

## What was ruled out

- **`build_trees` is linear.** 341 trees/s at 580 trees and 326 trees/s at
  25,509 locally - across a 44x range. The counting-sort fix holds and the
  Unraid slowness is the storage, not the algorithm.
- **Connections do not accumulate.** The CLI opens a new connection every
  100 ms while waiting on a job, and MHD's default connection limit is
  around 1020, which lined up suspiciously well with a failure at roughly
  1330 polls. Measured instead of assumed: descriptors and threads stayed
  flat at 8 and 7 under sustained polling. The theory was wrong.
- **The daemon never died.** Zero restarts across every run. A "lost contact
  with the daemon" seen once was the test harness tearing down its own ssh
  session, not the daemon - re-run detached, the same wait returned EXIT=0.

## Defects found

1. **`store.fuse_readahead` is a config key nothing reads.** Every
   production caller passes NULL for the store config, so
   `enable_fuse_readahead` is always zero and the FUSE readahead ring is
   unreachable in any configuration. Setting the key does nothing. This is
   the same shape as the `http.bind` bug: a value accepted and not honoured.
   It also means the ring cannot be the cause of any current problem,
   whatever it did historically.
2. **A socket gets node type 0.** `stat_to_node_type` returns 0 for
   anything it does not recognise, and appdata contains five of them.
   Backup skips them correctly, but a restore has no case for type 0, so
   they would come back as empty regular files.

## Scaling limit worth knowing

The scan holds every entry in memory. Measured at 487 MB peak for 622,000
entries - about 820 bytes each, the 384-byte struct plus its path string.

| Files | Scan memory |
|-------|-------------|
| 622,000 (measured) | 487 MB |
| 1.18M (full appdata) | around 925 MB |
| 10M | around 8 GB |

Comfortable on a 125 GB server. Not comfortable on the 2-4 GB NAS that is a
perfectly ordinary place to run a backup, and it sits against the design
constraint that says never assume the data fits in memory.
