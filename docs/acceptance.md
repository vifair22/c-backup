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
