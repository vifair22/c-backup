# UI Refresh

## Overview
Rebuild the UI as an Electron app. Replace the tkinter Python viewer with a modern desktop application that adds first-class connection and repository management while maintaining full feature parity with the existing tool.

## Architecture Decisions
- **Platform**: Electron
- **Credential storage**: Electron safeStorage / OS keychain (encrypted at rest, supports export/import)
- **RPC layer**: Reuse existing JSON RPC protocol (session mode preferred, one-shot fallback)

## Design Principles
- **Feature parity != layout parity**. The old tkinter viewer's 11-tab layout is a reference for _what_ functionality exists, not _how_ to present it. The new UI should rethink information architecture, consolidate where it makes sense, and take advantage of Electron's capabilities (richer components, better navigation, responsive layouts). The old tab structure should not be replicated blindly.
- The existing functional spec is a checklist of capabilities to preserve, not a wireframe to copy.

---

## New Feature: Connection & Repository Management

### Connection Model

Connections are a first-class concept. Each connection represents a way to reach a machine (local or remote). Repos live under connections.

**Sidebar tree layout**:
```
My Workstation (local) ● connected
  ├── /backup/home
  ├── /backup/projects
Production Server (SSH) ● connected
  ├── /var/backups/db
  ├── /var/backups/app
  └── /var/backups/logs
Dev Box (SSH) ○ disconnected
  └── /home/dev/backup
```

### Connection Types

**Local connection**:
- Name (user-defined label)
- Binary path (defaults to `backup` in PATH, configurable)
- Sudo promotion: optional, runs binary via `sudo -S` for root access
- Multiple local connections supported (e.g., different binary versions)

**Remote connection (SSH)**:
- Name (user-defined label)
- Host / port
- Username
- Auth method: key file, password, or agent
- Remote binary path (defaults to `backup`)
- Sudo promotion: optional, prompts for sudo password to run binary as root

### Connection Lifecycle
- **Persistent**: Connections stay alive as long as the app is open
- **Status indicators**: Connected (green) / Disconnected (gray) / Error (red) per host in sidebar
- **Auto-reconnect**: Attempt reconnect on transient failures
- **Manual disconnect/reconnect**: Right-click context menu on connection

### Repository Management
- Repos are added manually by entering a path under a connection
- App validates the path is a valid c-backup repo on add
- Repo list persists across app restarts (stored in app config)
- Each repo gets its own RPC session when opened

### Multi-Session Support
- Multiple repos can be open simultaneously
- Each open repo has its own independent RPC session
- Clicking a repo in the sidebar switches the main content area to that repo's tab view
- Repo tab state (scroll position, selected sub-tab, etc.) is preserved when switching

### Credential Storage
- Use Electron's safeStorage API backed by the OS keychain
- Non-secret config (connection names, hosts, repo paths, binary paths) in a local JSON/TOML config file
- Secrets (passwords, sudo passwords) encrypted via safeStorage
- Export/import support: export config (with option to include or exclude secrets)

---

## New Feature: Background Task Management (C-side changes)

### Problem
Long-running operations (backup run, gc, prune, pack, verify, restore) die if the JSON session stream drops (SSH hiccup, app restart, etc.).

### Solution: Fork-and-Detach Model
When the UI kicks off a heavy operation via `task_start`, the session process forks a detached child. The child runs independently -- if the session dies, the task keeps going. Status is persisted to files the UI can read on reconnect.

**No daemon, no server, no REST API.** The session remains a lightweight RPC interface. Heavy work happens in detached children.

### How It Integrates with Existing Locking
The existing repo locking model is unchanged:
- Single `<repo>/lock` file using `flock(2)`
- **Exclusive** (non-blocking, immediate fail): run, gc, prune, pack, tag set/delete, snapshot delete
- **Shared** (concurrent): list, ls, cat, diff, verify, stats, export
- **Crash recovery** on exclusive lock acquisition: prune-resume, tmp/ cleanup, pack-resume

Task integration:
- The forked child acquires the exclusive lock itself (same as CLI). If contended, `task_start` returns an error.
- The session holds a shared lock (or none). No competition with the child.
- If the child crashes, `flock` auto-releases. Next exclusive lock acquisition triggers existing crash recovery.
- `task_status` / `task_list` read status files only -- no lock needed.
- `task_cancel` sends SIGTERM to child PID. Child cleans up and releases lock on exit.

### Task Status Files
Location: `<repo>/.backup/tasks/<task-id>.json`

Contents:
- `task_id`: unique identifier
- `command`: operation type (run, gc, prune, pack, verify, restore)
- `pid`: child process ID
- `started`: timestamp
- `state`: running | completed | failed
- `progress`: operation-specific (files processed, bytes, current path, etc.)
- `exit_code`: set on completion
- `error`: set on failure

### New RPC Actions
| Action | Params | Returns |
|--------|--------|---------|
| `task_start` | `command`, command-specific params | task_id (or error if lock contended) |
| `task_list` | none | all tasks in this repo (active + recent) |
| `task_status` | `task_id` | current status/progress for one task |
| `task_cancel` | `task_id` | sends SIGTERM, returns acknowledgment |

### CLI Impact
None. CLI commands (`backup run`, `backup gc`, etc.) continue to work exactly as today -- synchronous, blocking, direct. The fork-and-detach path only activates via `task_start` RPC.

---

## New Feature: Operation Journal (C-side changes)

### Purpose
A structured, append-only log of every operation against a repo -- CLI and UI alike. Provides audit trail, feeds the health dashboard, and powers notifications/alerts.

### Storage
Location: `<repo>/logs/journal.jsonl` -- one JSON line per entry. Append-only, unbounded (useful for growth metrics over time).

### Journal Entry Lifecycle
Every operation writes two entries linked by `op_id`:

1. **Start entry** (written at operation begin):
   ```json
   {"state":"started","op_id":"...","timestamp":"...","operation":"run","source":"cli","user":"vifair"}
   ```

2. **Completion entry** (written at operation end):
   ```json
   {"state":"completed","op_id":"...","timestamp":"...","duration_ms":12340,"result":"success","summary":{...}}
   ```

### Entry Fields
- `op_id`: unique identifier linking start/complete pair
- `timestamp`: ISO 8601
- `operation`: run, prune, gc, pack, verify, restore, list, ls, diff, search, tag, snapshot-delete, etc. (all operations, read and write)
- `source`: `cli` | `ui`
- `user`: unix user or SSH user who initiated
- `state`: `started` | `completed`
- `duration_ms`: elapsed time (completion entry only)
- `result`: success | failed | cancelled | crash (completion entry only)
- `summary`: operation-specific data -- files backed up, bytes written, objects pruned, etc. (completion entry only)
- `error`: error message (failed/crash only)
- `task_id`: link to background task if run via task system
- `signal`: signal number (crash only)

### Crash Journaling
Catastrophic failures (segfault, SIGABRT, etc.) must still produce a journal entry:

1. **Signal handler**: Registered at startup for SIGSEGV, SIGABRT, SIGBUS, etc. Uses a pre-allocated buffer and direct `write()` syscall (no malloc, no stdio, no JSON library). Writes a minimal completion entry with `"result":"crash"` and signal number, then re-raises.

2. **Orphan detection**: If the journal has a "started" entry with no matching "completed" (e.g., SIGKILL, power loss, kernel panic), the UI infers a crash/abnormal termination and flags it.

### Scope
All operations are journaled -- reads and writes, CLI and UI. This ensures the health dashboard and notifications have complete visibility.

### New RPC Actions
| Action | Params | Returns |
|--------|--------|---------|
| `journal` | `offset?`, `limit?`, `operation?`, `result?`, `since?` | Filtered/paginated journal entries |

---

## New Feature: Restore Workflow

### Purpose
Guided file/directory restoration from the UI. Transforms the app from a passive viewer into an active management tool.

### Workflow
1. Browse a snapshot's directory tree (existing `snap_dir_children` lazy expansion)
2. Select one or more files/directories (checkbox selection)
3. Choose destination path (local path on the connection's machine)
4. Optional: preview what will be restored (file count, total size)
5. Kick off restore via `task_start` with `command: "restore"`
6. Monitor progress via task management UI

### Considerations
- Restore runs as a background task (fork-and-detach) so it survives disconnects
- Destination path validation before starting
- Conflict handling: overwrite, skip, or rename (user selects policy before starting)
- Permissions: may require sudo on remote connections to restore as original owner

---

## New Feature: Backup Run Trigger

### Purpose
Kick off a backup run from the UI instead of requiring CLI access.

### Workflow
1. Select a repo in the sidebar
2. Click "Run Backup" (toolbar button or context menu)
3. Optional: review current policy before starting
4. Fires `task_start` with `command: "run"`
5. Monitor progress via task management UI
6. Journal entry written on completion

### Considerations
- Uses the repo's existing `policy.toml` for paths/excludes/retention
- Lock contention surfaced as clear error ("another backup is already running")
- Post-run actions (auto_pack, auto_gc, auto_prune) handled by the C side as usual

---

## New Feature: Repo Health Dashboard

### Purpose
At-a-glance view per repo: "is this repo healthy and current?" without clicking through multiple tabs.

### Data Sources
- Journal: last backup time, last verify, recent failures, operation frequency
- `repo_stats`: storage usage, compression ratios
- `list`: snapshot count, GFS coverage gaps
- `scan`: format version, pack count

### Dashboard Panels
- **Last backup**: timestamp + age (with warning thresholds: >24h yellow, >7d red)
- **Last verify**: timestamp + result (pass/fail)
- **Recent failures**: list of failed/crashed operations from journal
- **Storage summary**: total size, compression ratio, pack vs loose split
- **Snapshot count**: total + trend (growth over time from journal)
- **GFS coverage**: are retention tiers being met? gaps highlighted
- **Warnings**: surfaced automatically (no recent backup, failed verify, GFS gaps, orphaned journal entries indicating crashes)

---

## New Feature: Snapshot Notes

### Purpose
Let users annotate snapshots with human-readable notes beyond tags (e.g., "before migration", "known good state", "pre-deploy v2.3").

### Storage
Notes stored in the repo, likely `<repo>/notes/<snap-id>` or as metadata in a notes file. Needs a new RPC action pair.

### New RPC Actions
| Action | Params | Returns |
|--------|--------|---------|
| `note_get` | `id` (snap ID) | note text (or empty) |
| `note_set` | `id`, `text` | `{"saved": true}` |
| `note_delete` | `id` | `{"deleted": true}` |
| `note_list` | none | all snap IDs with notes |

### UI Integration
- Visible in snapshot header sub-tab
- Editable inline (text field + save button)
- Shown as a column/icon in snapshot lists (overview, GFS tree)
- Searchable

---

## New Feature: Notifications / Alerts

### Purpose
Surface events to the user without requiring them to actively check each repo.

### Data Source
Polls the journal for new entries since last check. Detects:
- Task completion (success or failure)
- Connection drops / reconnects
- Repo warnings from health dashboard logic (no recent backup, failed verify, crash detected)

### UI
- In-app notification panel (bell icon with badge count)
- Toast notifications for important events (task complete, task failed, crash detected)
- Optional: OS-level notifications via Electron native notification API
- Notification history (scrollable, dismissable)

---

## New Feature: Repo Init from UI

### Purpose
Create new backup repositories from the UI without requiring CLI access.

### Wizard Flow
1. Select a connection in the sidebar
2. Choose target path via filesystem browser (local or remote)
   - Browser runs in the connection's most permissive context (sudo if enabled)
3. Validate target path is writable
4. Set up initial policy:
   - Choose a template (common presets from manual + user-created custom templates) or start blank
   - Configure paths, excludes, retention, automation settings
5. Run `init` + `save_policy`
6. Repo auto-added to the connection's repo list in the sidebar

### Filesystem Browser
- Available for both local and remote connections
- Remote browsing via SSH (runs commands on the remote host)
- Respects sudo context if the connection has sudo enabled
- Reused for restore destination selection and export destination

### Policy Templates
- Built-in templates: common presets from the manual (home directory, server, etc.)
- User-created templates: stored locally in the Electron app's config directory
- Template is just a pre-filled policy (paths, excludes, retention values)

---

## New Feature: Tag Management

### Purpose
Full tag CRUD from the UI. The existing viewer only reads tags.

### Operations
- **Create**: Name + snap ID + optional preserve flag
- **Delete**: Remove a tag
- **Rename**: Native C-side `tag_rename` RPC action (atomic, single operation)

### UI Touchpoints
- Tags tab: full management view (create, rename, delete)
- Right-click context menu on any snapshot row (overview, GFS tree, search results, diff): quick "Tag this snapshot" / "Preserve this snapshot"
- GFS Tree: prominent "Preserve" option for protecting snapshots from pruning

### New RPC Actions
| Action | Params | Returns |
|--------|--------|---------|
| `tag_set` | `name`, `snap_id`, `preserve?` | `{"saved": true}` |
| `tag_delete` | `name` | `{"deleted": true}` |
| `tag_rename` | `old_name`, `new_name` | `{"renamed": true}` |

---

## New Feature: Data Export

### Purpose
Export snapshots as bundle (native format, re-importable) or tar from the UI.

### UI
- Dialog launched from snapshot context menu (right-click any snapshot row)
- Format selection: bundle (default) or tar
- Subset selection: full snapshot or subtree (match C-side capabilities)
- Destination: prompt user to pick location
- Runs as a background task (via task system) -- survives disconnects

---

## New Feature: Local Export over SSH

### Purpose
Pull export/restore data from a remote repo back to the machine running the Electron app.

### Architecture
- **Side channel**: SFTP transfer (separate from the RPC session)
- **Flow**: C binary writes export to `<repo>/tmp/` on remote host -> app pulls via SFTP -> explicit cleanup of remote temp file on success
- **Resume**: SFTP offset-based resume for interrupted transfers

### Download Queue
- Multiple transfers can be queued
- Queue persists across app restarts (transfer manifest stored client-side in JSON)
- Manifest tracks: remote path, local destination, bytes transferred, integrity hash
- No auto-retry on failure -- fails immediately, user decides when to retry
- Progress indicators per transfer

### Restore to Local
- Restoring from a remote repo to the local machine always writes into a new folder
- No in-place restore to original paths (different machine, different UIDs)
- User always prompted for local destination folder

### Cleanup
- Remote temp file in `<repo>/tmp/` explicitly deleted after successful download
- Fallback: existing temp cleanup on next exclusive lock acquisition

---

## New Feature: Verify from UI

### Purpose
Trigger repository verification from the UI and surface results.

### Workflow
1. Select repo -> "Verify Repository" action (toolbar or context menu)
2. Optional: enable repair mode (with warning dialog: "Repair rewrites corrupted objects. This requires exclusive access to the repo.")
3. Runs as background task via `task_start`
4. Results journaled automatically

### Result Presentation
- Summary: objects checked, parity repairs, uncorrectable errors
- Failed objects list with classification:
  - **Warning**: corrupted but repairable via parity
  - **Error**: uncorrectable corruption
- Click-through from failed object -> Hash Lookup tab for deep dive

### Health Dashboard Integration
- Last verify timestamp + result shown on dashboard
- If verify finds uncorrectable errors: repo status indicator in sidebar turns red/warning
- Warning persists until next successful verify clears it

---

## New Feature: Dark Mode / Theming

### Design Language
- Professional, corporate aesthetic -- muted blues, grays, subtle accents
- Light touch of personality but not playful
- No neon, no bright/saturated colors

### Implementation
- Light and dark themes
- Default: follow OS system preference
- Manual override via app settings (global preference)
- Theme toggle accessible from app menu or settings

---

## Execution Plan

### Tech Stack
- **Electron 41** / Node.js 24 LTS
- **Renderer**: Svelte (SvelteKit SPA mode), TypeScript
- **Build**: Vite (renderer) + electron-builder (packaging)
- **SSH/SFTP**: `ssh2` npm package (pure JS, built-in SFTP)
- **LZ4**: `lz4js` (pure JS block decompression)
- **Distribution**: Portable Linux binary (deb/rpm/dmg later)
- **Project location**: `tools/electron/`

### IPC Architecture
- **Main process**: SSH connections, local process spawning, SFTP transfers, credential storage (safeStorage), RPC client
- **Renderer process**: Svelte SPA, communicates with main via ipcMain/ipcRenderer
- **RPC client** lives in main process (needs child process + SSH channel access)

### C-Side Journal Instrumentation Strategy
Journal hooks at dispatcher level (3-4 hook points total, no per-command changes):
- `src/cli/main.c` line ~171: wrap `entry->with_repo()` call with journal start/complete
- `src/cli/main.c` line ~149: wrap no-repo command execution
- `src/api/json_api.c` line ~2368: wrap one-shot handler call
- `src/api/json_api.c` line ~2458: wrap session handler call

### New C Files
- `src/ops/journal.h`, `src/ops/journal.c` -- Journal system
- `src/ops/task.h`, `src/ops/task.c` -- Task status file helpers
- `src/ops/task_run.c` -- Task execution wrappers per operation
- `src/ops/note.h`, `src/ops/note.c` -- Snapshot notes
- `src/common/crash_journal.h`, `src/common/crash_journal.c` -- Signal handler for crash journaling

### Modified C Files
- `src/api/json_api.c` -- ~10 new RPC handlers + journal wrapping in dispatch
- `src/cli/main.c` -- Journal wrapping at dispatch + crash signal handler registration
- `src/ops/gc.c` -- Extend `repo_verify` for structured per-object results
- `src/ops/gc.h` -- Extend `verify_opts_t` for result collection
- `Makefile` -- Add new source files

### New Repo Directories
- `<repo>/tasks/` -- Task status files (one JSON file per task)
- `<repo>/notes/` -- Snapshot notes (one file per annotated snapshot)
- `<repo>/logs/journal.jsonl` -- Operation journal (append-only)

### Testing Strategy
- **C-side**: Expand existing test infrastructure to cover new functionality (journal, tasks, notes, tags, verify results)
- **Electron**: New test infrastructure from the start (unit tests for RPC client, config storage; integration tests against real C binary)

### Risks & Mitigations
1. **Fork-and-detach over SSH**: Child process must survive SSH channel close. `setsid()` + closing terminal FDs should work, but test early against real SSH connections.
2. **Large journal files**: `journal` RPC should support tail-read (recent entries first), not just offset-from-start. Design the read to scan from end of file.
3. **Progress data polymorphism**: Each operation type (run/gc/verify/etc.) has different progress metrics. Define a schema per operation type as part of task 0.2.3.
4. **LZ4 block decompression**: Verify `lz4js` supports raw block decompression with explicit uncompressed size (not just frame format). Validate early in Phase 1.
5. **Concurrent task status reads/writes**: Child writes via tmp+rename (atomic). Reads always see consistent state.

### Parallel Work Streams
```
Stream A (C-side, ~3-4 weeks):         Stream C (Electron, starts day 1):
  0.1 Journal ──────────┐                1.1 Project setup
  0.2 Tasks ◄───────────┘                1.2 Config + credentials
  0.3 Tags (parallel w/ 0.2)            1.3 RPC + SSH + SFTP
  0.4 Notes (parallel w/ 0.2)              │
  0.5 Verify (after 0.2)                   ▼
         │                              Stream D (after Stream C):
         │                                2.x Connection & Repo UI
         │                                3.x Feature Parity views
         ▼                                   │
    ◄── merge point ──►                      ▼
                                          4.x New Features (needs Phase 0)
```

**Critical path**: Phase 0.1 → 0.2 (C) AND Phase 1.1 → 1.3 (Electron) → Phase 2 → Phase 3A → Phase 4A → Phase 4B

---

## Tasks (Dependency-Ordered)

Legend: `[BLOCKS: X.Y]` = must complete before task X.Y can start.

---

### Phase 0: C-Side Changes
_No UI dependencies. Can be developed in parallel with Phase 1._

**0.1 Operation Journal** _(no C-side dependencies, foundational for everything)_
- [ ] 0.1.1 Implement journal append-only storage (`<repo>/logs/journal.jsonl`)
- [ ] 0.1.2 Add journal start/complete entry writes to all CLI command paths [BLOCKS: 0.1.4]
- [ ] 0.1.3 Add journal start/complete entry writes to all JSON API action paths [BLOCKS: 0.1.4]
- [ ] 0.1.4 Implement crash signal handler (SIGSEGV, SIGABRT, SIGBUS) with pre-allocated buffer journal write (depends on 0.1.1)
- [ ] 0.1.5 Implement `journal` RPC action (filtered/paginated reads) [BLOCKS: 4.6, 4.7, 4.8]

**0.2 Background Tasks** _(depends on 0.1 -- tasks write journal entries)_
- [ ] 0.2.1 Implement task status file format and read/write helpers
- [ ] 0.2.2 Implement fork-and-detach mechanism in session handler (depends on 0.2.1)
- [ ] 0.2.3 Wire up heavy operations (run, gc, prune, pack, verify, restore, export) as taskable commands (depends on 0.2.2)
- [ ] 0.2.4 Implement `task_start` RPC action (depends on 0.2.3) [BLOCKS: 4.1, 4.4, 4.5, 4.10, 4.13]
- [ ] 0.2.5 Implement `task_list` RPC action (depends on 0.2.1) [BLOCKS: 4.1]
- [ ] 0.2.6 Implement `task_status` RPC action (depends on 0.2.1) [BLOCKS: 4.2]
- [ ] 0.2.7 Implement `task_cancel` RPC action (depends on 0.2.2) [BLOCKS: 4.1]
- [ ] 0.2.8 Task progress reporting from child process (periodic status file updates) (depends on 0.2.3) [BLOCKS: 4.2]
- [ ] 0.2.9 Add journal writes to task child processes (depends on 0.1.1, 0.2.3)
- [ ] 0.2.10 Task cleanup (purge old completed/failed task files) (depends on 0.2.1)

**0.3 Tag Management** _(no C-side dependencies)_
- [ ] 0.3.1 Implement `tag_set` RPC action [BLOCKS: 4.9]
- [ ] 0.3.2 Implement `tag_delete` RPC action [BLOCKS: 4.9]
- [ ] 0.3.3 Implement `tag_rename` RPC action (atomic rename) [BLOCKS: 4.9]

**0.4 Snapshot Notes** _(no C-side dependencies)_
- [ ] 0.4.1 Implement snapshot notes storage (`<repo>/notes/`) [BLOCKS: 0.4.2]
- [ ] 0.4.2 Implement `note_get`, `note_set`, `note_delete`, `note_list` RPC actions [BLOCKS: 4.7]

**0.5 Verify Enhancements** _(depends on 0.2 -- verify runs as background task)_
- [ ] 0.5.1 Implement verify as a taskable command with optional repair flag (depends on 0.2.3) [BLOCKS: 4.10]
- [ ] 0.5.2 Structured verify result output (per-object pass/fail with corruption classification) (depends on 0.5.1) [BLOCKS: 4.11]

---

### Phase 1: Electron Foundation
_Can start in parallel with Phase 0. No dependency on C-side changes._

**1.1 Project Setup** _(no dependencies)_
- [ ] 1.1.1 Initialize Electron project scaffolding [BLOCKS: everything in 1.2+]
- [ ] 1.1.2 Set up build pipeline (dev + production) (depends on 1.1.1)
- [ ] 1.1.3 Dark/light theme system (follow OS default, manual override) (depends on 1.1.1) [BLOCKS: all UI work]
- [ ] 1.1.4 Professional corporate color palette (muted blues, grays, subtle accents) (depends on 1.1.3)

**1.2 Config & Credentials** _(depends on 1.1)_
- [ ] 1.2.1 Implement config storage layer (JSON config + safeStorage for secrets) [BLOCKS: 1.2.2, 1.3, 2.1]
- [ ] 1.2.2 Implement config export/import (depends on 1.2.1)

**1.3 RPC Client** _(depends on 1.1)_
- [ ] 1.3.1 Implement RPC client module (session + one-shot, LZ4 decompression) [BLOCKS: 2.4, 3.x]
- [ ] 1.3.2 Implement SSH connection manager (key auth, password auth, ControlMaster) [BLOCKS: 1.3.3, 2.1]
- [ ] 1.3.3 Implement sudo promotion for remote connections (depends on 1.3.2) [BLOCKS: 2.6]
- [ ] 1.3.4 Implement connection lifecycle management (persistent, status tracking, reconnect) (depends on 1.3.2) [BLOCKS: 2.2]
- [ ] 1.3.5 Implement SFTP transfer module (depends on 1.3.2) [BLOCKS: 4.14]

---

### Phase 2: Connection & Repo UI
_Depends on Phase 1. Requires config storage and RPC client._

- [ ] 2.1 Build sidebar with connection/repo tree (depends on 1.2.1, 1.3.2) [BLOCKS: 2.2-2.5, 3.x]
- [ ] 2.2 Connection status indicators (connected/disconnected/error) (depends on 1.3.4, 2.1)
- [ ] 2.3 Add/edit/remove connection dialog (depends on 2.1)
- [ ] 2.4 Add/remove repo under connection (depends on 1.3.1, 2.1) [BLOCKS: 3.x]
- [ ] 2.5 Multi-repo tab switching in main content area (depends on 2.1) [BLOCKS: 3.x]
- [ ] 2.6 Filesystem browser component (local + remote, sudo context) (depends on 1.3.3) [BLOCKS: 2.7, 4.4, 4.13]
- [ ] 2.7 Repo init wizard (path selection, policy templates, init + save_policy) (depends on 2.6, 2.4)
- [ ] 2.8 Built-in policy templates from manual (depends on 2.7)
- [ ] 2.9 User-created custom policy template support (depends on 2.7, 1.2.1)

---

### Phase 3: Feature Parity
_Depends on Phase 2 (sidebar + repo switching) and Phase 1 (RPC client). Each view depends on 2.4 and 2.5._
_Note: "feature parity" means all existing capabilities are accessible, NOT that the layout mirrors the old 11-tab tkinter UI. Views should be redesigned for better UX -- consolidate, restructure, and improve as appropriate._

**3A: Core views** _(build first -- most used, needed by other views)_
- [ ] 3.1 Repo overview / dashboard (depends on 2.5) [BLOCKS: 4.6]
- [ ] 3.2 Snapshot exploration -- header, nodes, directory tree (depends on 2.5) [BLOCKS: 3.12, 4.4, 4.7, 4.9]
- [ ] 3.3 Policy tab with save_policy write support (depends on 2.5)
- [ ] 3.4 Tags tab (depends on 2.5) [BLOCKS: 4.9]

**3B: Data inspection tabs** _(independent of each other)_
- [ ] 3.5 Pack Files tab -- header, entries, index, pack map, global index (depends on 2.5)
- [ ] 3.6 Loose Objects tab -- info, content preview, object map (depends on 2.5)
- [ ] 3.7 Hash Lookup tab (depends on 2.5) [BLOCKS: 4.11]
- [ ] 3.8 Diff tab (depends on 2.5)
- [ ] 3.9 Analytics tab with charts (depends on 2.5) [BLOCKS: 4.6]

**3C: Navigation & discovery tabs** _(depend on Snapshots tab for cross-nav)_
- [ ] 3.10 Search tab (depends on 3.2 for jump-to-snapshots)
- [ ] 3.11 GFS Tree tab (depends on 3.2 for jump-to-snapshots) [BLOCKS: 4.9]
- [ ] 3.12 Cross-tab navigation wiring (depends on 3.2, 3.8, 3.10, 3.11)

---

### Phase 4: New Features
_Depends on Phase 3 + Phase 0 C-side changes._

**4A: Task System UI** _(depends on Phase 0.2 C-side task system)_
- [ ] 4.1 Task management UI -- start/monitor/cancel (depends on 0.2.4, 0.2.5, 0.2.7)
- [ ] 4.2 Task progress indicators -- progress bars, live polling (depends on 4.1, 0.2.6, 0.2.8)
- [ ] 4.3 Task history view -- completed/failed with details (depends on 4.1)

**4B: Active Operations** _(depend on task system UI + specific C-side support)_
- [ ] 4.4 Restore workflow UI -- browse tree, select files, destination, conflict policy (depends on 4.1, 3.2, 2.6, 0.2.4)
- [ ] 4.5 Backup run trigger -- toolbar/context menu per repo (depends on 4.1, 0.2.4)
- [ ] 4.10 Verify trigger from UI -- repair option + warning dialog (depends on 4.1, 0.5.1)
- [ ] 4.11 Verify results view -- object-level pass/fail, warn/error, click-through to Hash Lookup (depends on 4.10, 3.7, 0.5.2)
- [ ] 4.12 Verify status in health dashboard + sidebar indicator (depends on 4.11, 4.6)
- [ ] 4.13 Data export dialog -- bundle/tar, full/subset, destination prompt (depends on 4.1, 2.6, 0.2.4)

**4C: Journal, Health & Notifications** _(depend on Phase 0.1 journal)_
- [ ] 4.6 Repo health dashboard -- last backup, verify, failures, storage, GFS coverage, warnings (depends on 0.1.5, 3.1, 3.9)
- [ ] 4.7 Snapshot notes UI -- inline edit, list column, searchable (depends on 0.4.2, 3.2)
- [ ] 4.8 Journal viewer -- filterable/searchable operation history per repo (depends on 0.1.5)
- [ ] 4.14.6 Orphaned journal entry detection -- flag crashed/killed operations (depends on 4.8)
- [ ] 4.8.1 Notifications panel -- bell icon, badge, toasts, history (depends on 0.1.5, 4.6)
- [ ] 4.8.2 OS-level notifications via Electron native API (depends on 4.8.1)

**4D: Tag Management UI** _(depends on Phase 0.3 C-side tag RPC + Tags tab)_
- [ ] 4.9 Tag management UI -- create, rename, delete from Tags tab + context menus (depends on 0.3.1, 0.3.2, 0.3.3, 3.4, 3.2, 3.11)
- [ ] 4.9.1 Preserve tag quick-action in GFS Tree (depends on 4.9, 3.11)

**4E: Local Export over SSH** _(depends on SFTP module + export dialog)_
- [ ] 4.14 Local export over SSH -- SFTP side channel, download queue with progress (depends on 1.3.5, 4.13)
- [ ] 4.14.1 Download queue persistence across app restarts -- client-side manifest (depends on 4.14, 1.2.1)
- [ ] 4.14.2 Download retry -- manual, user-initiated (depends on 4.14)
- [ ] 4.14.3 Remote temp file cleanup after successful download (depends on 4.14)
- [ ] 4.14.4 Local restore from remote repo -- always into new folder, prompt destination (depends on 4.14, 4.4)

---

### Dependency Summary

```
Phase 0 (C-side) ──────────────────────────────┐
  0.1 Journal ─────────────────────────────────┐│
  0.2 Tasks (depends on 0.1) ─────────────────┐││
  0.3 Tags (independent) ────────────────────┐│││
  0.4 Notes (independent) ──────────────────┐││││
  0.5 Verify (depends on 0.2) ────────────┐│││││
                                           ││││││
Phase 1 (Electron foundation) ────────────┐││││││
  1.1 Setup ─┐                            │││││││
  1.2 Config ├── all independent          │││││││
  1.3 RPC ───┘                            │││││││
                                          │││││││
Phase 2 (Connection UI) ◄── Phase 1      │││││││
  2.1-2.5 Sidebar/connections             │││││││
  2.6-2.9 Filesystem browser, init wizard │││││││
                                          │││││││
Phase 3 (Feature parity) ◄── Phase 2     │││││││
  3A Core tabs (overview, snapshots, etc) │││││││
  3B Inspection tabs (packs, loose, etc)  │││││││
  3C Navigation tabs (search, gfs tree)   │││││││
                                          ▼▼▼▼▼▼▼
Phase 4 (New features) ◄── Phase 3 + Phase 0
  4A Task UI ◄── 0.2
  4B Active ops (restore, backup, verify, export) ◄── 4A + 0.2/0.5
  4C Journal/health/notifications ◄── 0.1
  4D Tag management ◄── 0.3
  4E Local export over SSH ◄── 1.3.5 + 4B
```

**Critical path**: 0.1 → 0.2 → Phase 1 (parallel) → Phase 2 → Phase 3A → Phase 4A → Phase 4B

**Parallelizable work streams**:
- Phase 0 (all C-side) can run in parallel with Phase 1 (all Electron)
- Phase 0.3 (tags), 0.4 (notes) are independent of 0.1/0.2
- Phase 3B tabs are all independent of each other
- Phase 4C, 4D, 4E are independent of each other (only share Phase 3 dependencies)

---

## Current Python Viewer - Functional Specification

### Communication Layer

The Python viewer talks to the C `backup` binary exclusively via a JSON RPC API over two modes:

1. **Session mode** (preferred): Persistent subprocess (`backup --json-session /repo`). The C side sends a ready banner with protocol version, compression support, and lock status. Requests/responses are newline-delimited JSON. Responses >= 256 bytes are LZ4-compressed (magic byte `0x00` + 4B uncomp len + 4B comp len + payload + `\n`).

2. **One-shot mode** (fallback): Spawns `backup --json /repo` per call, writes request to stdin, reads response from stdout. 120s timeout.

**Remote repos**: Supports `[user@]host:/path` format via SSH ControlMaster. Key auth tried first, then password auth via PTY. Custom remote binary path via `--remote-bin` or `CBACKUP_REMOTE_BIN` env var.

**Binary discovery**: `CBACKUP_BIN` env var > `build/bins/backup` relative to viewer > `backup` in PATH.

---

### RPC Actions (Complete List)

#### Snapshot Operations
| Action | Params | Returns |
|--------|--------|---------|
| `list` | none | All snapshots with headers (id, created_sec, node_count, dirent_count, phys_new_bytes, gfs_flags, snap_flags, logical_bytes) |
| `snap` | `id` | Full snapshot: header + all nodes + all dirents |
| `snap_header` | `id` | Header metadata only |
| `snap_dir_children` | `id`, `parent_node` | Direct children of a directory node (name, type, size, mode, has_children) |
| `diff` | `id1`, `id2` | Changes between snapshots. Codes: A=added, D=deleted, M=modified, m=metadata-only |
| `ls` | `id`, `path?`, `recursive?`, `type?`, `glob?` | Directory listing with node details |
| `search` | `query`, `id?`, `max_results?` | Filename substring matches across snapshots |

#### Tag Operations
| Action | Params | Returns |
|--------|--------|---------|
| `tags` | none | All tags (name, snap_id, preserve flag) |

#### Policy Operations
| Action | Params | Returns |
|--------|--------|---------|
| `policy` | none | Current policy.toml contents |
| `save_policy` | all policy fields (optional) | `{"saved": true}` -- **only write operation in the UI** |

#### Object Operations
| Action | Params | Returns |
|--------|--------|---------|
| `object_locate` | `hash` (64-char hex) | found, type, uncompressed_size |
| `object_content` | `hash`, `max_bytes?` | base64 payload, type, size, truncated flag |
| `object_layout` | `hash` | Physical layout segments of loose object file |
| `object_refs` | `hash` | All references across snapshots (snap_id, node_id, field) |

#### Pack Operations
| Action | Params | Returns |
|--------|--------|---------|
| `pack_entries` | `name` | All entries in a .dat file |
| `pack_index` | `name` | All entries in a .idx file |
| `all_pack_entries` | none | All entries across all packs |
| `global_pack_index` | `offset`, `limit` | Paginated global pack index with fanout table |

#### Statistics Operations
| Action | Params | Returns |
|--------|--------|---------|
| `repo_stats` | none | Per-type stats (packed & loose), compression ratios |
| `loose_stats` | none | Loose object stats only |
| `loose_list` | `offset`, `limit` | Paginated loose object list |

#### Repository Scan
| Action | Params | Returns |
|--------|--------|---------|
| `scan` | none | Physical repo structure (snapshot files, loose objects, packs, tags, format) |
| `repo_summary` | none | Aggregation of scan + list + tags + policy + loose_list + all_pack_entries + global_pack_index |

#### Session Control
| Action | Params | Returns |
|--------|--------|---------|
| `quit` | none | Closes session |

---

### UI Tabs (11 Total)

#### 1. Overview
- **Data**: `scan`, `list`
- **Shows**: Repo path, HEAD snapshot, snapshot count, format version, last written version, pack files list with sizes, snapshot table (id, creation time, node count, GFS flags, new bytes)

#### 2. Snapshots
- **Data**: `list`, `snap_header`, `snap_dir_children`, `snap`, `object_content`
- **Sub-tabs**:
  - **Header**: Snapshot metadata (version, created, node_count, dirent_count, GFS flags)
  - **Nodes**: Filterable/paginated table (node_id, type, mode, uid, gid, size, mtime, content_hash). Filter by hash prefix. Max 500 rows visible.
  - **Directory tree**: Lazy-expanding hierarchy. Right-click export files. Double-click file to preview object content.
- **Navigation**: `navigate_to_path(snap_id, path)` used by Search and GFS Tree tabs to jump here.

#### 3. Pack Files
- **Data**: `scan`, `global_pack_index`, `pack_entries`, `pack_index`, `object_layout`
- **Sub-tabs**:
  - **Header**: Pack metadata
  - **Entries (.dat)**: Filterable table (hash, type, compression, sizes, ratio). Context menu to view content.
  - **Index (.idx)**: Index entries (hash, dat_offset, entry_index)
  - **Pack Map**: Visual layout of pack file segments, color-coded by type (file header, entry headers, payloads by compression, parity, CRC). Preset bytes-per-row options.
  - **Global Index**: Paginated global pack index viewer

#### 4. Loose Objects
- **Data**: `loose_stats`, `loose_list`, `object_content`, `object_layout`
- **Sub-tabs**:
  - **Info**: Object metadata (hash, type, compression, sizes)
  - **Content Preview**: First 2MB hex dump or text
  - **Object Map**: Visual layout (similar to pack map)
- **Features**: Paginated listing (500/page), export payload, sparse region visualization

#### 5. Tags
- **Data**: `tags`
- **Shows**: Tag list on left, details on right (name, snap_id, preserve flag)

#### 6. Policy
- **Data**: `scan`, `policy`, `save_policy`
- **Editable fields**: paths (multiline), exclude paths (multiline), retention (keep_snaps, keep_daily/weekly/monthly/yearly), automation (auto_pack, auto_gc, auto_prune, verify_after, strict_meta)
- **Actions**: Save button calls `save_policy` RPC

#### 7. Hash Lookup
- **Data**: `object_locate`, `object_refs`
- **Workflow**: Enter 64-char SHA-256 -> locate object -> show type/size -> scan all snapshots for references (snap_id, node_id, field: content/xattr/acl)

#### 8. Diff
- **Data**: `list`, `diff`
- **Workflow**: Select two snapshots via dropdowns (defaults to last two) -> show change table (status, path, old/new size, old/new hash). Color-coded: green=added, dark red=deleted, brown=modified, dark blue=meta-only. Double-click for content preview/inline diff. Max 5000 rows.

#### 9. Analytics
- **Data**: `repo_stats`
- **Charts** (canvas-based):
  - Uncompressed size by type (stacked bar: FILE, SPARSE, XATTR, ACL)
  - Pack vs loose distribution
  - Compressibility breakdown (compressible vs incompressible, skip-marked, high-ratio)
- **Stats table**: Per-type count, uncompressed, compressed, ratio. Total savings.

#### 10. Search
- **Data**: `list`, `search`
- **Workflow**: Enter filename substring, optional "all snapshots" checkbox (default: HEAD only). Results table: snap_id, created, path, type, size, content_hash. Double-click jumps to Snapshots tab. Max 5000 results.

#### 11. GFS Tree
- **Data**: `list`
- **Shows**: Hierarchical tree grouped by year -> month. Color-coded by GFS tier: Yearly=brown, Monthly=blue, Weekly=green, Daily=cyan, Untagged=gray. Columns: snap ID, datetime, GFS flags, node count, new bytes. Summary counts per tier. Double-click jumps to Snapshots tab.

---

### Constants

**Node types**: REG=1, DIR=2, SYMLINK=3, HARDLINK=4, FIFO=5, CHR=6, BLK=7

**Object types**: FILE=1, XATTR=2, ACL=3, SPARSE=4

**Compression**: NONE=0, LZ4=1, LZ4_FRAME=2

**GFS flags**: DAILY=bit0, WEEKLY=bit1, MONTHLY=bit2, YEARLY=bit3

---

### Threading Model

- Background thread handles all RPC calls
- Thread-safe callback queue (`ui_call()`) delivers results to main/Tk thread
- Queue polled every 30ms on main thread
- Priority loading order: overview -> snapshots -> tags/policy -> repo_stats/loose_stats/global_pack_index
- Tabs lazy-load on first visit if dependencies aren't cached yet

### Tab Data Dependencies
| Tab | Required RPC cache keys |
|-----|------------------------|
| overview | scan, list |
| snapshots | list |
| packs | scan, global_pack_index |
| loose | loose_stats |
| tags | tags |
| policy | scan, policy |
| lookup | (none) |
| diff | list |
| analytics | repo_stats |
| search | list |
| gfs_tree | list |

### Cross-Tab Navigation
- Search -> Snapshots (jump to path)
- GFS Tree -> Snapshots (jump to snapshot)
- Diff -> Content preview
- Nodes sub-tab -> Directory tree (double-click)
- Pack/Loose entries -> Object content viewer
