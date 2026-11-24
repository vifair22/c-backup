/*
 * Every operation the desktop app can perform, as one closed set.
 *
 * The app is meant to reach parity with the CLI, and the CLI is held to a
 * written rule: an endpoint gets a command, and the end-to-end suite
 * exercises the whole table. This file is the same rule for the second
 * client - a request the daemon can serve should be expressible here, and
 * one that is expressible here should reach a route that exists.
 *
 * WHY A UNION RATHER THAN A CLIENT OBJECT
 *
 * It crosses an IPC boundary. The renderer cannot hold a client, so every
 * call is a message, and a message is only as safe as its type. One union
 * means the main process can switch on `kind` with an exhaustiveness check,
 * so adding a variant without handling it is a compile error rather than a
 * runtime surprise. Forty separate IPC channels would give the same safety
 * and forty times the boilerplate.
 *
 * WHAT IT REPLACED
 *
 * rpcCall(conn, repo, action, params), with action a bare string translated
 * by a table of cases. That shape let the app post to /repos/<id>/run for
 * four months - a route the daemon has never had - because nothing at build
 * time could object and nothing at run time was watching. `run` was simply a
 * string that stopped naming anything.
 *
 * It was worse than that in the end: several screens named actions the table
 * had no case for and threw on open, and most of the rest read fields out of
 * responses that carry different names, so they rendered rows of blanks and
 * failed nothing. That entry point and its table are now deleted, and this
 * union is the only way the renderer can reach the daemon.
 *
 * WHAT IS DELIBERATELY ABSENT
 *
 * Replication. The engine exists and the CLI drives it, but the desktop app
 * does not, on purpose: it takes a destination path and writes a repository
 * there, and that is a capability to expose deliberately rather than by
 * completing a table.
 */

/* ---------------------------------------------------------------- */
/* Requests                                                          */
/* ---------------------------------------------------------------- */

export type VerifyLevel = 'index' | 'structure' | 'deep'

/*
 * The maintenance operations a repository view offers.
 *
 * Named here rather than in a component so the menu and the handler cannot
 * disagree, and so each one is a `kind` the contract suite already proves
 * reaches a real route.
 */
export type MaintenanceOp = 'verify' | 'gc' | 'gfs' | 'reindex'
export type ObjectLocation = 'staged' | 'packed'

export type ApiRequest =
  /* --- repositories --- */
  | { kind: 'repoList' }
  | { kind: 'repoShow'; repoId: number }
  | { kind: 'repoAdd'; path: string; label?: string
      encrypt?: 'system' | 'passphrase'; passphrase?: string }
  | { kind: 'repoRemove'; repoId: number }

  /* --- encryption --- */
  | { kind: 'keyExport'; repoId: number }
  | { kind: 'keyImport'; repoId: number; key: string }
  | { kind: 'keyAck'; repoId: number }

  /* --- what a repo backs up, configured once --- */
  | { kind: 'sourcesGet'; repoId: number }
  | { kind: 'sourcesSet'; repoId: number; sources: string[]; exclude?: string[]
      /* Snapshot mode per source (db-detection.md 9.4, Manual 6.6). Left
       * out, the daemon keeps the modes of sources that stay; given, it
       * replaces them, and a source not listed is read live. */
      snapshot?: SourceSnapshot[] }

  /* --- the two that matter most, and the two that were broken ---
   * paths is optional: omitted, the daemon uses the repository's own
   * sources, which is what a button with nothing typed into it means. */
  | { kind: 'backup'; repoId: number; paths?: string[]
      label?: string; exclude?: string[]; oneFilesystem?: boolean }
  | { kind: 'restore'; repoId: number; snapshotId: number; dest: string
      subpath?: string; overwrite?: boolean
      /* A database with an engine copy in the snapshot comes back from the
       * copy, its stale -wal/-shm/-journal left out (db-detection.md 9.5). */
      preferEngineCopy?: boolean }

  /* --- snapshots and browsing --- */
  | { kind: 'snapshotList'; repoId: number; limit?: number; offset?: number }
  | { kind: 'snapshotDelete'; repoId: number; snapshotId: number }
  | { kind: 'snapshotTree'; repoId: number; snapshotId: number
      parent?: number; limit?: number; offset?: number
      /* The index generation parent came from: a stale one is a 409
       * rather than whatever now has that id. */
      gen?: number }
  | { kind: 'noteGet'; repoId: number; snapshotId: number }
  | { kind: 'noteSet'; repoId: number; snapshotId: number; note: string }
  | { kind: 'search'; repoId: number; query: string; limit?: number }
  | { kind: 'diff'; repoId: number; from: number; to: number }

  /* --- content search, which is a job --- */
  | { kind: 'grepStart'; repoId: number; pattern: string
      snapshot?: number; pathPrefix?: string; ignoreCase?: boolean; maxFiles?: number }
  | { kind: 'grepRuns'; repoId: number; limit?: number }
  | { kind: 'grepHits'; repoId: number; runId: number; limit?: number; offset?: number }

  /* --- maintenance, all of which return a job --- */
  | { kind: 'verify'; repoId: number; level?: VerifyLevel; repair?: boolean; dryRun?: boolean }
  | { kind: 'verifyRuns'; repoId: number; limit?: number }
  | { kind: 'verifyFindings'; repoId: number; runId: number }
  | { kind: 'gc'; repoId: number; dryRun?: boolean }
  | { kind: 'gfs'; repoId: number; dryRun?: boolean }
  | { kind: 'reindex'; repoId: number }

  /* --- introspection --- */
  | { kind: 'stats'; repoId: number }
  | { kind: 'summary'; repoId: number }
  | { kind: 'objectList'; repoId: number; location?: ObjectLocation; limit?: number; offset?: number }
  | { kind: 'objectInfo'; repoId: number; hash: string }
  | { kind: 'objectRefs'; repoId: number; hash: string }
  | { kind: 'objectLayout'; repoId: number; hash: string }
  | { kind: 'objectContent'; repoId: number; hash: string }
  | { kind: 'packList'; repoId: number; limit?: number; offset?: number }
  | { kind: 'packEntries'; repoId: number; packId: number; limit?: number; offset?: number }
  | { kind: 'fanout'; repoId: number }

  /* --- metadata --- */
  | { kind: 'policyGet'; repoId: number }
  | { kind: 'policySet'; repoId: number; key: string; value: string }
  | { kind: 'tagList'; repoId: number }
  | { kind: 'tagSet'; repoId: number; name: string; snapshotId: number; move?: boolean }
  | { kind: 'tagRemove'; repoId: number; name: string }
  | { kind: 'journal'; repoId: number; limit?: number; result?: string }

  /* --- database detection (docs/db-detection.md 7.4).
   * scope is exact: "unit:<provider>:<unit>" or "source:<path>". coverage
   * takes the section 5 names, or "none" for a finding nothing assessed. */
  | { kind: 'dbList'; repoId: number; scope?: string; engine?: string
      coverage?: string; state?: string; limit?: number; offset?: number }
  | { kind: 'dbShow'; repoId: number; findingId: number }
  /* Detection now, without a backup: a job. unit may itself hold ':'
   * (compose:immich), which is why provider and unit travel apart. */
  | { kind: 'unitDetect'; repoId: number; provider: string; unit: string }
  /* Acting on a finding (D2). Nothing but these three enables a template.
   * preview with no template asks for the finding's own suggestion; enable
   * names one, and params are the template's, checked by the daemon. */
  | { kind: 'dbPreview'; repoId: number; findingId: number
      template?: string; params?: Record<string, unknown> }
  | { kind: 'dbEnable'; repoId: number; findingId: number
      template: string; params?: Record<string, unknown> }
  | { kind: 'dbDismiss'; repoId: number; findingId: number; reason: string }

  /* --- units: VMs and containers through a source provider (Manual 22).
   * unit is a name or an id; an id may itself hold ':' (a compose project
   * is compose:<name>), which is why provider and unit travel apart. */
  | { kind: 'providerList' }
  | { kind: 'providerUnits'; provider: string; limit?: number; offset?: number }
  | { kind: 'providerUnit'; provider: string; unit: string }
  | { kind: 'repoUnits'; repoId: number; limit?: number; offset?: number }
  | { kind: 'unitHistory'; repoId: number; provider: string; unit: string
      limit?: number; offset?: number }
  /* `unit backup`: name units, or all with optional exclude globs (the
   * daemon refuses exclude without all, and names with all). require is a
   * consistency level, crash by default; pauseScope unit, group or job. */
  | { kind: 'unitBackup'; repoId: number; provider: string
      units?: string[]; all?: boolean; exclude?: string[]
      require?: UnitLevel; pauseScope?: PauseScope }
  /* `unit policy show` and `unit policy set`: values maps a key to its
   * new value, or to "" to drop the unit's own (the repository's applies
   * again). Several keys in one request, which the route takes. */
  | { kind: 'unitPolicyGet'; repoId: number; provider: string; unit: string }
  | { kind: 'unitPolicySet'; repoId: number; provider: string; unit: string
      values: Record<string, string> }
  /* `vm checkpoints` and `vm reset-incremental`. The CLI names libvirt;
   * the routes serve any provider's chain, so these take one. */
  | { kind: 'unitCheckpoints'; repoId: number; provider: string; unit: string }
  | { kind: 'unitResetIncremental'; repoId: number; provider: string; unit: string }
  /* `unit restore`: the whole snapshot as files under dest, one item to a
   * new file, or the unit itself re-created (define) in place or under a
   * new name with its data under targetRoot. */
  | { kind: 'unitRestore'; repoId: number; snapshotId: number; restore: UnitRestore }

  /* --- NBD exports of a VM disk (Manual 22.8): `vm export`, `vm exports`,
   * `vm export-show`, `vm unexport`. ttlS is 60 s to 7 days. */
  | { kind: 'exportCreate'; repoId: number; snapshotId: number; item: string; ttlS?: number }
  | { kind: 'exportList' }
  | { kind: 'exportShow'; exportId: number }
  | { kind: 'exportRevoke'; exportId: number }

  /* --- schedules --- */
  | { kind: 'scheduleList'; repoId: number }
  | { kind: 'scheduleAdd'; repoId: number; action: string
      trigger: ScheduleTrigger; onOverlap?: string; catchup?: string }
  | { kind: 'scheduleSetEnabled'; repoId: number; scheduleId: number; enabled: boolean }
  | { kind: 'scheduleRemove'; repoId: number; scheduleId: number }

  /* --- jobs --- */
  /* repoId: that repository's jobs, and replicate jobs into it (B283);
   * absent: the newest jobs daemon-wide. */
  | { kind: 'jobList'; repoId?: number }
  | { kind: 'jobGet'; jobId: number }
  | { kind: 'jobCancel'; jobId: number }

  /* --- daemon-wide --- */
  | { kind: 'health' }
  | { kind: 'ready' }
  | { kind: 'metrics' }
  | { kind: 'authSetupState' }
  | { kind: 'authSetup'; password: string }
  | { kind: 'authLogin'; password: string }

/* The levels a unit capture can achieve, lowest first (Manual 22.1). */
/*
 * A call answered either way: the body on success, or the daemon's refusal
 * with its status and the whole body, for a refusal that carries fields
 * beyond its message (B240).
 */
export type ApiCallResult<T = unknown> =
  | { ok: true; body: T }
  | { ok: false; status: number; error: string; body: unknown }

export type UnitLevel = 'inconsistent' | 'crash' | 'quiesced' | 'application' | 'offline'
export type PauseScope = 'unit' | 'group' | 'job'

/* The three restore modes `c-backup unit restore` builds (api.md, unit/restore). */
export type UnitRestore =
  | { mode: 'files'; dest: string; overwrite?: boolean }
  | { mode: 'item'; item: string; dest: string }
  | { mode: 'unit'; replace?: boolean; name?: string; targetRoot?: string
      pinDigest?: boolean; start?: boolean; keepMacs?: boolean }

export type ScheduleTrigger =
  | { type: 'interval'; seconds: number }
  | { type: 'calendar'; every: string; at: string }
  | { type: 'cron'; spec: string }

/* Every `kind`, as a type. Used by the contract suite to prove the set it
 * exercises is the whole set rather than the ones somebody remembered. */
export type ApiKind = ApiRequest['kind']

/* ---------------------------------------------------------------- */
/* Responses                                                         */
/* ---------------------------------------------------------------- */

/*
 * Deliberately loose for now.
 *
 * Response shapes are worth typing, but typing them wrongly is worse than
 * leaving them open: a shape asserted here that the daemon does not produce
 * is a lie the compiler will enforce. They get narrowed as each screen is
 * migrated and its actual payload is read, which is when the shape is known
 * rather than guessed.
 */
export type ApiResponse = unknown

/* Anything that starts work returns one of these. */
export interface JobAccepted {
  job_id: number
}

/*
 * A job, as the daemon actually reports it.
 *
 * Written down because the app was reading a different shape entirely: it
 * expected {tasks:[{task_id, command, pid, started, alive, ...}]} from the
 * one-shot CLI era and the daemon answers {jobs:[{id, action, state}]}. So
 * the poll set the task list to undefined, and the task bar - which renders
 * on every repository screen - called .filter on it. Opening any repository
 * blanked the window.
 *
 * The list and the detail now carry the same object, with JSON numbers.
 * "started"/"ended" are epoch seconds or null; "result" is what the job
 * reported about itself when it finished, or null.
 */
/* A source read from a ZFS or btrfs snapshot (Manual 6.6). strict: a source
 * that cannot be fails the backup; warn: it is read live and the job says so. */
export type SnapshotMode = 'strict' | 'warn'
export interface SourceSnapshot { path: string; mode: SnapshotMode }

export type JobState = 'queued' | 'running' | 'completed' | 'failed' | 'cancelled'

export interface Job {
  id: number
  repo_id?: number
  /* A replicate job's destination, when it is a registered repository;
   * null otherwise (B283). */
  dest_repo_id?: number | null
  action: string
  state: JobState
  started?: number | null
  ended?: number | null
  elapsed_s?: number
  phase?: string
  current?: number
  total?: number
  bytes_done?: number
  bytes_total?: number
  rate_bytes_per_s?: number
  error?: string
  result?: Record<string, unknown> | null
}
