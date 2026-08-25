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
 * WHAT IT REPLACES
 *
 * rpcCall(conn, repo, action, params) with action a bare string, translated
 * by a table of cases. That shape let the app post to /repos/<id>/run for
 * four months - a route the daemon has never had - because nothing at build
 * time could object and nothing at run time was watching. `run` was simply a
 * string that stopped naming anything.
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
export type ObjectLocation = 'loose' | 'packed'

export type ApiRequest =
  /* --- repositories --- */
  | { kind: 'repoList' }
  | { kind: 'repoShow'; repoId: number }
  | { kind: 'repoAdd'; path: string; label?: string
      encrypt?: 'system' | 'passphrase'; passphrase?: string }
  | { kind: 'repoRemove'; repoId: number; purge?: boolean }

  /* --- encryption --- */
  | { kind: 'keyExport'; repoId: number }
  | { kind: 'keyImport'; repoId: number; key: string }
  | { kind: 'keyAck'; repoId: number }

  /* --- the two that matter most, and the two that were broken --- */
  | { kind: 'backup'; repoId: number; paths: string[]
      label?: string; exclude?: string[]; oneFilesystem?: boolean }
  | { kind: 'restore'; repoId: number; snapshotId: number; dest: string
      subpath?: string; overwrite?: boolean }

  /* --- snapshots and browsing --- */
  | { kind: 'snapshotList'; repoId: number; limit?: number; offset?: number }
  | { kind: 'snapshotDelete'; repoId: number; snapshotId: number }
  | { kind: 'snapshotTree'; repoId: number; snapshotId: number
      parent?: number; limit?: number; offset?: number }
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
  | { kind: 'pack'; repoId: number }
  | { kind: 'reindex'; repoId: number }

  /* --- introspection --- */
  | { kind: 'stats'; repoId: number }
  | { kind: 'summary'; repoId: number }
  | { kind: 'objectList'; repoId: number; location?: ObjectLocation; limit?: number; offset?: number }
  | { kind: 'objectInfo'; repoId: number; hash: string }
  | { kind: 'objectRefs'; repoId: number; hash: string }
  | { kind: 'objectLayout'; repoId: number; hash: string }
  | { kind: 'objectContent'; repoId: number; hash: string }
  | { kind: 'packList'; repoId: number }
  | { kind: 'packEntries'; repoId: number; packId: number; limit?: number; offset?: number }
  | { kind: 'fanout'; repoId: number }

  /* --- metadata --- */
  | { kind: 'policyGet'; repoId: number }
  | { kind: 'policySet'; repoId: number; key: string; value: string }
  | { kind: 'tagList'; repoId: number }
  | { kind: 'tagSet'; repoId: number; name: string; snapshotId: number }
  | { kind: 'tagRemove'; repoId: number; name: string }
  | { kind: 'journal'; repoId: number; limit?: number; result?: string }

  /* --- schedules --- */
  | { kind: 'scheduleList'; repoId: number }
  | { kind: 'scheduleAdd'; repoId: number; action: string
      trigger: ScheduleTrigger; onOverlap?: string; catchup?: string }
  | { kind: 'scheduleSetEnabled'; repoId: number; scheduleId: number; enabled: boolean }
  | { kind: 'scheduleRemove'; repoId: number; scheduleId: number }

  /* --- jobs --- */
  | { kind: 'jobList' }
  | { kind: 'jobGet'; jobId: number }
  | { kind: 'jobCancel'; jobId: number }

  /* --- daemon-wide --- */
  | { kind: 'health' }
  | { kind: 'ready' }
  | { kind: 'metrics' }
  | { kind: 'authSetupState' }
  | { kind: 'authSetup'; password: string }
  | { kind: 'authLogin'; password: string }

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
