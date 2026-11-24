/*
 * The one place a request to the daemon is built.
 *
 * Every ApiRequest becomes a method, a path and a body here and nowhere
 * else. That is the point: the app used to construct paths by interpolating
 * an action string, so `run` and `restore` produced /repos/<id>/run and
 * /repos/<id>/restore, neither of which the daemon serves, and both survived
 * because the mistake could live in any of sixteen components and was
 * checked in none.
 *
 * Getting a path wrong here is still possible. What has changed is that
 * there is exactly one file to check, and tests/contract.test.ts checks it
 * against a running daemon - so a path that stops existing fails a build
 * rather than a backup.
 */

import type { ApiRequest, UnitRestore } from '../shared/api'

export interface Endpoint {
  method: 'GET' | 'POST' | 'PUT' | 'DELETE'
  path: string
  body?: unknown
}

/* Percent-encode a path segment. Tag names and object hashes reach these
 * paths from user input, and a name with a slash in it would otherwise
 * change which endpoint is addressed rather than which resource. */
const seg = (s: string): string => encodeURIComponent(s)

/* Build a query string from defined values only, so an absent option is
 * absent rather than "undefined". */
function qs(params: Record<string, string | number | boolean | undefined>): string {
  const parts: string[] = []
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined) continue
    parts.push(`${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
  }
  return parts.length ? `?${parts.join('&')}` : ''
}

/*
 * The exhaustiveness check.
 *
 * If a variant is added to ApiRequest and not handled below, `req` is not
 * `never` here and this stops compiling. That is the whole reason the
 * requests are one union rather than forty loose functions.
 */
function unhandled(req: never): never {
  throw new Error(`unhandled request: ${JSON.stringify(req)}`)
}

/*
 * The body `c-backup unit restore` sends for each mode: an item always as
 * raw (the only format this build writes), and the define options only
 * when set, so an absent one is the daemon's default rather than false.
 */
export function unitRestoreBody(r: UnitRestore): Record<string, unknown> {
  switch (r.mode) {
    case 'files':
      return r.overwrite ? { mode: 'files', dest: r.dest, overwrite: true } : { mode: 'files', dest: r.dest }
    case 'item':
      return { mode: 'item', format: 'raw', item: r.item, dest: r.dest }
    case 'unit': {
      const b: Record<string, unknown> = { mode: 'unit' }
      if (r.name) b.name = r.name
      if (r.targetRoot) b.target_root = r.targetRoot
      if (r.replace) b.replace = true
      if (r.pinDigest) b.pin_digest = true
      if (r.start) b.start = true
      if (r.keepMacs) b.keep_macs = true
      return b
    }
  }
}

export function toEndpoint(req: ApiRequest): Endpoint {
  switch (req.kind) {
    /* --- repositories --- */
    case 'repoList':
      return { method: 'GET', path: '/repos' }
    case 'repoShow':
      return { method: 'GET', path: `/repos/${req.repoId}` }
    case 'repoAdd':
      return {
        method: 'POST',
        path: '/repos',
        body: {
          path: req.path,
          label: req.label,
          encrypt: req.encrypt,
          passphrase: req.passphrase,
        },
      }
    case 'repoRemove':
      return {
        method: 'DELETE',
        path: `/repos/${req.repoId}`,
      }

    /* --- encryption --- */
    case 'keyExport':
      return { method: 'GET', path: `/repos/${req.repoId}/key` }
    case 'keyImport':
      return { method: 'POST', path: `/repos/${req.repoId}/key/import`, body: { key: req.key } }
    case 'keyAck':
      return { method: 'POST', path: `/repos/${req.repoId}/key/ack` }

    /* --- configured sources --- */
    case 'sourcesGet':
      return { method: 'GET', path: `/repos/${req.repoId}/sources` }
    case 'sourcesSet':
      return {
        method: 'PUT',
        path: `/repos/${req.repoId}/sources`,
        body: req.snapshot
          ? { sources: req.sources, exclude: req.exclude ?? [], snapshot: req.snapshot }
          : { sources: req.sources, exclude: req.exclude ?? [] },
      }

    /* --- backup and restore --- */
    case 'backup':
      return {
        method: 'POST',
        path: `/repos/${req.repoId}/backup`,
        body: {
          paths: req.paths,
          label: req.label,
          exclude: req.exclude,
          one_filesystem: req.oneFilesystem,
        },
      }
    case 'restore':
      /* Under the snapshot, not the repo. The app posted to
       * /repos/<id>/restore, which has never existed. */
      return {
        method: 'POST',
        path: `/repos/${req.repoId}/snapshots/${req.snapshotId}/restore`,
        body: {
          dest: req.dest, subpath: req.subpath, overwrite: req.overwrite ? 1 : undefined,
          prefer_engine_copy: req.preferEngineCopy ? true : undefined,
        },
      }

    /* --- snapshots and browsing --- */
    case 'snapshotList':
      return {
        method: 'GET',
        path: `/repos/${req.repoId}/snapshots${qs({ limit: req.limit, offset: req.offset })}`,
      }
    case 'snapshotDelete':
      return { method: 'DELETE', path: `/repos/${req.repoId}/snapshots/${req.snapshotId}` }
    case 'snapshotTree':
      return {
        method: 'GET',
        path: `/repos/${req.repoId}/snapshots/${req.snapshotId}/tree${qs({
          parent: req.parent, limit: req.limit, offset: req.offset, gen: req.gen })}`,
      }
    case 'noteGet':
      return { method: 'GET', path: `/repos/${req.repoId}/snapshots/${req.snapshotId}/note` }
    case 'noteSet':
      return {
        method: 'PUT',
        path: `/repos/${req.repoId}/snapshots/${req.snapshotId}/note`,
        body: { note: req.note },
      }
    case 'search':
      return {
        method: 'GET',
        path: `/repos/${req.repoId}/search${qs({ q: req.query, limit: req.limit })}`,
      }
    case 'diff':
      return {
        method: 'GET',
        path: `/repos/${req.repoId}/diff${qs({ from: req.from, to: req.to })}`,
      }

    /* --- content search --- */
    case 'grepStart':
      return {
        method: 'POST',
        path: `/repos/${req.repoId}/grep`,
        body: {
          pattern: req.pattern,
          snapshot: req.snapshot,
          path_prefix: req.pathPrefix,
          ignore_case: req.ignoreCase ? true : undefined,
          max_files: req.maxFiles,
        },
      }
    case 'grepRuns':
      return { method: 'GET', path: `/repos/${req.repoId}/grep${qs({ limit: req.limit })}` }
    case 'grepHits':
      return {
        method: 'GET',
        path: `/repos/${req.repoId}/grep/${req.runId}${qs({ limit: req.limit, offset: req.offset })}`,
      }

    /* --- maintenance --- */
    case 'verify':
      return {
        method: 'POST',
        path: `/repos/${req.repoId}/verify${qs({
          level: req.level,
          repair: req.repair === false ? 0 : undefined,
          dry_run: req.dryRun ? 1 : undefined,
        })}`,
      }
    case 'verifyRuns':
      return { method: 'GET', path: `/repos/${req.repoId}/verify${qs({ limit: req.limit })}` }
    case 'verifyFindings':
      return { method: 'GET', path: `/repos/${req.repoId}/verify/${req.runId}` }
    case 'gc':
      return {
        method: 'POST',
        path: `/repos/${req.repoId}/gc${qs({ dry_run: req.dryRun ? 1 : undefined })}`,
      }
    case 'gfs':
      return {
        method: 'POST',
        path: `/repos/${req.repoId}/gfs${qs({ dry_run: req.dryRun ? 1 : undefined })}`,
      }
    case 'reindex':
      return { method: 'POST', path: `/repos/${req.repoId}/reindex` }

    /* --- introspection --- */
    case 'stats':
      return { method: 'GET', path: `/repos/${req.repoId}/stats` }
    case 'summary':
      return { method: 'GET', path: `/repos/${req.repoId}/summary` }
    case 'objectList':
      return {
        method: 'GET',
        path: `/repos/${req.repoId}/objects${qs({
          location: req.location, limit: req.limit, offset: req.offset })}`,
      }
    case 'objectInfo':
      return { method: 'GET', path: `/repos/${req.repoId}/objects/${seg(req.hash)}` }
    case 'objectRefs':
      return { method: 'GET', path: `/repos/${req.repoId}/objects/${seg(req.hash)}/refs` }
    case 'objectLayout':
      return { method: 'GET', path: `/repos/${req.repoId}/objects/${seg(req.hash)}/layout` }
    case 'objectContent':
      return { method: 'GET', path: `/repos/${req.repoId}/objects/${seg(req.hash)}/content` }
    case 'packList':
      return {
        method: 'GET',
        path: `/repos/${req.repoId}/packs${qs({ limit: req.limit, offset: req.offset })}`,
      }
    case 'packEntries':
      return {
        method: 'GET',
        path: `/repos/${req.repoId}/packs/${req.packId}/entries${qs({
          limit: req.limit, offset: req.offset })}`,
      }
    case 'fanout':
      return { method: 'GET', path: `/repos/${req.repoId}/fanout` }

    /* --- metadata --- */
    case 'policyGet':
      return { method: 'GET', path: `/repos/${req.repoId}/policy` }
    case 'policySet':
      return {
        method: 'PUT',
        path: `/repos/${req.repoId}/policy`,
        body: { key: req.key, value: req.value },
      }
    case 'tagList':
      return { method: 'GET', path: `/repos/${req.repoId}/tags` }
    case 'tagSet':
      return {
        method: 'POST',
        path: `/repos/${req.repoId}/tags`,
        /* move only when asked: without it the daemon refuses to take an
         * existing name off another snapshot, which is the protection. */
        body: req.move
          ? { name: req.name, snap_id: req.snapshotId, move: true }
          : { name: req.name, snap_id: req.snapshotId },
      }
    case 'tagRemove':
      return { method: 'DELETE', path: `/repos/${req.repoId}/tags/${seg(req.name)}` }
    case 'journal':
      return {
        method: 'GET',
        path: `/repos/${req.repoId}/journal${qs({ limit: req.limit, result: req.result })}`,
      }

    /* --- database detection --- */
    case 'dbList':
      return {
        method: 'GET',
        path: `/repos/${req.repoId}/databases${qs({
          scope: req.scope, engine: req.engine, coverage: req.coverage, state: req.state,
          limit: req.limit, offset: req.offset })}`,
      }
    case 'dbShow':
      return { method: 'GET', path: `/repos/${req.repoId}/databases/${req.findingId}` }
    case 'unitDetect':
      return {
        method: 'POST',
        path: `/repos/${req.repoId}/units/${seg(req.provider)}/${seg(req.unit)}/detect`,
      }
    case 'dbPreview':
      return {
        method: 'POST',
        path: `/repos/${req.repoId}/databases/${req.findingId}/preview`,
        /* No body at all asks for the finding's own suggestion. */
        body: req.template === undefined
          ? undefined
          /* params absent, not {}: for an enabled finding the daemon then
           * previews the params it already runs. */
          : req.params === undefined
            ? { template: req.template }
            : { template: req.template, params: req.params },
      }
    case 'dbEnable':
      return {
        method: 'POST',
        path: `/repos/${req.repoId}/databases/${req.findingId}/enable`,
        body: { template: req.template, params: req.params ?? {} },
      }
    case 'dbDismiss':
      return {
        method: 'POST',
        path: `/repos/${req.repoId}/databases/${req.findingId}/dismiss`,
        body: { reason: req.reason },
      }

    /* --- units --- */
    case 'providerList':
      return { method: 'GET', path: '/providers' }
    case 'providerUnits':
      return {
        method: 'GET',
        path: `/providers/${seg(req.provider)}/units${qs({ limit: req.limit, offset: req.offset })}`,
      }
    case 'providerUnit':
      return { method: 'GET', path: `/providers/${seg(req.provider)}/units/${seg(req.unit)}` }
    case 'repoUnits':
      return {
        method: 'GET',
        path: `/repos/${req.repoId}/units${qs({ limit: req.limit, offset: req.offset })}`,
      }
    case 'unitHistory':
      return {
        method: 'GET',
        path: `/repos/${req.repoId}/units/${seg(req.provider)}/${seg(req.unit)}/snapshots${qs({
          limit: req.limit, offset: req.offset })}`,
      }
    case 'unitBackup':
      /* The same body `c-backup unit backup` builds: units or all, never
       * both, and exclude only with all. Absent keys are left out rather
       * than sent empty, so the daemon applies its own defaults. */
      return {
        method: 'POST',
        path: `/repos/${req.repoId}/units/backup`,
        body: {
          provider: req.provider,
          ...(req.all ? { all: true, exclude: req.exclude ?? [] } : { units: req.units ?? [] }),
          ...(req.require ? { require: req.require } : {}),
          ...(req.pauseScope ? { pause_scope: req.pauseScope } : {}),
        },
      }

    case 'unitPolicyGet':
      return { method: 'GET', path: `/repos/${req.repoId}/units/${seg(req.provider)}/${seg(req.unit)}/policy` }
    case 'unitPolicySet':
      return {
        method: 'PUT',
        path: `/repos/${req.repoId}/units/${seg(req.provider)}/${seg(req.unit)}/policy`,
        body: req.values,
      }
    case 'unitCheckpoints':
      return { method: 'GET', path: `/repos/${req.repoId}/units/${seg(req.provider)}/${seg(req.unit)}/checkpoints` }
    case 'unitResetIncremental':
      return {
        method: 'POST',
        path: `/repos/${req.repoId}/units/${seg(req.provider)}/${seg(req.unit)}/reset-incremental`,
      }
    case 'unitRestore':
      return {
        method: 'POST',
        path: `/repos/${req.repoId}/snapshots/${req.snapshotId}/unit/restore`,
        body: unitRestoreBody(req.restore),
      }

    /* --- NBD exports --- */
    case 'exportCreate':
      return {
        method: 'POST',
        path: `/repos/${req.repoId}/snapshots/${req.snapshotId}/unit/export`,
        body: req.ttlS === undefined ? { item: req.item } : { item: req.item, ttl_s: req.ttlS },
      }
    case 'exportList':
      return { method: 'GET', path: '/exports' }
    case 'exportShow':
      return { method: 'GET', path: `/exports/${req.exportId}` }
    case 'exportRevoke':
      return { method: 'DELETE', path: `/exports/${req.exportId}` }

    /* --- schedules --- */
    case 'scheduleList':
      return { method: 'GET', path: `/repos/${req.repoId}/schedules` }
    case 'scheduleAdd':
      return {
        method: 'POST',
        path: `/repos/${req.repoId}/schedules`,
        body: {
          action: req.action,
          trigger: req.trigger,
          on_overlap: req.onOverlap,
          catchup: req.catchup,
        },
      }
    case 'scheduleSetEnabled':
      return {
        method: 'PUT',
        path: `/repos/${req.repoId}/schedules/${req.scheduleId}`,
        body: { enabled: req.enabled },
      }
    case 'scheduleRemove':
      return { method: 'DELETE', path: `/repos/${req.repoId}/schedules/${req.scheduleId}` }

    /* --- jobs --- */
    case 'jobList':
      return { method: 'GET', path: req.repoId !== undefined ? `/jobs?repo=${req.repoId}` : '/jobs' }
    case 'jobGet':
      return { method: 'GET', path: `/jobs/${req.jobId}` }
    case 'jobCancel':
      return { method: 'POST', path: `/jobs/${req.jobId}/cancel` }

    /* --- daemon-wide --- */
    case 'health':
      return { method: 'GET', path: '/healthz' }
    case 'ready':
      return { method: 'GET', path: '/readyz' }
    case 'metrics':
      return { method: 'GET', path: '/metrics' }
    case 'authSetupState':
      return { method: 'GET', path: '/api/auth/setup' }
    case 'authSetup':
      return { method: 'POST', path: '/api/auth/setup', body: { password: req.password } }
    case 'authLogin':
      return { method: 'POST', path: '/api/auth/login', body: { password: req.password } }

    default:
      return unhandled(req)
  }
}
