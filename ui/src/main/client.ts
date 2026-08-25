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

import type { ApiRequest } from '../shared/api'

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
        path: `/repos/${req.repoId}${qs({ purge: req.purge ? 1 : undefined })}`,
      }

    /* --- encryption --- */
    case 'keyExport':
      return { method: 'GET', path: `/repos/${req.repoId}/key` }
    case 'keyImport':
      return { method: 'POST', path: `/repos/${req.repoId}/key/import`, body: { key: req.key } }
    case 'keyAck':
      return { method: 'POST', path: `/repos/${req.repoId}/key/ack` }

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
        body: { dest: req.dest, subpath: req.subpath, overwrite: req.overwrite ? 1 : undefined },
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
          parent: req.parent, limit: req.limit, offset: req.offset })}`,
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
          ignore_case: req.ignoreCase ? 1 : undefined,
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
    case 'pack':
      return { method: 'POST', path: `/repos/${req.repoId}/pack` }
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
      return { method: 'GET', path: `/repos/${req.repoId}/packs` }
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
        body: { name: req.name, snap_id: req.snapshotId },
      }
    case 'tagRemove':
      return { method: 'DELETE', path: `/repos/${req.repoId}/tags/${seg(req.name)}` }
    case 'journal':
      return {
        method: 'GET',
        path: `/repos/${req.repoId}/journal${qs({ limit: req.limit, result: req.result })}`,
      }

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
        body: { enabled: req.enabled ? 1 : 0 },
      }
    case 'scheduleRemove':
      return { method: 'DELETE', path: `/repos/${req.repoId}/schedules/${req.scheduleId}` }

    /* --- jobs --- */
    case 'jobList':
      return { method: 'GET', path: '/jobs' }
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
