/*
 * Action-to-endpoint mapping.
 *
 * The renderer calls rpcCall(conn, repo, action, params) because that is what
 * the old JSON-RPC transport exposed. The daemon speaks REST, so rather than
 * rewriting every component at once, actions are translated here.
 *
 * This is a bridge, not a destination. Several old actions have no successor
 * and are listed explicitly below with the reason, because a silent 404 is a
 * much worse way to discover a removed capability than a message naming it.
 */

export interface Endpoint {
  method: 'GET' | 'POST' | 'PUT' | 'DELETE'
  path: string
  body?: unknown
}

/*
 * Actions with no successor in the daemon API.
 *
 * The first three read the .idx sidecar, which no longer exists: pack contents
 * are rows in pack_objects, and the sidecar's 256-entry fanout table has no
 * equivalent at all. PackBrowser's fanout chart has to be removed or rebuilt
 * against a prefix histogram; it cannot be mapped.
 *
 * `snap` loaded an entire snapshot in one response. Every collection route
 * pages by design, because returning a 10k-entry directory row by row measured
 * 2304 us against 93 us for the first page. Restoring the full-load action
 * would reintroduce exactly the cost the pagination rule exists to avoid.
 */
export const REMOVED_ACTIONS: Record<string, string> = {
  pack_index:
    'the .idx sidecar is gone; pack contents are rows in pack_objects',
  global_pack_index:
    'the .idx sidecar is gone; its fanout table has no successor',
  all_pack_entries:
    'the .idx sidecar is gone; use paginated object listing instead',
  snap: 'full-snapshot loads are removed; page through the tree endpoint',
}

export class UnsupportedActionError extends Error {
  constructor(action: string, reason: string) {
    super(`'${action}' is no longer supported: ${reason}`)
    this.name = 'UnsupportedActionError'
  }
}

function q(params: Record<string, unknown> | undefined, key: string, dflt = ''): string {
  const v = params?.[key]
  return v === undefined || v === null ? dflt : encodeURIComponent(String(v))
}

/*
 * Translate one action into a request against a repo.
 *
 * repoId replaces the old repoPath: repos are registered with the daemon and
 * addressed by id, so a repo can move on disk without the app losing track
 * of it.
 */
export function mapAction(
  repoId: number,
  action: string,
  params?: Record<string, unknown>
): Endpoint {
  const removed = REMOVED_ACTIONS[action]
  if (removed) throw new UnsupportedActionError(action, removed)

  const base = `/repos/${repoId}`

  switch (action) {
    case 'list':
      return { method: 'GET', path: `${base}/snapshots?limit=${q(params, 'limit', '200')}` }

    case 'snap_header':
      return { method: 'GET', path: `${base}/snapshots?limit=200` }

    case 'snap_dir_children':
    case 'ls':
      return {
        method: 'GET',
        path: `${base}/snapshots/${q(params, 'id', '1')}/tree` +
              `?parent=${q(params, 'parent_node', '0')}` +
              `&limit=${q(params, 'limit', '500')}` +
              `&offset=${q(params, 'offset', '0')}`,
      }

    case 'search':
      return { method: 'GET', path: `${base}/search?q=${q(params, 'query')}` +
                                    `&limit=${q(params, 'max_results', '200')}` }

    case 'tags':
      return { method: 'GET', path: `${base}/tags` }
    case 'tag_set':
      return {
        method: 'POST',
        path: `${base}/tags`,
        body: { name: params?.name, snap_id: Number(params?.id ?? 0) },
      }
    case 'tag_delete':
      return { method: 'DELETE', path: `${base}/tags/${q(params, 'name')}` }

    case 'note_get':
      return { method: 'GET', path: `${base}/snapshots/${q(params, 'id')}/note` }
    case 'note_set':
      return {
        method: 'PUT',
        path: `${base}/snapshots/${q(params, 'id')}/note`,
        body: { body: params?.body },
      }

    case 'policy':
      return { method: 'GET', path: `${base}/policy` }
    case 'save_policy':
      return {
        method: 'PUT',
        path: `${base}/policy`,
        body: { key: params?.key, value: params?.value },
      }

    case 'journal':
      return { method: 'GET', path: `${base}/journal?limit=${q(params, 'limit', '100')}` }

    case 'task_start':
      /* Long work is a job now: the response carries a job id and progress is
       * polled, rather than a detached process writing a status file. */
      return {
        method: 'POST',
        path: `${base}/${q(params, 'command', 'backup')}`,
        body: params?.args ?? {},
      }
    case 'task_list':
      return { method: 'GET', path: `/jobs` }
    case 'task_status':
      return { method: 'GET', path: `/jobs/${q(params, 'task_id')}` }
    case 'task_cancel':
      return { method: 'POST', path: `/jobs/${q(params, 'task_id')}/cancel` }

    case 'repo_summary':
    case 'repo_stats':
    case 'stats':
    case 'loose_stats':
      /* No aggregate endpoint yet; the snapshot list carries the counts the
       * dashboard actually renders. */
      return { method: 'GET', path: `${base}/snapshots?limit=1` }

    default:
      throw new UnsupportedActionError(action, 'no endpoint is mapped for it')
  }
}
