/*
 * Walking a snapshot by path when the node ids in hand have gone stale.
 *
 * A node id is a handle into the daemon's index, and a reindex of the
 * snapshot numbers its rows afresh: after one, an id the browser kept can
 * name a different directory of the same snapshot. The daemon labels every
 * directory page with the snapshot's index generation, and answers a node id
 * sent back with a stale generation with a 409 instead of the wrong listing
 * (docs/api.md, "Node ids and index generations"). The path is the stable
 * name, so on that 409 the browser walks the path it was showing again from
 * the root, one component at a time, and carries on with the ids and the
 * generation that walk gives it.
 *
 * Kept free of React and of the IPC bridge so tests/browse.test.ts can drive
 * it against a fake daemon under node:test. No imports other than types,
 * because the renderer resolves modules through Vite and the tests through
 * Node, and the two disagree about extensions.
 */

/* The daemon's message for a stale generation, exactly as it sends it. The
 * IPC bridge carries the error's message and not its status, so this is how
 * the 409 is recognised. */
export const INDEX_REBUILT = 'index rebuilt; re-resolve by path'

export function isIndexRebuilt(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err)
  return msg.includes(INDEX_REBUILT)
}

export interface DirEntry {
  id: number
  name: string
  type: number
}

export interface DirPage<E extends DirEntry> {
  entries: E[]
  /* undefined from a daemon that predates generations. */
  gen?: number
}

/* One page of a directory: parent 0 is the root, gen undefined sends none. */
export type ListDir<E extends DirEntry> =
  (parent: number, gen: number | undefined, offset: number) => Promise<DirPage<E>>

export interface ResolvedStep<E extends DirEntry> {
  nodeId: number
  name: string
  /* The step's own first page, read when it is a directory to expand. */
  entries: E[] | null
}

export interface Resolved<E extends DirEntry> {
  gen?: number
  root: E[]
  /* As far as the path resolved; shorter than the path when a component is
   * not in this snapshot any more, which is an answer, not an error. */
  steps: ResolvedStep<E>[]
}

const NODE_DIR = 2

/*
 * Resolve path components from the root under the current generation.
 *
 * The root is read without a generation, which is what makes it the fresh
 * start; every later read passes the one the root reported, so a rebuild
 * that lands part way through the walk is a 409 here too, and the walk
 * starts again rather than mixing two numberings. A component past the
 * first page of its directory is found by reading further pages: the
 * directory is paged by the daemon and a name may sit anywhere in it.
 *
 * expandLast reads the last component's first page when it is a directory,
 * which is what a browser opening that directory wants.
 */
export async function resolvePath<E extends DirEntry>(
  list: ListDir<E>, segments: string[], pageSize: number,
  expandLast = true, attempts = 3,
): Promise<Resolved<E>> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await walk(list, segments, pageSize, expandLast)
    } catch (err) {
      if (!isIndexRebuilt(err) || attempt >= attempts) throw err
    }
  }
}

async function walk<E extends DirEntry>(
  list: ListDir<E>, segments: string[], pageSize: number, expandLast: boolean,
): Promise<Resolved<E>> {
  const first = await list(0, undefined, 0)
  const gen = first.gen
  const out: Resolved<E> = { gen, root: first.entries, steps: [] }

  let parent = 0
  let page = first.entries
  for (let i = 0; i < segments.length; i++) {
    let match = page.find(e => e.name === segments[i])
    /* Not on the first page: read on until it turns up or the directory
     * ends, which a short page says. */
    if (!match && pageSize > 0 && page.length >= pageSize) {
      for (let offset = page.length; ;) {
        const more = await list(parent, gen, offset)
        match = more.entries.find(e => e.name === segments[i])
        if (match || more.entries.length < pageSize) break
        offset += more.entries.length
      }
    }
    if (!match) break

    const last = i === segments.length - 1
    let entries: E[] | null = null
    if (match.type === NODE_DIR && (!last || expandLast)) {
      entries = (await list(match.id, gen, 0)).entries
    }
    out.steps.push({ nodeId: match.id, name: match.name, entries })
    if (match.type !== NODE_DIR || entries === null) break
    parent = match.id
    page = entries
  }
  return out
}

/* A path as the browser shows it, "/a/b", into components. */
export function pathSegments(path: string): string[] {
  return path.split('/').filter(Boolean)
}
