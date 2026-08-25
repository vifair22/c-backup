/*
 * Finding a file by name, across every snapshot.
 *
 * Migrated off the action bridge. It had been reading `r.node.size` and
 * `r.node.content_hash` from a response whose results are flat, and a
 * `truncated` flag the route has never sent, so every row rendered its type,
 * size and hash as blanks.
 *
 * The name index is a trigram index over the whole repository rather than
 * one snapshot, so this searches all of them by construction - the old "All
 * snapshots" toggle chose between two behaviours the daemon does not
 * distinguish, and is gone rather than left there doing nothing.
 */

import React, { useCallback, useState } from 'react'
import { fmtSize, fmtNum, NODE_TYPE_NAMES } from './format'
import { ContextMenu, type ContextMenuItem } from './ContextMenu'

const api = window.cbackup

/* The route caps a page at 1000, so asking for more silently gets 1000. */
const RESULT_CAP = 1000

/* Every integer arrives as a string. */
interface RawResult {
  node_id: string
  snap_id: string
  name: string
  path?: string
  type?: string
  size?: string
  hash?: string
}

interface SearchResult {
  nodeId: number
  snapId: number
  name: string
  path: string
  type: number
  size: number
  hash: string
}

const parse = (r: RawResult): SearchResult => ({
  nodeId: Number(r.node_id),
  snapId: Number(r.snap_id),
  name: r.name,
  path: r.path ?? '',
  type: Number(r.type ?? '0'),
  size: Number(r.size ?? '0'),
  hash: r.hash ?? '',
})

interface Props {
  connName: string
  repoPath: string
  onNavigateToFile?: (snapId: number, path: string) => void
  onBack: () => void
}

export function FileSearch({ connName, repoPath, onNavigateToFile, onBack }: Props): React.ReactElement {
  const repoId = Number(repoPath)

  const [query, setQuery] = useState('')
  const [results, setResults] = useState<SearchResult[] | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; items: ContextMenuItem[] } | null>(null)

  const runSearch = useCallback(async () => {
    const q = query.trim()
    if (!q) return
    setLoading(true)
    setError(null)
    setResults(null)
    try {
      const resp = await api.apiCall<{ results?: RawResult[] }>(connName, {
        kind: 'search', repoId, query: q, limit: RESULT_CAP,
      })
      setResults((resp.results ?? []).map(parse))
    } catch (err) {
      /* The daemon refuses a query under three characters, because the index
       * is built from trigrams and an empty list would read as "no such
       * file". Its message says so, so it is shown as it arrived. */
      setError(String(err))
    }
    setLoading(false)
  }, [connName, repoId, query])

  const showRowMenu = (e: React.MouseEvent, r: SearchResult) => {
    e.preventDefault()
    if (!onNavigateToFile || !r.path) return
    setContextMenu({
      x: e.clientX, y: e.clientY,
      items: [{ label: 'Show in snapshot browser', onClick: () => onNavigateToFile(r.snapId, r.path) }]
    })
  }

  return (
    <div>
      {/* Header */}
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Dashboard
        </button>
        <h3 className="text-lg font-medium m-0">File Search</h3>
      </div>

      {/* Search bar */}
      <div className="flex flex-wrap items-center gap-3 mb-4 bg-surface-secondary border border-border-default rounded-lg px-4 py-3">
        <form onSubmit={e => { e.preventDefault(); runSearch() }} className="flex items-center gap-2 flex-1 min-w-0">
          <input value={query} onChange={e => setQuery(e.target.value)}
            placeholder="Filename contains..."
            className="flex-1 min-w-0 px-2 py-1 border border-border-default rounded text-sm bg-surface-primary text-text-primary focus:outline-none focus:border-accent font-mono"
            autoFocus />
          <button type="submit" disabled={!query.trim() || loading}
            className="text-xs px-3 py-1 rounded bg-accent text-accent-text hover:bg-accent-hover border-none cursor-pointer disabled:opacity-50 disabled:cursor-default shrink-0">
            {loading ? 'Searching...' : 'Search'}
          </button>
        </form>
        <span className="text-xs text-text-muted shrink-0">
          Searches every snapshot
        </span>
      </div>

      {error && <div className="text-status-error text-sm mb-3">{error}</div>}

      {/* Results */}
      {results && (
        <>
          <div className="flex items-center gap-2 mb-3">
            <span className="text-xs text-text-muted">
              {fmtNum(results.length)} result{results.length !== 1 ? 's' : ''}
              {/* The route caps a page rather than reporting that it did, so
                * a full page is the only evidence there may be more. */}
              {results.length >= RESULT_CAP && ` (first ${fmtNum(RESULT_CAP)}, narrow the search)`}
            </span>
          </div>

          {results.length > 0 ? (
            <div className="bg-surface-secondary border border-border-default rounded-lg overflow-hidden">
              <table className="w-full text-xs table-fixed">
                <thead>
                  <tr className="text-text-muted text-left">
                    <th className="font-medium pb-2 pt-4 pl-4 w-14">Snap</th>
                    <th className="font-medium pb-2 pt-4">Path</th>
                    <th className="font-medium pb-2 pt-4 w-16">Type</th>
                    <th className="font-medium pb-2 pt-4 text-right w-20">Size</th>
                    <th className="font-medium pb-2 pt-4 text-right w-32 pr-4">Hash</th>
                  </tr>
                </thead>
                <tbody>
                  {results.map((r, i) => (
                    <tr key={`${r.snapId}-${r.nodeId}-${i}`}
                      onClick={() => { if (r.path) onNavigateToFile?.(r.snapId, r.path) }}
                      onContextMenu={e => showRowMenu(e, r)}
                      className={`border-t border-border-default text-text-primary ${
                        r.path ? 'cursor-pointer hover:bg-surface-hover' : ''}`}>
                      <td className="py-1.5 pl-4 font-mono">#{r.snapId}</td>
                      <td className="py-1.5 font-mono truncate" title={r.path || r.name}>
                        {r.path || r.name}
                      </td>
                      <td className="py-1.5 text-text-muted">{NODE_TYPE_NAMES[r.type] ?? `${r.type}`}</td>
                      <td className="py-1.5 text-right">{fmtSize(r.size)}</td>
                      <td className="py-1.5 text-right pr-4 font-mono text-text-muted truncate" title={r.hash}>
                        {r.hash ? `${r.hash.slice(0, 12)}...` : '-'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-text-muted text-xs text-center py-8">No files found matching "{query}".</div>
          )}
        </>
      )}

      {/* Context menu */}
      {contextMenu && (
        <ContextMenu x={contextMenu.x} y={contextMenu.y} items={contextMenu.items}
          onClose={() => setContextMenu(null)} />
      )}
    </div>
  )
}
