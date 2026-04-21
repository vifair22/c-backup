import React, { useCallback, useEffect, useState } from 'react'
import { fmtSize, fmtNum, absoluteTime, NODE_TYPE_NAMES } from './format'
import { ContextMenu, type ContextMenuItem } from './ContextMenu'

const api = window.cbackup

const RESULT_CAP = 5000

interface Snapshot {
  id: number
  created_sec: number
}

interface SnapList {
  head: number
  snapshots: Snapshot[]
}

interface SearchNode {
  node_id: number
  type: number
  size: number
  mode: number
  content_hash: string
}

interface SearchResult {
  snap_id: number
  path: string
  node: SearchNode
}

interface SearchResponse {
  results: SearchResult[]
  count: number
  truncated: boolean
}

interface Props {
  connName: string
  repoPath: string
  onNavigateToFile?: (snapId: number, path: string) => void
  onBack: () => void
}

export function FileSearch({ connName, repoPath, onNavigateToFile, onBack }: Props): React.ReactElement {
  const [query, setQuery] = useState('')
  const [allSnapshots, setAllSnapshots] = useState(false)
  const [results, setResults] = useState<SearchResult[] | null>(null)
  const [truncated, setTruncated] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [snapList, setSnapList] = useState<SnapList | null>(null)
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; items: ContextMenuItem[] } | null>(null)

  // Load snapshot list for created_sec lookup
  useEffect(() => {
    api.rpcCall<SnapList>(connName, repoPath, 'list')
      .then(setSnapList)
      .catch(() => {})
  }, [connName, repoPath])

  const snapById = new Map(snapList?.snapshots.map(s => [s.id, s]) ?? [])
  const headId = snapList?.snapshots.length ? snapList.snapshots[snapList.snapshots.length - 1].id : null

  const runSearch = useCallback(async () => {
    const q = query.trim()
    if (!q) return
    setLoading(true)
    setError(null)
    setResults(null)
    try {
      const params: Record<string, unknown> = { query: q, max_results: RESULT_CAP }
      if (!allSnapshots && headId !== null) params.id = headId
      const resp = await api.rpcCall<SearchResponse>(connName, repoPath, 'search', params)
      setResults(resp.results)
      setTruncated(resp.truncated)
    } catch (err) {
      setError(String(err))
    }
    setLoading(false)
  }, [connName, repoPath, query, allSnapshots, headId])

  const showRowMenu = (e: React.MouseEvent, r: SearchResult) => {
    e.preventDefault()
    if (!onNavigateToFile) return
    setContextMenu({
      x: e.clientX, y: e.clientY,
      items: [{ label: 'Show in snapshot browser', onClick: () => onNavigateToFile(r.snap_id, r.path) }]
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
        <label className="text-xs flex items-center gap-1.5 cursor-pointer text-text-secondary shrink-0">
          <input type="checkbox" checked={allSnapshots} onChange={e => setAllSnapshots(e.target.checked)} />
          All snapshots
        </label>
      </div>

      {error && <div className="text-status-error text-sm mb-3">{error}</div>}

      {/* Results */}
      {results && (
        <>
          <div className="flex items-center gap-2 mb-3">
            <span className="text-xs text-text-muted">
              {fmtNum(results.length)} result{results.length !== 1 ? 's' : ''}
              {truncated && ` (capped at ${fmtNum(RESULT_CAP)})`}
            </span>
          </div>

          {results.length > 0 ? (
            <div className="bg-surface-secondary border border-border-default rounded-lg overflow-hidden">
              <table className="w-full text-xs table-fixed">
                <thead>
                  <tr className="text-text-muted text-left">
                    {allSnapshots && <th className="font-medium pb-2 pt-4 pl-4 w-14">Snap</th>}
                    {allSnapshots && <th className="font-medium pb-2 pt-4 w-36">Date</th>}
                    <th className={`font-medium pb-2 pt-4 ${allSnapshots ? '' : 'pl-4'}`}>Path</th>
                    <th className="font-medium pb-2 pt-4 w-16">Type</th>
                    <th className="font-medium pb-2 pt-4 text-right w-20">Size</th>
                    <th className="font-medium pb-2 pt-4 text-right w-32 pr-4">Hash</th>
                  </tr>
                </thead>
                <tbody>
                  {results.map((r, i) => {
                    const snap = snapById.get(r.snap_id)
                    return (
                      <tr key={i}
                        onClick={() => onNavigateToFile?.(r.snap_id, r.path)}
                        onContextMenu={e => showRowMenu(e, r)}
                        className="border-t border-border-default cursor-pointer hover:bg-surface-hover text-text-primary">
                        {allSnapshots && <td className="py-1.5 pl-4 font-mono">#{r.snap_id}</td>}
                        {allSnapshots && <td className="py-1.5 text-text-muted">{snap ? absoluteTime(snap.created_sec) : ''}</td>}
                        <td className={`py-1.5 font-mono truncate ${allSnapshots ? '' : 'pl-4'}`} title={r.path}>{r.path}</td>
                        <td className="py-1.5 text-text-muted">{NODE_TYPE_NAMES[r.node.type] ?? `${r.node.type}`}</td>
                        <td className="py-1.5 text-right">{fmtSize(r.node.size)}</td>
                        <td className="py-1.5 text-right pr-4 font-mono text-text-muted truncate" title={r.node.content_hash}>
                          {r.node.content_hash?.slice(0, 12)}...
                        </td>
                      </tr>
                    )
                  })}
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
