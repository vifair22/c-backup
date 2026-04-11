import React, { useCallback, useEffect, useRef, useState } from 'react'
import { fmtSize, fmtNum, fmtMode, absoluteTime } from './format'
import { ContextMenu, type ContextMenuItem } from './ContextMenu'

const api = window.cbackup

const DIFF_ROW_CAP = 5000
const CONTENT_MAX_BYTES = 2 * 1024 * 1024 // 2 MiB

interface Snapshot {
  id: number
  created_sec: number
  node_count: number
}

interface SnapList {
  head: number
  snapshots: Snapshot[]
}

interface DiffNode {
  mode: number
  uid: number
  gid: number
  size: number
  mtime_sec: number
  content_hash: string
}

interface DiffChange {
  change: 'A' | 'D' | 'M' | 'm'
  path: string
  old_node?: DiffNode
  new_node?: DiffNode
}

interface DiffResponse {
  changes: DiffChange[]
  count: number
}

interface ObjectContent {
  type: number
  size: number
  truncated: boolean
  content_base64: string
}

type ChangeType = 'A' | 'D' | 'M' | 'm'
type SortKey = 'change' | 'path' | 'old_size' | 'new_size' | 'delta'
type SortDir = 'asc' | 'desc'

const CHANGE_LABELS: Record<ChangeType, { label: string; badge: string; color: string }> = {
  A: { label: 'Added', badge: 'A', color: 'bg-green-600 text-white' },
  D: { label: 'Deleted', badge: 'D', color: 'bg-red-600 text-white' },
  M: { label: 'Modified', badge: 'M', color: 'bg-amber-600 text-white' },
  m: { label: 'Metadata', badge: 'm', color: 'bg-blue-600 text-white' },
}

const ROW_TINT: Record<ChangeType, string> = {
  A: 'bg-green-500/5',
  D: 'bg-red-500/5',
  M: '',
  m: 'bg-blue-500/5',
}

function sizeDelta(c: DiffChange): number | null {
  if (c.change === 'A') return c.new_node?.size ?? null
  if (c.change === 'D') return c.old_node ? -(c.old_node.size) : null
  if (c.old_node && c.new_node) return c.new_node.size - c.old_node.size
  return null
}

function fmtDelta(delta: number): string {
  if (delta === 0) return '0 B'
  const sign = delta > 0 ? '+' : ''
  return sign + fmtSize(Math.abs(delta))
}

function metaDiffLines(old_node: DiffNode, new_node: DiffNode): string[] {
  const lines: string[] = []
  if (old_node.mode !== new_node.mode) lines.push(`mode: ${fmtMode(old_node.mode)} \u2192 ${fmtMode(new_node.mode)}`)
  if (old_node.uid !== new_node.uid) lines.push(`uid: ${old_node.uid} \u2192 ${new_node.uid}`)
  if (old_node.gid !== new_node.gid) lines.push(`gid: ${old_node.gid} \u2192 ${new_node.gid}`)
  if (old_node.mtime_sec !== new_node.mtime_sec) lines.push(`mtime: ${absoluteTime(old_node.mtime_sec)} \u2192 ${absoluteTime(new_node.mtime_sec)}`)
  if (old_node.size !== new_node.size) lines.push(`size: ${fmtSize(old_node.size)} \u2192 ${fmtSize(new_node.size)}`)
  return lines.length > 0 ? lines : ['(no visible metadata differences)']
}

interface FileDiffData {
  path: string
  change: ChangeType
  oldText: string | null
  newText: string | null
}

interface Props {
  connName: string
  repoPath: string
  initialSnapA?: number
  initialSnapB?: number
  onBack: () => void
  onNavigateToFile?: (snapId: number, path: string) => void
}

export function SnapshotDiff({ connName, repoPath, initialSnapA, initialSnapB, onBack, onNavigateToFile }: Props): React.ReactElement {
  const [snapList, setSnapList] = useState<SnapList | null>(null)
  const [snapA, setSnapA] = useState<number | null>(initialSnapA ?? null)
  const [snapB, setSnapB] = useState<number | null>(initialSnapB ?? null)
  const [changes, setChanges] = useState<DiffChange[] | null>(null)
  const [totalCount, setTotalCount] = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [filter, setFilter] = useState<Set<ChangeType>>(new Set(['A', 'D', 'M', 'm']))
  const [sortKey, setSortKey] = useState<SortKey>('path')
  const [sortDir, setSortDir] = useState<SortDir>('asc')
  const [initLoading, setInitLoading] = useState(true)

  // Context menu
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; items: ContextMenuItem[] } | null>(null)

  // Metadata modal
  const [metaModal, setMetaModal] = useState<{ path: string; lines: string[] } | null>(null)

  // File diff modal
  const [fileDiff, setFileDiff] = useState<FileDiffData | null>(null)
  const [diffLoading, setDiffLoading] = useState(false)

  // Synced scroll refs for side-by-side diff
  const leftRef = useRef<HTMLDivElement>(null)
  const rightRef = useRef<HTMLDivElement>(null)
  const syncing = useRef(false)
  const syncScroll = (source: 'left' | 'right') => {
    if (syncing.current) return
    syncing.current = true
    const src = source === 'left' ? leftRef.current : rightRef.current
    const dst = source === 'left' ? rightRef.current : leftRef.current
    if (src && dst) {
      dst.scrollTop = src.scrollTop
      dst.scrollLeft = src.scrollLeft
    }
    syncing.current = false
  }

  // Loading indicator for content fetch
  const [fetchingPath, setFetchingPath] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    api.rpcCall<SnapList>(connName, repoPath, 'list')
      .then(data => {
        if (cancelled) return
        setSnapList(data)
        const snaps = data.snapshots
        if (snaps.length >= 2) {
          if (!snapA) setSnapA(snaps[snaps.length - 2].id)
          if (!snapB) setSnapB(snaps[snaps.length - 1].id)
        }
      })
      .catch(err => { if (!cancelled) setError(String(err)) })
      .finally(() => { if (!cancelled) setInitLoading(false) })
    return () => { cancelled = true }
  }, [connName, repoPath])

  const runDiff = useCallback(async () => {
    if (!snapA || !snapB || snapA === snapB) return
    setLoading(true)
    setError(null)
    setChanges(null)
    try {
      const resp = await api.rpcCall<DiffResponse>(connName, repoPath, 'diff', { id1: snapA, id2: snapB })
      setChanges(resp.changes.slice(0, DIFF_ROW_CAP))
      setTotalCount(resp.count)
    } catch (err) {
      setError(String(err))
    }
    setLoading(false)
  }, [connName, repoPath, snapA, snapB])

  useEffect(() => {
    if (snapA && snapB && !changes && !loading && !initLoading) {
      runDiff()
    }
  }, [snapA, snapB, initLoading])

  const toggleFilter = (type: ChangeType) => {
    setFilter(prev => {
      const next = new Set(prev)
      if (next.has(type)) next.delete(type)
      else next.add(type)
      return next
    })
  }

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setSortKey(key); setSortDir('asc') }
  }

  const sortIndicator = (key: SortKey) => {
    if (sortKey !== key) return ''
    return sortDir === 'asc' ? ' \u25B2' : ' \u25BC'
  }

  // Click row handler
  const handleRowClick = async (change: DiffChange) => {
    // Metadata-only: in-app modal
    if (change.change === 'm' && change.old_node && change.new_node) {
      setMetaModal({ path: change.path, lines: metaDiffLines(change.old_node, change.new_node) })
      return
    }

    // Content change: in-app diff modal
    const oldSize = change.old_node?.size ?? 0
    const newSize = change.new_node?.size ?? 0
    if (oldSize > CONTENT_MAX_BYTES || newSize > CONTENT_MAX_BYTES) {
      setMetaModal({ path: change.path, lines: [`File too large for diff view (${fmtSize(Math.max(oldSize, newSize))})`] })
      return
    }

    setFetchingPath(change.path)
    setDiffLoading(true)
    setFileDiff({ path: change.path, change: change.change, oldText: null, newText: null })

    try {
      const zeroHash = '0'.repeat(64)
      const [oldContent, newContent] = await Promise.all([
        change.old_node?.content_hash && change.old_node.content_hash !== zeroHash
          ? api.rpcCall<ObjectContent>(connName, repoPath, 'object_content', { hash: change.old_node.content_hash, max_bytes: CONTENT_MAX_BYTES })
          : null,
        change.new_node?.content_hash && change.new_node.content_hash !== zeroHash
          ? api.rpcCall<ObjectContent>(connName, repoPath, 'object_content', { hash: change.new_node.content_hash, max_bytes: CONTENT_MAX_BYTES })
          : null,
      ])

      const decode = (c: ObjectContent | null): string | null => {
        if (!c) return null
        try { return atob(c.content_base64) } catch { return null }
      }

      const oldText = decode(oldContent)
      const newText = decode(newContent)

      const isBinary = (s: string | null) => s !== null && /[\x00-\x08\x0e-\x1f]/.test(s.slice(0, 4096))
      if (isBinary(oldText) || isBinary(newText)) {
        setFileDiff(null)
        setMetaModal({ path: change.path, lines: ['Binary file \u2014 cannot display diff'] })
      } else {
        setFileDiff({ path: change.path, change: change.change, oldText, newText })
      }
    } catch (err) {
      setFileDiff(null)
      setMetaModal({ path: change.path, lines: [`Error loading content: ${err}`] })
    }
    setDiffLoading(false)
    setFetchingPath(null)
  }

  // Row right-click
  const showRowMenu = (e: React.MouseEvent, change: DiffChange) => {
    e.preventDefault()
    const targetSnap = change.change === 'D' ? snapA : snapB
    const items: ContextMenuItem[] = []
    if (onNavigateToFile && targetSnap) {
      items.push({ label: 'Show in snapshot browser', onClick: () => onNavigateToFile(targetSnap, change.path) })
    }
    if (items.length > 0) setContextMenu({ x: e.clientX, y: e.clientY, items })
  }

  // Filter and sort
  const filtered = changes?.filter(c => filter.has(c.change)) ?? []
  const sorted = [...filtered].sort((a, b) => {
    let av: string | number, bv: string | number
    switch (sortKey) {
      case 'change': av = a.change; bv = b.change; break
      case 'path': av = a.path; bv = b.path; break
      case 'old_size': av = a.old_node?.size ?? -1; bv = b.old_node?.size ?? -1; break
      case 'new_size': av = a.new_node?.size ?? -1; bv = b.new_node?.size ?? -1; break
      case 'delta': av = sizeDelta(a) ?? 0; bv = sizeDelta(b) ?? 0; break
    }
    if (typeof av === 'string') {
      const cmp = av.localeCompare(bv as string)
      return sortDir === 'asc' ? cmp : -cmp
    }
    return sortDir === 'asc' ? (av as number) - (bv as number) : (bv as number) - (av as number)
  })

  const counts = {
    A: changes?.filter(c => c.change === 'A').length ?? 0,
    D: changes?.filter(c => c.change === 'D').length ?? 0,
    M: changes?.filter(c => c.change === 'M').length ?? 0,
    m: changes?.filter(c => c.change === 'm').length ?? 0,
  }

  const snaps = snapList?.snapshots ?? []

  return (
    <div>
      {/* Header */}
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Back
        </button>
        <h3 className="text-lg font-medium m-0">Compare Snapshots</h3>
      </div>

      {initLoading && <div className="text-text-muted text-sm">Loading...</div>}
      {error && <div className="text-status-error text-sm mb-3">{error}</div>}

      {snapList && (
        <>
          {/* Selectors */}
          <div className="flex flex-wrap items-center gap-3 mb-4 bg-surface-secondary border border-border-default rounded-lg px-4 py-3">
            <div className="flex items-center gap-2">
              <label className="text-xs text-text-muted whitespace-nowrap">Snap A:</label>
              <select value={snapA ?? ''} onChange={e => setSnapA(Number(e.target.value))}
                className="text-xs px-2 py-1 border border-border-default rounded bg-surface-primary text-text-primary focus:outline-none focus:border-accent min-w-0">
                <option value="">Select...</option>
                {snaps.map(s => (
                  <option key={s.id} value={s.id}>#{s.id} — {absoluteTime(s.created_sec)}</option>
                ))}
              </select>
            </div>
            <div className="flex items-center gap-2">
              <label className="text-xs text-text-muted whitespace-nowrap">Snap B:</label>
              <select value={snapB ?? ''} onChange={e => setSnapB(Number(e.target.value))}
                className="text-xs px-2 py-1 border border-border-default rounded bg-surface-primary text-text-primary focus:outline-none focus:border-accent min-w-0">
                <option value="">Select...</option>
                {snaps.map(s => (
                  <option key={s.id} value={s.id}>#{s.id} — {absoluteTime(s.created_sec)}</option>
                ))}
              </select>
            </div>
            <button onClick={runDiff}
              disabled={!snapA || !snapB || snapA === snapB || loading}
              className="text-xs px-3 py-1 rounded bg-accent text-accent-text hover:bg-accent-hover border-none cursor-pointer disabled:opacity-50 disabled:cursor-default">
              {loading ? 'Comparing...' : 'Compare'}
            </button>
            {snapA === snapB && snapA !== null && (
              <span className="text-xs text-status-warning">Same snapshot selected</span>
            )}
          </div>

          {/* Summary + filters */}
          {changes && (
            <div className="flex flex-wrap items-center gap-2 mb-3">
              {(['A', 'D', 'M', 'm'] as ChangeType[]).map(type => {
                const info = CHANGE_LABELS[type]
                const active = filter.has(type)
                const count = counts[type]
                return (
                  <button key={type} onClick={() => toggleFilter(type)}
                    className={`text-[11px] px-2 py-0.5 rounded cursor-pointer border-none flex items-center gap-1.5 ${
                      active ? 'bg-surface-tertiary text-text-primary' : 'bg-surface-tertiary/50 text-text-muted line-through'
                    }`}>
                    <span className={`inline-block px-1 rounded text-[10px] font-semibold ${info.color}`}>{info.badge}</span>
                    {info.label} <span className="font-semibold">{fmtNum(count)}</span>
                  </button>
                )
              })}
              {totalCount > DIFF_ROW_CAP && (
                <span className="text-xs text-status-warning">
                  Showing {fmtNum(DIFF_ROW_CAP)} of {fmtNum(totalCount)} changes
                </span>
              )}
            </div>
          )}

          {/* Change table */}
          {changes && sorted.length > 0 && (
            <div className="bg-surface-secondary border border-border-default rounded-lg overflow-hidden">
              <table className="w-full text-xs table-fixed">
                <thead>
                  <tr className="text-text-muted text-left">
                    <th className="font-medium pb-2 pt-4 pl-4 w-14 cursor-pointer" onClick={() => toggleSort('change')}>
                      Status{sortIndicator('change')}
                    </th>
                    <th className="font-medium pb-2 pt-4 cursor-pointer" onClick={() => toggleSort('path')}>
                      Path{sortIndicator('path')}
                    </th>
                    <th className="font-medium pb-2 pt-4 text-right w-[72px] cursor-pointer" onClick={() => toggleSort('old_size')}>
                      Old{sortIndicator('old_size')}
                    </th>
                    <th className="font-medium pb-2 pt-4 text-right w-[72px] cursor-pointer" onClick={() => toggleSort('new_size')}>
                      New{sortIndicator('new_size')}
                    </th>
                    <th className="font-medium pb-2 pt-4 text-right w-[72px] pr-4 cursor-pointer" onClick={() => toggleSort('delta')}>
                      Delta{sortIndicator('delta')}
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {sorted.map((c, i) => {
                    const info = CHANGE_LABELS[c.change]
                    const delta = sizeDelta(c)
                    const isFetching = fetchingPath === c.path
                    return (
                      <tr key={i}
                        onClick={() => !isFetching && handleRowClick(c)}
                        onContextMenu={e => showRowMenu(e, c)}
                        className={`border-t border-border-default cursor-pointer hover:bg-surface-hover ${ROW_TINT[c.change]}`}>
                        <td className="py-1.5 pl-4">
                          <span className={`inline-block px-1.5 py-0 rounded text-[10px] font-semibold ${info.color}`}>
                            {info.badge}
                          </span>
                        </td>
                        <td className="py-1.5 font-mono truncate" title={c.path}>
                          {c.path}
                          {isFetching && <span className="text-text-muted text-[10px] ml-2">loading...</span>}
                          {c.change === 'm' && c.old_node && c.new_node && (
                            <span className="text-text-muted text-[10px] ml-2">
                              {metaDiffLines(c.old_node, c.new_node).length} field(s)
                            </span>
                          )}
                        </td>
                        <td className="py-1.5 text-right text-text-muted">
                          {c.old_node ? fmtSize(c.old_node.size) : ''}
                        </td>
                        <td className="py-1.5 text-right text-text-muted">
                          {c.new_node ? fmtSize(c.new_node.size) : ''}
                        </td>
                        <td className={`py-1.5 text-right pr-4 ${
                          delta !== null && delta > 0 ? 'text-red-400' : delta !== null && delta < 0 ? 'text-green-400' : 'text-text-muted'
                        }`}>
                          {delta !== null ? fmtDelta(delta) : ''}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}

          {changes && sorted.length === 0 && !loading && (
            <div className="text-text-muted text-xs text-center py-8">
              {changes.length === 0 ? 'No changes between these snapshots.' : 'No changes match the current filter.'}
            </div>
          )}
        </>
      )}

      {/* Context menu */}
      {contextMenu && (
        <ContextMenu x={contextMenu.x} y={contextMenu.y} items={contextMenu.items}
          onClose={() => setContextMenu(null)} />
      )}

      {/* Metadata modal */}
      {metaModal && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
          <div className="bg-surface-primary rounded-lg p-5 w-[480px] max-h-[60vh] shadow-xl border border-border-default flex flex-col">
            <h3 className="text-sm font-semibold m-0 mb-1">{metaModal.path.split('/').pop()}</h3>
            <div className="text-[11px] text-text-muted mb-3 font-mono">{metaModal.path}</div>
            <div className="flex-1 overflow-auto">
              {metaModal.lines.map((line, i) => (
                <div key={i} className="text-xs text-text-secondary py-1 font-mono border-b border-border-default last:border-0">
                  {line}
                </div>
              ))}
            </div>
            <div className="flex justify-end mt-3">
              <button onClick={() => setMetaModal(null)}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none">
                Close
              </button>
            </div>
          </div>
        </div>
      )}

      {/* File diff modal */}
      {fileDiff && (
        <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50">
          <div className="bg-surface-primary rounded-lg shadow-xl border border-border-default flex flex-col"
            style={{ width: 'min(95vw, 1000px)', height: 'min(85vh, 700px)' }}>
            {/* Header */}
            <div className="flex items-center justify-between px-4 py-3 border-b border-border-default shrink-0">
              <div className="min-w-0">
                <div className="text-sm font-semibold truncate">
                  {fileDiff.path.split('/').pop()}
                  <span className={`ml-2 inline-block px-1.5 py-0 rounded text-[10px] font-semibold ${CHANGE_LABELS[fileDiff.change].color}`}>
                    {CHANGE_LABELS[fileDiff.change].label}
                  </span>
                </div>
                <div className="text-[10px] text-text-muted font-mono truncate">{fileDiff.path}</div>
              </div>
              <button onClick={() => setFileDiff(null)}
                className="text-text-muted hover:text-text-primary bg-transparent border-none cursor-pointer text-lg px-2 shrink-0">
                &times;
              </button>
            </div>

            {/* Diff content */}
            <div className="flex-1 overflow-auto min-h-0">
              {diffLoading ? (
                <div className="flex items-center justify-center h-full text-text-muted text-sm">Loading diff...</div>
              ) : fileDiff.change === 'M' && fileDiff.oldText !== null && fileDiff.newText !== null ? (
                /* Side by side */
                <div className="grid grid-cols-2 h-full">
                  <div ref={leftRef} onScroll={() => syncScroll('left')} className="border-r border-border-default overflow-auto">
                    <div className="px-3 py-1.5 bg-surface-secondary border-b border-border-default text-[10px] text-red-400 font-semibold sticky top-0">
                      Old ({fileDiff.oldText.split('\n').length} lines)
                    </div>
                    {fileDiff.oldText.split('\n').slice(0, 5000).map((line, i) => {
                      const newLines = fileDiff.newText!.split('\n')
                      const changed = i >= newLines.length || line !== newLines[i]
                      return (
                        <div key={i} className={`flex text-[11px] font-mono leading-relaxed ${changed ? 'bg-red-500/10 text-red-400' : 'text-text-primary'}`}>
                          <span className="w-10 shrink-0 text-right pr-2 text-text-muted select-none border-r border-border-default">{i + 1}</span>
                          <pre className="m-0 px-2 whitespace-pre-wrap break-all">{line}</pre>
                        </div>
                      )
                    })}
                  </div>
                  <div ref={rightRef} onScroll={() => syncScroll('right')} className="overflow-auto">
                    <div className="px-3 py-1.5 bg-surface-secondary border-b border-border-default text-[10px] text-green-400 font-semibold sticky top-0">
                      New ({fileDiff.newText.split('\n').length} lines)
                    </div>
                    {fileDiff.newText.split('\n').slice(0, 5000).map((line, i) => {
                      const oldLines = fileDiff.oldText!.split('\n')
                      const changed = i >= oldLines.length || line !== oldLines[i]
                      return (
                        <div key={i} className={`flex text-[11px] font-mono leading-relaxed ${changed ? 'bg-green-500/10 text-green-400' : 'text-text-primary'}`}>
                          <span className="w-10 shrink-0 text-right pr-2 text-text-muted select-none border-r border-border-default">{i + 1}</span>
                          <pre className="m-0 px-2 whitespace-pre-wrap break-all">{line}</pre>
                        </div>
                      )
                    })}
                  </div>
                </div>
              ) : (
                /* Single side (added or deleted) */
                <div className="overflow-auto h-full">
                  {(() => {
                    const text = fileDiff.change === 'D' ? fileDiff.oldText : fileDiff.newText
                    const isDelete = fileDiff.change === 'D'
                    const lines = text?.split('\n') ?? []
                    const prefix = isDelete ? '-' : '+'
                    const lineClass = isDelete ? 'bg-red-500/10 text-red-400' : 'bg-green-500/10 text-green-400'
                    return lines.slice(0, 5000).map((line, i) => (
                      <div key={i} className={`flex text-[11px] font-mono leading-relaxed ${lineClass}`}>
                        <span className="w-10 shrink-0 text-right pr-2 text-text-muted select-none border-r border-border-default">{i + 1}</span>
                        <pre className="m-0 px-2 whitespace-pre-wrap break-all">{prefix} {line}</pre>
                      </div>
                    ))
                  })()}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
