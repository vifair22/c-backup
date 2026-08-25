/*
 * What changed between two snapshots.
 *
 * Migrated off the action bridge, which fed it a response of a shape that
 * has not existed since the rewrite. It expected single-letter change codes
 * and an old_node/new_node pair carrying mode, uid, gid, mtime and a content
 * hash; the daemon walks manifest trees and reports a change word, a path
 * and sizes. Every size column was blank, the filter buttons matched nothing
 * so the table was always empty, and opening a row to see the content diff
 * read a hash out of an object that was not there.
 *
 * The daemon now sends both hashes and both sizes, which is what makes the
 * side-by-side view possible at all - and that view is the reason to have
 * this screen rather than read `c-backup diff`.
 *
 * The metadata-only change type is gone. A tree entry is compared by hash,
 * so a permission change either alters the entry - in which case it is a
 * modification like any other - or it does not appear. There is no third
 * category to show.
 */

import React, { useCallback, useEffect, useRef, useState } from 'react'
import { fmtSize, fmtNum, absoluteTime } from './format'
import { ContextMenu, type ContextMenuItem } from './ContextMenu'

const api = window.cbackup

/* The daemon stops emitting rows at 2000 and says so, while still counting
 * the whole comparison. */
const DIFF_ROW_CAP = 2000
const CONTENT_MAX_BYTES = 2 * 1024 * 1024 // 2 MiB

interface RawSnapshot { id: string; ts: string; label?: string }
interface Snapshot { id: number; ts: number; label: string }

/* Every integer arrives as a string; a hash is empty for the side that does
 * not exist. */
interface RawChange {
  change: string
  path: string
  size?: string
  old_size?: string
  new_size?: string
  old_hash?: string
  new_hash?: string
}

type ChangeType = 'added' | 'removed' | 'modified'

interface DiffChange {
  change: ChangeType
  path: string
  oldSize: number
  newSize: number
  oldHash: string
  newHash: string
}

interface RawDiff {
  changes?: RawChange[]
  added?: string
  removed?: string
  modified?: string
  truncated?: string
}

interface ObjectContent {
  base64?: string
  too_large?: boolean
  size?: number
}

type SortKey = 'change' | 'path' | 'old_size' | 'new_size' | 'delta'
type SortDir = 'asc' | 'desc'

const CHANGE_LABELS: Record<ChangeType, { label: string; badge: string; color: string }> = {
  added:    { label: 'Added',    badge: 'A', color: 'bg-green-600 text-white' },
  removed:  { label: 'Removed',  badge: 'D', color: 'bg-red-600 text-white' },
  modified: { label: 'Modified', badge: 'M', color: 'bg-amber-600 text-white' },
}

const ROW_TINT: Record<ChangeType, string> = {
  added: 'bg-green-500/5',
  removed: 'bg-red-500/5',
  modified: '',
}

const parseChange = (c: RawChange): DiffChange => ({
  change: (c.change as ChangeType),
  path: c.path,
  oldSize: Number(c.old_size ?? '0'),
  newSize: Number(c.new_size ?? '0'),
  oldHash: c.old_hash ?? '',
  newHash: c.new_hash ?? '',
})

function sizeDelta(c: DiffChange): number | null {
  if (c.change === 'added') return c.newSize
  if (c.change === 'removed') return -c.oldSize
  return c.newSize - c.oldSize
}

function fmtDelta(delta: number): string {
  if (delta === 0) return '0 B'
  const sign = delta > 0 ? '+' : ''
  return sign + fmtSize(Math.abs(delta))
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
  const repoId = Number(repoPath)

  const [snaps, setSnaps] = useState<Snapshot[]>([])
  const [snapA, setSnapA] = useState<number | null>(initialSnapA ?? null)
  const [snapB, setSnapB] = useState<number | null>(initialSnapB ?? null)
  const [changes, setChanges] = useState<DiffChange[] | null>(null)
  const [totals, setTotals] = useState<{ added: number; removed: number; modified: number; truncated: boolean } | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [filter, setFilter] = useState<Set<ChangeType>>(new Set(['added', 'removed', 'modified']))
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
    api.apiCall<{ snapshots?: RawSnapshot[] }>(connName, { kind: 'snapshotList', repoId, limit: 200 })
      .then(data => {
        if (cancelled) return
        const list = (data.snapshots ?? []).map(x => ({
          id: Number(x.id), ts: Number(x.ts), label: x.label ?? '',
        }))
        setSnaps(list)
        /* Newest first, so the default comparison is the previous snapshot
         * against the newest one - which is what somebody opening this
         * almost always wants to see. */
        if (list.length >= 2) {
          if (!snapA) setSnapA(list[1].id)
          if (!snapB) setSnapB(list[0].id)
        }
      })
      .catch(err => { if (!cancelled) setError(String(err)) })
      .finally(() => { if (!cancelled) setInitLoading(false) })
    return () => { cancelled = true }
  }, [connName, repoId])

  const runDiff = useCallback(async () => {
    if (!snapA || !snapB || snapA === snapB) return
    setLoading(true)
    setError(null)
    setChanges(null)
    try {
      const resp = await api.apiCall<RawDiff>(connName, {
        kind: 'diff', repoId, from: snapA, to: snapB,
      })
      setChanges((resp.changes ?? []).map(parseChange))
      setTotals({
        added: Number(resp.added ?? '0'),
        removed: Number(resp.removed ?? '0'),
        modified: Number(resp.modified ?? '0'),
        truncated: resp.truncated === 'true',
      })
    } catch (err) {
      setError(String(err))
    }
    setLoading(false)
  }, [connName, repoId, snapA, snapB])

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
    if (change.oldSize > CONTENT_MAX_BYTES || change.newSize > CONTENT_MAX_BYTES) {
      setMetaModal({ path: change.path, lines: [
        `Too large to diff in the app (${fmtSize(Math.max(change.oldSize, change.newSize))}).`,
      ] })
      return
    }

    setFetchingPath(change.path)
    setDiffLoading(true)
    setFileDiff({ path: change.path, change: change.change, oldText: null, newText: null })

    try {
      const zeroHash = '0'.repeat(64)
      const fetchSide = (hash: string) =>
        hash && hash !== zeroHash
          ? api.apiCall<ObjectContent>(connName, { kind: 'objectContent', repoId, hash })
          : Promise.resolve(null)

      const [oldContent, newContent] = await Promise.all([
        fetchSide(change.oldHash),
        fetchSide(change.newHash),
      ])

      const decode = (c: ObjectContent | null): string | null => {
        if (!c || c.too_large || !c.base64) return null
        try { return atob(c.base64) } catch { return null }
      }

      const oldText = decode(oldContent)
      const newText = decode(newContent)

      const isBinary = (s: string | null) => s !== null && /[\x00-\x08\x0e-\x1f]/.test(s.slice(0, 4096))
      if (isBinary(oldText) || isBinary(newText)) {
        setFileDiff(null)
        setMetaModal({ path: change.path, lines: ['Binary file - nothing readable to compare'] })
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
    const targetSnap = change.change === 'removed' ? snapA : snapB
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
      case 'old_size': av = a.oldSize; bv = b.oldSize; break
      case 'new_size': av = a.newSize; bv = b.newSize; break
      case 'delta': av = sizeDelta(a) ?? 0; bv = sizeDelta(b) ?? 0; break
    }
    if (typeof av === 'string') {
      const cmp = av.localeCompare(bv as string)
      return sortDir === 'asc' ? cmp : -cmp
    }
    return sortDir === 'asc' ? (av as number) - (bv as number) : (bv as number) - (av as number)
  })

  /*
   * Counts come from the daemon, which counted the whole comparison even
   * when it stopped emitting rows. Counting the rows here would report 2000
   * changes for a snapshot that changed a hundred thousand files.
   */
  const counts: Record<ChangeType, number> = {
    added: totals?.added ?? 0,
    removed: totals?.removed ?? 0,
    modified: totals?.modified ?? 0,
  }

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

      {snaps.length > 0 && (
        <>
          {/* Selectors */}
          <div className="flex flex-wrap items-center gap-3 mb-4 bg-surface-secondary border border-border-default rounded-lg px-4 py-3">
            <div className="flex items-center gap-2">
              <label className="text-xs text-text-muted whitespace-nowrap">Snap A:</label>
              <select value={snapA ?? ''} onChange={e => setSnapA(Number(e.target.value))}
                className="text-xs px-2 py-1 border border-border-default rounded bg-surface-primary text-text-primary focus:outline-none focus:border-accent min-w-0">
                <option value="">Select...</option>
                {snaps.map(s => (
                  <option key={s.id} value={s.id}>#{s.id} - {absoluteTime(s.ts)}</option>
                ))}
              </select>
            </div>
            <div className="flex items-center gap-2">
              <label className="text-xs text-text-muted whitespace-nowrap">Snap B:</label>
              <select value={snapB ?? ''} onChange={e => setSnapB(Number(e.target.value))}
                className="text-xs px-2 py-1 border border-border-default rounded bg-surface-primary text-text-primary focus:outline-none focus:border-accent min-w-0">
                <option value="">Select...</option>
                {snaps.map(s => (
                  <option key={s.id} value={s.id}>#{s.id} - {absoluteTime(s.ts)}</option>
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
              {(['added', 'removed', 'modified'] as ChangeType[]).map(type => {
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
              {/* The counts above are the whole comparison; the table is
                * capped. Saying so is the difference between "a hundred
                * thousand files changed" and a list that looks complete. */}
              {totals?.truncated && (
                <span className="text-xs text-status-warning">
                  Listing the first {fmtNum(DIFF_ROW_CAP)} of{' '}
                  {fmtNum(counts.added + counts.removed + counts.modified)} changes
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
                    <th className="font-medium pb-2 pt-4 pl-4 pr-4 w-16 cursor-pointer" onClick={() => toggleSort('change')}>
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
                        <td className="py-1.5 pl-4 pr-4">
                          <span className={`inline-block px-1.5 py-0 rounded text-[10px] font-semibold ${info.color}`}>
                            {info.badge}
                          </span>
                        </td>
                        <td className="py-1.5 font-mono truncate" title={c.path}>
                          {c.path}
                          {isFetching && <span className="text-text-muted text-[10px] ml-2">loading...</span>}
                        </td>
                        <td className="py-1.5 text-right text-text-muted">
                          {c.change === 'added' ? '' : fmtSize(c.oldSize)}
                        </td>
                        <td className="py-1.5 text-right text-text-muted">
                          {c.change === 'removed' ? '' : fmtSize(c.newSize)}
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
              ) : fileDiff.change === 'modified' && fileDiff.oldText !== null && fileDiff.newText !== null ? (
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
                    const text = fileDiff.change === 'removed' ? fileDiff.oldText : fileDiff.newText
                    const isDelete = fileDiff.change === 'removed'
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
