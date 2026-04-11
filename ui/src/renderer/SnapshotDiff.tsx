import React, { useCallback, useEffect, useState } from 'react'
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

function escapeHtml(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
}

function getDiffTheme() {
  const isDark = document.documentElement.classList.contains('dark') ||
    window.matchMedia('(prefers-color-scheme: dark)').matches
  return {
    isDark,
    bg: isDark ? '#1e1e2e' : '#ffffff',
    fg: isDark ? '#cdd6f4' : '#1e1e1e',
    mutedFg: isDark ? '#6c7086' : '#888888',
    addBg: isDark ? 'rgba(166,227,161,0.1)' : 'rgba(40,167,69,0.08)',
    addFg: isDark ? '#a6e3a1' : '#22863a',
    delBg: isDark ? 'rgba(243,139,168,0.1)' : 'rgba(220,53,69,0.08)',
    delFg: isDark ? '#f38ba8' : '#cb2431',
    headerBg: isDark ? '#313244' : '#f0f0f0',
    border: isDark ? '#45475a' : '#ddd',
  }
}

function diffStyles(t: ReturnType<typeof getDiffTheme>): string {
  return `
    body { margin:0; background:${t.bg}; color:${t.fg}; font-family:-apple-system,BlinkMacSystemFont,sans-serif; font-size:12px; }
    .header { display:flex; justify-content:space-between; padding:8px 12px; background:${t.headerBg}; border-bottom:1px solid ${t.border}; font-weight:600; }
    .loading { display:flex; align-items:center; justify-content:center; height:100vh; color:${t.mutedFg}; }
    @keyframes pulse { 0%,100% { opacity:1 } 50% { opacity:.4 } }
    .loading { animation: pulse 1.5s ease-in-out infinite; }
    table { width:100%; border-collapse:collapse; }
    td { padding:0 8px; vertical-align:top; }
    td pre { margin:0; white-space:pre-wrap; word-break:break-all; font-family:'SF Mono',Menlo,Consolas,monospace; font-size:11px; line-height:1.5; }
    td.ln { width:40px; text-align:right; color:${t.mutedFg}; user-select:none; padding:0 4px; border-right:1px solid ${t.border}; }
    tr:hover td { background:${t.isDark ? 'rgba(255,255,255,0.03)' : 'rgba(0,0,0,0.02)'}; }
  `
}

function openDiffWindowLoading(title: string): Window | null {
  const t = getDiffTheme()
  const html = `<!DOCTYPE html><html><head><title>${escapeHtml(title)}</title><style>${diffStyles(t)}</style></head><body><div class="loading">Loading diff...</div></body></html>`
  const blob = new Blob([html], { type: 'text/html' })
  const url = URL.createObjectURL(blob)
  const w = window.open(url, '_blank', 'width=900,height=600')
  URL.revokeObjectURL(url)
  return w
}

function fillDiffWindow(w: Window, title: string, oldText: string | null, newText: string | null, change: ChangeType) {
  const oldLines = oldText?.split('\n') ?? []
  const newLines = newText?.split('\n') ?? []

  const t = getDiffTheme()

  let body = ''

  if (change === 'M' && oldText !== null && newText !== null) {
    const maxLines = Math.max(oldLines.length, newLines.length)
    let rows = ''
    for (let i = 0; i < Math.min(maxLines, 5000); i++) {
      const ol = i < oldLines.length ? escapeHtml(oldLines[i]) : ''
      const nl = i < newLines.length ? escapeHtml(newLines[i]) : ''
      const olStyle = i < oldLines.length && (i >= newLines.length || oldLines[i] !== newLines[i]) ? `background:${t.delBg};color:${t.delFg}` : ''
      const nlStyle = i < newLines.length && (i >= oldLines.length || oldLines[i] !== newLines[i]) ? `background:${t.addBg};color:${t.addFg}` : ''
      rows += `<tr><td class="ln">${i + 1}</td><td style="${olStyle}"><pre>${ol}</pre></td><td class="ln">${i + 1}</td><td style="${nlStyle}"><pre>${nl}</pre></td></tr>`
    }
    if (maxLines > 5000) rows += `<tr><td colspan="4" style="color:${t.mutedFg};text-align:center;padding:8px">... ${maxLines - 5000} more lines</td></tr>`
    body = `
      <div class="header">
        <span style="color:${t.delFg}">Old (${oldLines.length} lines)</span>
        <span style="color:${t.addFg}">New (${newLines.length} lines)</span>
      </div>
      <table>${rows}</table>`
  } else {
    const lines = change === 'D' ? oldLines : newLines
    const text = change === 'D' ? oldText : newText
    const lineBg = change === 'D' ? t.delBg : t.addBg
    const lineFg = change === 'D' ? t.delFg : t.addFg
    const prefix = change === 'D' ? '-' : '+'
    let rows = ''
    if (text !== null) {
      for (let i = 0; i < Math.min(lines.length, 5000); i++) {
        rows += `<tr><td class="ln">${i + 1}</td><td style="background:${lineBg};color:${lineFg}"><pre>${prefix} ${escapeHtml(lines[i])}</pre></td></tr>`
      }
      if (lines.length > 5000) rows += `<tr><td colspan="2" style="color:${t.mutedFg};text-align:center;padding:8px">... ${lines.length - 5000} more lines</td></tr>`
    }
    body = `<table>${rows}</table>`
  }

  const html = `<!DOCTYPE html><html><head><title>${escapeHtml(title)}</title><style>${diffStyles(t)}</style></head><body>${body}</body></html>`
  w.document.open()
  w.document.write(html)
  w.document.close()
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

    // Content change: pop-out window
    const oldSize = change.old_node?.size ?? 0
    const newSize = change.new_node?.size ?? 0
    if (oldSize > CONTENT_MAX_BYTES || newSize > CONTENT_MAX_BYTES) {
      setMetaModal({ path: change.path, lines: [`File too large for diff view (${fmtSize(Math.max(oldSize, newSize))})`] })
      return
    }

    const filename = change.path.split('/').pop() || change.path
    const windowTitle = `${filename} \u2014 ${CHANGE_LABELS[change.change].label}`

    // Open window immediately with loading skeleton
    const diffWin = openDiffWindowLoading(windowTitle)
    if (!diffWin) return

    setFetchingPath(change.path)
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
      if (diffWin.closed) { /* user closed it */ }
      else if (isBinary(oldText) || isBinary(newText)) {
        diffWin.close()
        setMetaModal({ path: change.path, lines: ['Binary file \u2014 cannot display diff'] })
      } else {
        fillDiffWindow(diffWin, windowTitle, oldText, newText, change.change)
      }
    } catch (err) {
      if (!diffWin.closed) diffWin.close()
      setMetaModal({ path: change.path, lines: [`Error loading content: ${err}`] })
    }
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
    </div>
  )
}
