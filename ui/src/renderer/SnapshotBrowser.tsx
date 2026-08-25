/*
 * Walking a snapshot's tree.
 *
 * Migrated off the action bridge, which is what made this screen work at
 * all: it asked for `snap_dir_children` and read `resp.children` out of a
 * response that carries `entries`, so every directory came back empty and
 * the browser rendered a snapshot with nothing in it. `snap_header` had no
 * successor route at all and resolved to the snapshot list, whose fields it
 * then read by the wrong names.
 *
 * The tree is fetched one directory at a time, by design rather than by
 * omission: a directory is an index range scan on the daemon side, and
 * loading a whole snapshot up front is the exact cost the pagination rule
 * exists to avoid.
 */

import React, { useCallback, useEffect, useState } from 'react'
import { fmtSize, fmtNum, fmtMode, absoluteTime, gfsBadges, NODE_TYPE_NAMES } from './format'
import { ContentViewer } from './ContentViewer'

const api = window.cbackup

const EMPTY_HASH = '0'.repeat(64)

/*
 * How much of one directory is fetched at a time.
 *
 * A directory is a paged route and this asked for a page without ever
 * saying so, which meant a directory of 20,000 files showed the first 500
 * and looked complete. Silent truncation in a backup browser is the worst
 * kind: somebody concludes a file was not backed up.
 *
 * It is also the right answer for rendering. 500 rows is a table; 20,000 is
 * a frozen window.
 */
const DIR_PAGE = 500

const NODE_REG = 1
const NODE_DIR = 2
const NODE_SYMLINK = 3
const NODE_HARDLINK = 4

function nodeStyle(type: number, size: number): string {
  if (type === NODE_DIR) return 'text-blue-500 font-medium'
  if (type === NODE_SYMLINK) return 'text-text-muted italic'
  if (type === NODE_HARDLINK) return 'text-amber-600'
  if (type === NODE_REG && size === 0) return 'text-text-muted italic'
  return ''
}

/*
 * What a snapshot is, as far as this screen cares. There is no per-snapshot
 * route to ask - the daemon serves snapshots as a paged list - so the header
 * is the row for this id out of that list.
 */
interface SnapHeader {
  id: number
  ts: number
  label: string
  files: number
  size: number
  gfsFlags: number
}

interface TreeNode {
  nodeId: number
  name: string
  type: number
  size: number
  mode: number
  hasChildren: boolean
  hash: string
}

/* As the daemon sends it: every integer is a string. */
interface RawSnapshotRow {
  id: string
  ts: string
  label?: string
  files?: string
  size?: string
  gfs_flags?: string
}

interface RawEntry {
  id: string
  name: string
  type: string
  mode: string
  size: string
  hash?: string
  has_children?: string
}

const parseEntry = (e: RawEntry): TreeNode => ({
  nodeId: Number(e.id),
  name: e.name,
  type: Number(e.type),
  size: Number(e.size),
  mode: Number(e.mode),
  hasChildren: e.has_children === '1',
  hash: e.hash ?? '',
})

interface Breadcrumb {
  nodeId: number
  name: string
}

/*
 * The daemon already returns a directory in name order, because the index
 * carries name as its trailing column and serving it sorted costs nothing.
 * This only lifts directories above files, which is a display choice and
 * not something worth asking the database to do.
 */
function sortChildren(children: TreeNode[]): TreeNode[] {
  return [...children].sort((a, b) => {
    if (a.type === NODE_DIR && b.type !== NODE_DIR) return -1
    if (a.type !== NODE_DIR && b.type === NODE_DIR) return 1
    return a.name.localeCompare(b.name)
  })
}

interface Props {
  connName: string
  repoPath: string
  snapId: number
  initialPath?: string
  onBack: () => void
  onRestore?: (snapId: number, filePath?: string) => void
}

export function SnapshotBrowser({ connName, repoPath, snapId, initialPath, onBack, onRestore }: Props): React.ReactElement {
  const repoId = Number(repoPath)

  const [header, setHeader] = useState<SnapHeader | null>(null)
  const [tree, setTree] = useState<Map<number, TreeNode[]>>(new Map())
  const [expanded, setExpanded] = useState<Set<number>>(new Set())
  const [loading, setLoading] = useState<Set<number>>(new Set())
  /* Directories known to be fully loaded, so a short page is not mistaken
   * for a page that has more behind it. */
  const [complete, setComplete] = useState<Set<number>>(new Set())
  const [breadcrumb, setBreadcrumb] = useState<Breadcrumb[]>([])
  const [error, setError] = useState<string | null>(null)
  const [highlightNodeId, setHighlightNodeId] = useState<number | null>(null)
  const [initLoading, setInitLoading] = useState(true)
  const [selectedFile, setSelectedFile] = useState<{ node: TreeNode; path: string } | null>(null)
  const [note, setNote] = useState<string | null>(null)
  const [noteEditing, setNoteEditing] = useState(false)
  const [noteText, setNoteText] = useState('')
  const [noteSaving, setNoteSaving] = useState(false)
  const [viewerHash, setViewerHash] = useState<{ hash: string; filename: string } | null>(null)

  const readDir = useCallback(async (parentNodeId: number, offset = 0): Promise<TreeNode[]> => {
    const resp = await api.apiCall<{ entries?: RawEntry[] }>(connName, {
      kind: 'snapshotTree', repoId, snapshotId: snapId,
      parent: parentNodeId, limit: DIR_PAGE, offset,
    })
    return sortChildren((resp.entries ?? []).map(parseEntry))
  }, [connName, repoId, snapId])

  /*
   * Fetch the next page of a directory already open. The route reports no
   * total, so a full page is the only evidence there is another one - the
   * same rule every other paged screen here follows.
   */
  const loadMore = useCallback(async (parentNodeId: number) => {
    const have = tree.get(parentNodeId)?.length ?? 0
    setLoading(prev => new Set(prev).add(parentNodeId))
    try {
      const next = await readDir(parentNodeId, have)
      setTree(prev => {
        const map = new Map(prev)
        map.set(parentNodeId, sortChildren([...(prev.get(parentNodeId) ?? []), ...next]))
        return map
      })
      if (next.length < DIR_PAGE) {
        setComplete(prev => new Set(prev).add(parentNodeId))
      }
    } catch (err) {
      setError(`Failed to load directory: ${err}`)
    }
    setLoading(prev => {
      const n = new Set(prev)
      n.delete(parentNodeId)
      return n
    })
  }, [readDir, tree])

  const fetchChildren = useCallback(async (parentNodeId: number) => {
    setLoading(prev => new Set(prev).add(parentNodeId))
    try {
      const children = await readDir(parentNodeId)
      setTree(prev => {
        const next = new Map(prev)
        next.set(parentNodeId, children)
        return next
      })
      if (children.length < DIR_PAGE) {
        setComplete(prev => new Set(prev).add(parentNodeId))
      }
    } catch (err) {
      setError(`Failed to load directory: ${err}`)
    }
    setLoading(prev => {
      const next = new Set(prev)
      next.delete(parentNodeId)
      return next
    })
  }, [readDir])

  useEffect(() => {
    let cancelled = false
    setInitLoading(true)
    setError(null)
    setTree(new Map())
    setExpanded(new Set())
    setComplete(new Set())
    setBreadcrumb([])
    setNote(null)
    setNoteEditing(false)

    /*
     * A snapshot with no note 404s, which is the correct answer and not an
     * error worth showing: most snapshots have none.
     */
    api.apiCall<{ body?: string }>(connName, { kind: 'noteGet', repoId, snapshotId: snapId })
      .then(r => { if (!cancelled) setNote(r.body || null) })
      .catch(() => { if (!cancelled) setNote(null) })

    Promise.all([
      api.apiCall<{ snapshots?: RawSnapshotRow[] }>(connName, {
        kind: 'snapshotList', repoId, limit: 200,
      }),
      readDir(0),
    ]).then(async ([list, rootChildren]) => {
      if (cancelled) return
      const row = (list.snapshots ?? []).find(x => Number(x.id) === snapId)
      setHeader(row ? {
        id: Number(row.id),
        ts: Number(row.ts),
        label: row.label ?? '',
        files: Number(row.files ?? '0'),
        size: Number(row.size ?? '0'),
        gfsFlags: Number(row.gfs_flags ?? '0'),
      } : null)
      const treeMap = new Map([[0, rootChildren]])
      const expandedSet = new Set<number>()
      const completeSet = new Set<number>()
      if (rootChildren.length < DIR_PAGE) completeSet.add(0)

      // Auto-expand to initialPath if provided
      if (initialPath) {
        const segments = initialPath.replace(/^\/+/, '').split('/').filter(Boolean)
        let parentId = 0
        const crumbs: Breadcrumb[] = []

        for (let i = 0; i < segments.length; i++) {
          const seg = segments[i]
          const children = treeMap.get(parentId)
          if (!children) break

          const match = children.find(c => c.name === seg)
          if (!match) break

          const isLast = i === segments.length - 1
          if (isLast) {
            setHighlightNodeId(match.nodeId)
            if (match.type === NODE_DIR) {
              expandedSet.add(match.nodeId)
              crumbs.push({ nodeId: match.nodeId, name: match.name })
              try {
                const kids = await readDir(match.nodeId)
                treeMap.set(match.nodeId, kids)
                if (kids.length < DIR_PAGE) completeSet.add(match.nodeId)
              } catch { /* the row is still highlighted; only the expansion is lost */ }
            }
          } else if (match.type === NODE_DIR && match.hasChildren) {
            expandedSet.add(match.nodeId)
            crumbs.push({ nodeId: match.nodeId, name: match.name })
            try {
              const kids = await readDir(match.nodeId)
              treeMap.set(match.nodeId, kids)
              if (kids.length < DIR_PAGE) completeSet.add(match.nodeId)
            } catch { break }
            parentId = match.nodeId
          } else {
            break
          }
        }
        setBreadcrumb(crumbs)
      }

      setTree(treeMap)
      setExpanded(expandedSet)
      setComplete(completeSet)
    }).catch(err => {
      if (!cancelled) setError(String(err))
    }).finally(() => {
      if (!cancelled) setInitLoading(false)
    })

    return () => { cancelled = true }
  }, [connName, repoId, snapId, initialPath, readDir])

  const toggleDir = async (node: TreeNode, depth: number, parentChain: Breadcrumb[]) => {
    if (node.type !== NODE_DIR) return

    if (expanded.has(node.nodeId)) {
      // Collapse
      setExpanded(prev => {
        const next = new Set(prev)
        next.delete(node.nodeId)
        return next
      })
      return
    }

    // Expand
    if (!tree.has(node.nodeId)) {
      await fetchChildren(node.nodeId)
    }
    setExpanded(prev => new Set(prev).add(node.nodeId))
    setBreadcrumb(parentChain)
  }

  /* ---------------------------------------------------------------- */
  /* Render tree                                                       */
  /* ---------------------------------------------------------------- */

  const renderChildren = (parentId: number, depth: number, parentChain: Breadcrumb[]): React.ReactNode[] => {
    const children = tree.get(parentId)
    if (!children) return []

    const rows: React.ReactNode[] = []
    for (const node of children) {
      const isDir = node.type === NODE_DIR
      const isExpanded = expanded.has(node.nodeId)
      const isLoading = loading.has(node.nodeId)
      const chain = isDir ? [...parentChain, { nodeId: node.nodeId, name: node.name }] : parentChain

      const filePath = '/' + [...parentChain.map(b => b.name), node.name].join('/')
      const isSelected = selectedFile?.node.nodeId === node.nodeId

      const isHighlighted = highlightNodeId === node.nodeId

      rows.push(
        <tr key={`${parentId}-${node.nodeId}`}
          ref={isHighlighted ? (el) => { el?.scrollIntoView({ block: 'center', behavior: 'smooth' }) } : undefined}
          onClick={isDir
            ? () => toggleDir(node, depth, parentChain)
            : () => setSelectedFile({ node, path: filePath })
          }
          className={`border-t border-border-default hover:bg-surface-hover text-text-primary cursor-pointer ${
            isHighlighted ? 'bg-accent/10 ring-1 ring-accent/30' :
            isSelected ? 'bg-accent/10' : ''
          }`}>
          <td className="py-1 pr-2" style={{ paddingLeft: `${depth * 20 + 8}px` }}>
            <span className="inline-flex items-center gap-1">
              {isDir ? (
                <button
                  onClick={() => toggleDir(node, depth, parentChain)}
                  className="w-4 h-4 inline-flex items-center justify-center bg-transparent border-none cursor-pointer text-text-muted p-0">
                  {isLoading ? (
                    <span className="text-[10px]">...</span>
                  ) : (
                    <span className={`text-[10px] transition-transform origin-center ${isExpanded ? 'rotate-90' : ''}`}>&#9654;</span>
                  )}
                </button>
              ) : (
                <span className="w-4" />
              )}
              <span className={nodeStyle(node.type, node.size)}>{node.name}</span>
              {isDir && <span className="text-blue-400 text-[10px]">/</span>}
              {node.type === NODE_SYMLINK && <span className="text-text-muted text-[10px] ml-1">symlink</span>}
              {node.type === NODE_HARDLINK && <span className="text-amber-500 text-[10px] ml-1">hardlink</span>}
            </span>
          </td>
          <td className="py-1 text-text-muted text-[11px]">{NODE_TYPE_NAMES[node.type] ?? `${node.type}`}</td>
          <td className="py-1 text-right">{isDir ? '' : fmtSize(node.size)}</td>
          <td className="py-1 text-right font-mono text-text-muted">{fmtMode(node.mode)}</td>
        </tr>
      )

      if (isDir && isExpanded) {
        rows.push(...renderChildren(node.nodeId, depth + 1, chain))
      }
    }

    /*
     * The tail of a directory that has more behind it. Rendered as a row
     * rather than a corner control so it sits where the missing entries
     * would be, at the right indent, in the directory it belongs to.
     */
    if (tree.has(parentId) && !complete.has(parentId)) {
      const busy = loading.has(parentId)
      rows.push(
        <tr key={`more-${parentId}`} className="border-t border-border-default">
          <td colSpan={4} className="py-1.5" style={{ paddingLeft: `${depth * 20 + 28}px` }}>
            <button onClick={() => void loadMore(parentId)} disabled={busy}
              className="text-[11px] underline cursor-pointer bg-transparent border-none
                         text-accent p-0 disabled:opacity-50">
              {busy ? 'Loading...' : `${fmtNum(children.length)} shown, load more`}
            </button>
          </td>
        </tr>
      )
    }
    return rows
  }

  /* ---------------------------------------------------------------- */
  /* Render                                                            */
  /* ---------------------------------------------------------------- */

  const badges = header ? gfsBadges(header.gfsFlags) : []

  return (
    <div>
      {/* Navigation */}
      <div className="flex items-center gap-2 mb-3 text-xs">
        {/* History back, not "go to the dashboard" - reaching this screen
          * from the snapshot list and being sent past it was the label's
          * doing, not the navigation's. */}
        <button onClick={onBack}
          className="text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Back
        </button>
        <span className="text-text-muted">/</span>
        <span className="text-text-primary font-medium">Snapshot #{snapId}</span>
        {breadcrumb.length > 0 && breadcrumb.map((bc, i) => (
          <React.Fragment key={bc.nodeId}>
            <span className="text-text-muted">/</span>
            <span className="text-text-secondary">{bc.name}</span>
          </React.Fragment>
        ))}
      </div>

      {/* Header */}
      {header && (
        <div className="flex items-center gap-4 mb-4 bg-surface-secondary border border-border-default rounded-lg px-4 py-3">
          <div>
            <span className="text-sm font-semibold">Snapshot #{header.id}</span>
            <span className="text-xs text-text-muted ml-2">{absoluteTime(header.ts)}</span>
            {header.label && (
              <span className="text-xs text-text-secondary ml-2">{header.label}</span>
            )}
          </div>
          <div className="text-xs text-text-secondary">
            {fmtNum(header.files)} files
          </div>
          <div className="text-xs text-text-secondary">
            {fmtSize(header.size)}
          </div>
          {badges.length > 0 && (
            <div className="inline-flex gap-1">
              {badges.map(b => (
                <span key={b.label} className={`inline-block px-1.5 py-0 rounded text-[10px] font-semibold ${b.color}`}>
                  {b.label}
                </span>
              ))}
            </div>
          )}
          {onRestore && (
            <button onClick={() => onRestore(snapId)}
              className="text-xs px-3 py-1 rounded bg-accent text-accent-text hover:bg-accent-hover border-none cursor-pointer ml-auto">
              Restore
            </button>
          )}
        </div>
      )}

      {/* Note */}
      {header && (
        <div className="mb-4">
          {noteEditing ? (
            <div className="bg-surface-secondary border border-border-default rounded-lg p-3">
              <textarea value={noteText} onChange={e => setNoteText(e.target.value)}
                placeholder="Add a note for this snapshot..."
                className="w-full px-2 py-1.5 border border-border-default rounded text-xs bg-surface-primary text-text-primary focus:outline-none focus:border-accent resize-y min-h-[60px]"
                rows={3} autoFocus />
              <div className="flex justify-end gap-2 mt-2">
                <button onClick={() => setNoteEditing(false)}
                  className="text-[11px] px-2 py-0.5 rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none cursor-pointer">
                  Cancel
                </button>
                {note !== null && (
                  <button onClick={async () => {
                    setNoteSaving(true)
                    /* The daemon serves GET and PUT on a note and nothing
                     * else, so clearing one is writing an empty body rather
                     * than a DELETE that would 404. */
                    try { await api.apiCall(connName, { kind: 'noteSet', repoId, snapshotId: snapId, note: '' }); setNote(null); setNoteEditing(false) }
                    catch (err) { setError(String(err)) }
                    setNoteSaving(false)
                  }}
                    className="text-[11px] px-2 py-0.5 rounded bg-surface-tertiary text-status-error hover:bg-surface-hover border-none cursor-pointer"
                    disabled={noteSaving}>
                    Delete
                  </button>
                )}
                <button onClick={async () => {
                  if (!noteText.trim()) return
                  setNoteSaving(true)
                  try { await api.apiCall(connName, { kind: 'noteSet', repoId, snapshotId: snapId, note: noteText.trim() }); setNote(noteText.trim()); setNoteEditing(false) }
                  catch (err) { setError(String(err)) }
                  setNoteSaving(false)
                }}
                  className="text-[11px] px-2 py-0.5 rounded bg-accent text-accent-text hover:bg-accent-hover border-none cursor-pointer"
                  disabled={noteSaving || !noteText.trim()}>
                  Save
                </button>
              </div>
            </div>
          ) : note ? (
            <div onClick={() => { setNoteText(note); setNoteEditing(true) }}
              className="bg-surface-secondary border border-border-default rounded-lg px-3 py-2 text-xs text-text-secondary cursor-pointer hover:bg-surface-hover">
              <span className="text-[10px] text-text-muted uppercase tracking-wide mr-2">Note:</span>
              {note}
            </div>
          ) : (
            <button onClick={() => { setNoteText(''); setNoteEditing(true) }}
              className="text-[11px] text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
              + Add note
            </button>
          )}
        </div>
      )}

      {initLoading && <div className="text-text-muted text-sm">Loading snapshot...</div>}
      {error && <div className="text-status-error text-sm mb-3">{error}</div>}

      {/* File tree */}
      {!initLoading && tree.has(0) && (
        <div className="bg-surface-secondary border border-border-default rounded-lg p-4 overflow-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-text-muted text-left">
                <th className="font-medium pb-2">Name</th>
                <th className="font-medium pb-2 w-16">Type</th>
                <th className="font-medium pb-2 text-right w-20">Size</th>
                <th className="font-medium pb-2 text-right w-16">Mode</th>
              </tr>
            </thead>
            <tbody>
              {renderChildren(0, 0, [])}
            </tbody>
          </table>
        </div>
      )}

      {/* File detail modal */}
      {selectedFile && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
          <div className="bg-surface-primary rounded-lg p-5 w-96 shadow-xl border border-border-default">
            <div className="flex items-start justify-between mb-3">
              <div className="min-w-0 flex-1">
                <div className="text-sm font-semibold truncate">{selectedFile.node.name}</div>
                <div className="text-[11px] text-text-muted font-mono truncate mt-0.5">{selectedFile.path}</div>
              </div>
              <button onClick={() => setSelectedFile(null)}
                className="text-text-muted hover:text-text-primary bg-transparent border-none cursor-pointer text-lg px-1 shrink-0">
                &times;
              </button>
            </div>
            <div className="grid grid-cols-2 gap-3 mb-3">
              <div>
                <div className="text-[10px] text-text-muted uppercase tracking-wide">Type</div>
                <div className="text-xs text-text-primary">{NODE_TYPE_NAMES[selectedFile.node.type] ?? `${selectedFile.node.type}`}</div>
              </div>
              <div>
                <div className="text-[10px] text-text-muted uppercase tracking-wide">Size</div>
                <div className="text-xs text-text-primary">{fmtSize(selectedFile.node.size)}</div>
              </div>
              <div>
                <div className="text-[10px] text-text-muted uppercase tracking-wide">Mode</div>
                <div className="text-xs text-text-primary font-mono">{fmtMode(selectedFile.node.mode)}</div>
              </div>
              <div>
                <div className="text-[10px] text-text-muted uppercase tracking-wide">Hash</div>
                <div className="text-xs text-text-muted font-mono truncate" title={selectedFile.node.hash}>
                  {selectedFile.node.hash ? `${selectedFile.node.hash.slice(0, 16)}...` : '-'}
                </div>
              </div>
            </div>
            <div className="flex justify-end gap-2">
              <button onClick={() => setSelectedFile(null)}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none">
                Close
              </button>
              {selectedFile.node.hash && selectedFile.node.hash !== EMPTY_HASH && (
                <button onClick={() => { setViewerHash({ hash: selectedFile.node.hash, filename: selectedFile.node.name }); setSelectedFile(null) }}
                  className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none">
                  View Content
                </button>
              )}
              {onRestore && (
                <button onClick={() => { onRestore(snapId, selectedFile.path); setSelectedFile(null) }}
                  className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text hover:bg-accent-hover border-none">
                  Restore File
                </button>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Content viewer modal */}
      {viewerHash && (
        <ContentViewer
          connName={connName}
          repoPath={repoPath}
          hash={viewerHash.hash}
          filename={viewerHash.filename}
          onClose={() => setViewerHash(null)}
        />
      )}
    </div>
  )
}
