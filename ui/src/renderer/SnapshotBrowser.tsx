import React, { useCallback, useEffect, useRef, useState } from 'react'
import { fmtSize, fmtNum, fmtMode, absoluteTime, gfsBadges, NODE_TYPE_NAMES } from './format'
import { ContentViewer } from './ContentViewer'

const api = window.cbackup

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

interface SnapHeader {
  snap_id: number
  version: number
  created_sec: number
  phys_new_bytes: number
  node_count: number
  dirent_count: number
  gfs_flags: number
  snap_flags: number
}

interface TreeNode {
  node_id: number
  name: string
  type: number
  size: number
  mode: number
  has_children: boolean
  content_hash?: string
}

interface DirChildrenResponse {
  children: TreeNode[]
}

interface Breadcrumb {
  nodeId: number
  name: string
}

function sortChildren(children: TreeNode[]): TreeNode[] {
  return [...children].sort((a, b) => {
    // Dirs first
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
  const [header, setHeader] = useState<SnapHeader | null>(null)
  const [tree, setTree] = useState<Map<number, TreeNode[]>>(new Map())
  const [expanded, setExpanded] = useState<Set<number>>(new Set())
  const [loading, setLoading] = useState<Set<number>>(new Set())
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

  const fetchChildren = useCallback(async (parentNodeId: number) => {
    setLoading(prev => new Set(prev).add(parentNodeId))
    try {
      const resp = await api.rpcCall<DirChildrenResponse>(connName, repoPath, 'snap_dir_children', {
        id: snapId, parent_node: parentNodeId,
      })
      setTree(prev => {
        const next = new Map(prev)
        next.set(parentNodeId, sortChildren(resp.children))
        return next
      })
    } catch (err) {
      setError(`Failed to load directory: ${err}`)
    }
    setLoading(prev => {
      const next = new Set(prev)
      next.delete(parentNodeId)
      return next
    })
  }, [connName, repoPath, snapId])

  useEffect(() => {
    let cancelled = false
    setInitLoading(true)
    setError(null)
    setTree(new Map())
    setExpanded(new Set())
    setBreadcrumb([])
    setNote(null)
    setNoteEditing(false)

    // Load note (fire and forget)
    api.rpcCall<{ text: string }>(connName, repoPath, 'note_get', { id: snapId })
      .then(r => { if (!cancelled) setNote(r.text) })
      .catch(() => { if (!cancelled) setNote(null) })

    Promise.all([
      api.rpcCall<SnapHeader>(connName, repoPath, 'snap_header', { id: snapId }),
      api.rpcCall<DirChildrenResponse>(connName, repoPath, 'snap_dir_children', { id: snapId, parent_node: 0 }),
    ]).then(async ([h, root]) => {
      if (cancelled) return
      setHeader(h)
      const rootChildren = sortChildren(root.children)
      const treeMap = new Map([[0, rootChildren]])
      const expandedSet = new Set<number>()

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
            // Highlight the target file/dir
            setHighlightNodeId(match.node_id)
            if (match.type === NODE_DIR) {
              // Expand it too
              expandedSet.add(match.node_id)
              crumbs.push({ nodeId: match.node_id, name: match.name })
              try {
                const resp = await api.rpcCall<DirChildrenResponse>(connName, repoPath, 'snap_dir_children', {
                  id: snapId, parent_node: match.node_id,
                })
                treeMap.set(match.node_id, sortChildren(resp.children))
              } catch { /* ignore */ }
            }
          } else if (match.type === NODE_DIR && match.has_children) {
            // Expand intermediate directory
            expandedSet.add(match.node_id)
            crumbs.push({ nodeId: match.node_id, name: match.name })
            try {
              const resp = await api.rpcCall<DirChildrenResponse>(connName, repoPath, 'snap_dir_children', {
                id: snapId, parent_node: match.node_id,
              })
              treeMap.set(match.node_id, sortChildren(resp.children))
            } catch { break }
            parentId = match.node_id
          } else {
            break
          }
        }
        setBreadcrumb(crumbs)
      }

      setTree(treeMap)
      setExpanded(expandedSet)
    }).catch(err => {
      if (!cancelled) setError(String(err))
    }).finally(() => {
      if (!cancelled) setInitLoading(false)
    })

    return () => { cancelled = true }
  }, [connName, repoPath, snapId])

  const toggleDir = async (node: TreeNode, depth: number, parentChain: Breadcrumb[]) => {
    if (node.type !== NODE_DIR) return

    if (expanded.has(node.node_id)) {
      // Collapse
      setExpanded(prev => {
        const next = new Set(prev)
        next.delete(node.node_id)
        return next
      })
      return
    }

    // Expand
    if (!tree.has(node.node_id)) {
      await fetchChildren(node.node_id)
    }
    setExpanded(prev => new Set(prev).add(node.node_id))
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
      const isExpanded = expanded.has(node.node_id)
      const isLoading = loading.has(node.node_id)
      const chain = isDir ? [...parentChain, { nodeId: node.node_id, name: node.name }] : parentChain

      const filePath = '/' + [...parentChain.map(b => b.name), node.name].join('/')
      const isSelected = selectedFile?.node.node_id === node.node_id

      const isHighlighted = highlightNodeId === node.node_id

      rows.push(
        <tr key={`${parentId}-${node.node_id}`}
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
        rows.push(...renderChildren(node.node_id, depth + 1, chain))
      }
    }
    return rows
  }

  /* ---------------------------------------------------------------- */
  /* Render                                                            */
  /* ---------------------------------------------------------------- */

  const badges = header ? gfsBadges(header.gfs_flags) : []

  return (
    <div>
      {/* Navigation */}
      <div className="flex items-center gap-2 mb-3 text-xs">
        <button onClick={onBack}
          className="text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Dashboard
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
            <span className="text-sm font-semibold">Snapshot #{header.snap_id}</span>
            <span className="text-xs text-text-muted ml-2">{absoluteTime(header.created_sec)}</span>
          </div>
          <div className="text-xs text-text-secondary">
            {fmtNum(header.node_count)} nodes
          </div>
          <div className="text-xs text-text-secondary">
            {fmtSize(header.phys_new_bytes)} new data
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
                    try { await api.rpcCall(connName, repoPath, 'note_delete', { id: snapId }); setNote(null); setNoteEditing(false) }
                    catch { /* ignore */ }
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
                  try { await api.rpcCall(connName, repoPath, 'note_set', { id: snapId, text: noteText.trim() }); setNote(noteText.trim()); setNoteEditing(false) }
                  catch { /* ignore */ }
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
                <div className="text-xs text-text-muted font-mono truncate" title={selectedFile.node.content_hash}>
                  {selectedFile.node.content_hash?.slice(0, 16)}...
                </div>
              </div>
            </div>
            <div className="flex justify-end gap-2">
              <button onClick={() => setSelectedFile(null)}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none">
                Close
              </button>
              {selectedFile.node.content_hash && selectedFile.node.content_hash !== '0'.repeat(64) && (
                <button onClick={() => { setViewerHash({ hash: selectedFile.node.content_hash!, filename: selectedFile.node.name }); setSelectedFile(null) }}
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
