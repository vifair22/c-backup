import React, { useCallback, useEffect, useState } from 'react'
import { fmtSize, fmtNum, fmtMode, absoluteTime, gfsBadges, NODE_TYPE_NAMES } from './format'

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
  onBack: () => void
}

export function SnapshotBrowser({ connName, repoPath, snapId, onBack }: Props): React.ReactElement {
  const [header, setHeader] = useState<SnapHeader | null>(null)
  const [tree, setTree] = useState<Map<number, TreeNode[]>>(new Map())
  const [expanded, setExpanded] = useState<Set<number>>(new Set())
  const [loading, setLoading] = useState<Set<number>>(new Set())
  const [breadcrumb, setBreadcrumb] = useState<Breadcrumb[]>([])
  const [error, setError] = useState<string | null>(null)
  const [initLoading, setInitLoading] = useState(true)

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

    Promise.all([
      api.rpcCall<SnapHeader>(connName, repoPath, 'snap_header', { id: snapId }),
      api.rpcCall<DirChildrenResponse>(connName, repoPath, 'snap_dir_children', { id: snapId, parent_node: 0 }),
    ]).then(([h, root]) => {
      if (cancelled) return
      setHeader(h)
      setTree(new Map([[0, sortChildren(root.children)]]))
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

      rows.push(
        <tr key={`${parentId}-${node.node_id}`}
          onClick={isDir ? () => toggleDir(node, depth, parentChain) : undefined}
          className={`border-t border-border-default hover:bg-surface-hover text-text-primary ${isDir ? 'cursor-pointer' : ''}`}>
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
    </div>
  )
}
