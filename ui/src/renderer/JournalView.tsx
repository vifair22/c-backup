import React, { useCallback, useEffect, useState } from 'react'
import { fmtNum } from './format'

const api = window.cbackup

const PAGE_SIZE = 200

interface JournalEntry {
  op_id: string
  state: string
  timestamp: string
  operation: string
  source?: string
  user?: string
  result?: string
  duration_ms?: number
  error?: string
  task_id?: string
  signal?: number
  summary?: Record<string, unknown>
}

interface JournalResponse {
  entries: JournalEntry[]
  count: number
  orphan_count?: number
}

/* Merged operation: started + completed entries collapsed into one row */
interface MergedOp {
  op_id: string
  operation: string
  started?: JournalEntry
  completed?: JournalEntry
  /* Derived */
  timestamp: string
  state: 'running' | 'ok' | 'error' | 'cancelled' | 'crashed'
  duration_ms?: number
  source?: string
  user?: string
  error?: string
  task_id?: string
  signal?: number
  summary?: Record<string, unknown>
}

function mergeEntries(entries: JournalEntry[]): MergedOp[] {
  const byOpId = new Map<string, { started?: JournalEntry; completed?: JournalEntry }>()

  for (const e of entries) {
    let pair = byOpId.get(e.op_id)
    if (!pair) { pair = {}; byOpId.set(e.op_id, pair) }
    if (e.state === 'started') pair.started = e
    else pair.completed = e
  }

  const ops: MergedOp[] = []
  for (const [op_id, pair] of byOpId) {
    const primary = pair.completed ?? pair.started!
    let state: MergedOp['state'] = 'running'
    if (pair.completed) {
      if (pair.completed.signal) state = 'crashed'
      else if (pair.completed.result === 'success' || pair.completed.result === 'ok') state = 'ok'
      else if (pair.completed.result === 'cancelled') state = 'cancelled'
      else state = 'error'
    }

    ops.push({
      op_id,
      operation: primary.operation,
      started: pair.started,
      completed: pair.completed,
      timestamp: pair.started?.timestamp ?? primary.timestamp,
      state,
      duration_ms: pair.completed?.duration_ms,
      source: primary.source,
      user: primary.user,
      error: pair.completed?.error,
      task_id: primary.task_id,
      signal: pair.completed?.signal,
      summary: pair.completed?.summary,
    })
  }

  return ops
}

const STATUS_BADGE: Record<string, { color: string; label: string }> = {
  running:   { color: 'bg-blue-600 text-white', label: 'running' },
  ok:        { color: 'bg-green-600 text-white', label: 'ok' },
  error:     { color: 'bg-red-600 text-white', label: 'error' },
  cancelled: { color: 'bg-amber-600 text-white', label: 'cancelled' },
  crashed:   { color: 'bg-red-600 text-white', label: 'crashed' },
}

function fmtDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`
  const mins = Math.floor(ms / 60_000)
  const secs = Math.round((ms % 60_000) / 1000)
  return `${mins}m ${secs}s`
}

function fmtTimestamp(ts: string): string {
  try {
    return new Date(ts).toLocaleString()
  } catch {
    return ts
  }
}

type OpFilter = string | null

/* Two groups: write operations and read/browse operations */
const WRITE_OPS = ['run', 'gc', 'pack', 'prune', 'verify', 'restore', 'export', 'import', 'init',
                   'save_policy', 'tag_set', 'tag_delete', 'tag_rename', 'note_set', 'note_delete',
                   'task_start', 'task_cancel']
const READ_OPS = ['stats', 'list', 'snap', 'snap_header', 'snap_dir_children', 'diff', 'ls',
                  'search', 'scan', 'policy', 'tags', 'repo_stats', 'repo_summary', 'journal',
                  'object_locate', 'object_content', 'object_refs', 'object_layout',
                  'pack_entries', 'pack_index', 'loose_list', 'loose_stats',
                  'all_pack_entries', 'global_pack_index',
                  'note_get', 'note_list', 'task_list', 'task_status']

interface Props {
  connName: string
  repoPath: string
  onBack: () => void
}

export function JournalView({ connName, repoPath, onBack }: Props): React.ReactElement {
  const [entries, setEntries] = useState<JournalEntry[]>([])
  const [totalCount, setTotalCount] = useState(0)
  const [orphanCount, setOrphanCount] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [offset, setOffset] = useState(0)
  const [opFilter, setOpFilter] = useState<OpFilter>(null)
  const [showReads, setShowReads] = useState(false)
  const [expandedId, setExpandedId] = useState<string | null>(null)

  const fetchPage = useCallback(async (pageOffset: number, operation: OpFilter, includeReads: boolean) => {
    setLoading(true)
    setError(null)
    try {
      // Fetch more when hiding reads since most entries may be reads
      const limit = includeReads ? PAGE_SIZE : 2000
      const params: Record<string, unknown> = { offset: pageOffset, limit }
      if (operation) params.operation = operation
      const resp = await api.rpcCall<JournalResponse>(connName, repoPath, 'journal', params)
      setEntries(resp.entries)
      setTotalCount(resp.count)
      if (resp.orphan_count !== undefined) setOrphanCount(resp.orphan_count)
    } catch (err) {
      setError(String(err))
    }
    setLoading(false)
  }, [connName, repoPath])

  useEffect(() => {
    fetchPage(0, opFilter, showReads)
  }, [connName, repoPath, opFilter, showReads])

  const goToPage = (newOffset: number) => {
    setOffset(newOffset)
    setExpandedId(null)
    fetchPage(newOffset, opFilter, showReads)
  }

  const changeFilter = (op: OpFilter) => {
    setOpFilter(op)
    setOffset(0)
    setExpandedId(null)
  }

  // Merge started/completed pairs
  const merged = mergeEntries(entries)

  // Filter out reads unless toggled on
  const filtered = showReads ? merged : merged.filter(op => !READ_OPS.includes(op.operation))

  const pageCount = Math.ceil(totalCount / PAGE_SIZE)
  const currentPage = Math.floor(offset / PAGE_SIZE) + 1

  return (
    <div>
      {/* Header */}
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Dashboard
        </button>
        <h3 className="text-lg font-medium m-0">Operation Journal</h3>
        <span className="text-xs text-text-muted">{fmtNum(totalCount)} entries</span>
        {orphanCount > 0 && (
          <span className="text-xs text-status-error">{orphanCount} crashed</span>
        )}
      </div>

      {/* Filter bar */}
      <div className="flex flex-wrap items-center gap-2 mb-4">
        <button onClick={() => changeFilter(null)}
          className={`text-[11px] px-2 py-0.5 rounded cursor-pointer border-none ${
            opFilter === null ? 'bg-accent text-accent-text' : 'bg-surface-tertiary text-text-secondary hover:bg-surface-hover'
          }`}>
          All
        </button>
        {WRITE_OPS.slice(0, 10).map(op => (
          <button key={op} onClick={() => changeFilter(opFilter === op ? null : op)}
            className={`text-[11px] px-2 py-0.5 rounded cursor-pointer border-none ${
              opFilter === op ? 'bg-accent text-accent-text' : 'bg-surface-tertiary text-text-secondary hover:bg-surface-hover'
            }`}>
            {op}
          </button>
        ))}
        <div className="border-l border-border-default h-4 mx-0.5" />
        <label className="text-[11px] flex items-center gap-1 cursor-pointer text-text-muted">
          <input type="checkbox" checked={showReads} onChange={e => setShowReads(e.target.checked)} />
          Show reads
        </label>
      </div>

      {error && <div className="text-status-error text-sm mb-3">{error}</div>}

      {/* Table */}
      {!loading && filtered.length > 0 && (
        <div className="bg-surface-secondary border border-border-default rounded-lg overflow-hidden">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-text-muted text-left">
                <th className="font-medium pb-2 pt-4 pl-4 w-40">Time</th>
                <th className="font-medium pb-2 pt-4 w-28">Operation</th>
                <th className="font-medium pb-2 pt-4 w-16">Status</th>
                <th className="font-medium pb-2 pt-4 w-20 text-right">Duration</th>
                <th className="font-medium pb-2 pt-4 px-2">Details</th>
                <th className="font-medium pb-2 pt-4 w-10 pr-4">Src</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map(op => {
                const isExpanded = expandedId === op.op_id
                const badge = STATUS_BADGE[op.state]

                return (
                  <React.Fragment key={op.op_id}>
                    <tr onClick={() => setExpandedId(isExpanded ? null : op.op_id)}
                      className="border-t border-border-default cursor-pointer hover:bg-surface-hover">
                      <td className="py-1.5 pl-4 text-text-muted whitespace-nowrap">{fmtTimestamp(op.timestamp)}</td>
                      <td className="py-1.5 font-medium truncate">{op.operation}</td>
                      <td className="py-1.5">
                        {badge && (
                          <span className={`inline-block px-1.5 py-0 rounded text-[10px] font-semibold ${badge.color}`}>
                            {badge.label}
                          </span>
                        )}
                      </td>
                      <td className="py-1.5 text-text-muted text-right">
                        {op.duration_ms ? fmtDuration(op.duration_ms) : ''}
                      </td>
                      <td className="py-1.5 px-2 truncate text-text-muted" title={op.error || ''}>
                        {op.error && <span className="text-status-error">{op.error}</span>}
                        {op.signal && !op.error && <span className="text-status-error">signal {op.signal}</span>}
                      </td>
                      <td className="py-1.5 pr-4 text-text-muted">{op.source === 'cli' ? 'CLI' : op.source === 'ui' ? 'UI' : ''}</td>
                    </tr>
                    {isExpanded && (
                      <tr>
                        <td colSpan={6} className="p-0">
                          <div className="px-4 py-3 bg-surface-primary border-t border-border-default space-y-1">
                            <div className="text-[10px] text-text-muted">
                              <span className="font-semibold">Op ID:</span> <span className="font-mono">{op.op_id}</span>
                            </div>
                            {op.started && (
                              <div className="text-[10px] text-text-muted">
                                <span className="font-semibold">Started:</span> {fmtTimestamp(op.started.timestamp)}
                              </div>
                            )}
                            {op.completed && (
                              <div className="text-[10px] text-text-muted">
                                <span className="font-semibold">Completed:</span> {fmtTimestamp(op.completed.timestamp)}
                              </div>
                            )}
                            {op.user && (
                              <div className="text-[10px] text-text-muted">
                                <span className="font-semibold">User:</span> {op.user}
                              </div>
                            )}
                            {op.task_id && (
                              <div className="text-[10px] text-text-muted">
                                <span className="font-semibold">Task ID:</span> <span className="font-mono">{op.task_id}</span>
                              </div>
                            )}
                            {op.error && (
                              <div className="text-[10px] text-status-error">
                                <span className="font-semibold">Error:</span> {op.error}
                              </div>
                            )}
                            {op.summary && (
                              <div>
                                <div className="text-[10px] text-text-muted font-semibold mb-1">Summary:</div>
                                <pre className="text-[10px] font-mono text-text-secondary m-0 bg-surface-secondary rounded p-2 overflow-auto max-h-40">
                                  {JSON.stringify(op.summary, null, 2)}
                                </pre>
                              </div>
                            )}
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {!loading && filtered.length === 0 && (
        <div className="text-text-muted text-xs text-center py-8">
          {opFilter ? `No "${opFilter}" operations found.` : 'No journal entries.'}
        </div>
      )}

      {loading && <div className="text-text-muted text-sm">Loading journal...</div>}

      {/* Pagination */}
      {pageCount > 1 && (
        <div className="flex items-center justify-center gap-2 mt-4">
          <button onClick={() => goToPage(offset - PAGE_SIZE)}
            disabled={offset === 0}
            className="text-xs px-2 py-1 rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none cursor-pointer disabled:opacity-50 disabled:cursor-default">
            Prev
          </button>
          <span className="text-xs text-text-muted">
            Page {currentPage} of {pageCount}
          </span>
          <button onClick={() => goToPage(offset + PAGE_SIZE)}
            disabled={offset + PAGE_SIZE >= totalCount}
            className="text-xs px-2 py-1 rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none cursor-pointer disabled:opacity-50 disabled:cursor-default">
            Next
          </button>
        </div>
      )}
    </div>
  )
}
