import React, { useCallback, useEffect, useState } from 'react'
import { fmtNum } from './format'

const api = window.cbackup

const PAGE_SIZE = 50

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

const RESULT_BADGE: Record<string, { color: string; label: string }> = {
  ok: { color: 'bg-green-600 text-white', label: 'ok' },
  error: { color: 'bg-red-600 text-white', label: 'error' },
  cancelled: { color: 'bg-amber-600 text-white', label: 'cancelled' },
}

const STATE_BADGE: Record<string, { color: string; label: string }> = {
  started: { color: 'bg-blue-600 text-white', label: 'running' },
  completed: { color: 'bg-green-600 text-white', label: 'done' },
  orphan: { color: 'bg-red-600 text-white', label: 'crashed' },
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

const OPERATIONS = ['backup', 'gc', 'pack', 'prune', 'verify', 'restore', 'init', 'policy']

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
  const [expandedId, setExpandedId] = useState<string | null>(null)

  const fetchPage = useCallback(async (pageOffset: number, operation: OpFilter) => {
    setLoading(true)
    setError(null)
    try {
      const params: Record<string, unknown> = { offset: pageOffset, limit: PAGE_SIZE }
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
    fetchPage(0, opFilter)
  }, [connName, repoPath, opFilter])

  const goToPage = (newOffset: number) => {
    setOffset(newOffset)
    setExpandedId(null)
    fetchPage(newOffset, opFilter)
  }

  const changeFilter = (op: OpFilter) => {
    setOpFilter(op)
    setOffset(0)
    setExpandedId(null)
  }

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
          <span className="text-xs text-status-error">{orphanCount} crashed operation{orphanCount !== 1 ? 's' : ''}</span>
        )}
      </div>

      {/* Filter bar */}
      <div className="flex flex-wrap gap-2 mb-4">
        <button onClick={() => changeFilter(null)}
          className={`text-[11px] px-2 py-0.5 rounded cursor-pointer border-none ${
            opFilter === null ? 'bg-accent text-accent-text' : 'bg-surface-tertiary text-text-secondary hover:bg-surface-hover'
          }`}>
          All
        </button>
        {OPERATIONS.map(op => (
          <button key={op} onClick={() => changeFilter(opFilter === op ? null : op)}
            className={`text-[11px] px-2 py-0.5 rounded cursor-pointer border-none ${
              opFilter === op ? 'bg-accent text-accent-text' : 'bg-surface-tertiary text-text-secondary hover:bg-surface-hover'
            }`}>
            {op}
          </button>
        ))}
      </div>

      {error && <div className="text-status-error text-sm mb-3">{error}</div>}

      {/* Table */}
      {!loading && entries.length > 0 && (
        <div className="bg-surface-secondary border border-border-default rounded-lg overflow-hidden">
          <table className="w-full text-xs table-fixed">
            <thead>
              <tr className="text-text-muted text-left">
                <th className="font-medium pb-2 pt-4 pl-4 w-36">Time</th>
                <th className="font-medium pb-2 pt-4 w-20">Operation</th>
                <th className="font-medium pb-2 pt-4 w-16">Status</th>
                <th className="font-medium pb-2 pt-4 w-20">Duration</th>
                <th className="font-medium pb-2 pt-4">Details</th>
                <th className="font-medium pb-2 pt-4 w-16 pr-4">Source</th>
              </tr>
            </thead>
            <tbody>
              {entries.map(e => {
                const isExpanded = expandedId === e.op_id
                const resultInfo = e.result ? RESULT_BADGE[e.result] : null
                const stateInfo = STATE_BADGE[e.state]
                const badge = e.state === 'completed' && resultInfo ? resultInfo : stateInfo

                return (
                  <React.Fragment key={e.op_id + e.state}>
                    <tr onClick={() => setExpandedId(isExpanded ? null : e.op_id)}
                      className="border-t border-border-default cursor-pointer hover:bg-surface-hover">
                      <td className="py-1.5 pl-4 text-text-muted">{fmtTimestamp(e.timestamp)}</td>
                      <td className="py-1.5 font-medium">{e.operation}</td>
                      <td className="py-1.5">
                        {badge && (
                          <span className={`inline-block px-1.5 py-0 rounded text-[10px] font-semibold ${badge.color}`}>
                            {badge.label}
                          </span>
                        )}
                      </td>
                      <td className="py-1.5 text-text-muted">
                        {e.duration_ms ? fmtDuration(e.duration_ms) : ''}
                      </td>
                      <td className="py-1.5 truncate text-text-muted" title={e.error || ''}>
                        {e.error && <span className="text-status-error">{e.error}</span>}
                        {e.signal && <span className="text-status-error">signal {e.signal}</span>}
                      </td>
                      <td className="py-1.5 pr-4 text-text-muted">{e.source || ''}</td>
                    </tr>
                    {isExpanded && (
                      <tr>
                        <td colSpan={6} className="p-0">
                          <div className="px-4 py-3 bg-surface-primary border-t border-border-default space-y-1">
                            <div className="text-[10px] text-text-muted">
                              <span className="font-semibold">Op ID:</span> <span className="font-mono">{e.op_id}</span>
                            </div>
                            {e.user && (
                              <div className="text-[10px] text-text-muted">
                                <span className="font-semibold">User:</span> {e.user}
                              </div>
                            )}
                            {e.task_id && (
                              <div className="text-[10px] text-text-muted">
                                <span className="font-semibold">Task ID:</span> <span className="font-mono">{e.task_id}</span>
                              </div>
                            )}
                            {e.error && (
                              <div className="text-[10px] text-status-error">
                                <span className="font-semibold">Error:</span> {e.error}
                              </div>
                            )}
                            {e.summary && (
                              <div>
                                <div className="text-[10px] text-text-muted font-semibold mb-1">Summary:</div>
                                <pre className="text-[10px] font-mono text-text-secondary m-0 bg-surface-secondary rounded p-2 overflow-auto max-h-40">
                                  {JSON.stringify(e.summary, null, 2)}
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

      {!loading && entries.length === 0 && (
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
