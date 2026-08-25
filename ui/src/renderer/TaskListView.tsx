import React, { useState } from 'react'
import { fmtNum, absoluteTime } from './format'
import { ConfirmDialog } from './ConfirmDialog'
import type { Job } from '../shared/api'

const api = window.cbackup

function fmtElapsed(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`
}

const STATE_CONFIG: Record<string, { color: string; bg: string; label: string }> = {
  running:   { color: 'text-blue-400',  bg: 'bg-blue-500/10',  label: 'Running' },
  completed: { color: 'text-green-400', bg: 'bg-green-500/10', label: 'Completed' },
  failed:    { color: 'text-red-400',   bg: 'bg-red-500/10',   label: 'Failed' },
}

interface Props {
  jobs: Job[]
  connName: string
  repoPath: string
  onBack: () => void
}

export function TaskListView({ jobs, connName, repoPath, onBack }: Props): React.ReactElement {
  const [error, setError] = useState<string | null>(null)
  const [confirmCancel, setConfirmCancel] = useState<string | null>(null)

  const handleCancel = async (taskId: string) => {
    setConfirmCancel(null)
    try {
      await api.apiCall(connName, { kind: 'jobCancel', jobId: Number(taskId) })
    } catch (err) {
      setError(String(err))
    }
  }

  const list = jobs ?? []
  const hasRunning = list.some(j => j.state === 'running')

  /* Running first, then newest. The daemon has no start time on a job, so
   * the id stands in for one: they are handed out in order. */
  const sorted = [...list].sort((a, b) => {
    if (a.state === 'running' && b.state !== 'running') return -1
    if (a.state !== 'running' && b.state === 'running') return 1
    return Number(b.id) - Number(a.id)
  })

  return (
    <div>
      {/* Header */}
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Back
        </button>
        <h3 className="text-lg font-medium m-0">Background Tasks</h3>
        {hasRunning && <span className="text-xs text-blue-400 animate-pulse">Active</span>}
      </div>

      {error && <div className="text-status-error text-sm mb-3">{error}</div>}

      {sorted.length > 0 && (
        <div className="space-y-2">
          {sorted.map(t => {
            const cfg = STATE_CONFIG[t.state] ?? STATE_CONFIG.running
            const isRunning = t.state === 'running'
            const pct = Number(t.total ?? 0) > 0
              ? Math.round((Number(t.current ?? 0) / Number(t.total)) * 100)
              : null

            return (
              <div key={t.id}
                className={`border border-border-default rounded-lg p-3 ${cfg.bg}`}>
                {/* Top row: command, state, time */}
                <div className="flex items-center gap-3 mb-1">
                  <span className="text-sm font-semibold text-text-primary capitalize">{t.action}</span>
                  <span className={`text-[10px] font-semibold ${cfg.color}`}>{cfg.label}</span>
                  {/* The daemon reports no start time on a job, so there is
                    * nothing honest to render here. Showing absoluteTime of
                    * undefined printed the epoch. */}
                  <span className="text-[11px] text-text-muted ml-auto">
                    job {t.id}
                  </span>
                </div>

                {/* Progress bar for running tasks */}
                {isRunning && (
                  <div className="flex items-center gap-2 mb-2">
                    {t.phase && (
                      <span className="text-xs text-text-muted w-20 shrink-0">{t.phase}</span>
                    )}
                    {pct !== null ? (
                      <>
                        <div className="flex-1 h-1.5 rounded-full bg-surface-tertiary overflow-hidden">
                          <div className="h-full bg-accent rounded-full transition-all" style={{ width: `${pct}%` }} />
                        </div>
                        <span className="text-[11px] text-text-muted w-8 text-right">{pct}%</span>
                      </>
                    ) : (
                      <div className="flex-1 h-1.5 rounded-full bg-surface-tertiary overflow-hidden">
                        <div className="h-full bg-accent/50 rounded-full animate-pulse w-1/3" />
                      </div>
                    )}
                  </div>
                )}

                {/* Error for failed tasks */}
                {t.state === 'failed' && t.error && (
                  <div className="text-xs text-status-error mb-2">{t.error}</div>
                )}

                {/* Bottom row: details + actions */}
                <div className="flex items-center gap-3 text-[10px] text-text-muted">
                  {/* No pid either: work runs on the daemon's own pool
                    * rather than as a detached process, which is the whole
                    * point of the rewrite. */}
                  {Number(t.total ?? 0) > 0 && (
                    <span>{fmtNum(Number(t.current ?? 0))} / {fmtNum(Number(t.total))}</span>
                  )}
                  {t.error && <span className="text-status-error">{t.error}</span>}
                  {isRunning && (
                    <button onClick={() => setConfirmCancel(t.id)}
                      className="text-[10px] px-2 py-0.5 rounded bg-surface-tertiary text-status-error hover:bg-surface-hover border-none cursor-pointer ml-auto">
                      Cancel
                    </button>
                  )}
                </div>
              </div>
            )
          })}
        </div>
      )}

      {list.length === 0 && (
        <div className="text-text-muted text-xs text-center py-8">No background jobs.</div>
      )}

      {/* Cancel confirmation */}
      {confirmCancel && (
        <ConfirmDialog
          title="Cancel Task"
          message="Are you sure you want to cancel this task? The operation will be terminated."
          confirmLabel="Cancel Task"
          onConfirm={() => handleCancel(confirmCancel)}
          onCancel={() => setConfirmCancel(null)}
        />
      )}
    </div>
  )
}
