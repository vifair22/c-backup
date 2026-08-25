import React from 'react'
import type { Job } from '../shared/api'

const api = window.cbackup


interface Props {
  jobs: Job[]
  connName: string
  repoPath: string
  onViewTasks: () => void
}

export function TaskBar({ jobs, connName, repoPath, onViewTasks }: Props): React.ReactElement | null {
  /*
   * Defaulted, not assumed.
   *
   * This component renders on every repository screen, so an unexpected
   * response here does not degrade a panel - it blanks the window. It did:
   * the poll read `tasks` from a response whose array is called `jobs`, set
   * undefined, and this line threw on every repository that was opened.
   */
  const list = jobs ?? []
  const running = list.filter(j => j.state === 'running')
  const recentFailed = list.filter(j => j.state === 'failed')

  if (running.length === 0 && recentFailed.length === 0) return null

  const active = running[0]
  /* current and total arrive as strings from the list route and numbers
   * from the detail one, so both are coerced rather than trusted. */
  const cur = Number(active?.current ?? 0)
  const tot = Number(active?.total ?? 0)
  const pct = tot > 0 ? Math.round((cur / tot) * 100) : null

  const handleCancel = async (taskId: string, e: React.MouseEvent) => {
    e.stopPropagation()
    try {
      await api.apiCall(connName, { kind: 'jobCancel', jobId: Number(taskId) })
    } catch { /* ignore */ }
  }

  return (
    <div onClick={onViewTasks}
      className="border-t border-border-default bg-surface-secondary px-3 py-2 cursor-pointer hover:bg-surface-hover flex items-center gap-3 text-xs">
      {running.length > 0 && active ? (
        <>
          <span className="font-medium text-text-primary">{active.action}</span>
          {active.phase && (
            <span className="text-text-muted">{active.phase}</span>
          )}
          {pct !== null ? (
            <div className="flex-1 flex items-center gap-2">
              <div className="flex-1 h-1.5 rounded-full bg-surface-tertiary overflow-hidden">
                <div className="h-full bg-accent rounded-full transition-all" style={{ width: `${pct}%` }} />
              </div>
              <span className="text-text-muted w-8 text-right">{pct}%</span>
            </div>
          ) : (
            <div className="flex-1 h-1.5 rounded-full bg-surface-tertiary overflow-hidden">
              <div className="h-full bg-accent/50 rounded-full animate-pulse w-1/3" />
            </div>
          )}
          {running.length > 1 && (
            <span className="text-text-muted">+{running.length - 1} more</span>
          )}
          <button onClick={(e) => handleCancel(active.id, e)}
            className="text-[10px] px-1.5 py-0.5 rounded bg-surface-tertiary text-status-error hover:bg-surface-hover border-none cursor-pointer">
            Cancel
          </button>
        </>
      ) : recentFailed.length > 0 ? (
        <span className="text-status-error">
          {recentFailed.length} failed job{recentFailed.length !== 1 ? 's' : ''} - click to view
        </span>
      ) : null}
    </div>
  )
}


