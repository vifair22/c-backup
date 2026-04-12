import React from 'react'

const api = window.cbackup

interface TaskProgress {
  current: number
  total: number
  phase: string
}

interface TaskInfo {
  task_id: string
  command: string
  pid: number
  started: number
  state: 'running' | 'completed' | 'failed'
  exit_code: number
  error?: string
  alive: boolean
  progress?: TaskProgress
}

interface Props {
  tasks: TaskInfo[]
  connName: string
  repoPath: string
  onViewTasks: () => void
}

export function TaskBar({ tasks, connName, repoPath, onViewTasks }: Props): React.ReactElement | null {
  const running = tasks.filter(t => t.state === 'running' && t.alive)
  const recentFailed = tasks.filter(t => t.state === 'failed' && (Date.now() / 1000 - t.started) < 3600)

  if (running.length === 0 && recentFailed.length === 0) return null

  const active = running[0]
  const pct = active?.progress && active.progress.total > 0
    ? Math.round((active.progress.current / active.progress.total) * 100)
    : null

  const handleCancel = async (taskId: string, e: React.MouseEvent) => {
    e.stopPropagation()
    try {
      await api.rpcCall(connName, repoPath, 'task_cancel', { task_id: taskId })
    } catch { /* ignore */ }
  }

  return (
    <div onClick={onViewTasks}
      className="border-t border-border-default bg-surface-secondary px-3 py-2 cursor-pointer hover:bg-surface-hover flex items-center gap-3 text-xs">
      {running.length > 0 && active ? (
        <>
          <span className="font-medium text-text-primary">{active.command}</span>
          {active.progress?.phase && (
            <span className="text-text-muted">{active.progress.phase}</span>
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
          <button onClick={(e) => handleCancel(active.task_id, e)}
            className="text-[10px] px-1.5 py-0.5 rounded bg-surface-tertiary text-status-error hover:bg-surface-hover border-none cursor-pointer">
            Cancel
          </button>
        </>
      ) : recentFailed.length > 0 ? (
        <span className="text-status-error">
          {recentFailed.length} failed task{recentFailed.length !== 1 ? 's' : ''} — click to view
        </span>
      ) : null}
    </div>
  )
}

export type { TaskInfo }
