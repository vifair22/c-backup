import React, { useCallback, useEffect, useState } from 'react'
import { fmtNum, absoluteTime } from './format'
import { ConfirmDialog } from './ConfirmDialog'

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

interface TaskListResponse {
  tasks: TaskInfo[]
}

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
  connName: string
  repoPath: string
  onBack: () => void
}

export function TaskListView({ connName, repoPath, onBack }: Props): React.ReactElement {
  const [tasks, setTasks] = useState<TaskInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [confirmCancel, setConfirmCancel] = useState<string | null>(null)

  const refresh = useCallback(async () => {
    try {
      const resp = await api.rpcCall<TaskListResponse>(connName, repoPath, 'task_list')
      setTasks(resp.tasks)
    } catch (err) {
      setError(String(err))
    }
    setLoading(false)
  }, [connName, repoPath])

  useEffect(() => {
    refresh()
    const interval = setInterval(refresh, 2000)
    return () => clearInterval(interval)
  }, [refresh])

  const handleCancel = async (taskId: string) => {
    setConfirmCancel(null)
    try {
      await api.rpcCall(connName, repoPath, 'task_cancel', { task_id: taskId })
      await refresh()
    } catch (err) {
      setError(String(err))
    }
  }

  const hasRunning = tasks.some(t => t.state === 'running' && t.alive)

  // Sort: running first, then by started desc
  const sorted = [...tasks].sort((a, b) => {
    if (a.state === 'running' && b.state !== 'running') return -1
    if (a.state !== 'running' && b.state === 'running') return 1
    return b.started - a.started
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
      {loading && <div className="text-text-muted text-sm">Loading tasks...</div>}

      {!loading && sorted.length > 0 && (
        <div className="space-y-2">
          {sorted.map(t => {
            const cfg = STATE_CONFIG[t.state] ?? STATE_CONFIG.running
            const isRunning = t.state === 'running' && t.alive
            const pct = t.progress && t.progress.total > 0
              ? Math.round((t.progress.current / t.progress.total) * 100)
              : null

            return (
              <div key={t.task_id}
                className={`border border-border-default rounded-lg p-3 ${cfg.bg}`}>
                {/* Top row: command, state, time */}
                <div className="flex items-center gap-3 mb-1">
                  <span className="text-sm font-semibold text-text-primary capitalize">{t.command}</span>
                  <span className={`text-[10px] font-semibold ${cfg.color}`}>{cfg.label}</span>
                  <span className="text-[11px] text-text-muted ml-auto">
                    {absoluteTime(t.started)}
                    {isRunning && (
                      <span className="ml-2">{fmtElapsed(Date.now() / 1000 - t.started)}</span>
                    )}
                  </span>
                </div>

                {/* Progress bar for running tasks */}
                {isRunning && (
                  <div className="flex items-center gap-2 mb-2">
                    {t.progress?.phase && (
                      <span className="text-xs text-text-muted w-20 shrink-0">{t.progress.phase}</span>
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
                  <span className="font-mono">{t.task_id.slice(0, 12)}</span>
                  <span>PID {t.pid}</span>
                  {t.progress && t.progress.total > 0 && (
                    <span>{fmtNum(t.progress.current)} / {fmtNum(t.progress.total)}</span>
                  )}
                  {isRunning && (
                    <button onClick={() => setConfirmCancel(t.task_id)}
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

      {!loading && tasks.length === 0 && (
        <div className="text-text-muted text-xs text-center py-8">No background tasks.</div>
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
