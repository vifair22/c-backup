import React, { useState } from 'react'
import { fmtNum, fmtSize, absoluteTime } from './format'
import { ConfirmDialog } from './ConfirmDialog'
import type { Job } from '../shared/api'
import { jobWarnings, jobDisplayState } from '../shared/jobs'

const api = window.cbackup

function fmtElapsed(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`
}

const STATE_CONFIG: Record<string, { color: string; bg: string; label: string }> = {
  running:   { color: 'text-blue-400',  bg: 'bg-blue-500/10',  label: 'Running' },
  completed: { color: 'text-green-400', bg: 'bg-green-500/10', label: 'Completed' },
  warning:   { color: 'text-amber-400', bg: 'bg-amber-500/10', label: 'Completed with warnings' },
  failed:    { color: 'text-red-400',   bg: 'bg-red-500/10',   label: 'Failed' },
}

/* What a backup's result says about its sources in snapshot mode (Manual
 * 6.6): those read from a snapshot, and those in warn mode read live. */
function snapshotReport(t: Job): { snapped: [string, string][]; live: { path: string; reason: string }[] } {
  const r = (t.result ?? {}) as Record<string, unknown>
  const snapped = Object.entries((r.source_snapshots ?? {}) as Record<string, string>)
  const live = Array.isArray(r.live_sources) ? r.live_sources as { path: string; reason: string }[] : []
  return { snapped, live }
}

interface Props {
  jobs: Job[]
  connName: string
  repoPath: string
  onBack: () => void
}

export function TaskListView({ jobs, connName, repoPath, onBack }: Props): React.ReactElement {
  const [error, setError] = useState<string | null>(null)
  const [confirmCancel, setConfirmCancel] = useState<number | null>(null)
  /* Jobs whose every warning is shown; the card shows the first. */
  const [openWarnings, setOpenWarnings] = useState<Set<number>>(new Set())
  const toggleWarnings = (id: number) => setOpenWarnings(prev => {
    const next = new Set(prev)
    if (next.has(id)) next.delete(id); else next.add(id)
    return next
  })

  const handleCancel = async (taskId: number) => {
    setConfirmCancel(null)
    try {
      await api.apiCall(connName, { kind: 'jobCancel', jobId: taskId })
    } catch (err) {
      setError(String(err))
    }
  }

  const list = jobs ?? []
  const hasRunning = list.some(j => j.state === 'running')

  /* Running first, then newest; ids are handed out in order. */
  const sorted = [...list].sort((a, b) => {
    if (a.state === 'running' && b.state !== 'running') return -1
    if (a.state !== 'running' && b.state === 'running') return 1
    return b.id - a.id
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
            const cfg = STATE_CONFIG[jobDisplayState(t)] ?? STATE_CONFIG.running
            const warnings = t.state === 'completed' ? jobWarnings(t) : []
            const allShown = openWarnings.has(t.id)
            const isRunning = t.state === 'running'
            /* Bytes when the job reports them: entry counts crawl through
             * large files and say little about how far along it is. */
            const pct = (t.bytes_total ?? 0) > 0
              ? Math.round(((t.bytes_done ?? 0) / (t.bytes_total ?? 1)) * 100)
              : (t.total ?? 0) > 0
                ? Math.round(((t.current ?? 0) / (t.total ?? 1)) * 100)
                : null

            return (
              <div key={t.id} data-testid="job-card" data-job-id={t.id}
                className={`border border-border-default rounded-lg p-3 ${cfg.bg}`}>
                {/* Top row: command, state, time */}
                <div className="flex items-center gap-3 mb-1">
                  <span className="text-sm font-semibold text-text-primary capitalize">{t.action}</span>
                  <span className={`text-[10px] font-semibold ${cfg.color}`}>{cfg.label}</span>
                  <span className="text-[11px] text-text-muted ml-auto">
                    job {t.id}
                    {t.started ? `, started ${absoluteTime(t.started)}` : ''}
                    {t.elapsed_s ? `, ${fmtElapsed(t.elapsed_s)}` : ''}
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

                {/* What a job that succeeded still needs an operator to
                  * hear (B241): the first warning on the card, every one
                  * on request. */}
                {warnings.length > 0 && (
                  <div className="text-xs mb-2 rounded border border-amber-500/40 bg-amber-500/10 p-2"
                    data-testid="job-warnings">
                    <div className="flex items-center gap-2 mb-1">
                      <span className="font-semibold text-amber-400">
                        {warnings.length === 1 ? 'Warning' : `${warnings.length} warnings`}
                      </span>
                      {warnings.length > 1 && (
                        <button onClick={() => toggleWarnings(t.id)}
                          className="text-[10px] text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer p-0 ml-auto">
                          {allShown ? 'Show first only' : `Show all ${warnings.length}`}
                        </button>
                      )}
                    </div>
                    <ul className="m-0 pl-4 text-text-primary break-words">
                      {(allShown ? warnings : warnings.slice(0, 1)).map((w, i) => (
                        <li key={i} data-testid="job-warning">{w}</li>
                      ))}
                    </ul>
                  </div>
                )}

                {/* The job's error, shown once and in full. Not only for
                  * failed jobs: a cancelled one can carry one too. */}
                {t.error && (
                  <div className="text-xs text-status-error mb-2 break-words"
                    data-testid="job-error">{t.error}</div>
                )}
                {/* A strict snapshot-mode source that could not be read
                  * from a snapshot failed the backup before anything was
                  * read; say what the operator can do about it. */}
                {t.state === 'failed' && t.error?.includes('strict snapshot mode') && (
                  <div className="text-xs text-text-secondary mb-2" data-testid="snapshot-strict-hint">
                    Nothing was backed up. Fix the reason given, or set that source to Warn in the
                    backup dialog to read it live instead (Manual 6.6).
                  </div>
                )}
                {(() => {
                  const { snapped, live } = snapshotReport(t)
                  return (
                    <>
                      {live.length > 0 && (
                        <div className="text-xs mb-2 rounded border border-amber-500/40 bg-amber-500/10 p-2"
                          data-testid="snapshot-live-sources">
                          <div className="font-semibold text-amber-400 mb-1">
                            Read live, not from a snapshot ({live.length})
                          </div>
                          <ul className="m-0 pl-4">
                            {live.map(l => (
                              <li key={l.path}>
                                <span className="font-mono">{l.path}</span>: {l.reason}
                              </li>
                            ))}
                          </ul>
                        </div>
                      )}
                      {snapped.length > 0 && (
                        <div className="text-[11px] text-text-muted mb-2" data-testid="snapshot-sources">
                          Read from a snapshot: {snapped.map(([p, k]) => `${p} (${k})`).join(', ')}
                        </div>
                      )}
                    </>
                  )
                })()}

                {/* Bottom row: details + actions */}
                <div className="flex items-center gap-3 text-[10px] text-text-muted">
                  {/* No pid either: work runs on the daemon's own pool
                    * rather than as a detached process, which is the whole
                    * point of the rewrite. */}
                  {(t.total ?? 0) > 0 && (
                    <span>{fmtNum(t.current ?? 0)} / {fmtNum(t.total)}</span>
                  )}
                  {(t.bytes_total ?? 0) > 0 && (
                    <span>{fmtSize(t.bytes_done ?? 0)} / {fmtSize(t.bytes_total)}</span>
                  )}
                  {isRunning && (t.rate_bytes_per_s ?? 0) > 0 && (
                    <span>{fmtSize(t.rate_bytes_per_s)}/s</span>
                  )}
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
