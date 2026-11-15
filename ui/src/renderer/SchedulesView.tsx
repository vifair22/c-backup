/*
 * What this repository does on its own.
 *
 * A backup that runs when somebody remembers is not a backup strategy, and
 * until now the app had no way to arrange one at all: schedules were
 * reachable from the CLI and from nothing else.
 *
 * Built on the typed union rather than the action-string bridge, so every
 * call here is one the contract suite proves reaches a real route. That
 * matters more than usual on this screen - a schedule that quietly fails is
 * indistinguishable from one that has not fired yet, which is exactly how
 * scheduled backups managed to never run while showing as enabled.
 */

import React, { useCallback, useEffect, useState } from 'react'
import { ConfirmDialog } from './ConfirmDialog'
import type { MaintenanceOp } from '../shared/api'

const api = window.cbackup

interface Trigger {
  type?: string
  seconds?: number
  every?: string
  at?: string
  on?: string
  on_day?: number
  on_month?: number
}

interface ScheduleParams {
  dest?: string
  paths?: string[]
  pattern?: string
}

/* trigger and params arrive as objects. They were strings of JSON until
 * the daemon stopped double-encoding them; a string is still accepted so
 * an older daemon renders too. id and next_due are numbers and enabled a
 * boolean (B008); this compared enabled with '1'. */
interface Schedule {
  id: number
  action: string
  trigger: Trigger | string
  overlap: string
  catchup: string
  enabled: boolean
  next_due: number
  next_due_iso?: string
  params?: ScheduleParams | string
}

function asObject<T>(v: T | string | undefined): T | null {
  if (v === undefined || v === '') return null
  if (typeof v !== 'string') return v
  try { return JSON.parse(v) as T } catch { return null }
}

interface Props {
  connName: string
  repoPath: string
  onBack: () => void
}

/*
 * What can be scheduled, and what each one is for.
 *
 * The daemon refuses an action it cannot dispatch, so this list exists to
 * keep the operator from reaching that refusal rather than to decide
 * anything. Restore is absent for the same reason it is absent there: a
 * restore on a timer repeatedly overwrites a tree.
 */
const ACTIONS: { value: 'backup' | MaintenanceOp; label: string; hint: string }[] = [
  { value: 'backup',  label: 'Backup',          hint: 'Back up the configured sources' },
  { value: 'verify',  label: 'Verify',          hint: 'Prove stored objects still read back' },
  { value: 'gfs',     label: 'Apply retention', hint: 'Drop snapshots the policy no longer keeps' },
  { value: 'gc',      label: 'Garbage collect', hint: 'Reclaim space from unreferenced objects' },
  { value: 'pack',    label: 'Pack',            hint: 'Coalesce loose objects' },
  { value: 'reindex', label: 'Reindex',         hint: 'Rebuild the index from the repository' },
]

/* Common cadences, so the usual case is a click rather than a cron string. */
const CADENCES: { label: string; seconds: number }[] = [
  { label: 'Every hour',  seconds: 3600 },
  { label: 'Every 6 hours', seconds: 21600 },
  { label: 'Daily',       seconds: 86400 },
  { label: 'Weekly',      seconds: 604800 },
]

function describeTrigger(raw: Trigger | string): string {
  const t = asObject<Trigger>(raw)
  if (t?.type === 'interval' && typeof t.seconds === 'number') {
    const known = CADENCES.find(c => c.seconds === t.seconds)
    if (known) return known.label
    if (t.seconds % 86400 === 0) return `Every ${t.seconds / 86400} days`
    if (t.seconds % 3600 === 0) return `Every ${t.seconds / 3600} hours`
    return `Every ${t.seconds}s`
  }
  if (t?.type === 'calendar' && t.at) {
    if (t.every === 'day') return `Daily at ${t.at}`
    if (t.every === 'week') return `Weekly on ${t.on ?? '?'} at ${t.at}`
    if (t.every === 'month') return `Monthly on day ${t.on_day ?? '?'} at ${t.at}`
    if (t.every === 'year') return `Yearly on ${t.on_month ?? '?'}/${t.on_day ?? '?'} at ${t.at}`
  }
  /* A trigger the daemon stored and this cannot read is still worth
   * showing: the raw form tells an operator more than "unknown" does. */
  return typeof raw === 'string' ? raw : JSON.stringify(raw)
}

/* The destination, sources or pattern a schedule carries, if any. */
function describeParams(raw?: ScheduleParams | string): string | null {
  const p = asObject<ScheduleParams>(raw)
  if (!p) return null
  if (p.dest) return p.dest
  if (p.paths?.length) return p.paths.join(', ')
  if (p.pattern) return `/${p.pattern}/`
  return null
}

export function SchedulesView({ connName, repoPath, onBack }: Props): React.ReactElement {
  const repoId = Number(repoPath)

  const [schedules, setSchedules] = useState<Schedule[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [confirmRemove, setConfirmRemove] = useState<Schedule | null>(null)

  const [adding, setAdding] = useState(false)
  const [newAction, setNewAction] = useState<string>('backup')
  const [newSeconds, setNewSeconds] = useState(86400)
  const [busy, setBusy] = useState(false)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const r = await api.apiCall<{ schedules?: Schedule[] }>(connName, {
        kind: 'scheduleList', repoId,
      })
      setSchedules(r.schedules ?? [])
    } catch (err) {
      setError(String(err))
    }
    setLoading(false)
  }, [connName, repoId])

  useEffect(() => { void load() }, [load])

  const handleAdd = async () => {
    setBusy(true)
    setError(null)
    try {
      await api.apiCall(connName, {
        kind: 'scheduleAdd',
        repoId,
        action: newAction,
        trigger: { type: 'interval', seconds: newSeconds },
      })
      setAdding(false)
      await load()
    } catch (err) {
      /*
       * The daemon refuses a backup schedule for a repository with no
       * sources, and refuses an action it cannot dispatch. Both are worth
       * showing verbatim: they say what to do next, which a generic failure
       * would not.
       */
      setError(String(err))
    }
    setBusy(false)
  }

  const handleToggle = async (s: Schedule) => {
    setError(null)
    try {
      await api.apiCall(connName, {
        kind: 'scheduleSetEnabled',
        repoId,
        scheduleId: s.id,
        enabled: !s.enabled,
      })
      await load()
    } catch (err) {
      setError(String(err))
    }
  }

  const handleRemove = async (s: Schedule) => {
    setConfirmRemove(null)
    setError(null)
    try {
      await api.apiCall(connName, {
        kind: 'scheduleRemove', repoId, scheduleId: s.id,
      })
      await load()
    } catch (err) {
      setError(String(err))
    }
  }

  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="flex items-center gap-2 mb-3">
        <button onClick={onBack}
          className="px-2 py-1 text-xs cursor-pointer rounded bg-surface-tertiary
                     text-text-secondary hover:bg-surface-hover border-none">
          Back
        </button>
        <h2 className="text-sm font-semibold m-0">Schedules</h2>
        <div className="flex-1" />
        {!adding && (
          <button onClick={() => { setAdding(true); setError(null) }}
            className="px-2 py-1 text-xs cursor-pointer rounded bg-accent text-accent-text border-none">
            Add schedule
          </button>
        )}
      </div>

      {error && (
        <div className="text-xs mb-3 p-2 rounded bg-error-bg text-error-text border border-error-border">
          {error}
        </div>
      )}

      {adding && (
        <div className="mb-3 p-3 rounded bg-surface-secondary border border-border-default">
          <div className="flex gap-2 items-end flex-wrap">
            <label className="text-xs text-text-secondary">
              <div className="mb-1">Action</div>
              <select value={newAction} onChange={e => setNewAction(e.target.value)}
                className="text-xs p-1 rounded bg-surface-tertiary text-text-primary
                           border border-border-default">
                {ACTIONS.map(a => <option key={a.value} value={a.value}>{a.label}</option>)}
              </select>
            </label>
            <label className="text-xs text-text-secondary">
              <div className="mb-1">How often</div>
              <select value={newSeconds} onChange={e => setNewSeconds(Number(e.target.value))}
                className="text-xs p-1 rounded bg-surface-tertiary text-text-primary
                           border border-border-default">
                {CADENCES.map(c => <option key={c.seconds} value={c.seconds}>{c.label}</option>)}
              </select>
            </label>
            <button onClick={handleAdd} disabled={busy}
              className="px-2 py-1 text-xs cursor-pointer rounded bg-accent text-accent-text border-none">
              {busy ? 'Adding...' : 'Add'}
            </button>
            <button onClick={() => setAdding(false)} disabled={busy}
              className="px-2 py-1 text-xs cursor-pointer rounded bg-surface-tertiary
                         text-text-secondary border-none">
              Cancel
            </button>
          </div>
          <div className="text-xs text-text-muted mt-2">
            {ACTIONS.find(a => a.value === newAction)?.hint}
            {newAction === 'backup' && ' - set them from the backup dialog first.'}
          </div>
        </div>
      )}

      {loading ? (
        <div className="text-xs text-text-muted">Loading...</div>
      ) : schedules.length === 0 ? (
        <div className="text-xs text-text-muted">
          Nothing scheduled. A backup that runs only when somebody remembers is
          not a backup strategy.
        </div>
      ) : (
        <div className="overflow-auto">
          <table className="w-full text-xs border-collapse">
            <thead>
              <tr className="text-text-secondary text-left">
                <th className="py-1 pr-3 font-medium">Action</th>
                <th className="py-1 pr-3 font-medium">When</th>
                <th className="py-1 pr-3 font-medium">Target</th>
                <th className="py-1 pr-3 font-medium">State</th>
                <th className="py-1 font-medium" />
              </tr>
            </thead>
            <tbody>
              {schedules.map(s => {
                const target = describeParams(s.params)
                return (
                  <tr key={s.id} className="border-t border-border-subtle">
                    <td className="py-1.5 pr-3">{s.action}</td>
                    <td className="py-1.5 pr-3" title={s.next_due_iso ? `next: ${s.next_due_iso}` : undefined}>{describeTrigger(s.trigger)}</td>
                    <td className="py-1.5 pr-3 font-mono truncate max-w-[18rem]">
                      {target ?? <span className="text-text-muted">-</span>}
                    </td>
                    <td className="py-1.5 pr-3">
                      {s.enabled
                        ? <span className="text-status-connected">enabled</span>
                        : <span className="text-text-muted">paused</span>}
                    </td>
                    <td className="py-1.5 text-right whitespace-nowrap">
                      <button onClick={() => handleToggle(s)}
                        className="underline cursor-pointer bg-transparent border-none
                                   text-accent p-0 text-xs mr-3">
                        {s.enabled ? 'Pause' : 'Resume'}
                      </button>
                      <button onClick={() => setConfirmRemove(s)}
                        className="underline cursor-pointer bg-transparent border-none
                                   text-status-error p-0 text-xs">
                        Remove
                      </button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {confirmRemove && (
        <ConfirmDialog
          title="Remove schedule"
          message={`Stop running ${confirmRemove.action} on this repository?`}
          confirmLabel="Remove"
          danger
          onConfirm={() => handleRemove(confirmRemove)}
          onCancel={() => setConfirmRemove(null)}
        />
      )}
    </div>
  )
}
