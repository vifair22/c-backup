/*
 * Is the daemon well.
 *
 * The CLI has had `health` and `metrics` since the rewrite and the app had
 * no equivalent, which meant the single most important number in a backup
 * system - how long since anything was backed up - was reachable from
 * Prometheus and from a terminal, and from nowhere a person would look.
 *
 * Everything here is aggregate across the connection rather than scoped to
 * one repository, because that is how the daemon reports it and because the
 * question is about the daemon.
 *
 * /metrics is Prometheus exposition format, so it is parsed rather than
 * decoded. That is a deliberately small parser: name, optional labels,
 * value, one per line, ignoring HELP and TYPE. It is not a general
 * Prometheus client and does not need to be.
 */

import React, { useCallback, useEffect, useState } from 'react'
import { fmtNum, fmtSize } from './format'

const api = window.cbackup

/* Refreshed while the screen is open. Slow enough to be free, fast enough
 * that a running job's count moves while somebody watches it. */
const POLL_MS = 5000

interface Health {
  ok: boolean
  ready: boolean
  version: string
  error?: string
}

function parseMetrics(text: string): Record<string, number> {
  const out: Record<string, number> = {}
  for (const line of text.split('\n')) {
    if (!line || line.startsWith('#')) continue
    /* name{labels} value, or name value. Only the unlabelled scalars are
     * used here; a labelled series would need a key per label set. */
    const m = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(-?[\d.eE+]+)\s*$/)
    if (!m || m[2]) continue
    const v = Number(m[3])
    if (Number.isFinite(v)) out[m[1]] = v
  }
  return out
}

function fmtAge(seconds: number | undefined): string {
  if (seconds === undefined) return '-'
  if (seconds < 60) return `${Math.round(seconds)}s ago`
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h ago`
  return `${Math.round(seconds / 86400)}d ago`
}

/*
 * How stale is too stale.
 *
 * A day is the point at which a daily backup has plainly not run, and a week
 * is the point at which nobody is watching. Both are judgements rather than
 * facts, which is why the number itself is always shown next to the verdict.
 */
const SNAPSHOT_WARN = 86400
const SNAPSHOT_BAD = 604800

interface Props {
  connName: string
  onBack: () => void
}

export function DaemonStatus({ connName, onBack }: Props): React.ReactElement {
  const [health, setHealth] = useState<Health | null>(null)
  const [metrics, setMetrics] = useState<Record<string, number>>({})
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  const load = useCallback(async () => {
    try {
      /* Liveness and readiness are different questions and readiness is the
       * one that fails in practice: the process stays up while the database
       * stops answering. Both are asked. */
      const [live, ready, m] = await Promise.all([
        api.apiCall<{ status?: string }>(connName, { kind: 'health' }).catch(() => null),
        api.apiCall<{ status?: string; version?: string }>(connName, { kind: 'ready' })
          .catch((e: unknown) => ({ status: 'error', version: '', error: String(e) })),
        api.apiCall<{ text?: string }>(connName, { kind: 'metrics' }).catch(() => ({ text: '' })),
      ])
      setHealth({
        ok: live?.status === 'ok',
        ready: ready?.status === 'ready',
        version: ready?.version ?? '',
        error: (ready as { error?: string })?.error,
      })
      setMetrics(parseMetrics(m.text ?? ''))
      setError(null)
    } catch (err) {
      setError(String(err))
    }
    setLoading(false)
  }, [connName])

  useEffect(() => {
    void load()
    const t = setInterval(() => void load(), POLL_MS)
    return () => clearInterval(t)
  }, [load])

  const m = metrics
  const snapAge = m['cbackup_last_snapshot_age_seconds']
  const verifyAge = m['cbackup_last_verify_age_seconds']
  const unescrowed = m['cbackup_repos_key_unescrowed'] ?? 0
  const failed = m['cbackup_jobs_failed_total'] ?? 0
  const unrecoverable = m['cbackup_verify_unrecoverable_objects'] ?? 0
  const repaired = m['cbackup_verify_repaired_objects'] ?? 0

  const snapClass = snapAge === undefined ? 'text-text-muted'
    : snapAge > SNAPSHOT_BAD ? 'text-status-error'
    : snapAge > SNAPSHOT_WARN ? 'text-status-warning'
    : 'text-status-connected'

  /*
   * What is actually wrong, said once at the top. These are the conditions
   * worth interrupting somebody over; everything below is reference.
   */
  const alarms: { text: string; bad: boolean }[] = []
  if (health && !health.ready) {
    alarms.push({ text: health.error ?? 'The daemon is up but not ready: its database is not answering.', bad: true })
  }
  if (unrecoverable > 0) {
    alarms.push({ text: `${fmtNum(unrecoverable)} object(s) could not be recovered by the last scrub. Data is lost unless another copy has them.`, bad: true })
  }
  if (snapAge !== undefined && snapAge > SNAPSHOT_BAD) {
    alarms.push({ text: `Nothing has been backed up in ${fmtAge(snapAge)}.`, bad: true })
  } else if (snapAge !== undefined && snapAge > SNAPSHOT_WARN) {
    alarms.push({ text: `The newest snapshot is ${fmtAge(snapAge)}.`, bad: false })
  }
  if (unescrowed > 0) {
    alarms.push({ text: `${fmtNum(unescrowed)} encrypted repositor${unescrowed === 1 ? 'y has' : 'ies have'} a key that exists only on this machine.`, bad: false })
  }
  if (repaired > 0) {
    alarms.push({ text: `Parity repaired ${fmtNum(repaired)} object(s). The data survived, but a disk returned bytes it was not given.`, bad: false })
  }

  return (
    <div>
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Back
        </button>
        <h3 className="text-lg font-medium m-0">Daemon</h3>
        {health && (
          <span className={`text-xs ${health.ready ? 'text-status-connected' : 'text-status-error'}`}>
            {health.ready ? 'ready' : health.ok ? 'up, not ready' : 'not answering'}
          </span>
        )}
        {health?.version && <span className="text-xs text-text-muted">{health.version}</span>}
      </div>

      {error && <div className="text-status-error text-sm mb-3">{error}</div>}
      {loading && <div className="text-text-muted text-sm">Loading...</div>}

      {!loading && alarms.length > 0 && (
        <div className="mb-4 flex flex-col gap-2" style={{ maxWidth: '44rem' }}>
          {alarms.map((a, i) => (
            <div key={i} className={`text-xs p-2 rounded border ${
              a.bad ? 'bg-error-bg text-error-text border-error-border'
                    : 'bg-surface-secondary text-status-warning border-status-warning'}`}>
              {a.text}
            </div>
          ))}
        </div>
      )}

      {!loading && alarms.length === 0 && (
        <div className="mb-4 text-xs text-status-connected">
          Nothing to report. Backups are current, the last scrub was clean, and
          no key is unaccounted for.
        </div>
      )}

      {!loading && (
        <div className="grid gap-4" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(14rem, 1fr))', maxWidth: '60rem' }}>
          <Panel title="Backups">
            <Row label="Newest snapshot" value={fmtAge(snapAge)} cls={snapClass} />
            <Row label="Last scrub" value={fmtAge(verifyAge)} />
            <Row label="Snapshots" value={fmtNum(m['cbackup_snapshots_total'] ?? 0)} />
            <Row label="Repositories" value={fmtNum(m['cbackup_repos_total'] ?? 0)} />
            <Row label="Schedules enabled" value={fmtNum(m['cbackup_schedules_enabled'] ?? 0)} />
          </Panel>

          <Panel title="Work">
            <Row label="Running" value={fmtNum(m['cbackup_jobs_running'] ?? 0)} />
            <Row label="Queued" value={fmtNum(m['cbackup_jobs_queued'] ?? 0)} />
            <Row label="Completed" value={fmtNum(m['cbackup_jobs_completed_total'] ?? 0)} />
            <Row label="Failed" value={fmtNum(failed)}
                 cls={failed > 0 ? 'text-status-error' : undefined} />
          </Panel>

          <Panel title="Storage">
            {/* On disk, not logical: header and parity trailer included,
              * because that is what fills the volume. */}
            <Row label="On disk" value={fmtSize(m['cbackup_objects_bytes'] ?? 0)} />
            <Row label="Objects" value={fmtNum(m['cbackup_objects_total'] ?? 0)} />
            <Row label="Indexed nodes" value={fmtNum(m['cbackup_nodes_total'] ?? 0)} />
          </Panel>

          <Panel title="Encryption">
            <Row label="Encrypted repos" value={fmtNum(m['cbackup_repos_encrypted'] ?? 0)} />
            <Row label="Key not saved elsewhere" value={fmtNum(unescrowed)}
                 cls={unescrowed > 0 ? 'text-status-error' : undefined} />
          </Panel>
        </div>
      )}
    </div>
  )
}

function Panel({ title, children }: { title: string; children: React.ReactNode }): React.ReactElement {
  return (
    <div className="bg-surface-secondary border border-border-default rounded-lg p-4">
      <div className="text-[11px] text-text-muted uppercase tracking-wide mb-2">{title}</div>
      <div className="flex flex-col gap-1">{children}</div>
    </div>
  )
}

function Row({ label, value, cls }: { label: string; value: string; cls?: string }): React.ReactElement {
  return (
    <div className="flex justify-between items-baseline text-xs gap-4">
      <span className="text-text-secondary">{label}</span>
      <span className={`font-mono ${cls ?? 'text-text-primary'}`}>{value}</span>
    </div>
  )
}
