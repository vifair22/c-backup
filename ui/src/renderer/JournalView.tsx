/*
 * What this repository has done, and how it went.
 *
 * Rewritten rather than migrated. The old screen merged pairs of journal
 * rows - a "started" entry and a "completed" one sharing an op_id - into a
 * single operation, and reported orphans where a start had no finish. That
 * format is gone: the journal is now one row per finished operation, with
 * its result and how long it took. The merging machinery had nothing to
 * merge and every field it keyed on (op_id, state, duration_ms, summary)
 * was undefined, so the screen rendered rows of blanks.
 *
 * The daemon writes a journal row per operation whether it was run from the
 * CLI, the app or the scheduler, which is what makes this the one place that
 * answers "did the nightly backup actually run".
 */

import React, { useCallback, useEffect, useState } from 'react'
import { absoluteTime, relativeTime } from './format'

const api = window.cbackup

/* Every integer arrives as a string; duration_ms is empty when the daemon
 * recorded none, which is not the same as zero. */
interface RawEntry {
  id?: string
  ts?: string
  source?: string
  operation?: string
  result?: string
  duration_ms?: string
  message?: string
}

interface Entry {
  id: number
  ts: number
  source: string
  operation: string
  result: string
  durationMs: number | null
  message: string
}

const parse = (r: RawEntry, i: number): Entry => ({
  id: Number(r.id ?? i),
  ts: Number(r.ts ?? '0'),
  source: r.source ?? '',
  operation: r.operation ?? '',
  result: r.result ?? '',
  durationMs: r.duration_ms ? Number(r.duration_ms) : null,
  message: r.message ?? '',
})

const PAGE = 200

/*
 * The exact strings the job manager writes, not a guess at them: it records
 * "success", "cancelled" or "failed", and the backup path adds "warning" for
 * an unescrowed key. Filtering happens daemon-side - the route takes a
 * result - so a value that does not match spells an empty list rather than
 * an error, which is why these are copied from the writer.
 */
const RESULTS = ['success', 'failed', 'cancelled'] as const

function resultClass(result: string): string {
  if (result === 'success') return 'text-status-connected'
  if (result === 'failed') return 'text-status-error'
  if (result === 'cancelled' || result === 'warning') return 'text-status-warning'
  return 'text-text-muted'
}

function fmtDuration(ms: number | null): string {
  if (ms === null) return '-'
  if (ms < 1000) return `${ms} ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`
  const mins = Math.floor(ms / 60_000)
  const secs = Math.round((ms % 60_000) / 1000)
  return `${mins}m ${secs}s`
}

interface Props {
  connName: string
  repoPath: string
  onBack: () => void
}

export function JournalView({ connName, repoPath, onBack }: Props): React.ReactElement {
  const repoId = Number(repoPath)

  const [entries, setEntries] = useState<Entry[]>([])
  const [filter, setFilter] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [expanded, setExpanded] = useState<number | null>(null)

  const load = useCallback(async (result: string | null) => {
    setLoading(true)
    setError(null)
    try {
      const r = await api.apiCall<{ entries?: RawEntry[] }>(connName, {
        kind: 'journal', repoId, limit: PAGE,
        ...(result ? { result } : {}),
      })
      setEntries((r.entries ?? []).map(parse))
    } catch (err) {
      setError(String(err))
    }
    setLoading(false)
  }, [connName, repoId])

  useEffect(() => { void load(filter) }, [load, filter])

  const failures = entries.filter(e => e.result === 'failed').length

  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Back
        </button>
        <h3 className="text-lg font-medium m-0">Activity</h3>
        {!loading && (
          <span className="text-xs text-text-muted">
            {entries.length}{entries.length >= PAGE ? '+' : ''} operation
            {entries.length === 1 ? '' : 's'}
            {/* Worth saying without being asked: a page of history with
              * failures in it is the reason somebody opened this. */}
            {failures > 0 && !filter && (
              <span className="text-status-error ml-2">{failures} failed</span>
            )}
          </span>
        )}
      </div>

      <div className="flex gap-2 mb-3 items-center">
        <button onClick={() => setFilter(null)}
          className={`text-[11px] px-2 py-0.5 rounded cursor-pointer border-none ${
            filter === null ? 'bg-accent text-accent-text'
                            : 'bg-surface-tertiary text-text-secondary hover:bg-surface-hover'}`}>
          All
        </button>
        {RESULTS.map(res => (
          <button key={res} onClick={() => setFilter(filter === res ? null : res)}
            className={`text-[11px] px-2 py-0.5 rounded cursor-pointer border-none ${
              filter === res ? 'bg-accent text-accent-text'
                             : 'bg-surface-tertiary text-text-secondary hover:bg-surface-hover'}`}>
            {res}
          </button>
        ))}
      </div>

      {error && <div className="text-status-error text-sm mb-3">{error}</div>}

      {loading ? (
        <div className="text-text-muted text-sm">Loading...</div>
      ) : entries.length === 0 ? (
        <div className="text-text-muted text-sm">
          {filter
            ? `Nothing here has ${filter === 'success' ? 'succeeded' : filter}.`
            : 'Nothing has run against this repository yet.'}
        </div>
      ) : (
        <div className="bg-surface-secondary border border-border-default rounded-lg p-4 overflow-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-text-muted text-left">
                <th className="font-medium pb-2 w-40">When</th>
                <th className="font-medium pb-2 w-28">Operation</th>
                <th className="font-medium pb-2 w-20">Result</th>
                <th className="font-medium pb-2 text-right w-20 pr-6">Took</th>
                <th className="font-medium pb-2 w-20">Source</th>
                <th className="font-medium pb-2">Detail</th>
              </tr>
            </thead>
            <tbody>
              {entries.map(e => (
                <tr key={e.id}
                  onClick={() => setExpanded(expanded === e.id ? null : e.id)}
                  className="text-text-primary border-t border-border-default align-top
                             cursor-pointer hover:bg-surface-hover">
                  <td className="py-1.5" title={absoluteTime(e.ts)}>{relativeTime(e.ts)}</td>
                  <td className="py-1.5 font-mono">{e.operation}</td>
                  <td className={`py-1.5 ${resultClass(e.result)}`}>{e.result}</td>
                  <td className="py-1.5 text-right font-mono text-text-muted pr-6">
                    {fmtDuration(e.durationMs)}
                  </td>
                  <td className="py-1.5 text-text-muted">{e.source}</td>
                  {/* A failure's message is the whole reason to be here, so
                    * it is not truncated once the row is opened. */}
                  <td className={`py-1.5 ${expanded === e.id ? 'break-all' : 'truncate max-w-[24rem]'}`}
                      title={e.message}>
                    {e.message || <span className="text-text-muted">-</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
