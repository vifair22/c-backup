/*
 * Every database detection found in this repository, by unit and source.
 *
 * docs/db-detection.md section 12 asks for three surfaces: a panel on each
 * unit page, a panel on the sources page, and a repository-wide list
 * filterable by coverage. The app has no unit pages and keeps its sources in
 * the backup dialog, so this one screen carries all three: the list is
 * grouped under headings per unit and per source, a unit's heading carries
 * its Detect now button, and the backup dialog links here with the scope
 * filter set to one source.
 *
 * Each card carries its own Preview, Enable and Dismiss (D2); the list
 * reloads after either of the last two so the new state shows in place.
 */

import React, { useCallback, useEffect, useRef, useState } from 'react'
import type { Job } from '../shared/api'
import type { WireDbDetectResult, WireDbFinding, WireDbFindingPage } from '../shared/wire'
import {
  COVERAGES, ENGINES, coverageLabel, engineTitle, groupByScope, needsAttention,
  parseScope, scopeTitle, uncoveredByScope,
} from '../shared/databases'
import { DatabaseCard, Badge } from './DatabaseCard'
import { fmtNum } from './format'

const api = window.cbackup

/* The route's default; its maximum is 1000. Cards are tall, so a page much
 * bigger than this is a long scroll before the pager. */
const PAGE = 100

const STATES = ['new', 'suggested', 'enabled', 'dismissed'] as const

interface Props {
  connName: string
  repoPath: string
  /* Opened from somewhere that is about one unit or source. */
  initialScope?: string
  onBack: () => void
}

interface Detecting {
  jobId: number
  message?: string
  error?: string
}

export function DatabasesView({ connName, repoPath, initialScope, onBack }: Props): React.ReactElement {
  const repoId = Number(repoPath)

  const [scope, setScope] = useState<string | undefined>(initialScope)
  const [coverage, setCoverage] = useState<string>('')
  const [engine, setEngine] = useState<string>('')
  const [state, setState] = useState<string>('')
  const [offset, setOffset] = useState(0)

  const [findings, setFindings] = useState<WireDbFinding[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  /* Keyed by unit key, so two units can be detecting at once and each
   * heading shows its own progress. */
  const [detecting, setDetecting] = useState<Record<string, Detecting>>({})
  const polls = useRef<Map<string, ReturnType<typeof setInterval>>>(new Map())

  useEffect(() => { setScope(initialScope); setOffset(0) }, [initialScope])
  useEffect(() => () => {
    for (const t of polls.current.values()) clearInterval(t)
    polls.current.clear()
  }, [])

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const r = await api.apiCall<Partial<WireDbFindingPage>>(connName, {
        kind: 'dbList', repoId, limit: PAGE, offset,
        ...(scope ? { scope } : {}),
        ...(coverage ? { coverage } : {}),
        ...(engine ? { engine } : {}),
        ...(state ? { state } : {}),
      })
      setFindings(r.findings ?? [])
      setTotal(r.total ?? 0)
    } catch (err) {
      setError(String(err))
      setFindings([])
      setTotal(0)
    }
    setLoading(false)
  }, [connName, repoId, scope, coverage, engine, state, offset])

  useEffect(() => { void load() }, [load])

  /* Every filter change starts from the first page: an offset kept across a
   * narrower filter lands past its end and shows an empty page. */
  const refilter = (fn: () => void) => { fn(); setOffset(0) }

  /*
   * Detect now, followed to the end the way GrepView follows a search: the
   * job id is the only thing that identifies this run's result, and the
   * list is reloaded once it finishes so the new findings appear in place.
   */
  const detect = async (provider: string, unit: string, key: string) => {
    try {
      const started = await api.apiCall<{ job_id?: number }>(connName, {
        kind: 'unitDetect', repoId, provider, unit,
      })
      const jobId = started.job_id
      if (!jobId) throw new Error('the daemon did not return a job id')
      setDetecting(d => ({ ...d, [key]: { jobId } }))
      const old = polls.current.get(key)
      if (old) clearInterval(old)
      const t = setInterval(async () => {
        try {
          const job = await api.apiCall<Job>(connName, { kind: 'jobGet', jobId })
          if (job.state === 'queued' || job.state === 'running') return
          clearInterval(t)
          polls.current.delete(key)
          if (job.state === 'completed') {
            const db = (job.result as WireDbDetectResult | null | undefined)?.databases
            const n = db?.total ?? db?.findings?.length ?? 0
            setDetecting(d => ({ ...d, [key]: {
              jobId, message: `found ${fmtNum(n)} database${n === 1 ? '' : 's'}`,
            } }))
          } else {
            setDetecting(d => ({ ...d, [key]: { jobId, error: job.error || job.state } }))
          }
          void load()
        } catch (err) {
          clearInterval(t)
          polls.current.delete(key)
          setDetecting(d => ({ ...d, [key]: { jobId, error: String(err) } }))
        }
      }, 750)
      polls.current.set(key, t)
    } catch (err) {
      setDetecting(d => ({ ...d, [key]: { jobId: 0, error: String(err) } }))
    }
  }

  const DetectButton = ({ provider, unit, unitKey }: {
    provider: string; unit: string; unitKey: string
  }) => {
    const d = detecting[unitKey]
    const running = !!d && !d.message && !d.error && d.jobId > 0
    return (
      <span className="flex items-center gap-2">
        {d?.message && <span className="text-[11px] text-text-muted">{d.message}</span>}
        {d?.error && <span className="text-[11px] text-status-error">{d.error}</span>}
        <button onClick={() => void detect(provider, unit, unitKey)} disabled={running}
          title="Run detection now, without a backup: reads only file headers and the unit's processes"
          className="px-2 py-0.5 text-[11px] rounded bg-surface-tertiary text-text-secondary
                     hover:bg-surface-hover border-none cursor-pointer disabled:opacity-50">
          {running ? 'Detecting...' : 'Detect now'}
        </button>
      </span>
    )
  }

  const groups = groupByScope(findings)
  const attention = findings.filter(f => f.state !== 'dismissed' && needsAttention(f.coverage)).length
  const scoped = scope ? parseScope(scope) : null
  const uncovered = uncoveredByScope(findings)

  const chip = (active: boolean) =>
    `text-[11px] px-2 py-0.5 rounded cursor-pointer border-none ${
      active ? 'bg-accent text-accent-text' : 'bg-surface-tertiary text-text-secondary hover:bg-surface-hover'}`
  const select = 'text-[11px] px-1.5 py-0.5 rounded bg-surface-tertiary text-text-primary border border-border-default'

  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Back
        </button>
        <h3 className="text-lg font-medium m-0">Databases</h3>
        {!loading && (
          <span className="text-xs text-text-muted">
            {fmtNum(total)} finding{total === 1 ? '' : 's'}
            {attention > 0 && (
              <span className="text-status-error ml-2">{fmtNum(attention)} need attention on this page</span>
            )}
          </span>
        )}
      </div>

      <div className="flex flex-wrap gap-2 mb-3 items-center">
        <button onClick={() => refilter(() => setCoverage(''))} className={chip(coverage === '')}>All</button>
        <button onClick={() => refilter(() => setCoverage(coverage === 'uncovered' ? '' : 'uncovered'))}
          className={chip(coverage === 'uncovered')}>
          Uncovered only
        </button>

        <label className="text-[11px] text-text-muted flex items-center gap-1 ml-2">
          Coverage
          <select aria-label="Coverage" value={coverage} className={select}
            onChange={e => refilter(() => setCoverage(e.target.value))}>
            <option value="">any</option>
            {COVERAGES.map(c => <option key={c} value={c}>{coverageLabel(c)}</option>)}
            <option value="none">not assessed</option>
          </select>
        </label>
        <label className="text-[11px] text-text-muted flex items-center gap-1">
          Engine
          <select aria-label="Engine" value={engine} className={select}
            onChange={e => refilter(() => setEngine(e.target.value))}>
            <option value="">any</option>
            {ENGINES.map(en => <option key={en} value={en}>{engineTitle(en)}</option>)}
          </select>
        </label>
        <label className="text-[11px] text-text-muted flex items-center gap-1">
          State
          <select aria-label="State" value={state} className={select}
            onChange={e => refilter(() => setState(e.target.value))}>
            <option value="">any</option>
            {STATES.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
        </label>

        {scope && (
          <span className="flex items-center gap-1 text-[11px] bg-surface-tertiary rounded px-2 py-0.5">
            <span className="text-text-muted">{scoped?.kind === 'unit' ? 'Unit' : scoped?.kind === 'source' ? 'Source' : 'Scope'}</span>
            <span className="font-mono text-text-primary break-all">{scoped ? scopeTitle(scoped) : scope}</span>
            <button onClick={() => refilter(() => setScope(undefined))} aria-label="Clear scope"
              className="bg-transparent border-none cursor-pointer text-text-muted hover:text-text-primary p-0 ml-1">
              x
            </button>
          </span>
        )}
      </div>

      {error && <div className="text-status-error text-sm mb-3">{error}</div>}

      {loading ? (
        <div className="text-text-muted text-sm">Loading...</div>
      ) : findings.length === 0 ? (
        <div className="text-text-muted text-sm">
          {coverage === 'uncovered'
            ? 'No live database is captured uncovered.'
            : coverage || engine || state
              ? 'Nothing matches these filters.'
              : 'No databases found yet. Detection runs during backups.'}
          {scoped?.kind === 'unit' && (
            <div className="mt-2">
              <DetectButton provider={scoped.provider} unit={scoped.unit} unitKey={scoped.key} />
            </div>
          )}
        </div>
      ) : (
        <div className="overflow-auto space-y-4">
          {groups.map(g => {
            const p = g.parsed
            const groupUncovered = uncovered.get(g.scope) ?? 0
            return (
              <section key={g.scope}>
                <div className="flex items-center gap-2 mb-1.5">
                  <span className="text-[11px] text-text-muted uppercase tracking-wide">
                    {p.kind === 'unit' ? 'Unit' : p.kind === 'source' ? 'Source' : 'Scope'}
                  </span>
                  <span className="text-xs font-mono text-text-primary break-all">{scopeTitle(p)}</span>
                  {groupUncovered > 0 && (
                    <Badge className="bg-status-error text-white">{groupUncovered} uncovered</Badge>
                  )}
                  {!scope && (
                    <button onClick={() => refilter(() => setScope(g.scope))}
                      className="text-[11px] text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer p-0">
                      only this
                    </button>
                  )}
                  <span className="ml-auto">
                    {p.kind === 'unit' && (
                      <DetectButton provider={p.provider} unit={p.unit} unitKey={p.key} />
                    )}
                  </span>
                </div>
                <div className="space-y-2">
                  {g.findings.map(f => (
                    <DatabaseCard key={f.id} finding={f} connName={connName} repoId={repoId}
                      onChanged={() => void load()} />
                  ))}
                </div>
              </section>
            )
          })}
        </div>
      )}

      {(offset > 0 || offset + findings.length < total) && (
        <div className="flex items-center gap-2 mt-3 text-xs">
          <button onClick={() => setOffset(Math.max(0, offset - PAGE))} disabled={offset === 0}
            className="px-2 py-0.5 rounded bg-surface-tertiary text-text-secondary border-none cursor-pointer disabled:opacity-50">
            Prev
          </button>
          <span className="text-text-muted">
            {fmtNum(offset + 1)}-{fmtNum(offset + findings.length)} of {fmtNum(total)}
          </span>
          <button onClick={() => setOffset(offset + PAGE)} disabled={offset + findings.length >= total}
            className="px-2 py-0.5 rounded bg-surface-tertiary text-text-secondary border-none cursor-pointer disabled:opacity-50">
            Next
          </button>
        </div>
      )}
    </div>
  )
}

/*
 * Uncovered live databases in a repository, for the badge on the Databases
 * entry and beside each source in the backup dialog.
 *
 * One request at the route's maximum page, counted here, because the route
 * cannot filter "not dismissed" and a dismissed finding is one the operator
 * already answered. Past a thousand the count says so with a "+" rather than
 * paging for a badge. null while unknown or when the daemon has no such
 * route (an older one), so the badge is absent rather than wrong.
 */
export interface UncoveredSummary {
  count: number
  more: boolean
  byScope: Map<string, number>
}

export function useUncoveredDatabases(connName: string, repoId: number, refresh = 0): UncoveredSummary | null {
  const [summary, setSummary] = useState<UncoveredSummary | null>(null)
  useEffect(() => {
    let cancelled = false
    setSummary(null)
    /* An empty connection is a caller with nothing open to ask about. */
    if (!connName) return
    api.apiCall<Partial<WireDbFindingPage>>(connName, {
      kind: 'dbList', repoId, coverage: 'uncovered', limit: 1000,
    }).then(r => {
      if (cancelled) return
      const list = r.findings ?? []
      const byScope = uncoveredByScope(list)
      let count = 0
      for (const n of byScope.values()) count += n
      setSummary({ count, more: (r.total ?? 0) > list.length, byScope })
    }).catch(() => { /* no route, or no repo: no badge */ })
    return () => { cancelled = true }
  }, [connName, repoId, refresh])
  return summary
}
