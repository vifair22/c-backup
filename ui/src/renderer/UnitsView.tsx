/*
 * The units of one repository: VMs and containers backed up through a
 * source provider (Manual 22), with their state and a backup run now.
 *
 * One provider at a time, because a units job is for one provider: the
 * same body `c-backup unit backup` sends. The table merges the provider's
 * live enumeration (what exists and its state) with the repository's unit
 * history (last snapshot, level, how many), so a unit that has since gone
 * still shows with its snapshots. A row opens the unit's page: history,
 * journal, restore and exports.
 *
 * Every unit here is one units.allow takes: the daemon lists nothing
 * outside it, and the screen says what the rail is so an empty list is
 * not read as "no units on this host".
 */

import React, { useCallback, useEffect, useState } from 'react'
import type { Job, PauseScope, UnitLevel } from '../shared/api'
import type {
  WireLiveUnitPage, WireProvider, WireProviders, WireRepoUnit, WireUnitBackupResult,
} from '../shared/wire'
import {
  DEFAULT_LEVEL, LEVEL_WORDS, PAUSE_SCOPES, SCOPE_WORDS, UNIT_LEVELS, mergeUnits, parseGlobs,
  type UnitRow,
} from '../shared/units'
import { absoluteTime, fmtNum, fmtSize } from './format'
import { followJob, jobIdOf } from './followJob'
import { ExportsPanel } from './ExportsPanel'

const api = window.cbackup

/* The live enumeration's default page; its maximum is 1000. */
const PAGE = 100

interface Props {
  connName: string
  repoPath: string
  onBack: () => void
  onOpenUnit: (provider: string, unitId: string, name: string) => void
  /* A provider to open on, from a unit page's Back. */
  initialProvider?: string
}

export const selectCls =
  'text-[11px] px-1.5 py-0.5 rounded bg-surface-tertiary text-text-primary border border-border-default'
export const inputCls =
  'w-full px-2 py-1 border border-border-default rounded text-xs bg-surface-primary text-text-primary focus:outline-none focus:border-accent'
export const btnCls =
  'px-2.5 py-1 text-xs rounded bg-surface-secondary border border-border-default text-text-secondary hover:bg-surface-hover cursor-pointer disabled:opacity-50 disabled:cursor-default'
export const primaryCls =
  'px-3 py-1 text-xs rounded bg-accent text-accent-text hover:bg-accent-hover cursor-pointer border-none disabled:opacity-50 disabled:cursor-default'

export function stateClass(state: string | null): string {
  if (state === 'running') return 'text-status-connected'
  if (state === 'paused' || state === 'suspended') return 'text-status-warning'
  if (state === null) return 'text-text-muted italic'
  return 'text-text-secondary'
}

/* The level and pause-scope selects, shared with the unit page. */
export function BackupOptions({ level, setLevel, scope, setScope, showScope = true }: {
  level: UnitLevel; setLevel: (l: UnitLevel) => void
  scope?: PauseScope; setScope?: (s: PauseScope) => void; showScope?: boolean
}): React.ReactElement {
  return (
    <>
      <label className="text-[11px] text-text-muted flex items-center gap-1"
        title="The lowest level a unit's capture may achieve; one below it fails that unit">
        Require
        <select aria-label="Require" value={level} className={selectCls}
          onChange={e => setLevel(e.target.value as UnitLevel)}>
          {UNIT_LEVELS.map(l => <option key={l} value={l} title={LEVEL_WORDS[l]}>{l}</option>)}
        </select>
      </label>
      {showScope && scope && setScope && (
        <label className="text-[11px] text-text-muted flex items-center gap-1"
          title="Which units are captured together (Manual 22.6.1)">
          Pause scope
          <select aria-label="Pause scope" value={scope} className={selectCls}
            onChange={e => setScope(e.target.value as PauseScope)}>
            {PAUSE_SCOPES.map(s => <option key={s} value={s} title={SCOPE_WORDS[s]}>{s}</option>)}
          </select>
        </label>
      )}
    </>
  )
}

export interface BackupRun {
  jobId: number
  phase?: string
  job?: Job
  error?: string
}

/*
 * What a units job came to: each unit's outcome, snapshot and level, every
 * warning, and the job's error as the daemon wrote it. A job that fails
 * because one unit did still produced snapshots for the rest, so the
 * per-unit lines are shown for a failed job too.
 */
export function BackupResult({ run, onOpenSnapshot }: {
  run: BackupRun; onOpenSnapshot?: (id: number) => void
}): React.ReactElement {
  const job = run.job
  if (run.error) {
    return <div className="unit-backup-result text-xs text-status-error whitespace-pre-wrap break-words">{run.error}</div>
  }
  if (!job) {
    return (
      <div className="unit-backup-result text-xs text-text-muted">
        Backing up (job #{run.jobId}){run.phase ? `: ${run.phase}` : '...'}
      </div>
    )
  }
  const res = (job.result ?? null) as Partial<WireUnitBackupResult> | null
  return (
    <div className="unit-backup-result text-xs space-y-1">
      <div className={job.state === 'completed' ? 'text-status-connected' : 'text-status-error'}>
        Job #{job.id} {job.state}
        {res && typeof res.ok === 'number' && (
          <span className="text-text-muted ml-2">
            {fmtNum(res.ok)} backed up, {fmtNum(res.failed ?? 0)} failed
            {res.filtered ? `, ${fmtNum(res.filtered)} left out by units.allow` : ''}
          </span>
        )}
      </div>
      {job.error && <div className="text-status-error whitespace-pre-wrap break-words">{job.error}</div>}
      {res?.note && <div className="text-status-warning whitespace-pre-wrap">{res.note}</div>}
      {(res?.units ?? []).map(u => (
        <div key={u.unit_key} className="pl-2 border-l-2 border-border-default">
          <span className="font-mono">{u.name}</span>{' '}
          {u.status === 'ok' ? (
            <span className="text-status-connected">ok</span>
          ) : (
            <span className="text-status-error">failed</span>
          )}
          {u.snapshot_id !== undefined && (
            onOpenSnapshot ? (
              <button onClick={() => onOpenSnapshot(u.snapshot_id!)}
                className="ml-2 text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer p-0 text-xs">
                snapshot #{u.snapshot_id}
              </button>
            ) : <span className="ml-2">snapshot #{u.snapshot_id}</span>
          )}
          {u.consistency && <span className="ml-2 text-text-muted">level {u.consistency}</span>}
          {u.bytes_read !== undefined && <span className="ml-2 text-text-muted">read {fmtSize(u.bytes_read)}</span>}
          {u.freeze_ms !== undefined && <span className="ml-2 text-text-muted">frozen {fmtNum(u.freeze_ms)} ms</span>}
          {u.error && <div className="text-status-error whitespace-pre-wrap break-words">{u.error}</div>}
          {(u.warnings ?? (u.warning ? [u.warning] : [])).map((w, i) => (
            <div key={i} className="text-status-warning whitespace-pre-wrap break-words">{w}</div>
          ))}
        </div>
      ))}
    </div>
  )
}

/* Start a units job and follow it, reporting into setRun. */
export async function runUnitBackup(
  connName: string,
  req: { repoId: number; provider: string; units?: string[]; all?: boolean; exclude?: string[]
         require: UnitLevel; pauseScope?: PauseScope },
  setRun: (r: BackupRun) => void,
): Promise<void> {
  try {
    const started = await api.apiCall<{ job_id?: number }>(connName, {
      kind: 'unitBackup', repoId: req.repoId, provider: req.provider,
      units: req.units, all: req.all, exclude: req.exclude,
      /* The defaults are left to the daemon rather than restated. */
      require: req.require === DEFAULT_LEVEL ? undefined : req.require,
      pauseScope: req.pauseScope === 'unit' ? undefined : req.pauseScope,
    })
    const jobId = jobIdOf(started)
    setRun({ jobId })
    const job = await followJob(connName, jobId, j => setRun({ jobId, phase: j.phase }))
    setRun({ jobId, job })
  } catch (err) {
    setRun({ jobId: 0, error: String(err instanceof Error ? err.message : err) })
  }
}

export function UnitsView({ connName, repoPath, onBack, onOpenUnit, initialProvider }: Props): React.ReactElement {
  const repoId = Number(repoPath)

  const [providers, setProviders] = useState<WireProvider[] | null>(null)
  const [allow, setAllow] = useState('')
  const [provider, setProvider] = useState<string | undefined>(initialProvider)
  const [providersError, setProvidersError] = useState<string | null>(null)

  const [rows, setRows] = useState<UnitRow[]>([])
  const [total, setTotal] = useState(0)
  const [offset, setOffset] = useState(0)
  const [loading, setLoading] = useState(false)
  const [listError, setListError] = useState<string | null>(null)

  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [level, setLevel] = useState<UnitLevel>(DEFAULT_LEVEL)
  const [scope, setScope] = useState<PauseScope>('unit')
  const [excludeText, setExcludeText] = useState('')
  const [run, setRun] = useState<BackupRun | null>(null)

  const enabled = (providers ?? []).filter(p => p.enabled)

  useEffect(() => {
    let cancelled = false
    api.apiCall<Partial<WireProviders>>(connName, { kind: 'providerList' }).then(r => {
      if (cancelled) return
      const list = r.providers ?? []
      setProviders(list)
      setAllow(r.allow ?? '')
      setProvider(p => p ?? list.find(x => x.enabled)?.name)
    }).catch(err => { if (!cancelled) setProvidersError(String(err instanceof Error ? err.message : err)) })
    return () => { cancelled = true }
  }, [connName])

  const load = useCallback(async () => {
    if (!provider) return
    setLoading(true)
    setListError(null)
    /* History first: it never needs the provider, so a helper that cannot
     * be reached still leaves the units with snapshots reachable. */
    let history: WireRepoUnit[] = []
    try {
      const h = await api.apiCall<{ units?: WireRepoUnit[] }>(connName, {
        kind: 'repoUnits', repoId, limit: 1000,
      })
      history = h.units ?? []
    } catch (err) {
      setListError(String(err instanceof Error ? err.message : err))
    }
    try {
      const live = await api.apiCall<Partial<WireLiveUnitPage>>(connName, {
        kind: 'providerUnits', provider, limit: PAGE, offset,
      })
      setRows(mergeUnits(provider, live.units ?? [], history, offset === 0))
      setTotal(live.total ?? 0)
    } catch (err) {
      setRows(mergeUnits(provider, [], history))
      setTotal(0)
      setListError(`${provider} cannot list its units: ${err instanceof Error ? err.message : String(err)}`)
    }
    setLoading(false)
  }, [connName, repoId, provider, offset])

  useEffect(() => { void load() }, [load])

  const pick = (p: string) => { setProvider(p); setOffset(0); setSelected(new Set()); setRun(null) }
  const toggle = (key: string) => setSelected(s => {
    const next = new Set(s)
    if (next.has(key)) next.delete(key)
    else next.add(key)
    return next
  })

  const busy = !!run && !run.job && !run.error
  const backUp = async (all: boolean) => {
    if (!provider) return
    /* By name, as the CLI names them; a unit the provider no longer lists
     * cannot be backed up and is not offered. */
    const names = rows.filter(r => selected.has(r.key) && r.state !== null).map(r => r.name)
    await runUnitBackup(connName, {
      repoId, provider, require: level, pauseScope: scope,
      ...(all ? { all: true, exclude: parseGlobs(excludeText) } : { units: names }),
    }, setRun)
    void load()
  }

  const current = enabled.find(p => p.name === provider)
  const selectable = rows.filter(r => r.state !== null)
  const nSelected = selectable.filter(r => selected.has(r.key)).length

  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Back
        </button>
        <h3 className="text-lg font-medium m-0">Units</h3>
        <span className="text-xs text-text-muted">virtual machines and containers</span>
      </div>

      {providersError && <div className="text-status-error text-sm mb-3">{providersError}</div>}

      {providers !== null && enabled.length === 0 ? (
        <div className="text-text-muted text-sm">
          No source provider is enabled on this daemon, so it backs up no units.
          Providers are enabled in the daemon's configuration and the helper's policy (Manual 22.2).
        </div>
      ) : (
        <>
          <div className="flex flex-wrap items-center gap-2 mb-2">
            <span className="text-[11px] text-text-muted">Provider</span>
            {enabled.map(p => (
              <button key={p.name} onClick={() => pick(p.name)} aria-pressed={p.name === provider}
                className={`text-[11px] px-2 py-0.5 rounded cursor-pointer border-none ${
                  p.name === provider ? 'bg-accent text-accent-text' : 'bg-surface-tertiary text-text-secondary hover:bg-surface-hover'}`}>
                {p.name}
              </button>
            ))}
            {current?.via === 'helper' && current.connection && (
              <span className={`text-[11px] ${current.connection.state === 'connected' ? 'text-text-muted' : 'text-status-error'}`}>
                through the helper, {current.connection.state}
                {current.connection.last_error ? `: ${current.connection.last_error}` : ''}
              </span>
            )}
            {allow && (
              <span className="text-[11px] text-text-muted ml-auto" title="units.allow: the daemon lists and touches only units these globs match (Manual 22.9)">
                units.allow: <span className="font-mono">{allow}</span>
              </span>
            )}
          </div>

          <div className="flex flex-wrap items-center gap-3 mb-3 p-2 rounded border border-border-default bg-surface-secondary">
            <BackupOptions level={level} setLevel={setLevel} scope={scope} setScope={setScope} />
            <button className={primaryCls} disabled={busy || nSelected === 0} onClick={() => void backUp(false)}>
              Back up selected{nSelected ? ` (${nSelected})` : ''}
            </button>
            <span className="border-l border-border-default h-4" />
            <label className="text-[11px] text-text-muted flex items-center gap-1">
              Exclude
              <input aria-label="Exclude" value={excludeText} onChange={e => setExcludeText(e.target.value)}
                placeholder="globs, comma separated" className={`${inputCls} w-48`} />
            </label>
            <button className={btnCls} disabled={busy || !provider} onClick={() => void backUp(true)}
              title="Every unit the provider lists when the job starts, less the excluded">
              Back up all
            </button>
          </div>

          {run && (
            <div className="mb-3 p-2 rounded border border-border-default">
              <BackupResult run={run} />
            </div>
          )}

          {listError && <div className="text-status-error text-xs mb-2 whitespace-pre-wrap">{listError}</div>}

          {loading && rows.length === 0 ? (
            <div className="text-text-muted text-sm">Loading...</div>
          ) : rows.length === 0 ? (
            <div className="text-text-muted text-sm">
              {provider} lists no unit that units.allow takes, and none has history in this repository.
            </div>
          ) : (
            <div className="overflow-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="text-text-muted text-left">
                    <th className="font-medium pb-2 w-6">
                      <input type="checkbox" aria-label="Select all"
                        checked={selectable.length > 0 && nSelected === selectable.length}
                        onChange={e => setSelected(e.target.checked ? new Set(selectable.map(r => r.key)) : new Set())} />
                    </th>
                    <th className="font-medium pb-2">Unit</th>
                    <th className="font-medium pb-2">State</th>
                    <th className="font-medium pb-2">Last backup</th>
                    <th className="font-medium pb-2">Level</th>
                    <th className="font-medium pb-2 text-right">Snapshots</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map(r => (
                    <tr key={r.key} data-unit={r.name}
                      className="unit-row text-text-primary border-t border-border-default cursor-pointer hover:bg-surface-hover"
                      onClick={() => onOpenUnit(r.provider, r.unitId, r.name)}>
                      <td className="py-1.5" onClick={e => e.stopPropagation()}>
                        {r.state !== null && (
                          <input type="checkbox" aria-label={`Select ${r.name}`}
                            checked={selected.has(r.key)} onChange={() => toggle(r.key)} />
                        )}
                      </td>
                      <td className="py-1.5">
                        <span className="font-mono">{r.name}</span>
                        <span className="text-text-muted ml-2 text-[10px]">{r.key}</span>
                      </td>
                      <td className={`py-1.5 ${stateClass(r.state)}`}>{r.state ?? 'not listed'}</td>
                      <td className="py-1.5">
                        {r.history ? (
                          <span>#{r.history.last_snapshot} <span className="text-text-muted">{absoluteTime(r.history.last_ts)}</span></span>
                        ) : <span className="text-text-muted">never</span>}
                      </td>
                      <td className="py-1.5">{r.history?.consistency ?? <span className="text-text-muted">-</span>}</td>
                      <td className="py-1.5 text-right">{r.history ? fmtNum(r.history.snapshots) : '0'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          <div className="mt-4"><ExportsPanel connName={connName} repoId={repoId} /></div>

          {(offset > 0 || offset + PAGE < total) && (
            <div className="flex items-center gap-2 mt-3 text-xs">
              <button onClick={() => setOffset(Math.max(0, offset - PAGE))} disabled={offset === 0} className={btnCls}>Prev</button>
              <span className="text-text-muted">
                {fmtNum(offset + 1)}-{fmtNum(Math.min(offset + PAGE, total))} of {fmtNum(total)} listed
              </span>
              <button onClick={() => setOffset(offset + PAGE)} disabled={offset + PAGE >= total} className={btnCls}>Next</button>
            </div>
          )}
        </>
      )}
    </div>
  )
}
