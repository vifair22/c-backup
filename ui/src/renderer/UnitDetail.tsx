/*
 * One unit in one repository: its state now, a backup run now, its
 * snapshots here with how each item was read, and the journal entries for
 * unit operations (Manual 22.6, 22.7).
 *
 * The state comes from the provider (GET /providers/<p>/units/<u>), the
 * history from the repository; either can be missing on its own. A unit
 * its provider no longer lists keeps its history and restores, and a unit
 * never backed up here has a state and no history.
 */

import React, { useCallback, useEffect, useState } from 'react'
import type { UnitLevel } from '../shared/api'
import type { WireJournalEntry, WireUnitDesc, WireUnitHistory, WireUnitSnapshot } from '../shared/wire'
import { DEFAULT_LEVEL, itemKindLabel, unitJournal } from '../shared/units'
import { absoluteTime, fmtNum, fmtSize, gfsBadges } from './format'
import {
  BackupOptions, BackupResult, btnCls, primaryCls, runUnitBackup, stateClass, type BackupRun,
} from './UnitsView'

const api = window.cbackup

/* The history route's default page; it carries no total, so a full page
 * is what says there may be more. */
const PAGE = 100

/* Journal rows read to find the unit operations among them. */
const JOURNAL_WINDOW = 500

interface Props {
  connName: string
  repoPath: string
  provider: string
  unitId: string
  name: string
  onBack: () => void
  onBrowseSnapshot: (snapId: number) => void
}

export function UnitDetail({ connName, repoPath, provider, unitId, name, onBack, onBrowseSnapshot }: Props): React.ReactElement {
  const repoId = Number(repoPath)

  const [desc, setDesc] = useState<WireUnitDesc | null>(null)
  const [descError, setDescError] = useState<string | null>(null)
  const [snaps, setSnaps] = useState<WireUnitSnapshot[]>([])
  const [key, setKey] = useState(`${provider}:${unitId}`)
  const [offset, setOffset] = useState(0)
  const [histError, setHistError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [journal, setJournal] = useState<WireJournalEntry[]>([])
  const [onlyNamed, setOnlyNamed] = useState(false)

  const [level, setLevel] = useState<UnitLevel>(DEFAULT_LEVEL)
  const [run, setRun] = useState<BackupRun | null>(null)

  const loadState = useCallback(async () => {
    try {
      setDesc(await api.apiCall<WireUnitDesc>(connName, { kind: 'providerUnit', provider, unit: unitId }))
      setDescError(null)
    } catch (err) {
      setDesc(null)
      setDescError(String(err instanceof Error ? err.message : err))
    }
  }, [connName, provider, unitId])

  const loadHistory = useCallback(async () => {
    setLoading(true)
    try {
      const h = await api.apiCall<Partial<WireUnitHistory>>(connName, {
        kind: 'unitHistory', repoId, provider, unit: unitId, limit: PAGE, offset,
      })
      setSnaps(h.snapshots ?? [])
      if (h.unit_key) setKey(h.unit_key)
      setHistError(null)
    } catch (err) {
      setSnaps([])
      setHistError(String(err instanceof Error ? err.message : err))
    }
    try {
      const j = await api.apiCall<{ entries?: WireJournalEntry[] }>(connName, {
        kind: 'journal', repoId, limit: JOURNAL_WINDOW,
      })
      setJournal(j.entries ?? [])
    } catch { /* the journal is a convenience here; the history is what matters */ }
    setLoading(false)
  }, [connName, repoId, provider, unitId, offset])

  useEffect(() => { void loadState() }, [loadState])
  useEffect(() => { void loadHistory() }, [loadHistory])

  const busy = !!run && !run.job && !run.error
  const backUp = async () => {
    await runUnitBackup(connName, { repoId, provider, units: [desc?.name ?? name], require: level }, setRun)
    void loadHistory()
    void loadState()
  }

  const snapIds = new Set(snaps.map(s => s.id))
  const entries = unitJournal(journal, desc?.name ?? name, key, snapIds)
  const shown = onlyNamed ? entries.filter(e => e.names) : entries

  return (
    <div className="flex flex-col h-full overflow-auto">
      <div className="flex items-center gap-3 mb-3">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Back
        </button>
        <h3 className="text-lg font-medium m-0 font-mono">{desc?.name ?? name}</h3>
        <span className="text-xs text-text-muted font-mono">{key}</span>
      </div>

      <section className="mb-4 p-3 rounded border border-border-default bg-surface-secondary text-xs">
        <div className="flex flex-wrap items-center gap-4">
          <span>
            <span className="text-text-muted">State </span>
            <span className={`unit-state ${stateClass(desc ? desc.state : null)}`}>
              {desc ? desc.state : 'not listed'}
            </span>
          </span>
          {desc && desc.caps.length > 0 && (
            <span><span className="text-text-muted">Can </span>{desc.caps.join(', ')}</span>
          )}
          {desc && (
            <span className="text-text-muted">
              {desc.items.map(i => `${i.id} (${itemKindLabel(i.kind)}${i.size ? `, ${fmtSize(i.size)}` : ''})`).join(', ')}
            </span>
          )}
        </div>
        {descError && (
          <div className="text-text-muted mt-1">
            Its provider does not list it now: <span className="text-status-error">{descError}</span>.
            {' '}Its snapshots here can still be browsed and restored.
          </div>
        )}
        {desc && (
          <div className="flex flex-wrap items-center gap-3 mt-2">
            <BackupOptions level={level} setLevel={setLevel} showScope={false} />
            <button className={primaryCls} disabled={busy} onClick={() => void backUp()}>Back up now</button>
          </div>
        )}
        {run && <div className="mt-2"><BackupResult run={run} /></div>}
      </section>

      <section className="mb-4">
        <h4 className="text-sm font-medium m-0 mb-2">Snapshots in this repository</h4>
        {histError && <div className="text-status-error text-xs mb-2">{histError}</div>}
        {loading && snaps.length === 0 ? (
          <div className="text-text-muted text-xs">Loading...</div>
        ) : snaps.length === 0 ? (
          <div className="text-text-muted text-xs">No snapshot of this unit here yet.</div>
        ) : (
          <table className="w-full text-xs">
            <thead>
              <tr className="text-text-muted text-left">
                <th className="font-medium pb-2 w-16">Snapshot</th>
                <th className="font-medium pb-2">Time</th>
                <th className="font-medium pb-2">Level</th>
                <th className="font-medium pb-2">Items, as read</th>
                <th className="font-medium pb-2 text-right">Files</th>
                <th className="font-medium pb-2 text-right">Size</th>
                <th className="font-medium pb-2" />
              </tr>
            </thead>
            <tbody>
              {snaps.map(s => (
                <tr key={s.id} data-snapshot={s.id}
                  className="unit-snapshot text-text-primary border-t border-border-default align-top">
                  <td className="py-1.5 font-mono">
                    #{s.id}
                    {gfsBadges(s.gfs_flags).map(b => (
                      <span key={b.label} className={`ml-1 inline-block px-1 rounded text-[9px] font-semibold ${b.color}`}>{b.label}</span>
                    ))}
                  </td>
                  <td className="py-1.5">{absoluteTime(s.ts)}</td>
                  <td className="py-1.5">{s.consistency}</td>
                  <td className="py-1.5">
                    {s.items.map(i => (
                      <div key={i.id} title={i.path}>
                        <span className="font-mono">{i.id}</span>
                        <span className="text-text-muted"> {itemKindLabel(i.kind)}
                          {i.size ? `, ${fmtSize(i.size)}` : ''}, {i.read_mode}
                          {i.reason ? ` (${i.reason})` : ''}
                          {i.bytes_read ? `, read ${fmtSize(i.bytes_read)}` : ''}
                        </span>
                      </div>
                    ))}
                  </td>
                  <td className="py-1.5 text-right">{fmtNum(s.files)}</td>
                  <td className="py-1.5 text-right">{fmtSize(s.bytes)}</td>
                  <td className="py-1.5 text-right whitespace-nowrap">
                    <button className={btnCls} onClick={() => onBrowseSnapshot(s.id)}>Browse</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {(offset > 0 || snaps.length === PAGE) && (
          <div className="flex items-center gap-2 mt-2 text-xs">
            <button onClick={() => setOffset(Math.max(0, offset - PAGE))} disabled={offset === 0} className={btnCls}>Newer</button>
            <button onClick={() => setOffset(offset + PAGE)} disabled={snaps.length < PAGE} className={btnCls}>Older</button>
          </div>
        )}
      </section>

      <section className="mb-4">
        <div className="flex items-center gap-3 mb-2">
          <h4 className="text-sm font-medium m-0">Journal</h4>
          <label className="text-[11px] text-text-muted flex items-center gap-1">
            <input type="checkbox" checked={onlyNamed} onChange={e => setOnlyNamed(e.target.checked)} />
            only entries naming this unit or its snapshots
          </label>
        </div>
        <div className="text-[11px] text-text-muted mb-2">
          Unit operations in this repository's newest {fmtNum(JOURNAL_WINDOW)} journal entries. The journal
          does not record which unit an entry is for, so a backup of several units says how many rather than
          which, and a restore's entry may carry no message.
        </div>
        {shown.length === 0 ? (
          <div className="text-text-muted text-xs">No unit operations in the journal window.</div>
        ) : (
          <table className="w-full text-xs">
            <tbody>
              {shown.map(e => (
                <tr key={e.id} className={`unit-journal border-t border-border-default ${e.names ? 'text-text-primary' : 'text-text-muted'}`}>
                  <td className="py-1 whitespace-nowrap pr-2">{absoluteTime(e.ts)}</td>
                  <td className="py-1 pr-2 font-mono">{e.operation}</td>
                  <td className="py-1 pr-2">{e.result}</td>
                  <td className="py-1 break-words">{e.message || '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  )
}
