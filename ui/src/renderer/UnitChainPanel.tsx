/*
 * A unit's incremental chain (vm-backup.md 5.8, Manual 6.5): `vm
 * checkpoints` and `vm reset-incremental`.
 *
 * Whether incrementals are on for the unit, why they were turned off
 * after a failed bitmap audit, and one checkpoint token per disk. Reset
 * turns them back on and makes every token unusable, so the next backup
 * reads in full under a new checkpoint; it is journalled, because it is
 * an operator deciding a bitmap is to be trusted again. The CLI names
 * these for VMs (libvirt); the routes answer for any provider's unit, so
 * the panel is on every unit's page and an empty chain says so.
 */

import React, { useCallback, useEffect, useState } from 'react'
import type { WireUnitChain, WireUnitReset } from '../shared/wire'
import { absoluteTime, fmtNum } from './format'
import { ConfirmDialog } from './ConfirmDialog'
import { btnCls } from './UnitsView'

const api = window.cbackup

export function UnitChainPanel({ connName, repoId, provider, unitId, onReset }: {
  connName: string; repoId: number; provider: string; unitId: string
  /* The reset is journalled: the page reloads its journal. */
  onReset?: () => void
}): React.ReactElement {
  const [chain, setChain] = useState<WireUnitChain | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [asking, setAsking] = useState(false)
  const [reset, setReset] = useState<WireUnitReset | null>(null)

  const load = useCallback(async () => {
    try {
      setChain(await api.apiCall<WireUnitChain>(connName, { kind: 'unitCheckpoints', repoId, provider, unit: unitId }))
      setError(null)
    } catch (err) {
      setError(String(err instanceof Error ? err.message : err))
    }
  }, [connName, repoId, provider, unitId])

  useEffect(() => { void load() }, [load])

  const doReset = async () => {
    setAsking(false)
    try {
      setReset(await api.apiCall<WireUnitReset>(connName, {
        kind: 'unitResetIncremental', repoId, provider, unit: unitId,
      }))
      onReset?.()
    } catch (err) {
      setError(String(err instanceof Error ? err.message : err))
    }
    void load()
  }

  const inc = chain?.incremental
  return (
    <section className="unit-chain mb-4">
      <div className="flex items-center gap-3 mb-2">
        <h4 className="text-sm font-medium m-0">Incremental chain</h4>
        {chain && <button className={btnCls} onClick={() => setAsking(true)}>Reset incrementals</button>}
      </div>
      {error && <div className="unit-chain-error text-status-error text-xs mb-2 whitespace-pre-wrap break-words">{error}</div>}
      {reset && (
        <div className="unit-chain-reset text-xs text-status-connected mb-2">
          Reset: the next backup reads in full under a new checkpoint.
          {reset.was_disabled
            ? ` A failed audit had turned them off: ${reset.was_disabled}`
            : ' No failed audit had turned them off.'}
        </div>
      )}
      {inc && (
        <div className="text-xs space-y-1">
          <div>
            <span className="text-text-muted">Incrementals </span>
            <span className={`incremental-state ${inc.enabled ? 'text-status-connected' : 'text-status-warning'}`}>
              {inc.enabled ? 'on' : 'off'}
            </span>
            {inc.disabled_reason && (
              <span className="text-status-warning"> since {absoluteTime(inc.disabled_at)}: {inc.disabled_reason}</span>
            )}
            <span className="text-text-muted ml-3">
              audit every {fmtNum(inc.audit_days)} days
              {inc.last_audit_at ? `, last ${absoluteTime(inc.last_audit_at)}` : ', never audited'}
              ; a delta over {fmtNum(inc.delta_full_pct)}% of a disk reads it in full
            </span>
          </div>
          {chain.tokens.length === 0 ? (
            <div className="text-text-muted">
              No checkpoint: the next backup reads in full. A chain starts once the provider offers
              incremental reads and vm.incremental is on.
            </div>
          ) : (
            <table className="w-full">
              <thead>
                <tr className="text-text-muted text-left">
                  <th className="font-medium">Disk</th><th className="font-medium">Checkpoint</th>
                  <th className="font-medium">Snapshot</th><th className="font-medium">Usable</th>
                  <th className="font-medium">Created</th>
                </tr>
              </thead>
              <tbody>
                {chain.tokens.map(t => (
                  <tr key={t.item} className="border-t border-border-default" title={t.identity}>
                    <td className="py-0.5 font-mono">{t.item}</td>
                    <td className="py-0.5 font-mono">{t.checkpoint}</td>
                    <td className="py-0.5">#{t.snapshot}</td>
                    <td className={`py-0.5 ${t.usable ? '' : 'text-status-warning'}`}>{t.usable ? 'yes' : 'no'}</td>
                    <td className="py-0.5">{absoluteTime(t.created_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
      {asking && (
        <ConfirmDialog title="Reset incrementals?"
          message="Incrementals are turned back on for this unit and every checkpoint is made unusable, so its next backup reads every disk in full under a new checkpoint. Journalled."
          confirmLabel="Reset" danger={false} onConfirm={() => void doReset()} onCancel={() => setAsking(false)} />
      )}
    </section>
  )
}
