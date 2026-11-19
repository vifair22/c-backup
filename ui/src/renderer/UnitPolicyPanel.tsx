/*
 * A unit's own policy over the repository's (Manual 22.6.2): `unit policy
 * show` and `unit policy set`.
 *
 * Retention counts the unit sets replace the repository's for its group;
 * capture.exclude (patterns on recorded paths, one per line) and
 * capture.group (units captured together under pause scope group) exist
 * only per unit. Each field shows the unit's own value, empty meaning the
 * repository's applies, and the retention in force beside it. Saving sends
 * only what changed; a refusal is the daemon's, shown whole.
 */

import React, { useCallback, useEffect, useState } from 'react'
import type { WireUnitPolicy } from '../shared/wire'
import { RETENTION_KEYS, policyChanges } from '../shared/units'
import { btnCls, inputCls, primaryCls } from './UnitsView'

const api = window.cbackup

export function UnitPolicyPanel({ connName, repoId, provider, unitId }: {
  connName: string; repoId: number; provider: string; unitId: string
}): React.ReactElement {
  const [policy, setPolicy] = useState<WireUnitPolicy | null>(null)
  const [draft, setDraft] = useState<Record<string, string>>({})
  const [editing, setEditing] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [saved, setSaved] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  /* By id, never by name: for a unit with no snapshot here the route keys
   * a read by the name as given and a write by the id the provider
   * resolves, so by name a set would not show (bug, reported). */
  const load = useCallback(async () => {
    try {
      const p = await api.apiCall<WireUnitPolicy>(connName, { kind: 'unitPolicyGet', repoId, provider, unit: unitId })
      setPolicy(p)
      setDraft({ ...p.unit })
      setError(null)
    } catch (err) {
      setError(String(err instanceof Error ? err.message : err))
    }
  }, [connName, repoId, provider, unitId])

  useEffect(() => { void load() }, [load])

  const save = async () => {
    if (!policy) return
    const values = policyChanges(policy.unit, draft)
    if (Object.keys(values).length === 0) { setEditing(false); return }
    setBusy(true)
    setError(null)
    try {
      const p = await api.apiCall<WireUnitPolicy>(connName, {
        kind: 'unitPolicySet', repoId, provider, unit: unitId, values,
      })
      setPolicy(p)
      setDraft({ ...p.unit })
      setEditing(false)
      setSaved(`saved ${Object.keys(values).join(', ')}`)
    } catch (err) {
      setError(String(err instanceof Error ? err.message : err))
    }
    setBusy(false)
  }

  const own = policy?.unit ?? {}
  const field = (label: string, el: React.ReactElement) => (
    <label className="block text-xs text-text-secondary">{label}{el}</label>
  )

  return (
    <section className="unit-policy mb-4">
      <div className="flex items-center gap-3 mb-2">
        <h4 className="text-sm font-medium m-0">Policy</h4>
        {!editing && policy && (
          <button className={btnCls} onClick={() => { setEditing(true); setSaved(null) }}>Edit</button>
        )}
        {saved && <span className="text-[11px] text-status-connected">{saved}</span>}
      </div>
      {error && <div className="unit-policy-error text-status-error text-xs mb-2 whitespace-pre-wrap break-words">{error}</div>}
      {policy && (
        <div className="text-xs">
          <table className="mb-2">
            <tbody>
              {RETENTION_KEYS.map(k => (
                <tr key={k} data-key={k} className="unit-policy-row">
                  <td className="pr-3 py-0.5 font-mono text-text-muted">{k}</td>
                  <td className="pr-3 py-0.5 w-28">
                    {editing ? (
                      <input aria-label={k} value={draft[k] ?? ''} placeholder="repository's"
                        onChange={e => setDraft(d => ({ ...d, [k]: e.target.value }))} className={inputCls} />
                    ) : own[k] !== undefined ? (
                      <span className="text-text-primary">{own[k]} <span className="text-text-muted">(unit)</span></span>
                    ) : <span className="text-text-muted">repository's</span>}
                  </td>
                  <td className="py-0.5 text-text-muted">in force: <span className="effective text-text-primary">{policy.effective[k]}</span></td>
                </tr>
              ))}
            </tbody>
          </table>
          {editing ? (
            <div className="space-y-2 max-w-xl">
              {field('capture.exclude: patterns on recorded paths, one per line',
                <textarea aria-label="capture.exclude" rows={3} value={draft['capture.exclude'] ?? ''}
                  onChange={e => setDraft(d => ({ ...d, 'capture.exclude': e.target.value }))}
                  className={`${inputCls} mt-1 font-mono`} />)}
              {field('capture.group: units sharing it are captured together under pause scope group',
                <input aria-label="capture.group" value={draft['capture.group'] ?? ''}
                  onChange={e => setDraft(d => ({ ...d, 'capture.group': e.target.value }))}
                  className={`${inputCls} mt-1`} />)}
              <div className="text-[11px] text-text-muted">An empty field drops the unit's own value.</div>
              <div className="flex gap-2">
                <button className={primaryCls} disabled={busy} onClick={() => void save()}>Save policy</button>
                <button className={btnCls} onClick={() => { setEditing(false); setDraft({ ...own }); setError(null) }}>Cancel</button>
              </div>
            </div>
          ) : (
            <div className="space-y-0.5">
              <div><span className="font-mono text-text-muted">capture.exclude </span>
                {own['capture.exclude']
                  ? <span className="capture-exclude font-mono whitespace-pre-wrap">{own['capture.exclude']}</span>
                  : <span className="text-text-muted">none</span>}</div>
              <div><span className="font-mono text-text-muted">capture.group </span>
                {own['capture.group']
                  ? <span className="capture-group font-mono">{own['capture.group']}</span>
                  : <span className="text-text-muted">none</span>}</div>
            </div>
          )}
        </div>
      )}
    </section>
  )
}
