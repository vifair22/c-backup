/*
 * Retention: how much history this repository keeps.
 *
 * Rewritten rather than migrated. The old editor was built around a policy
 * struct that no longer exists anywhere: it offered paths, exclude,
 * auto_pack, auto_gc, auto_prune, verify_after and strict_meta, and read
 * them out of a response that is a flat list of key/value rows. Nothing in
 * the daemon reads any of those seven. Sources and excludes moved to the
 * backup dialog, where they belong, and the five retention counters are the
 * only policy values anything acts on.
 *
 * Offering a switch that changes nothing is worse than not offering it, so
 * the five that work are here and the rest are gone. Any other key set from
 * the CLI is listed read-only, because a stored value the app hides is the
 * same problem in the other direction.
 */

import React, { useCallback, useEffect, useState } from 'react'

const api = window.cbackup

interface RawPolicy { key: string; value: string }

/*
 * The daemon's own fallbacks, from the gfs job. Shown as the value in force
 * when a repository has set nothing, rather than as a blank field: "7" and
 * "" mean very different things to somebody deciding whether to change it.
 */
const FIELDS = [
  { key: 'retention.keep_daily',   label: 'Daily',   dflt: 7,
    hint: 'Days of daily snapshots to keep' },
  { key: 'retention.keep_weekly',  label: 'Weekly',  dflt: 4,
    hint: 'Weeks of weekly snapshots' },
  { key: 'retention.keep_monthly', label: 'Monthly', dflt: 12,
    hint: 'Months of monthly snapshots' },
  { key: 'retention.keep_yearly',  label: 'Yearly',  dflt: 3,
    hint: 'Years of yearly snapshots' },
  { key: 'retention.keep_snaps',   label: 'Recent',  dflt: 0,
    hint: 'Most recent snapshots kept regardless of tier. 0 means no extra window' },
] as const

const KNOWN = new Set<string>(FIELDS.map(f => f.key))

interface Props {
  connName: string
  repoPath: string
  onBack: () => void
}

export function PolicyEditor({ connName, repoPath, onBack }: Props): React.ReactElement {
  const repoId = Number(repoPath)

  const [values, setValues] = useState<Record<string, string>>({})
  const [original, setOriginal] = useState<Record<string, string>>({})
  const [others, setOthers] = useState<RawPolicy[]>([])
  const [stored, setStored] = useState<Set<string>>(new Set())
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const r = await api.apiCall<{ policy?: RawPolicy[] }>(connName, { kind: 'policyGet', repoId })
      const rows = r.policy ?? []
      const next: Record<string, string> = {}
      for (const f of FIELDS) next[f.key] = String(f.dflt)
      for (const row of rows) if (KNOWN.has(row.key)) next[row.key] = row.value
      setValues(next)
      setOriginal(next)
      setStored(new Set(rows.filter(x => KNOWN.has(x.key)).map(x => x.key)))
      setOthers(rows.filter(x => !KNOWN.has(x.key)))
    } catch (err) {
      setError(String(err))
    }
    setLoading(false)
  }, [connName, repoId])

  useEffect(() => { void load() }, [load])

  const changed = FIELDS.filter(f => values[f.key] !== original[f.key])

  const handleSave = async () => {
    setSaving(true)
    setError(null)
    setSuccess(null)
    try {
      /*
       * The route takes one key per call, so a save is one request per
       * changed field. Sequential rather than concurrent: they all write the
       * same table behind one database handle, and a failure part-way
       * through is easier to reason about when the order is known.
       */
      for (const f of changed) {
        await api.apiCall(connName, {
          kind: 'policySet', repoId, key: f.key, value: values[f.key],
        })
      }
      setSuccess(changed.length === 1 ? 'Saved' : `Saved ${changed.length} values`)
      setTimeout(() => setSuccess(null), 3000)
      await load()
    } catch (err) {
      /* Reload either way: some of the writes may have landed, and leaving
       * the form showing what was typed would misreport what is stored. */
      setError(String(err))
      await load()
    }
    setSaving(false)
  }

  return (
    <div>
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Back
        </button>
        <h3 className="text-lg font-medium m-0">Retention</h3>
        {changed.length > 0 && (
          <span className="text-xs text-status-warning">
            {changed.length} unsaved change{changed.length === 1 ? '' : 's'}
          </span>
        )}
      </div>

      <div className="text-xs text-text-muted mb-4" style={{ maxWidth: '38rem' }}>
        Retention is applied when the Apply Retention operation runs, not at
        backup time, and it only ever considers closed calendar days. A tagged
        snapshot is kept regardless of any of this.
      </div>

      {error && <div className="text-status-error text-sm mb-3">{error}</div>}
      {success && <div className="text-status-connected text-sm mb-3">{success}</div>}

      {loading ? (
        <div className="text-text-muted text-sm">Loading...</div>
      ) : (
        <>
          <div className="bg-surface-secondary border border-border-default rounded-lg p-4 mb-4"
               style={{ maxWidth: '38rem' }}>
            {FIELDS.map(f => (
              <div key={f.key} className="flex items-baseline gap-3 mb-3 last:mb-0">
                <label className="text-xs text-text-secondary w-20 shrink-0">{f.label}</label>
                <input
                  type="number" min={0} value={values[f.key] ?? ''}
                  onChange={e => setValues({ ...values, [f.key]: e.target.value })}
                  className="w-20 px-2 py-1 border border-border-default rounded text-xs
                             bg-surface-primary text-text-primary focus:outline-none focus:border-accent" />
                <span className="text-xs text-text-muted">
                  {f.hint}
                  {!stored.has(f.key) && values[f.key] === String(f.dflt) && (
                    <span className="ml-2">(daemon default)</span>
                  )}
                </span>
              </div>
            ))}
          </div>

          <div className="flex gap-2 items-center mb-6">
            <button onClick={handleSave} disabled={saving || changed.length === 0}
              className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text
                         hover:bg-accent-hover border-none disabled:opacity-50 disabled:cursor-default">
              {saving ? 'Saving...' : 'Save'}
            </button>
            {changed.length > 0 && (
              <button onClick={() => setValues(original)} disabled={saving}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary
                           text-text-secondary hover:bg-surface-hover border-none">
                Discard
              </button>
            )}
          </div>

          {others.length > 0 && (
            <div style={{ maxWidth: '38rem' }}>
              <div className="text-xs text-text-secondary mb-1">Other stored values</div>
              <div className="text-xs text-text-muted mb-2">
                Set from the CLI. Listed so nothing stored here is invisible;
                nothing in the daemon currently reads these.
              </div>
              <table className="w-full text-xs">
                <tbody>
                  {others.map(o => (
                    <tr key={o.key} className="border-t border-border-subtle">
                      <td className="py-1.5 pr-4 font-mono text-text-secondary">{o.key}</td>
                      <td className="py-1.5 font-mono">{o.value}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  )
}
