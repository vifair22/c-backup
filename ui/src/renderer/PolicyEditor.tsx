import React, { useEffect, useState } from 'react'

const api = window.cbackup

interface Policy {
  paths: string[]
  exclude: string[]
  keep_snaps: number
  keep_daily: number
  keep_weekly: number
  keep_monthly: number
  keep_yearly: number
  auto_pack: boolean
  auto_gc: boolean
  auto_prune: boolean
  verify_after: boolean
  strict_meta: boolean
}

const DEFAULT_POLICY: Policy = {
  paths: [], exclude: [],
  keep_snaps: 0, keep_daily: 7, keep_weekly: 4, keep_monthly: 12, keep_yearly: 3,
  auto_pack: true, auto_gc: true, auto_prune: true,
  verify_after: false, strict_meta: false,
}

interface Props {
  connName: string
  repoPath: string
  onBack: () => void
}

export function PolicyEditor({ connName, repoPath, onBack }: Props): React.ReactElement {
  const [policy, setPolicy] = useState<Policy | null>(null)
  const [original, setOriginal] = useState<string>('') // JSON for dirty check
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState<string | null>(null)
  const [noPolicy, setNoPolicy] = useState(false)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    api.rpcCall<Policy | null>(connName, repoPath, 'policy')
      .then(data => {
        if (cancelled) return
        if (!data) {
          setNoPolicy(true)
          const p = { ...DEFAULT_POLICY }
          setPolicy(p)
          setOriginal(JSON.stringify(p))
        } else {
          setPolicy(data)
          setOriginal(JSON.stringify(data))
        }
      })
      .catch(err => { if (!cancelled) setError(String(err)) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [connName, repoPath])

  const isDirty = policy ? JSON.stringify(policy) !== original : false

  const handleSave = async () => {
    if (!policy) return
    setSaving(true)
    setError(null)
    setSuccess(null)
    try {
      await api.rpcCall(connName, repoPath, 'save_policy', policy as unknown as Record<string, unknown>)
      setOriginal(JSON.stringify(policy))
      setNoPolicy(false)
      setSuccess('Policy saved')
      setTimeout(() => setSuccess(null), 3000)
    } catch (err) {
      setError(String(err))
    }
    setSaving(false)
  }

  const updateNum = (key: keyof Policy, value: string) => {
    if (!policy) return
    const n = parseInt(value)
    if (isNaN(n) || n < 0) return
    setPolicy({ ...policy, [key]: n })
  }

  const updateBool = (key: keyof Policy, value: boolean) => {
    if (!policy) return
    setPolicy({ ...policy, [key]: value })
  }

  const updateList = (key: 'paths' | 'exclude', value: string) => {
    if (!policy) return
    setPolicy({ ...policy, [key]: value.split('\n').filter(l => l.trim()) })
  }

  const inputCls = 'px-2 py-1 border border-border-default rounded text-sm bg-surface-primary text-text-primary focus:outline-none focus:border-accent w-20 text-right'

  return (
    <div>
      {/* Header */}
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Dashboard
        </button>
        <h3 className="text-lg font-medium m-0">Backup Policy</h3>
        {isDirty && <span className="text-xs text-status-warning">Unsaved changes</span>}
      </div>

      {loading && <div className="text-text-muted text-sm">Loading policy...</div>}
      {error && <div className="text-status-error text-sm mb-3">{error}</div>}
      {success && <div className="text-status-connected text-sm mb-3">{success}</div>}

      {noPolicy && !loading && (
        <div className="text-xs text-status-warning bg-status-warning/10 rounded px-3 py-2 mb-4">
          No policy file found. Showing defaults — save to create one.
        </div>
      )}

      {policy && !loading && (
        <div className="space-y-4">
          {/* Backup paths */}
          <div className="bg-surface-secondary border border-border-default rounded-lg p-4">
            <div className="text-[11px] text-text-muted uppercase tracking-wide mb-2">Backup Paths</div>
            <textarea
              value={policy.paths.join('\n')}
              onChange={e => updateList('paths', e.target.value)}
              placeholder="One path per line"
              className="w-full px-2 py-1.5 border border-border-default rounded text-xs bg-surface-primary text-text-primary focus:outline-none focus:border-accent font-mono min-h-[60px] resize-y"
              rows={Math.max(3, policy.paths.length + 1)}
            />
          </div>

          {/* Exclude patterns */}
          <div className="bg-surface-secondary border border-border-default rounded-lg p-4">
            <div className="text-[11px] text-text-muted uppercase tracking-wide mb-2">Exclude Patterns</div>
            <textarea
              value={policy.exclude.join('\n')}
              onChange={e => updateList('exclude', e.target.value)}
              placeholder="One pattern per line"
              className="w-full px-2 py-1.5 border border-border-default rounded text-xs bg-surface-primary text-text-primary focus:outline-none focus:border-accent font-mono min-h-[60px] resize-y"
              rows={Math.max(3, policy.exclude.length + 1)}
            />
          </div>

          {/* Retention */}
          <div className="bg-surface-secondary border border-border-default rounded-lg p-4">
            <div className="text-[11px] text-text-muted uppercase tracking-wide mb-3">Retention (GFS)</div>
            <div className="grid grid-cols-5 gap-4">
              {([
                { key: 'keep_snaps' as const, label: 'Total Snaps', hint: '0 = unlimited' },
                { key: 'keep_daily' as const, label: 'Daily', hint: '' },
                { key: 'keep_weekly' as const, label: 'Weekly', hint: '' },
                { key: 'keep_monthly' as const, label: 'Monthly', hint: '' },
                { key: 'keep_yearly' as const, label: 'Yearly', hint: '' },
              ]).map(({ key, label, hint }) => (
                <div key={key}>
                  <label className="block text-xs text-text-secondary mb-1">{label}</label>
                  <input type="number" min={0}
                    value={policy[key]}
                    onChange={e => updateNum(key, e.target.value)}
                    className={inputCls}
                  />
                  {hint && <div className="text-[10px] text-text-muted mt-0.5">{hint}</div>}
                </div>
              ))}
            </div>
          </div>

          {/* Automation */}
          <div className="bg-surface-secondary border border-border-default rounded-lg p-4">
            <div className="text-[11px] text-text-muted uppercase tracking-wide mb-3">Automation</div>
            <div className="grid grid-cols-2 gap-3">
              {([
                { key: 'auto_pack', label: 'Auto pack', desc: 'Pack loose objects after backup' },
                { key: 'auto_gc', label: 'Auto GC', desc: 'Garbage collect after prune' },
                { key: 'auto_prune', label: 'Auto prune', desc: 'Prune expired snapshots after backup' },
                { key: 'verify_after', label: 'Verify after', desc: 'Verify integrity after backup' },
                { key: 'strict_meta', label: 'Strict metadata', desc: 'Fail on metadata errors (xattr, ACL)' },
              ] as const).map(({ key, label, desc }) => (
                <label key={key} className="flex items-start gap-2 cursor-pointer">
                  <input type="checkbox"
                    checked={policy[key] as boolean}
                    onChange={e => updateBool(key, e.target.checked)}
                    className="mt-0.5"
                  />
                  <div>
                    <div className="text-xs text-text-primary">{label}</div>
                    <div className="text-[10px] text-text-muted">{desc}</div>
                  </div>
                </label>
              ))}
            </div>
          </div>

          {/* Save */}
          <div className="flex justify-end">
            <button onClick={handleSave}
              disabled={!isDirty || saving}
              className="px-4 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text hover:bg-accent-hover border-none disabled:opacity-50 disabled:cursor-default">
              {saving ? 'Saving...' : 'Save Policy'}
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
