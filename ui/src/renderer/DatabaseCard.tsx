/*
 * One database finding, as docs/db-detection.md 7.3 describes the card.
 *
 * The actions row carries D2's Preview, Enable and Dismiss. Enable posts
 * exactly the template and params the last preview showed, so what the
 * operator read is what gets recorded; there is no Enable before a preview.
 * A hint has no data to capture and a dismissed finding is answered, so
 * neither offers actions.
 */

import React, { useState } from 'react'
import type { WireDbFinding, WireDbPreview } from '../shared/wire'
import {
  engineTitle, coverageSeverity, coverageLabel, coverageWords, methodLabel,
  suggestionTitle, suggestionTradeoff, evidenceLines, CONFIDENCE_MEANING,
  canAct, policyRefusal, reasonProblem, argvLine,
  type Severity,
} from '../shared/databases'
import { absoluteTime, relativeTime } from './format'

/* Alarm is filled, so an uncovered database stands out in a list of covered
 * ones at a glance; the rest are outlined. */
const SEVERITY_BADGE: Record<Severity, string> = {
  alarm: 'bg-status-error text-white',
  caution: 'bg-status-warning text-white',
  ok: 'border border-status-connected text-status-connected',
  none: 'border border-border-default text-text-muted',
}

const SEVERITY_EDGE: Record<Severity, string> = {
  alarm: 'border-l-status-error',
  caution: 'border-l-status-warning',
  ok: 'border-l-status-connected',
  none: 'border-l-border-default',
}

const CONFIDENCE_BADGE: Record<string, string> = {
  live: 'bg-accent text-accent-text',
  dormant: 'bg-surface-tertiary text-text-secondary',
  hint: 'border border-border-default text-text-muted',
}

const STATE_BADGE: Record<string, string> = {
  new: 'text-accent',
  suggested: 'text-status-warning',
  enabled: 'text-status-connected',
  dismissed: 'text-text-muted line-through',
}

export function Badge({ className, children, title }: {
  className: string; children: React.ReactNode; title?: string
}): React.ReactElement {
  return (
    <span title={title}
      className={`inline-block px-1.5 py-0 rounded text-[10px] font-semibold whitespace-nowrap ${className}`}>
      {children}
    </span>
  )
}

export function CoverageBadge({ finding }: { finding: Pick<WireDbFinding, 'coverage'> }): React.ReactElement {
  return (
    <Badge className={SEVERITY_BADGE[coverageSeverity(finding.coverage)]}>
      {coverageLabel(finding.coverage)}
    </Badge>
  )
}

const BTN = 'px-2 py-0.5 text-[11px] rounded border-none cursor-pointer disabled:opacity-50'
const BTN_PLAIN = `${BTN} bg-surface-tertiary text-text-secondary hover:bg-surface-hover`

/*
 * Asks why a finding is being dismissed. Shaped like ConfirmDialog; the
 * reason is checked against the route's rule as it is typed so a refusal
 * never has to come back from the daemon.
 */
function DismissDialog({ finding, onConfirm, onCancel }: {
  finding: WireDbFinding; onConfirm: (reason: string) => void; onCancel: () => void
}): React.ReactElement {
  const [reason, setReason] = useState('')
  const problem = reasonProblem(reason)
  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
      <div className="bg-surface-primary rounded-lg p-5 w-96 shadow-xl border border-border-default">
        <h3 className="text-sm font-semibold m-0 mb-2">Dismiss {engineTitle(finding.engine)} finding</h3>
        <p className="text-xs text-text-secondary m-0 mb-2 break-all">
          {finding.path || finding.scope}
        </p>
        <input autoFocus value={reason} onChange={e => setReason(e.target.value)}
          aria-label="Reason"
          placeholder="Why this finding is wrong or not wanted"
          onKeyDown={e => {
            if (e.key === 'Enter' && !problem) onConfirm(reason.trim())
            if (e.key === 'Escape') onCancel()
          }}
          className="w-full text-xs px-2 py-1.5 rounded bg-surface-secondary text-text-primary
                     border border-border-default box-border" />
        <div className="text-[11px] text-text-muted mt-1 mb-4 min-h-[1em]">
          {reason && problem ? <span className="text-status-error">{problem}</span> : 'Recorded in the journal with your name.'}
        </div>
        <div className="flex justify-end gap-2">
          <button onClick={onCancel}
            className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none">
            Cancel
          </button>
          <button onClick={() => onConfirm(reason.trim())} disabled={!!problem}
            className="px-3 py-1.5 text-xs cursor-pointer rounded border-none bg-accent text-accent-text
                       hover:bg-accent-hover disabled:opacity-50">
            Dismiss
          </button>
        </div>
      </div>
    </div>
  )
}

function PreviewPanel({ p }: { p: WireDbPreview }): React.ReactElement {
  const rows: [string, React.ReactNode][] = []
  rows.push(['Template', <span>{p.template} <span className="text-text-muted">({p.kind})</span></span>])
  if (p.container) rows.push(['Runs in', `${p.container}${p.user ? ` as ${p.user}` : ''}`])
  if (p.argv) rows.push(['Command', <span className="font-mono">{argvLine(p.argv)}</span>])
  if (p.post_argv) rows.push(['After', <span className="font-mono">{argvLine(p.post_argv)}</span>])
  if (p.output) rows.push(['Output', <span className="font-mono">{p.output}</span>])
  if (p.log) rows.push(['Log', <span className="font-mono">{p.log}</span>])
  if (p.needs_quiesce) rows.push(['Quiesce', 'the unit is paused while this runs'])

  const probe = p.probe
  return (
    <div className="mt-2 border border-border-default rounded p-2 bg-surface-primary">
      {p.summary && <div className="text-xs text-text-primary mb-1">{p.summary}</div>}
      <table className="w-full text-[11px]">
        <tbody>
          {rows.map(([label, value]) => (
            <tr key={label} className="align-top">
              <td className="text-text-muted pr-3 py-0.5 whitespace-nowrap w-0">{label}</td>
              <td className="text-text-primary py-0.5 break-all">{value}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {!p.allowed && p.policy_line && (
        <div className="text-[11px] mt-1.5">
          <div className="text-status-warning">The helper's policy does not allow this template. Add:</div>
          <code className="block font-mono text-text-primary bg-surface-tertiary rounded px-1.5 py-0.5 mt-0.5 break-all select-all">
            {p.policy_line}
          </code>
        </div>
      )}
      {probe && (
        <div className="text-[11px] mt-1.5">
          <div className={p.verified ? 'text-status-connected' : 'text-status-error'}>
            Probe {probe.timed_out ? 'timed out' : `exited ${probe.exit}`}
            {p.verified ? ': verified' : ': not verified'}
            {probe.argv?.length ? <span className="text-text-muted font-mono"> ({argvLine(probe.argv)})</span> : null}
          </div>
          {probe.output && (
            <pre className="font-mono text-text-secondary bg-surface-tertiary rounded px-1.5 py-1 mt-0.5 mb-0
                            max-h-40 overflow-auto whitespace-pre-wrap break-all">
              {probe.output}
            </pre>
          )}
          {probe.truncated && <div className="text-text-muted">output truncated at 64 KiB</div>}
        </div>
      )}
    </div>
  )
}

interface Props {
  finding: WireDbFinding
  /* Where the actions post. */
  connName: string
  repoId: number
  /* After an enable or dismiss, so the list shows the new state. */
  onChanged?: () => void
  /* Shown when the card is not already under its scope's heading. */
  showScope?: boolean
}

export function DatabaseCard({ finding: f, connName, repoId, onChanged, showScope }: Props): React.ReactElement {
  const [showEvidence, setShowEvidence] = useState(false)
  const [preview, setPreview] = useState<WireDbPreview | null>(null)
  const [busy, setBusy] = useState<'preview' | 'enable' | 'dismiss' | null>(null)
  const [actionError, setActionError] = useState<string | null>(null)
  const [policyLine, setPolicyLine] = useState<string | null>(null)
  const [dismissing, setDismissing] = useState(false)

  const act = async (what: 'preview' | 'enable' | 'dismiss', fn: () => Promise<void>) => {
    setBusy(what)
    setActionError(null)
    setPolicyLine(null)
    try {
      await fn()
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      const refused = policyRefusal(msg)
      setActionError(refused ? refused.error : msg)
      setPolicyLine(refused ? refused.policyLine : null)
    }
    setBusy(null)
  }

  const doPreview = () => act('preview', async () => {
    setPreview(null)
    setPreview(await window.cbackup.apiCall<WireDbPreview>(connName, {
      kind: 'dbPreview', repoId, findingId: f.id,
    }))
  })

  const doEnable = () => act('enable', async () => {
    if (!preview) return
    await window.cbackup.apiCall(connName, {
      kind: 'dbEnable', repoId, findingId: f.id, template: preview.template,
      ...(preview.params ? { params: preview.params } : {}),
    })
    setPreview(null)
    onChanged?.()
  })

  const doDismiss = (reason: string) => {
    setDismissing(false)
    void act('dismiss', async () => {
      await window.cbackup.apiCall(connName, { kind: 'dbDismiss', repoId, findingId: f.id, reason })
      setPreview(null)
      onChanged?.()
    })
  }

  const unverified = preview?.verified === false
  const sev = coverageSeverity(f.coverage)
  const evidence = evidenceLines(f.evidence)
  const words = coverageWords(f)
  const method = methodLabel(f.method)

  return (
    <div className={`bg-surface-secondary border border-border-default border-l-4 rounded-lg p-3
                     ${SEVERITY_EDGE[sev]}`}>
      <div className="flex items-start gap-2 flex-wrap">
        <div className="text-sm font-medium text-text-primary">
          {engineTitle(f.engine)}
          {f.version ? <span className="text-text-secondary font-normal"> {f.version}</span> : null}
        </div>
        <Badge className={CONFIDENCE_BADGE[f.confidence] ?? CONFIDENCE_BADGE.hint}
          title={CONFIDENCE_MEANING[f.confidence]}>
          {f.confidence}
        </Badge>
        <CoverageBadge finding={f} />
        <span className={`ml-auto text-[11px] ${STATE_BADGE[f.state] ?? 'text-text-muted'}`}
          title={f.decided_at ? `${f.decided_by ?? ''} ${absoluteTime(f.decided_at)}`.trim() : undefined}>
          {f.state}
        </span>
      </div>

      <div className="text-xs font-mono text-text-secondary mt-1 break-all">
        {/* A hint has no path: nothing was found on disk, which is the point
          * of it, so it says so rather than leaving a gap. */}
        {f.path || <span className="text-text-muted font-sans">no data files in the backed-up paths</span>}
      </div>
      {showScope && (
        <div className="text-[11px] text-text-muted mt-0.5 break-all">{f.scope}</div>
      )}

      {words && (
        <div className={`text-xs mt-2 ${sev === 'alarm' ? 'text-status-error' : 'text-text-primary'}`}>
          {words}
        </div>
      )}
      {method && (
        <div className="text-[11px] text-text-muted mt-0.5">Captured by {method}</div>
      )}

      {(f.suggestion || f.optional) && (
        <div className="mt-2 space-y-1">
          {f.suggestion && (
            <div className="text-xs">
              <span className="text-text-secondary">Suggested: </span>
              <span className="font-medium text-text-primary">{suggestionTitle(f.suggestion)}</span>
              {suggestionTradeoff(f.suggestion) && (
                <span className="text-text-muted"> - {suggestionTradeoff(f.suggestion)}</span>
              )}
            </div>
          )}
          {f.optional && (
            <div className="text-xs">
              <span className="text-text-secondary">Optional: </span>
              <span className="text-text-primary">{suggestionTitle(f.optional)}</span>
              {suggestionTradeoff(f.optional) && (
                <span className="text-text-muted"> - {suggestionTradeoff(f.optional)}</span>
              )}
            </div>
          )}
        </div>
      )}

      {f.state === 'enabled' && f.template && (
        <div className="text-[11px] text-text-muted mt-1">Enabled: {f.template}</div>
      )}
      {f.state === 'dismissed' && f.reason && (
        <div className="text-[11px] text-text-muted mt-1">Dismissed: {f.reason}</div>
      )}

      <div className="flex items-center gap-3 mt-2 text-[11px] text-text-muted">
        {evidence.length > 0 && (
          <button onClick={() => setShowEvidence(!showEvidence)}
            className="text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer p-0 text-[11px]">
            {showEvidence ? 'Hide evidence' : `Evidence (${evidence.length})`}
          </button>
        )}
        <span title={absoluteTime(f.first_seen)}>first seen {relativeTime(f.first_seen)}</span>
        <span title={absoluteTime(f.last_seen)}>last seen {relativeTime(f.last_seen)}</span>
        {f.last_snap_id !== null && f.last_snap_id !== undefined && (
          <span>snapshot #{f.last_snap_id}</span>
        )}
      </div>

      {showEvidence && evidence.length > 0 && (
        <table className="w-full text-[11px] mt-1.5">
          <tbody>
            {evidence.map((e, i) => (
              <tr key={i} className="align-top">
                <td className="text-text-muted pr-3 py-0.5 whitespace-nowrap w-0">{e.label}</td>
                <td className="text-text-primary py-0.5 font-mono break-all">{e.value}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {canAct(f) && (
        <>
          {preview && <PreviewPanel p={preview} />}
          <div className="db-card-actions flex items-center gap-2 mt-2" data-finding-id={f.id}>
            <button onClick={() => void doPreview()} disabled={busy !== null} className={BTN_PLAIN}
              title="Ask the helper what this template would run, and run its probe">
              {busy === 'preview' ? 'Previewing...' : 'Preview'}
            </button>
            <button onClick={() => void doEnable()} disabled={busy !== null || !preview}
              title={!preview ? 'Preview first: Enable records the previewed template'
                : unverified ? 'The probe did not succeed; enabling may fail at backup time'
                : `Enable ${preview.template} for this database`}
              className={`${BTN} ${unverified
                ? 'bg-status-warning text-white hover:opacity-90'
                : 'bg-accent text-accent-text hover:bg-accent-hover'}`}>
              {busy === 'enable' ? 'Enabling...' : unverified ? 'Enable (unverified)' : 'Enable'}
            </button>
            <button onClick={() => setDismissing(true)} disabled={busy !== null} className={BTN_PLAIN}>
              {busy === 'dismiss' ? 'Dismissing...' : 'Dismiss'}
            </button>
          </div>
          {actionError && <div className="text-[11px] text-status-error mt-1 break-all">{actionError}</div>}
          {policyLine && (
            <div className="text-[11px] mt-1">
              <span className="text-text-muted">Add to the helper's policy: </span>
              <code className="font-mono text-text-primary bg-surface-tertiary rounded px-1.5 py-0.5 break-all select-all">
                {policyLine}
              </code>
            </div>
          )}
        </>
      )}
      {dismissing && (
        <DismissDialog finding={f} onConfirm={doDismiss} onCancel={() => setDismissing(false)} />
      )}
    </div>
  )
}
