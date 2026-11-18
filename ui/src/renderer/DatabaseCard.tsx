/*
 * One database finding, as docs/db-detection.md 7.3 describes the card.
 *
 * Read-only by design for D1. The actions row at the bottom is where D2's
 * Preview, Enable and Dismiss go; it is rendered empty on purpose so the
 * layout already has room for them and nobody has to rediscover where they
 * belong. Nothing here posts.
 */

import React, { useState } from 'react'
import type { WireDbFinding } from '../shared/wire'
import {
  engineTitle, coverageSeverity, coverageLabel, coverageWords, methodLabel,
  suggestionTitle, suggestionTradeoff, evidenceLines, CONFIDENCE_MEANING,
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

interface Props {
  finding: WireDbFinding
  /* Shown when the card is not already under its scope's heading. */
  showScope?: boolean
}

export function DatabaseCard({ finding: f, showScope }: Props): React.ReactElement {
  const [showEvidence, setShowEvidence] = useState(false)
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

      {/* D2: Preview / Enable / Dismiss go here. Left empty so D1 ships
        * read-only and the card already reserves their place. */}
      <div className="db-card-actions flex gap-2 empty:hidden" data-finding-id={f.id} />
    </div>
  )
}
