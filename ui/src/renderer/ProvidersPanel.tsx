/*
 * `c-backup unit providers` on the Units screen: every provider compiled
 * into the daemon, enabled or not, how it is reached, what its probe says,
 * and the helper as its hello described it (Manual 22.2.4). It answers
 * "why are there no units" and "is the helper up" without a shell.
 */

import React from 'react'
import type { WireProviders } from '../shared/wire'
import { absoluteTime, fmtNum } from './format'

export function ProvidersPanel({ data }: { data: WireProviders }): React.ReactElement {
  const h = data.helper
  return (
    <div className="providers-panel text-xs space-y-2">
      <table className="w-full">
        <thead>
          <tr className="text-text-muted text-left">
            <th className="font-medium pb-1 pr-3">Provider</th>
            <th className="font-medium pb-1 pr-3">Enabled</th>
            <th className="font-medium pb-1 pr-3">Via</th>
            <th className="font-medium pb-1 pr-3">Probe</th>
          </tr>
        </thead>
        <tbody>
          {data.providers.map(p => (
            <tr key={p.name} data-provider={p.name} className="provider-row border-t border-border-default align-top">
              <td className="py-1 pr-3 font-mono">{p.name}</td>
              <td className={`py-1 pr-3 ${p.enabled ? 'text-status-connected' : 'text-text-muted'}`}>{p.enabled ? 'yes' : 'no'}</td>
              <td className="py-1 pr-3">
                {p.via ?? '-'}
                {p.connection && (
                  <span className={p.connection.state === 'connected' ? 'text-text-muted' : 'text-status-error'}>
                    {' '}({p.connection.state}{p.connection.last_error ? `: ${p.connection.last_error}` : ''})
                  </span>
                )}
              </td>
              <td className="py-1 break-words">
                {p.probe ? (
                  <span className={p.probe.available ? '' : 'text-status-warning'}>
                    {p.probe.available ? 'available' : 'unavailable'}
                    {p.probe.version ? `, ${p.probe.version}` : ''}
                    {p.probe.detail ? <span className="text-text-muted"> - {p.probe.detail}</span> : null}
                  </span>
                ) : p.probe_error ? <span className="text-status-error">{p.probe_error}</span>
                  : <span className="text-text-muted">-</span>}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="text-text-muted space-x-4">
        <span>daemon id <span className="font-mono text-text-primary">{data.daemon_id}</span></span>
        <span>units.allow <span className="font-mono text-text-primary">{data.allow || '(any)'}</span></span>
        <span>holds open <span className="holds-open text-text-primary">{fmtNum(data.holds_open)}</span></span>
        {data.privileges_dropped !== undefined && (
          <span>privileges {data.privileges_dropped ? 'dropped' : 'not dropped'}</span>
        )}
      </div>
      {h && (
        <div className="helper-info text-text-muted">
          Helper at <span className="font-mono text-text-primary">{h.socket}</span>:{' '}
          <span className={h.state === 'connected' ? 'text-status-connected' : 'text-status-error'}>{h.state}</span>
          {h.since ? ` since ${absoluteTime(h.since)}` : ''}
          {h.last_error ? <span className="text-status-error">, last error: {h.last_error}</span> : null}
          {h.state === 'connected' && (
            <span>
              ; protocol {h.version}, offers {h.providers || 'nothing'}, its daemon id {h.daemon_id}
              {h.daemon_id && h.daemon_id !== data.daemon_id ? ' (not this daemon\'s)' : ''},
              policy {h.policy.slice(0, 16)}, path snapshots {h.path_snapshots ? 'yes' : 'no'}
              {h.holds !== undefined ? `, ${fmtNum(h.holds)} holds and ${fmtNum(h.captures ?? 0)} captures open` : ''}
              , {fmtNum(h.reconnects)} reconnects
            </span>
          )}
        </div>
      )}
    </div>
  )
}
