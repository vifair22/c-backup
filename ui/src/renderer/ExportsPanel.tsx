/*
 * NBD exports of a VM disk (Manual 22.8): open one, list the open ones,
 * revoke one.
 *
 * An export serves one disk of one snapshot read-only on a unix socket so
 * a filesystem in it can be mounted without restoring the image. Its name
 * (128 random bits) is in the create answer only and never listed again,
 * so the card that shows it says so; everything else is in the list.
 */

import React, { useCallback, useEffect, useState } from 'react'
import type { WireExport, WireExportCreated } from '../shared/wire'
import { ttlSeconds } from '../shared/units'
import { absoluteTime, fmtNum, fmtSize } from './format'
import { ConfirmDialog } from './ConfirmDialog'
import { btnCls, inputCls, primaryCls } from './UnitsView'

const api = window.cbackup

/* Open an export of one disk: the TTL as `vm export --ttl` takes it. */
export function ExportCreate({ connName, repoId, snapshotId, item, onCreated, onCancel }: {
  connName: string; repoId: number; snapshotId: number; item: string
  onCreated: (e: WireExportCreated) => void; onCancel: () => void
}): React.ReactElement {
  const [ttl, setTtl] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  const create = async () => {
    const secs = ttlSeconds(ttl)
    if (secs === 0) {
      setError('the lifetime must be 60 seconds to 7 days, as N, Ns, Nm, Nh or Nd')
      return
    }
    setBusy(true)
    setError(null)
    try {
      onCreated(await api.apiCall<WireExportCreated>(connName, {
        kind: 'exportCreate', repoId, snapshotId, item, ...(secs ? { ttlS: secs } : {}),
      }))
    } catch (err) {
      setError(String(err instanceof Error ? err.message : err))
    }
    setBusy(false)
  }

  return (
    <div className="export-create mt-1 p-2 rounded border border-border-default bg-surface-secondary text-xs">
      <div className="flex items-center gap-2">
        <span>Export <span className="font-mono">{item}</span> of #{snapshotId} for</span>
        <input aria-label="Lifetime" value={ttl} onChange={e => setTtl(e.target.value)}
          placeholder="default (4h)" className={`${inputCls} w-28`} />
        <button className={primaryCls} disabled={busy} onClick={() => void create()}>Create export</button>
        <button className={btnCls} onClick={onCancel}>Cancel</button>
      </div>
      {error && <div className="export-error text-status-error mt-1 whitespace-pre-wrap break-words">{error}</div>}
    </div>
  )
}

/* The answer to a create: the one place the export's name is shown. */
export function ExportCreated({ e, onDismiss }: { e: WireExportCreated; onDismiss: () => void }): React.ReactElement {
  return (
    <div className="export-created mb-3 p-2 rounded border border-accent text-xs space-y-1">
      <div className="flex items-center">
        <span className="text-status-connected">Export #{e.export_id} open</span>
        <span className="text-text-muted ml-2">{fmtSize(e.size)}, until {absoluteTime(e.expires)},
          closes after {fmtNum(Math.round(e.idle_timeout_s / 60))} min with no connection</span>
        <button onClick={onDismiss}
          className="ml-auto bg-transparent border-none cursor-pointer text-text-muted hover:text-text-primary text-xs">dismiss</button>
      </div>
      <div className="text-status-warning">
        Copy the name now: the daemon returns it once and never lists it again.
      </div>
      <div><span className="text-text-muted">Name </span><span className="font-mono break-all select-all">{e.name}</span></div>
      <div><span className="text-text-muted">Socket </span><span className="font-mono break-all">{e.socket}</span></div>
      <div className="text-text-muted">On the daemon's host, as root:</div>
      <pre className="font-mono bg-surface-secondary p-1.5 rounded whitespace-pre-wrap break-all m-0 select-all">{e.attach}</pre>
    </div>
  )
}

/*
 * The open exports of this repository, optionally only those of some
 * snapshots (a unit's page). refresh changes when the caller opened one.
 */
export function ExportsPanel({ connName, repoId, snapshots, refresh = 0 }: {
  connName: string; repoId: number; snapshots?: ReadonlySet<number>; refresh?: number
}): React.ReactElement {
  const [list, setList] = useState<WireExport[]>([])
  const [error, setError] = useState<string | null>(null)
  const [revoking, setRevoking] = useState<WireExport | null>(null)
  const [closed, setClosed] = useState<WireExport | null>(null)

  const load = useCallback(async () => {
    try {
      const r = await api.apiCall<{ exports?: WireExport[] }>(connName, { kind: 'exportList' })
      setList((r.exports ?? []).filter(x => x.repo_id === repoId))
      setError(null)
    } catch (err) {
      setError(String(err instanceof Error ? err.message : err))
    }
  }, [connName, repoId])

  useEffect(() => { void load() }, [load, refresh])

  const revoke = async (x: WireExport) => {
    setRevoking(null)
    try {
      await api.apiCall(connName, { kind: 'exportRevoke', exportId: x.id })
      /* What the daemon recorded, closed: its reason, as export-show says. */
      setClosed(await api.apiCall<WireExport>(connName, { kind: 'exportShow', exportId: x.id }))
    } catch (err) {
      setError(String(err instanceof Error ? err.message : err))
    }
    void load()
  }

  const shown = snapshots ? list.filter(x => snapshots.has(x.snap_id)) : list

  return (
    <section className="mb-4">
      <h4 className="text-sm font-medium m-0 mb-2">Open NBD exports</h4>
      {error && <div className="text-status-error text-xs mb-2 whitespace-pre-wrap">{error}</div>}
      {closed && (
        <div className="export-closed text-xs text-text-muted mb-2">
          Export #{closed.id} closed: {closed.close_reason ?? 'closed'}
          {closed.closed_at ? ` at ${absoluteTime(closed.closed_at)}` : ''}
        </div>
      )}
      {shown.length === 0 ? (
        <div className="text-text-muted text-xs">None. A disk of a snapshot can be exported from its row above.</div>
      ) : (
        <table className="w-full text-xs">
          <thead>
            <tr className="text-text-muted text-left">
              <th className="font-medium pb-1">Export</th>
              <th className="font-medium pb-1">Disk</th>
              <th className="font-medium pb-1">Socket</th>
              <th className="font-medium pb-1">Expires</th>
              <th className="font-medium pb-1 text-right">Connections</th>
              <th className="font-medium pb-1 text-right">Served</th>
              <th className="font-medium pb-1" />
            </tr>
          </thead>
          <tbody>
            {shown.map(x => (
              <tr key={x.id} data-export={x.id} className="export-row border-t border-border-default text-text-primary">
                <td className="py-1 font-mono">#{x.id}</td>
                <td className="py-1"><span className="font-mono">{x.item}</span> of #{x.snap_id}, {fmtSize(x.size)}</td>
                <td className="py-1 font-mono break-all">{x.socket}</td>
                <td className="py-1">{absoluteTime(x.expires)}</td>
                <td className="py-1 text-right">{fmtNum(x.connections ?? 0)}</td>
                <td className="py-1 text-right">{fmtSize(x.bytes_served ?? 0)}</td>
                <td className="py-1 text-right">
                  <button className={btnCls} onClick={() => setRevoking(x)}>Revoke</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {revoking && (
        <ConfirmDialog title={`Revoke export #${revoking.id}?`}
          message={`Every client of ${revoking.item} of snapshot #${revoking.snap_id} is disconnected and the socket removed.`}
          confirmLabel="Revoke" onConfirm={() => void revoke(revoking)} onCancel={() => setRevoking(null)} />
      )}
    </section>
  )
}
