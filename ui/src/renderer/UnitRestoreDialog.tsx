/*
 * Restore one unit snapshot (Manual 22.7): the four ways `c-backup unit
 * restore` offers, as one dialog.
 *
 *   files     the whole snapshot under a directory (each disk a sparse raw
 *             image), optionally over what is there
 *   item      one item to a new file, never over an existing path
 *   new name  the unit re-created beside the original, its data under a
 *             target root (define with name and target_root)
 *   in place  the unit stopped, its data swapped, the old data moved aside
 *             (define with replace); the name must be typed to confirm
 *
 * Nothing is checked here that the daemon checks: a refusal is the
 * daemon's, shown whole, because its wording is the instruction (the B188
 * host-bind rule, a B210 block device, a "cannot verify" naming the
 * restore to a path). Where a refusal names that way round, the dialog
 * offers it.
 */

import React, { useState } from 'react'
import type { Job, UnitRestore } from '../shared/api'
import type { WireUnitRestoreResult, WireUnitSnapshot } from '../shared/wire'
import { itemKindLabel, refusedNothingWritten, suggestsFilesRestore } from '../shared/units'
import { fmtNum, fmtSize } from './format'
import { followJob, jobIdOf } from './followJob'
import { btnCls, inputCls, primaryCls } from './UnitsView'

const api = window.cbackup

type Mode = 'files' | 'item' | 'copy' | 'inplace'

const MODES: Array<{ mode: Mode; label: string; words: string }> = [
  { mode: 'files', label: 'Files, to a directory',
    words: 'The whole snapshot under a directory: data at its recorded paths, each disk a sparse raw image, the definition under .cbackup-unit/.' },
  { mode: 'item', label: 'One item, to a file',
    words: 'One item written to a new file (through <file>.cbk-partial), never over an existing path. A disk comes back as raw.' },
  { mode: 'copy', label: 'As a new unit, under a new name',
    words: 'A second unit beside the original. Its data goes under the target root at its original paths; the original is never touched.' },
  { mode: 'inplace', label: 'In place, replacing the unit',
    words: 'The unit is stopped, its data replaced by the snapshot\'s and its definition re-applied. The old data is moved aside, never deleted.' },
]

interface Props {
  connName: string
  repoId: number
  snapshot: WireUnitSnapshot
  /* The unit's name now, which the in-place confirmation asks for. */
  unitName: string
  onClose: () => void
  /* A restore completed: the unit page reloads its state. */
  onDone: () => void
}

export function UnitRestoreDialog({ connName, repoId, snapshot, unitName, onClose, onDone }: Props): React.ReactElement {
  const [mode, setMode] = useState<Mode>('files')
  const [dest, setDest] = useState('')
  const [overwrite, setOverwrite] = useState(false)
  const [item, setItem] = useState(snapshot.items.find(i => i.kind === 'block')?.id ?? snapshot.items[0]?.id ?? '')
  const [name, setName] = useState('')
  const [targetRoot, setTargetRoot] = useState('')
  const [start, setStart] = useState(false)
  const [pinDigest, setPinDigest] = useState(false)
  const [keepMacs, setKeepMacs] = useState(false)
  const [confirm, setConfirm] = useState('')

  const [busy, setBusy] = useState(false)
  const [phase, setPhase] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [done, setDone] = useState<Job | null>(null)

  const body = (): UnitRestore => {
    switch (mode) {
      case 'files': return { mode: 'files', dest, overwrite }
      case 'item': return { mode: 'item', item, dest }
      case 'copy': return { mode: 'unit', name, targetRoot, start, pinDigest, keepMacs }
      case 'inplace': return { mode: 'unit', replace: true, start, pinDigest, keepMacs }
    }
  }

  const submit = async () => {
    setBusy(true)
    setError(null)
    setDone(null)
    setPhase(null)
    try {
      const started = await api.apiCall<{ job_id?: number }>(connName, {
        kind: 'unitRestore', repoId, snapshotId: snapshot.id, restore: body(),
      })
      const jobId = jobIdOf(started)
      setPhase(`job #${jobId} queued`)
      const job = await followJob(connName, jobId, j => setPhase(`job #${jobId} ${j.state}${j.phase ? `: ${j.phase}` : ''}`))
      if (job.state === 'completed') {
        setDone(job)
        onDone()
      } else {
        setError(job.error || `the restore ${job.state}`)
      }
    } catch (err) {
      setError(String(err instanceof Error ? err.message : err))
    }
    setPhase(null)
    setBusy(false)
  }

  const confirmed = mode !== 'inplace' || confirm === unitName
  const res = (done?.result ?? null) as Partial<WireUnitRestoreResult> | null
  const check = (label: string, value: boolean, set: (v: boolean) => void, title: string) => (
    <label className="flex items-center gap-1.5 text-xs text-text-secondary" title={title}>
      <input type="checkbox" checked={value} onChange={e => set(e.target.checked)} />
      {label}
    </label>
  )

  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
      <div role="dialog" aria-label="Restore unit snapshot"
        className="bg-surface-primary rounded-lg p-5 w-[36rem] max-h-[90vh] overflow-auto shadow-xl border border-border-default">
        <h3 className="text-sm font-semibold m-0 mb-1">Restore Unit Snapshot</h3>
        <div className="text-xs text-text-muted mb-3">
          <span className="font-mono">{unitName}</span>, snapshot #{snapshot.id} ({snapshot.consistency}).
          Paths are the daemon's, on its host.
        </div>

        <div className="space-y-1 mb-3">
          {MODES.map(m => (
            <label key={m.mode} className="flex items-start gap-2 text-xs cursor-pointer">
              <input type="radio" name="unit-restore-mode" checked={mode === m.mode}
                onChange={() => { setMode(m.mode); setError(null); setDone(null) }} className="mt-0.5" />
              <span>
                <span className="text-text-primary">{m.label}</span>
                {mode === m.mode && <span className="block text-text-muted">{m.words}</span>}
              </span>
            </label>
          ))}
        </div>

        <div className="space-y-2 mb-3">
          {(mode === 'files' || mode === 'item') && (
            <label className="block text-xs text-text-secondary">
              {mode === 'files' ? 'Destination directory' : 'Destination file'}
              <input aria-label="Destination" value={dest} onChange={e => setDest(e.target.value)}
                placeholder={mode === 'files' ? '/path/to/restore/directory' : '/path/to/new/file.raw'}
                className={`${inputCls} mt-1`} />
            </label>
          )}
          {mode === 'files' && check('Overwrite what is there', overwrite, setOverwrite,
            'Replace files that already exist under the destination')}
          {mode === 'item' && (
            <label className="block text-xs text-text-secondary">
              Item
              <select aria-label="Item" value={item} onChange={e => setItem(e.target.value)} className={`${inputCls} mt-1`}>
                {snapshot.items.map(i => (
                  <option key={i.id} value={i.id}>
                    {i.id} ({itemKindLabel(i.kind)}{i.size ? `, ${fmtSize(i.size)}` : ''})
                  </option>
                ))}
              </select>
            </label>
          )}
          {mode === 'copy' && (
            <>
              <label className="block text-xs text-text-secondary">
                New name
                <input aria-label="New name" value={name} onChange={e => setName(e.target.value)}
                  className={`${inputCls} mt-1`} />
              </label>
              <label className="block text-xs text-text-secondary">
                Target root
                <input aria-label="Target root" value={targetRoot} onChange={e => setTargetRoot(e.target.value)}
                  placeholder="/path/under/an/include/root" className={`${inputCls} mt-1`} />
              </label>
            </>
          )}
          {(mode === 'copy' || mode === 'inplace') && (
            <div className="flex flex-wrap gap-4">
              {check('Start it', start, setStart, '--start: start the unit once everything else is in place')}
              {check('Pin the image digest', pinDigest, setPinDigest,
                '--pin-digest (containers): fail rather than fall back to the recorded tag')}
              {check('Keep MAC addresses', keepMacs, setKeepMacs,
                '--keep-macs (VMs, LXC): keep the recorded MACs rather than generate new ones')}
            </div>
          )}
          {mode === 'inplace' && (
            <div className="p-2 rounded border border-error-border bg-error-bg text-xs text-error-text">
              This stops <span className="font-mono">{unitName}</span> and replaces its data and definition with
              snapshot #{snapshot.id}. The data it has now is moved aside, not deleted.
              <label className="block mt-2">
                Type <span className="font-mono font-semibold">{unitName}</span> to confirm
                <input aria-label="Confirm unit name" value={confirm} onChange={e => setConfirm(e.target.value)}
                  className={`${inputCls} mt-1`} />
              </label>
            </div>
          )}
        </div>

        {phase && <div className="text-xs text-text-muted mb-2">{phase}</div>}

        {error && (
          <div className="unit-restore-error mb-3 p-2 rounded border border-error-border bg-error-bg text-xs text-error-text">
            <div className="font-semibold mb-1">
              {refusedNothingWritten(error) ? 'Refused, nothing written' : 'The restore did not complete'}
            </div>
            <div className="whitespace-pre-wrap break-words">{error}</div>
            {suggestsFilesRestore(error) && mode !== 'files' && (
              <button className={`${btnCls} mt-2`} onClick={() => { setMode('files'); setError(null) }}>
                Restore the files to a path instead
              </button>
            )}
          </div>
        )}

        {done && (
          <div className="unit-restore-result mb-3 p-2 rounded border border-border-default text-xs space-y-0.5">
            <div className="text-status-connected">Restored (job #{done.id})</div>
            {res?.mode === 'files' && (
              <div>{fmtNum(res.files ?? 0)} files, {fmtNum(res.dirs ?? 0)} directories, {fmtSize(res.bytes ?? 0)}
                {res.failed ? <span className="text-status-error">, {fmtNum(res.failed)} failed</span> : null}</div>
            )}
            {res?.mode === 'item' && <div>{fmtSize(res.bytes ?? 0)} written</div>}
            {res?.mode === 'unit' && <div>Defined as <span className="font-mono">{res.name}</span></div>}
            {(res?.trees ?? []).map(t => (
              <div key={t.path} className="font-mono break-all">
                {t.path}
                {t.restored_to && <> restored to {t.restored_to}</>}
                {t.previous && <> (old data moved to {t.previous})</>}
              </div>
            ))}
            {(res?.warnings ?? (res?.warning ? [res.warning] : [])).map((w, i) => (
              <div key={i} className="text-status-warning">{w}</div>
            ))}
          </div>
        )}

        <div className="flex justify-end gap-2">
          <button onClick={onClose} className={btnCls}>{done ? 'Close' : 'Cancel'}</button>
          <button onClick={() => void submit()} disabled={busy || !confirmed}
            className={mode === 'inplace'
              ? 'px-3 py-1 text-xs rounded bg-status-error text-white hover:opacity-90 cursor-pointer border-none disabled:opacity-50 disabled:cursor-default'
              : primaryCls}>
            {busy ? 'Restoring...' : mode === 'inplace' ? 'Restore in place' : 'Start Restore'}
          </button>
        </div>
      </div>
    </div>
  )
}
