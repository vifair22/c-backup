/*
 * Names for snapshots worth keeping.
 *
 * Migrated off the action bridge, which is what made most of this screen
 * inert: 'tag_rename' had no endpoint mapped at all and threw on every use,
 * and the snapshot dates came from a response shape the daemon does not
 * send, so the Date column was blank for every row.
 *
 * The "preserve" checkbox is gone. It set a flag no table has, and the
 * behaviour it promised is already unconditional: gfs treats any tag as
 * protection, because a tag is how somebody says "keep this one" and
 * dropping a tagged snapshot when a retention window rolled past it would
 * be the worst kind of surprise. Saying so once is better than offering a
 * switch for it.
 *
 * Renaming is two calls rather than one route: setting the new name and
 * removing the old. The daemon has no rename verb and neither does the CLI,
 * and a tag set is an upsert, so this composes out of what exists instead of
 * asking for a route that only the app would use.
 */

import React, { useCallback, useEffect, useState } from 'react'
import { fmtNum, absoluteTime } from './format'
import { ContextMenu, type ContextMenuItem } from './ContextMenu'
import { ConfirmDialog } from './ConfirmDialog'

const api = window.cbackup

interface RawTag { name: string; snap_id?: string }
interface RawSnapshot { id: string; ts: string; label?: string }

interface Tag { name: string; snapId: number }
interface Snapshot { id: number; ts: number; label: string }

interface Props {
  connName: string
  repoPath: string
  onSelectSnapshot?: (snapId: number) => void
  onBack: () => void
}

export function TagsView({ connName, repoPath, onSelectSnapshot, onBack }: Props): React.ReactElement {
  const repoId = Number(repoPath)

  const [tags, setTags] = useState<Tag[]>([])
  const [snaps, setSnaps] = useState<Snapshot[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState<string | null>(null)

  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; items: ContextMenuItem[] } | null>(null)
  const [confirmDialog, setConfirmDialog] = useState<{ title: string; message: string; onConfirm: () => void } | null>(null)
  const [editDialog, setEditDialog] = useState<{ mode: 'create' | 'rename'; oldName?: string; name: string; snapId: number } | null>(null)

  const refresh = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const [t, s] = await Promise.all([
        api.apiCall<{ tags?: RawTag[] }>(connName, { kind: 'tagList', repoId }),
        api.apiCall<{ snapshots?: RawSnapshot[] }>(connName, { kind: 'snapshotList', repoId, limit: 200 }),
      ])
      setTags((t.tags ?? [])
        .map(x => ({ name: x.name, snapId: Number(x.snap_id ?? '0') }))
        .sort((a, b) => a.name.localeCompare(b.name)))
      setSnaps((s.snapshots ?? []).map(x => ({
        id: Number(x.id), ts: Number(x.ts), label: x.label ?? '',
      })))
    } catch (err) {
      setError(String(err))
    }
    setLoading(false)
  }, [connName, repoId])

  useEffect(() => { void refresh() }, [refresh])

  const snapById = new Map(snaps.map(s => [s.id, s]))

  const showSuccess = (msg: string) => {
    setSuccess(msg)
    setTimeout(() => setSuccess(null), 3000)
  }

  const handleDelete = (name: string) => {
    setConfirmDialog({
      title: 'Remove tag',
      message: `Remove the tag "${name}"? The snapshot itself stays, but it ` +
               'loses the protection a tag gives it against retention.',
      onConfirm: async () => {
        setConfirmDialog(null)
        try {
          await api.apiCall(connName, { kind: 'tagRemove', repoId, name })
          showSuccess(`Removed "${name}"`)
          await refresh()
        } catch (err) { setError(String(err)) }
      },
    })
  }

  const handleSaveTag = async () => {
    if (!editDialog || !editDialog.name.trim()) return
    const name = editDialog.name.trim()
    setError(null)
    try {
      /*
       * Set first, remove second. A tag set is an upsert, so the new name
       * exists before the old one goes; the reverse order would leave the
       * snapshot briefly untagged and unprotected if the second call failed.
       */
      await api.apiCall(connName, { kind: 'tagSet', repoId, name, snapshotId: editDialog.snapId })
      if (editDialog.mode === 'rename' && editDialog.oldName && editDialog.oldName !== name) {
        await api.apiCall(connName, { kind: 'tagRemove', repoId, name: editDialog.oldName })
      }
      showSuccess(editDialog.mode === 'create' ? `Tagged #${editDialog.snapId} as "${name}"`
                                               : `Updated "${name}"`)
      setEditDialog(null)
      await refresh()
    } catch (err) {
      setError(String(err))
    }
  }

  const showRowMenu = (e: React.MouseEvent, tag: Tag) => {
    e.preventDefault()
    setContextMenu({
      x: e.clientX, y: e.clientY,
      items: [
        ...(onSelectSnapshot ? [{ label: 'Browse snapshot', onClick: () => onSelectSnapshot(tag.snapId) }] : []),
        { label: 'Edit', onClick: () => setEditDialog({ mode: 'rename', oldName: tag.name, name: tag.name, snapId: tag.snapId }) },
        { label: 'Remove', danger: true, onClick: () => handleDelete(tag.name) },
      ],
    })
  }

  /* The list is newest first, so the newest snapshot is the first row. */
  const newestId = snaps.length > 0 ? snaps[0].id : 1

  return (
    <div>
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Back
        </button>
        <h3 className="text-lg font-medium m-0">Tags</h3>
        <span className="text-xs text-text-muted">{fmtNum(tags.length)} tag{tags.length !== 1 ? 's' : ''}</span>
        <button onClick={() => setEditDialog({ mode: 'create', name: '', snapId: newestId })}
          disabled={snaps.length === 0}
          className="text-xs px-2 py-0.5 rounded bg-surface-tertiary hover:bg-surface-hover
                     text-text-secondary cursor-pointer border-none ml-auto disabled:opacity-50">
          + Add tag
        </button>
      </div>

      <div className="text-xs text-text-muted mb-3" style={{ maxWidth: '38rem' }}>
        A tagged snapshot is never dropped by retention. That is what tagging
        is for: retention keeps a shape of history, and a tag says this
        particular one matters regardless.
      </div>

      {error && <div className="text-status-error text-sm mb-3">{error}</div>}
      {success && <div className="text-status-connected text-sm mb-3">{success}</div>}
      {loading && <div className="text-text-muted text-sm">Loading tags...</div>}

      {!loading && tags.length > 0 && (
        <div className="bg-surface-secondary border border-border-default rounded-lg overflow-hidden">
          <table className="w-full text-xs table-fixed">
            <thead>
              <tr className="text-text-muted text-left">
                <th className="font-medium pb-2 pt-4 pl-4">Name</th>
                <th className="font-medium pb-2 pt-4 w-16">Snap</th>
                <th className="font-medium pb-2 pt-4 w-44">Taken</th>
                <th className="font-medium pb-2 pt-4">Label</th>
              </tr>
            </thead>
            <tbody>
              {tags.map(tag => {
                const snap = snapById.get(tag.snapId)
                return (
                  <tr key={tag.name}
                    onClick={() => onSelectSnapshot?.(tag.snapId)}
                    onContextMenu={e => showRowMenu(e, tag)}
                    className="border-t border-border-default cursor-pointer hover:bg-surface-hover text-text-primary">
                    <td className="py-1.5 pl-4 font-medium">{tag.name}</td>
                    <td className="py-1.5 font-mono">#{tag.snapId}</td>
                    {/* Blank when the tag outlived the page of snapshots
                      * fetched, not when the snapshot is gone: removing one
                      * removes its tags with it. */}
                    <td className="py-1.5 text-text-muted">{snap ? absoluteTime(snap.ts) : ''}</td>
                    <td className="py-1.5 text-text-muted truncate">{snap?.label ?? ''}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {!loading && tags.length === 0 && (
        <div className="text-text-muted text-xs py-4">
          Nothing is tagged.
        </div>
      )}

      {contextMenu && (
        <ContextMenu x={contextMenu.x} y={contextMenu.y} items={contextMenu.items}
          onClose={() => setContextMenu(null)} />
      )}

      {confirmDialog && (
        <ConfirmDialog
          title={confirmDialog.title}
          message={confirmDialog.message}
          confirmLabel="Remove"
          danger
          onConfirm={confirmDialog.onConfirm}
          onCancel={() => setConfirmDialog(null)}
        />
      )}

      {editDialog && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
          <form onSubmit={e => { e.preventDefault(); void handleSaveTag() }}
            className="bg-surface-primary rounded-lg p-5 w-96 shadow-xl border border-border-default">
            <h3 className="text-sm font-semibold m-0 mb-3">
              {editDialog.mode === 'create' ? 'Add tag' : 'Edit tag'}
            </h3>
            <div className="mb-3">
              <label className="block text-xs font-medium text-text-secondary mb-1">Name</label>
              <input value={editDialog.name} onChange={e => setEditDialog({ ...editDialog, name: e.target.value })}
                placeholder="before-upgrade"
                className="w-full px-2 py-1 border border-border-default rounded text-sm
                           bg-surface-primary text-text-primary focus:outline-none focus:border-accent"
                autoFocus required />
            </div>
            <div className="mb-4">
              <label className="block text-xs font-medium text-text-secondary mb-1">Snapshot</label>
              <select value={editDialog.snapId}
                onChange={e => setEditDialog({ ...editDialog, snapId: Number(e.target.value) })}
                className="w-full px-2 py-1 border border-border-default rounded text-sm
                           bg-surface-primary text-text-primary focus:outline-none focus:border-accent">
                {snaps.map(s => (
                  <option key={s.id} value={s.id}>
                    #{s.id} - {absoluteTime(s.ts)}{s.label ? ` (${s.label})` : ''}
                  </option>
                ))}
              </select>
            </div>
            <div className="flex justify-end gap-2">
              <button type="button" onClick={() => setEditDialog(null)}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary
                           text-text-secondary hover:bg-surface-hover border-none">
                Cancel
              </button>
              <button type="submit"
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text
                           hover:bg-accent-hover border-none">
                {editDialog.mode === 'create' ? 'Add' : 'Save'}
              </button>
            </div>
          </form>
        </div>
      )}
    </div>
  )
}
