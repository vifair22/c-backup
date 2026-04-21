import React, { useCallback, useEffect, useState } from 'react'
import { fmtNum, absoluteTime } from './format'
import { ContextMenu, type ContextMenuItem } from './ContextMenu'
import { ConfirmDialog } from './ConfirmDialog'

const api = window.cbackup

interface Tag {
  name: string
  snap_id: number
  preserve?: boolean
}

interface TagsResponse {
  tags: Tag[]
}

interface Snapshot {
  id: number
  created_sec: number
}

interface SnapList {
  head: number
  snapshots: Snapshot[]
}

interface Props {
  connName: string
  repoPath: string
  onSelectSnapshot?: (snapId: number) => void
  onBack: () => void
}

export function TagsView({ connName, repoPath, onSelectSnapshot, onBack }: Props): React.ReactElement {
  const [tags, setTags] = useState<Tag[]>([])
  const [snapList, setSnapList] = useState<SnapList | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState<string | null>(null)

  // Dialogs
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; items: ContextMenuItem[] } | null>(null)
  const [confirmDialog, setConfirmDialog] = useState<{ title: string; message: string; onConfirm: () => void } | null>(null)
  const [editDialog, setEditDialog] = useState<{ mode: 'create' | 'rename'; oldName?: string; name: string; snapId: number; preserve: boolean } | null>(null)

  const refresh = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const [tagsResp, snaps] = await Promise.all([
        api.rpcCall<TagsResponse>(connName, repoPath, 'tags'),
        api.rpcCall<SnapList>(connName, repoPath, 'list'),
      ])
      setTags(tagsResp.tags.sort((a, b) => a.name.localeCompare(b.name)))
      setSnapList(snaps)
    } catch (err) {
      setError(String(err))
    }
    setLoading(false)
  }, [connName, repoPath])

  useEffect(() => { refresh() }, [refresh])

  const snapById = new Map(snapList?.snapshots.map(s => [s.id, s]) ?? [])

  const showSuccess = (msg: string) => {
    setSuccess(msg)
    setTimeout(() => setSuccess(null), 3000)
  }

  const handleDelete = (name: string) => {
    setConfirmDialog({
      title: 'Delete Tag',
      message: `Delete tag "${name}"? This does not affect the snapshot itself.`,
      onConfirm: async () => {
        setConfirmDialog(null)
        try {
          await api.rpcCall(connName, repoPath, 'tag_delete', { name })
          showSuccess(`Tag "${name}" deleted`)
          await refresh()
        } catch (err) { setError(String(err)) }
      }
    })
  }

  const handleSaveTag = async () => {
    if (!editDialog || !editDialog.name.trim()) return
    setError(null)
    try {
      if (editDialog.mode === 'rename' && editDialog.oldName) {
        if (editDialog.name.trim() !== editDialog.oldName) {
          await api.rpcCall(connName, repoPath, 'tag_rename', {
            old_name: editDialog.oldName, new_name: editDialog.name.trim()
          })
        }
        // Update snap_id and preserve if changed
        await api.rpcCall(connName, repoPath, 'tag_set', {
          name: editDialog.name.trim(),
          snap_id: editDialog.snapId,
          preserve: editDialog.preserve,
        })
        showSuccess(`Tag "${editDialog.name.trim()}" updated`)
      } else {
        await api.rpcCall(connName, repoPath, 'tag_set', {
          name: editDialog.name.trim(),
          snap_id: editDialog.snapId,
          preserve: editDialog.preserve,
        })
        showSuccess(`Tag "${editDialog.name.trim()}" created`)
      }
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
        ...(onSelectSnapshot ? [{ label: 'Browse snapshot', onClick: () => onSelectSnapshot(tag.snap_id) }] : []),
        { label: 'Edit', onClick: () => setEditDialog({ mode: 'rename', oldName: tag.name, name: tag.name, snapId: tag.snap_id, preserve: tag.preserve ?? false }) },
        { label: 'Delete', danger: true, onClick: () => handleDelete(tag.name) },
      ]
    })
  }

  const snaps = snapList?.snapshots ?? []
  const headId = snaps.length > 0 ? snaps[snaps.length - 1].id : 1

  return (
    <div>
      {/* Header */}
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Dashboard
        </button>
        <h3 className="text-lg font-medium m-0">Tags</h3>
        <span className="text-xs text-text-muted">{fmtNum(tags.length)} tag{tags.length !== 1 ? 's' : ''}</span>
        <button onClick={() => setEditDialog({ mode: 'create', name: '', snapId: headId, preserve: false })}
          className="text-xs px-2 py-0.5 rounded bg-surface-tertiary hover:bg-surface-hover text-text-secondary cursor-pointer border-none ml-auto">
          + Create Tag
        </button>
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
                <th className="font-medium pb-2 pt-4 w-40">Date</th>
                <th className="font-medium pb-2 pt-4 w-20 text-center">Preserve</th>
              </tr>
            </thead>
            <tbody>
              {tags.map(tag => {
                const snap = snapById.get(tag.snap_id)
                return (
                  <tr key={tag.name}
                    onClick={() => onSelectSnapshot?.(tag.snap_id)}
                    onContextMenu={e => showRowMenu(e, tag)}
                    className="border-t border-border-default cursor-pointer hover:bg-surface-hover text-text-primary">
                    <td className="py-1.5 pl-4 font-medium">{tag.name}</td>
                    <td className="py-1.5 font-mono">#{tag.snap_id}</td>
                    <td className="py-1.5 text-text-muted">{snap ? absoluteTime(snap.created_sec) : ''}</td>
                    <td className="py-1.5 text-center">
                      {tag.preserve && (
                        <span className="inline-block px-1.5 py-0 rounded text-[10px] font-semibold bg-amber-600 text-white">P</span>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {!loading && tags.length === 0 && (
        <div className="text-text-muted text-xs text-center py-8">No tags. Click "+ Create Tag" to add one.</div>
      )}

      {/* Context menu */}
      {contextMenu && (
        <ContextMenu x={contextMenu.x} y={contextMenu.y} items={contextMenu.items}
          onClose={() => setContextMenu(null)} />
      )}

      {/* Confirm dialog */}
      {confirmDialog && (
        <ConfirmDialog
          title={confirmDialog.title}
          message={confirmDialog.message}
          onConfirm={confirmDialog.onConfirm}
          onCancel={() => setConfirmDialog(null)}
        />
      )}

      {/* Create / Edit dialog */}
      {editDialog && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
          <form onSubmit={e => { e.preventDefault(); handleSaveTag() }}
            className="bg-surface-primary rounded-lg p-5 w-96 shadow-xl border border-border-default">
            <h3 className="text-sm font-semibold m-0 mb-3">
              {editDialog.mode === 'create' ? 'Create Tag' : 'Edit Tag'}
            </h3>
            <div className="mb-3">
              <label className="block text-xs font-medium text-text-secondary mb-1">Tag Name</label>
              <input value={editDialog.name} onChange={e => setEditDialog({ ...editDialog, name: e.target.value })}
                className="w-full px-2 py-1 border border-border-default rounded text-sm bg-surface-primary text-text-primary focus:outline-none focus:border-accent"
                autoFocus required />
            </div>
            <div className="mb-3">
              <label className="block text-xs font-medium text-text-secondary mb-1">Snapshot</label>
              <select value={editDialog.snapId} onChange={e => setEditDialog({ ...editDialog, snapId: Number(e.target.value) })}
                className="w-full px-2 py-1 border border-border-default rounded text-sm bg-surface-primary text-text-primary focus:outline-none focus:border-accent">
                {snaps.map(s => (
                  <option key={s.id} value={s.id}>#{s.id} — {absoluteTime(s.created_sec)}</option>
                ))}
              </select>
            </div>
            <div className="mb-4">
              <label className="text-xs flex items-center gap-1.5 cursor-pointer text-text-secondary">
                <input type="checkbox" checked={editDialog.preserve}
                  onChange={e => setEditDialog({ ...editDialog, preserve: e.target.checked })} />
                Preserve (protect from pruning)
              </label>
            </div>
            <div className="flex justify-end gap-2">
              <button type="button" onClick={() => setEditDialog(null)}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none">
                Cancel
              </button>
              <button type="submit"
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text hover:bg-accent-hover border-none">
                {editDialog.mode === 'create' ? 'Create' : 'Save'}
              </button>
            </div>
          </form>
        </div>
      )}
    </div>
  )
}
