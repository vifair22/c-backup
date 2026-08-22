import React, { useState } from 'react'
import type { ConnectionConfig } from '../shared/types'

/*
 * Connection form.
 *
 * Much smaller than it was, because a connection is now just an address. The
 * old form collected a binary path, a sudo toggle, an SSH host, username, auth
 * method and key path, all so the app could spawn a process on the far end.
 * The daemon is already running there; it only needs to be reached.
 */

interface Props {
  onAdd: (config: ConnectionConfig) => void
  onCancel: () => void
  initialConfig?: ConnectionConfig
}

const DEFAULT_SOCKET = '/run/c-backupd/c-backupd.sock'
const DEFAULT_PORT = 8765

export function AddConnectionDialog({
  onAdd,
  onCancel,
  initialConfig,
}: Props): React.ReactElement {
  const editing = !!initialConfig
  const [type, setType] = useState<'local' | 'remote'>(initialConfig?.type ?? 'local')
  const [name, setName] = useState(initialConfig?.name ?? '')
  const [socketPath, setSocketPath] = useState(
    initialConfig?.type === 'local' ? initialConfig.socketPath : DEFAULT_SOCKET
  )
  const [host, setHost] = useState(
    initialConfig?.type === 'remote' ? initialConfig.host : ''
  )
  const [port, setPort] = useState(
    initialConfig?.type === 'remote' ? initialConfig.port : DEFAULT_PORT
  )
  const [error, setError] = useState('')

  function submit(e: React.FormEvent): void {
    e.preventDefault()
    if (!name.trim()) {
      setError('a name is required')
      return
    }
    if (type === 'remote' && !host.trim()) {
      setError('a host is required')
      return
    }
    onAdd(
      type === 'local'
        ? { type: 'local', name: name.trim(), socketPath: socketPath.trim() }
        : { type: 'remote', name: name.trim(), host: host.trim(), port }
    )
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <form
        onSubmit={submit}
        className="w-[28rem] rounded-lg bg-zinc-900 p-6 shadow-xl ring-1 ring-zinc-700"
      >
        <h2 className="mb-4 text-lg font-medium text-zinc-100">
          {editing ? 'Edit connection' : 'Add connection'}
        </h2>

        <label className="mb-3 block">
          <span className="mb-1 block text-sm text-zinc-400">Name</span>
          <input
            className="w-full rounded bg-zinc-800 px-3 py-2 text-zinc-100 outline-none ring-1 ring-zinc-700 focus:ring-blue-500"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="nas"
            autoFocus
          />
        </label>

        <div className="mb-3 flex gap-2">
          <button
            type="button"
            onClick={() => setType('local')}
            className={`flex-1 rounded px-3 py-2 text-sm ${
              type === 'local'
                ? 'bg-blue-600 text-white'
                : 'bg-zinc-800 text-zinc-300 hover:bg-zinc-700'
            }`}
          >
            This machine
          </button>
          <button
            type="button"
            onClick={() => setType('remote')}
            className={`flex-1 rounded px-3 py-2 text-sm ${
              type === 'remote'
                ? 'bg-blue-600 text-white'
                : 'bg-zinc-800 text-zinc-300 hover:bg-zinc-700'
            }`}
          >
            Remote
          </button>
        </div>

        {type === 'local' ? (
          <label className="mb-3 block">
            <span className="mb-1 block text-sm text-zinc-400">Socket path</span>
            <input
              className="w-full rounded bg-zinc-800 px-3 py-2 font-mono text-sm text-zinc-100 outline-none ring-1 ring-zinc-700 focus:ring-blue-500"
              value={socketPath}
              onChange={(e) => setSocketPath(e.target.value)}
            />
            <span className="mt-1 block text-xs text-zinc-500">
              No password: access is controlled by the socket's file permissions.
            </span>
          </label>
        ) : (
          <>
            <label className="mb-3 block">
              <span className="mb-1 block text-sm text-zinc-400">Host</span>
              <input
                className="w-full rounded bg-zinc-800 px-3 py-2 text-zinc-100 outline-none ring-1 ring-zinc-700 focus:ring-blue-500"
                value={host}
                onChange={(e) => setHost(e.target.value)}
                placeholder="nas.internal"
              />
            </label>
            <label className="mb-3 block">
              <span className="mb-1 block text-sm text-zinc-400">Port</span>
              <input
                type="number"
                className="w-full rounded bg-zinc-800 px-3 py-2 text-zinc-100 outline-none ring-1 ring-zinc-700 focus:ring-blue-500"
                value={port}
                onChange={(e) => setPort(Number(e.target.value))}
              />
            </label>
            <p className="mb-3 text-xs text-zinc-500">
              You will be asked for the daemon's admin password when connecting.
              The token it returns is kept in the system keychain.
            </p>
          </>
        )}

        {error && <p className="mb-3 text-sm text-red-400">{error}</p>}

        <div className="flex justify-end gap-2">
          <button
            type="button"
            onClick={onCancel}
            className="rounded px-4 py-2 text-sm text-zinc-300 hover:bg-zinc-800"
          >
            Cancel
          </button>
          <button
            type="submit"
            className="rounded bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-500"
          >
            {editing ? 'Save' : 'Add'}
          </button>
        </div>
      </form>
    </div>
  )
}
