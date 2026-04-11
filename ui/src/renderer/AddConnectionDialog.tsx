import React, { useState } from 'react'
import type { ConnectionConfig } from '../shared/types'

interface Props {
  onAdd: (config: ConnectionConfig) => void
  onCancel: () => void
  initialConfig?: ConnectionConfig
}

export function AddConnectionDialog({ onAdd, onCancel, initialConfig }: Props): React.ReactElement {
  const editing = !!initialConfig
  const [type, setType] = useState<'local' | 'ssh'>(initialConfig?.type ?? 'local')
  const [name, setName] = useState(initialConfig?.name ?? '')
  const [binaryPath, setBinaryPath] = useState(
    initialConfig?.type === 'local' ? initialConfig.binaryPath : 'backup'
  )
  const [sudo, setSudo] = useState(initialConfig?.sudo ?? false)

  // SSH fields
  const sshInit = initialConfig?.type === 'ssh' ? initialConfig : undefined
  const [host, setHost] = useState(sshInit?.host ?? '')
  const [port, setPort] = useState(String(sshInit?.port ?? 22))
  const [username, setUsername] = useState(sshInit?.username ?? '')
  const [authMethod, setAuthMethod] = useState<'key' | 'password' | 'agent'>(sshInit?.authMethod ?? 'key')
  const [keyFilePath, setKeyFilePath] = useState(sshInit?.keyFilePath ?? '')
  const [remoteBinaryPath, setRemoteBinaryPath] = useState(sshInit?.remoteBinaryPath ?? 'backup')

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (!name.trim()) return

    if (type === 'local') {
      onAdd({ type: 'local', name: name.trim(), binaryPath: binaryPath || 'backup', sudo, repos: [] })
    } else {
      if (!host.trim() || !username.trim()) return
      onAdd({
        type: 'ssh', name: name.trim(), host: host.trim(),
        port: parseInt(port) || 22, username: username.trim(),
        authMethod, keyFilePath: authMethod === 'key' ? keyFilePath : undefined,
        remoteBinaryPath: remoteBinaryPath || 'backup', sudo, repos: [],
      })
    }
  }

  const inputCls = 'w-full px-2 py-1 border border-border-default rounded text-sm bg-surface-primary text-text-primary focus:outline-none focus:border-accent'

  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
      <form onSubmit={handleSubmit}
        className="bg-surface-primary rounded-lg p-5 w-96 shadow-xl border border-border-default">
        <h3 className="text-sm font-semibold m-0 mb-4">{editing ? 'Edit Connection' : 'Add Connection'}</h3>

        {/* Type selector */}
        <div className="mb-3">
          <label className="block text-xs font-medium text-text-secondary mb-1">Connection Type</label>
          <div className="flex gap-4">
            <label className="text-sm flex items-center gap-1 cursor-pointer">
              <input type="radio" checked={type === 'local'} onChange={() => setType('local')} /> Local
            </label>
            <label className="text-sm flex items-center gap-1 cursor-pointer">
              <input type="radio" checked={type === 'ssh'} onChange={() => setType('ssh')} /> SSH (Remote)
            </label>
          </div>
        </div>

        {/* Name */}
        <div className="mb-3">
          <label className="block text-xs font-medium text-text-secondary mb-1">Name</label>
          <input className={inputCls} value={name} onChange={e => setName(e.target.value)}
            placeholder="e.g. My Workstation" autoFocus required />
        </div>

        {/* SSH-specific fields */}
        {type === 'ssh' && (
          <>
            <div className="flex gap-2 mb-3">
              <div className="flex-1">
                <label className="block text-xs font-medium text-text-secondary mb-1">Host</label>
                <input className={inputCls} value={host} onChange={e => setHost(e.target.value)}
                  placeholder="hostname or IP" required />
              </div>
              <div className="w-20">
                <label className="block text-xs font-medium text-text-secondary mb-1">Port</label>
                <input className={inputCls} value={port} onChange={e => setPort(e.target.value)} type="number" />
              </div>
            </div>

            <div className="mb-3">
              <label className="block text-xs font-medium text-text-secondary mb-1">Username</label>
              <input className={inputCls} value={username} onChange={e => setUsername(e.target.value)}
                placeholder="ssh username" required />
            </div>

            <div className="mb-3">
              <label className="block text-xs font-medium text-text-secondary mb-1">Auth Method</label>
              <select className={inputCls} value={authMethod}
                onChange={e => setAuthMethod(e.target.value as 'key' | 'password' | 'agent')}>
                <option value="key">Key File</option>
                <option value="agent">SSH Agent</option>
                <option value="password">Password</option>
              </select>
            </div>

            {authMethod === 'key' && (
              <div className="mb-3">
                <label className="block text-xs font-medium text-text-secondary mb-1">Key File Path</label>
                <input className={inputCls} value={keyFilePath} onChange={e => setKeyFilePath(e.target.value)}
                  placeholder="~/.ssh/id_ed25519" />
              </div>
            )}

            <div className="mb-3">
              <label className="block text-xs font-medium text-text-secondary mb-1">Remote Binary Path</label>
              <input className={inputCls} value={remoteBinaryPath}
                onChange={e => setRemoteBinaryPath(e.target.value)} placeholder="backup" />
            </div>
          </>
        )}

        {/* Local binary path */}
        {type === 'local' && (
          <div className="mb-3">
            <label className="block text-xs font-medium text-text-secondary mb-1">Binary Path</label>
            <input className={inputCls} value={binaryPath} onChange={e => setBinaryPath(e.target.value)}
              placeholder="backup (or full path)" />
          </div>
        )}

        {/* Sudo */}
        <div className="mb-4">
          <label className="text-sm flex items-center gap-1.5 cursor-pointer">
            <input type="checkbox" checked={sudo} onChange={e => setSudo(e.target.checked)} />
            Run as root (sudo)
          </label>
          <span className="text-[11px] text-text-muted ml-5">
            Required for repos owned by root
          </span>
        </div>

        {/* Actions */}
        <div className="flex justify-end gap-2">
          <button type="button" onClick={onCancel}
            className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none">
            Cancel
          </button>
          <button type="submit"
            className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text hover:bg-accent-hover border-none">
            {editing ? 'Save' : 'Add'}
          </button>
        </div>
      </form>
    </div>
  )
}
