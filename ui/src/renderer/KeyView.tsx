/*
 * The repository key, and whether anyone has saved it.
 *
 * A system-managed key exists in one file, on the machine being backed up.
 * Lose that machine and every offsite copy of its repository is
 * unreadable - which is exactly the disaster the backups were for. The
 * daemon has counted unescrowed repositories on /metrics since the column
 * existed, and Prometheus is not where somebody finds out their key was
 * never written down.
 *
 * So the screen has one job: show the key once, and get an answer about
 * whether it has been stored somewhere else. Acknowledging is a claim the
 * operator makes, not something the daemon can verify - which is why it is a
 * deliberate button and not a checkbox that defaults to ticked.
 */

import React, { useCallback, useEffect, useState } from 'react'
import { ConfirmDialog } from './ConfirmDialog'

const api = window.cbackup

interface RepoInfo {
  id: number
  label?: string
  encryption?: string
  key_escrowed?: number | string
}

interface Props {
  connName: string
  repoPath: string
  onBack: () => void
}

const escrowed = (r: RepoInfo | null): boolean =>
  !!r && (r.key_escrowed === 1 || r.key_escrowed === '1')

export function KeyView({ connName, repoPath, onBack }: Props): React.ReactElement {
  const repoId = Number(repoPath)

  const [repo, setRepo] = useState<RepoInfo | null>(null)
  const [key, setKey] = useState<string | null>(null)
  const [keyId, setKeyId] = useState<string | null>(null)
  const [revealing, setRevealing] = useState(false)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [confirmAck, setConfirmAck] = useState(false)
  const [importing, setImporting] = useState(false)
  const [importValue, setImportValue] = useState('')

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      setRepo(await api.apiCall<RepoInfo>(connName, { kind: 'repoShow', repoId }))
    } catch (err) {
      setError(String(err))
    }
    setLoading(false)
  }, [connName, repoId])

  useEffect(() => { void load() }, [load])

  /* Fetched only when asked for. A key on screen because a page was opened
   * is a key on screen in a meeting, or in a screenshot. */
  const reveal = async () => {
    setError(null)
    try {
      const r = await api.apiCall<{ key?: string; key_id?: string }>(connName, {
        kind: 'keyExport', repoId,
      })
      setKey(r.key ?? null)
      setKeyId(r.key_id ?? null)
      setRevealing(true)
    } catch (err) {
      setError(String(err))
    }
  }

  const acknowledge = async () => {
    setConfirmAck(false)
    setError(null)
    try {
      await api.apiCall(connName, { kind: 'keyAck', repoId })
      setKey(null)
      setRevealing(false)
      await load()
    } catch (err) {
      setError(String(err))
    }
  }

  const importKey = async () => {
    setError(null)
    try {
      await api.apiCall(connName, { kind: 'keyImport', repoId, key: importValue.trim() })
      setImporting(false)
      setImportValue('')
      await load()
    } catch (err) {
      setError(String(err))
    }
  }

  const enc = repo?.encryption ?? 'none'

  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="flex items-center gap-2 mb-3">
        <button onClick={onBack}
          className="px-2 py-1 text-xs cursor-pointer rounded bg-surface-tertiary
                     text-text-secondary hover:bg-surface-hover border-none">
          Back
        </button>
        <h2 className="text-sm font-semibold m-0">Encryption</h2>
      </div>

      {error && (
        <div className="text-xs mb-3 p-2 rounded bg-danger-subtle text-danger border border-danger">
          {error}
        </div>
      )}

      {loading ? (
        <div className="text-xs text-text-tertiary">Loading...</div>
      ) : enc === 'none' ? (
        <div className="text-xs text-text-tertiary" style={{ maxWidth: '34rem' }}>
          This repository is not encrypted. Encryption is chosen when a
          repository is created and cannot be turned on afterwards - every
          object already written would still be in the clear, and a repository
          half in each state has no honest way to describe itself.
        </div>
      ) : (
        <div style={{ maxWidth: '38rem' }}>
          <div className="grid grid-cols-2 gap-x-6 gap-y-1 text-xs mb-4" style={{ maxWidth: '22rem' }}>
            <div className="text-text-secondary">Mode</div>
            <div className="font-mono">{enc}</div>
            <div className="text-text-secondary">Key saved elsewhere</div>
            <div className={escrowed(repo) ? 'text-success' : 'text-danger'}>
              {escrowed(repo) ? 'yes' : 'not confirmed'}
            </div>
          </div>

          {!escrowed(repo) && (
            <div className="text-xs mb-4 p-2 rounded bg-danger-subtle text-danger border border-danger">
              This key exists in one file, on this machine. If the machine is
              lost, every copy of this repository - including any offsite one -
              is unreadable. Save it somewhere else and confirm below.
            </div>
          )}

          {enc === 'system' && (
            <div className="mb-4">
              {!revealing ? (
                <button onClick={reveal}
                  className="px-2 py-1 text-xs cursor-pointer rounded bg-accent
                             text-accent-text border-none">
                  Show key
                </button>
              ) : (
                <>
                  <div className="text-xs text-text-secondary mb-1">
                    Copy this into a password manager or somewhere off this machine.
                  </div>
                  <div className="font-mono text-xs p-2 rounded bg-surface-tertiary
                                  border border-border-default break-all select-all">
                    {key}
                  </div>
                  {keyId && (
                    <div className="text-xs text-text-tertiary mt-1">
                      Fingerprint {keyId}
                    </div>
                  )}
                  <div className="flex gap-2 mt-2">
                    <button onClick={() => setConfirmAck(true)}
                      className="px-2 py-1 text-xs cursor-pointer rounded bg-accent
                                 text-accent-text border-none">
                      I have saved it
                    </button>
                    <button onClick={() => { setKey(null); setRevealing(false) }}
                      className="px-2 py-1 text-xs cursor-pointer rounded bg-surface-tertiary
                                 text-text-secondary border-none">
                      Hide
                    </button>
                  </div>
                </>
              )}
            </div>
          )}

          {enc === 'passphrase' && (
            <div className="text-xs text-text-tertiary mb-4">
              The key is wrapped by your passphrase and stored in the repository
              itself, so an offsite copy can be opened with the passphrase
              alone. There is nothing here to write down - but the passphrase
              is the only thing that opens it, and nothing can recover it.
            </div>
          )}

          <div>
            {!importing ? (
              <button onClick={() => setImporting(true)}
                className="underline cursor-pointer bg-transparent border-none
                           text-accent p-0 text-xs">
                Restore a key from a copy
              </button>
            ) : (
              <>
                <div className="text-xs text-text-secondary mb-1">
                  Paste the key for this repository. Used when the host was
                  rebuilt and the keyfile is gone.
                </div>
                <input value={importValue} onChange={e => setImportValue(e.target.value)}
                  placeholder="64 hex characters"
                  className="w-full font-mono text-xs p-2 rounded bg-surface-tertiary
                             text-text-primary border border-border-default" />
                <div className="flex gap-2 mt-2">
                  <button onClick={importKey} disabled={importValue.trim().length === 0}
                    className="px-2 py-1 text-xs cursor-pointer rounded bg-accent
                               text-accent-text border-none disabled:opacity-50">
                    Import
                  </button>
                  <button onClick={() => { setImporting(false); setImportValue('') }}
                    className="px-2 py-1 text-xs cursor-pointer rounded bg-surface-tertiary
                               text-text-secondary border-none">
                    Cancel
                  </button>
                </div>
              </>
            )}
          </div>
        </div>
      )}

      {confirmAck && (
        <ConfirmDialog
          title="Confirm the key is saved"
          message={
            'This records that a copy of the key exists somewhere other than ' +
            'this machine. Nothing can check that for you, so only confirm if ' +
            'it is true - the warning it silences is the one that matters most.'
          }
          confirmLabel="It is saved"
          danger={false}
          onConfirm={acknowledge}
          onCancel={() => setConfirmAck(false)}
        />
      )}
    </div>
  )
}
