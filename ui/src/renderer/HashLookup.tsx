import React, { useState } from 'react'
import { fmtSize, fmtNum } from './format'
import { ContentViewer } from './ContentViewer'

const api = window.cbackup

const OBJECT_TYPES: Record<number, string> = {
  1: 'FILE', 2: 'XATTR', 3: 'ACL', 4: 'SPARSE',
}

interface LocateResult {
  found: boolean
  type?: number
  uncompressed_size?: number
}

interface RefEntry {
  snap_id: number
  node_id: number
  field: string
}

interface RefsResult {
  refs: RefEntry[]
}

interface Props {
  connName: string
  repoPath: string
  onNavigateToSnapshot?: (snapId: number) => void
  onBack: () => void
}

export function HashLookup({ connName, repoPath, onNavigateToSnapshot, onBack }: Props): React.ReactElement {
  const [hash, setHash] = useState('')
  const [locate, setLocate] = useState<LocateResult | null>(null)
  const [refs, setRefs] = useState<RefEntry[] | null>(null)
  const [loading, setLoading] = useState(false)
  const [refsLoading, setRefsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [viewerHash, setViewerHash] = useState<string | null>(null)

  const doLookup = async () => {
    const h = hash.trim().toLowerCase()
    if (!h || h.length !== 64) { setError('Hash must be 64 hex characters'); return }
    if (!/^[0-9a-f]{64}$/.test(h)) { setError('Invalid hex hash'); return }

    setLoading(true)
    setError(null)
    setLocate(null)
    setRefs(null)

    try {
      const loc = await api.rpcCall<LocateResult>(connName, repoPath, 'object_locate', { hash: h })
      setLocate(loc)

      if (loc.found) {
        // Start async ref scan
        setRefsLoading(true)
        api.rpcCall<RefsResult>(connName, repoPath, 'object_refs', { hash: h })
          .then(r => setRefs(r.refs))
          .catch(() => setRefs([]))
          .finally(() => setRefsLoading(false))
      }
    } catch (err) {
      setError(String(err))
    }
    setLoading(false)
  }

  return (
    <div>
      {/* Header */}
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Dashboard
        </button>
        <h3 className="text-lg font-medium m-0">Hash Lookup</h3>
      </div>

      {/* Search bar */}
      <div className="bg-surface-secondary border border-border-default rounded-lg px-4 py-3 mb-4">
        <form onSubmit={e => { e.preventDefault(); doLookup() }} className="flex items-center gap-3">
          <input value={hash} onChange={e => setHash(e.target.value)}
            placeholder="Enter 64-character SHA-256 hex hash"
            className="flex-1 px-2 py-1 border border-border-default rounded text-sm bg-surface-primary text-text-primary focus:outline-none focus:border-accent font-mono"
            autoFocus spellCheck={false} />
          <button type="submit" disabled={loading || !hash.trim()}
            className="text-xs px-3 py-1 rounded bg-accent text-accent-text hover:bg-accent-hover border-none cursor-pointer disabled:opacity-50 disabled:cursor-default shrink-0">
            {loading ? 'Looking up...' : 'Look up'}
          </button>
        </form>
      </div>

      {error && <div className="text-status-error text-sm mb-3">{error}</div>}

      {/* Results */}
      {locate && (
        <div className="bg-surface-secondary border border-border-default rounded-lg p-4 mb-4">
          <div className="text-[11px] text-text-muted uppercase tracking-wide mb-2">Object</div>
          <div className="font-mono text-xs text-text-secondary break-all mb-3">{hash.trim().toLowerCase()}</div>

          {locate.found ? (
            <div className="space-y-3">
              <div className="flex items-center gap-4">
                <div>
                  <div className="text-[10px] text-text-muted uppercase">Type</div>
                  <span className="text-xs font-semibold text-text-primary">
                    {OBJECT_TYPES[locate.type ?? 0] ?? `Unknown (${locate.type})`}
                  </span>
                </div>
                <div>
                  <div className="text-[10px] text-text-muted uppercase">Size</div>
                  <span className="text-xs text-text-primary">
                    {fmtSize(locate.uncompressed_size ?? 0)}
                    <span className="text-text-muted ml-1">({fmtNum(locate.uncompressed_size ?? 0)} bytes)</span>
                  </span>
                </div>
                <button onClick={() => setViewerHash(hash.trim().toLowerCase())}
                  className="text-xs px-3 py-1 rounded bg-accent text-accent-text hover:bg-accent-hover border-none cursor-pointer ml-auto">
                  View Content
                </button>
              </div>
            </div>
          ) : (
            <div className="text-text-muted text-sm">Not found in repository.</div>
          )}
        </div>
      )}

      {/* Snapshot references */}
      {locate?.found && (
        <div className="bg-surface-secondary border border-border-default rounded-lg p-4">
          <div className="text-[11px] text-text-muted uppercase tracking-wide mb-2">
            Snapshot References
            {refsLoading && <span className="ml-2 animate-pulse">Scanning snapshots...</span>}
          </div>

          {refs && refs.length > 0 && (
            <table className="w-full text-xs">
              <thead>
                <tr className="text-text-muted text-left">
                  <th className="font-medium pb-1 w-16">Snap</th>
                  <th className="font-medium pb-1 w-24">Node ID</th>
                  <th className="font-medium pb-1">Field</th>
                </tr>
              </thead>
              <tbody>
                {refs.map((r, i) => (
                  <tr key={i}
                    onClick={() => onNavigateToSnapshot?.(r.snap_id)}
                    className="border-t border-border-default cursor-pointer hover:bg-surface-hover text-text-primary">
                    <td className="py-1 font-mono">#{r.snap_id}</td>
                    <td className="py-1 font-mono">{r.node_id}</td>
                    <td className="py-1">{r.field}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}

          {refs && refs.length === 0 && !refsLoading && (
            <div className="text-text-muted text-xs">No snapshot references found.</div>
          )}
        </div>
      )}

      {/* Content viewer */}
      {viewerHash && (
        <ContentViewer
          connName={connName}
          repoPath={repoPath}
          hash={viewerHash}
          onClose={() => setViewerHash(null)}
        />
      )}
    </div>
  )
}
