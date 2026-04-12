import React, { useCallback, useEffect, useState } from 'react'
import { fmtSize, fmtNum } from './format'
import { ContentViewer } from './ContentViewer'

const api = window.cbackup

const OBJECT_TYPES: Record<number, string> = { 1: 'FILE', 2: 'XATTR', 3: 'ACL', 4: 'SPARSE' }
const COMPRESS_NAMES: Record<number, string> = { 0: 'none', 1: 'lz4-block', 2: 'lz4-frame' }

interface ScanPack {
  name: string
  size: number
}

interface ScanResponse {
  packs: ScanPack[]
}

interface PackEntry {
  hash: string
  type: number
  compression: number
  uncompressed_size: number
  compressed_size: number
  payload_offset: number
}

interface PackEntriesResponse {
  entries: PackEntry[]
  version: number
  count: number
  file_size?: number
}

interface PackIdxEntry {
  hash: string
  dat_offset: number
}

interface PackIndexResponse {
  entries: PackIdxEntry[]
  version: number
  count: number
}

interface GlobalIndexHeader {
  magic: number
  version: number
  entry_count: number
  pack_count: number
}

interface GlobalIndexEntry {
  hash: string
  pack_num: number
  dat_offset: number
  pack_version: number
  entry_index: number
}

interface GlobalIndexResponse {
  header: GlobalIndexHeader
  fanout: number[]
  entries: GlobalIndexEntry[]
  offset: number
  limit: number
  has_more: boolean
}

type PackTab = 'entries' | 'index' | 'global'

interface Props {
  connName: string
  repoPath: string
  onBack: () => void
}

export function PackBrowser({ connName, repoPath, onBack }: Props): React.ReactElement {
  const [packs, setPacks] = useState<ScanPack[]>([])
  const [selectedPack, setSelectedPack] = useState<string | null>(null)
  const [packTab, setPackTab] = useState<PackTab>('entries')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Pack entries/index
  const [packEntries, setPackEntries] = useState<PackEntriesResponse | null>(null)
  const [packIndex, setPackIndex] = useState<PackIndexResponse | null>(null)
  const [packLoading, setPackLoading] = useState(false)

  // Global index
  const [globalIndex, setGlobalIndex] = useState<GlobalIndexResponse | null>(null)
  const [globalOffset, setGlobalOffset] = useState(0)
  const [globalLoading, setGlobalLoading] = useState(false)

  // Content viewer
  const [viewerHash, setViewerHash] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    api.rpcCall<ScanResponse>(connName, repoPath, 'scan')
      .then(r => {
        if (cancelled) return
        const sorted = (r.packs ?? []).sort((a, b) => a.name.localeCompare(b.name))
        setPacks(sorted)
      })
      .catch(err => { if (!cancelled) setError(String(err)) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [connName, repoPath])

  const loadPack = useCallback(async (name: string) => {
    setSelectedPack(name)
    setPackEntries(null)
    setPackIndex(null)
    setPackLoading(true)
    try {
      const [entries, idx] = await Promise.all([
        api.rpcCall<PackEntriesResponse>(connName, repoPath, 'pack_entries', { name }),
        api.rpcCall<PackIndexResponse>(connName, repoPath, 'pack_index', { name }),
      ])
      setPackEntries(entries)
      setPackIndex(idx)
    } catch (err) {
      setError(String(err))
    }
    setPackLoading(false)
  }, [connName, repoPath])

  const loadGlobalIndex = useCallback(async (offset: number) => {
    setGlobalLoading(true)
    try {
      const resp = await api.rpcCall<GlobalIndexResponse>(connName, repoPath, 'global_pack_index', {
        offset, limit: 500
      })
      setGlobalIndex(resp)
      setGlobalOffset(offset)
    } catch (err) {
      setError(String(err))
    }
    setGlobalLoading(false)
  }, [connName, repoPath])

  // Fanout distribution for the bar
  const fanoutMax = globalIndex ? Math.max(...globalIndex.fanout.map((v, i) => i === 0 ? v : v - globalIndex.fanout[i - 1])) : 0

  return (
    <div>
      {/* Header */}
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Dashboard
        </button>
        <h3 className="text-lg font-medium m-0">Pack Files</h3>
        <span className="text-xs text-text-muted">{packs.length} pack{packs.length !== 1 ? 's' : ''}</span>
      </div>

      {error && <div className="text-status-error text-sm mb-3">{error}</div>}
      {loading && <div className="text-text-muted text-sm">Loading...</div>}

      <div className="flex gap-4">
        {/* Pack list sidebar */}
        <div className="w-56 shrink-0">
          <div className="bg-surface-secondary border border-border-default rounded-lg overflow-hidden">
            <div className="px-3 py-2 border-b border-border-default text-[11px] text-text-muted uppercase tracking-wide">Packs</div>
            <div className="max-h-80 overflow-auto">
              {packs.map(p => (
                <div key={p.name}
                  onClick={() => { loadPack(p.name); setPackTab('entries') }}
                  className={`px-3 py-1.5 text-xs cursor-pointer hover:bg-surface-hover border-b border-border-default last:border-0 ${
                    selectedPack === p.name ? 'bg-accent/10 text-accent' : 'text-text-primary'
                  }`}>
                  <div className="font-mono truncate">{p.name}</div>
                  <div className="text-[10px] text-text-muted">{fmtSize(p.size)}</div>
                </div>
              ))}
            </div>
            {/* Global index button */}
            <div className="border-t border-border-default">
              <button onClick={() => { setSelectedPack(null); setPackTab('global'); loadGlobalIndex(0) }}
                className={`w-full text-left px-3 py-1.5 text-xs cursor-pointer border-none hover:bg-surface-hover ${
                  packTab === 'global' && !selectedPack ? 'bg-accent/10 text-accent' : 'bg-transparent text-text-secondary'
                }`}>
                Global Index
              </button>
            </div>
          </div>
        </div>

        {/* Main content */}
        <div className="flex-1 min-w-0">
          {selectedPack && (
            <>
              {/* Tabs */}
              <div className="flex gap-0 border-b border-border-default mb-3">
                {(['entries', 'index'] as const).map(tab => (
                  <button key={tab} onClick={() => setPackTab(tab)}
                    className={`px-4 py-2 text-xs cursor-pointer border-none bg-transparent ${
                      packTab === tab ? 'text-accent border-b-2 border-accent font-medium' : 'text-text-muted hover:text-text-primary'
                    }`}>
                    {tab === 'entries' ? `Entries (${packEntries?.count ?? '...'})` : `Index (${packIndex?.count ?? '...'})`}
                  </button>
                ))}
              </div>

              {packLoading && <div className="text-text-muted text-sm">Loading pack...</div>}

              {/* Pack header info */}
              {packEntries && packTab === 'entries' && (
                <>
                  <div className="flex gap-4 mb-3 text-xs text-text-muted">
                    <span>Version {packEntries.version}</span>
                    <span>{fmtNum(packEntries.count)} objects</span>
                    {packEntries.file_size && <span>{fmtSize(packEntries.file_size)} on disk</span>}
                  </div>
                  <div className="bg-surface-secondary border border-border-default rounded-lg overflow-hidden">
                    <table className="w-full text-xs table-fixed">
                      <thead>
                        <tr className="text-text-muted text-left">
                          <th className="font-medium pb-2 pt-3 pl-4">Hash</th>
                          <th className="font-medium pb-2 pt-3 w-16">Type</th>
                          <th className="font-medium pb-2 pt-3 w-20 text-right">Size</th>
                          <th className="font-medium pb-2 pt-3 w-20 text-right">Compressed</th>
                          <th className="font-medium pb-2 pt-3 w-16 text-right pr-4">Ratio</th>
                        </tr>
                      </thead>
                      <tbody>
                        {packEntries.entries.slice(0, 500).map((e, i) => {
                          const ratio = e.uncompressed_size > 0 ? (e.compressed_size / e.uncompressed_size * 100).toFixed(0) : '—'
                          return (
                            <tr key={i} onClick={() => setViewerHash(e.hash)}
                              className="border-t border-border-default cursor-pointer hover:bg-surface-hover">
                              <td className="py-1 pl-4 font-mono truncate" title={e.hash}>{e.hash}</td>
                              <td className="py-1">{OBJECT_TYPES[e.type] ?? e.type}</td>
                              <td className="py-1 text-right">{fmtSize(e.uncompressed_size)}</td>
                              <td className="py-1 text-right text-text-muted">{fmtSize(e.compressed_size)}</td>
                              <td className="py-1 text-right pr-4 text-text-muted">{ratio}%</td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                    {packEntries.count > 500 && (
                      <div className="text-text-muted text-[10px] text-center py-2">
                        Showing 500 of {fmtNum(packEntries.count)} entries
                      </div>
                    )}
                  </div>
                </>
              )}

              {/* Pack index */}
              {packIndex && packTab === 'index' && (
                <div className="bg-surface-secondary border border-border-default rounded-lg overflow-hidden">
                  <table className="w-full text-xs table-fixed">
                    <thead>
                      <tr className="text-text-muted text-left">
                        <th className="font-medium pb-2 pt-3 pl-4">Hash</th>
                        <th className="font-medium pb-2 pt-3 w-28 text-right pr-4">.dat Offset</th>
                      </tr>
                    </thead>
                    <tbody>
                      {packIndex.entries.slice(0, 500).map((e, i) => (
                        <tr key={i} className="border-t border-border-default">
                          <td className="py-1 pl-4 font-mono truncate" title={e.hash}>{e.hash}</td>
                          <td className="py-1 text-right pr-4 font-mono text-text-muted">0x{e.dat_offset.toString(16)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {packIndex.count > 500 && (
                    <div className="text-text-muted text-[10px] text-center py-2">
                      Showing 500 of {fmtNum(packIndex.count)} entries
                    </div>
                  )}
                </div>
              )}
            </>
          )}

          {/* Global index */}
          {packTab === 'global' && !selectedPack && (
            <>
              {globalLoading && <div className="text-text-muted text-sm">Loading global index...</div>}

              {globalIndex && (
                <>
                  {/* Header */}
                  <div className="flex gap-4 mb-3 text-xs text-text-muted">
                    <span>Version {globalIndex.header.version}</span>
                    <span>{fmtNum(globalIndex.header.entry_count)} entries</span>
                    <span>{globalIndex.header.pack_count} packs</span>
                  </div>

                  {/* Fanout distribution */}
                  <div className="bg-surface-secondary border border-border-default rounded-lg p-4 mb-3">
                    <div className="text-[11px] text-text-muted uppercase tracking-wide mb-2">Hash Distribution (fanout)</div>
                    <div className="flex items-end h-16 gap-px">
                      {globalIndex.fanout.map((cumulative, i) => {
                        const count = i === 0 ? cumulative : cumulative - globalIndex.fanout[i - 1]
                        const height = fanoutMax > 0 ? Math.max(1, (count / fanoutMax) * 100) : 0
                        return (
                          <div key={i} className="flex-1 bg-accent/60 hover:bg-accent rounded-t-sm transition-colors"
                            style={{ height: `${height}%` }}
                            title={`0x${i.toString(16).padStart(2, '0')}: ${fmtNum(count)} entries`} />
                        )
                      })}
                    </div>
                  </div>

                  {/* Entries table */}
                  <div className="bg-surface-secondary border border-border-default rounded-lg overflow-hidden">
                    <table className="w-full text-xs table-fixed">
                      <thead>
                        <tr className="text-text-muted text-left">
                          <th className="font-medium pb-2 pt-3 pl-4">Hash</th>
                          <th className="font-medium pb-2 pt-3 w-14 text-right">Pack</th>
                          <th className="font-medium pb-2 pt-3 w-24 text-right">.dat Offset</th>
                          <th className="font-medium pb-2 pt-3 w-12 text-right pr-4">Ver</th>
                        </tr>
                      </thead>
                      <tbody>
                        {globalIndex.entries.map((e, i) => (
                          <tr key={i} className="border-t border-border-default">
                            <td className="py-1 pl-4 font-mono truncate" title={e.hash}>{e.hash}</td>
                            <td className="py-1 text-right">{e.pack_num}</td>
                            <td className="py-1 text-right font-mono text-text-muted">0x{e.dat_offset.toString(16)}</td>
                            <td className="py-1 text-right pr-4 text-text-muted">v{e.pack_version}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>

                  {/* Pagination */}
                  {globalIndex.header.entry_count > 500 && (
                    <div className="flex items-center justify-center gap-2 mt-3">
                      <button onClick={() => loadGlobalIndex(globalOffset - 500)} disabled={globalOffset === 0}
                        className="text-xs px-2 py-1 rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none cursor-pointer disabled:opacity-50 disabled:cursor-default">
                        Prev
                      </button>
                      <span className="text-xs text-text-muted">
                        {fmtNum(globalOffset + 1)}–{fmtNum(Math.min(globalOffset + 500, globalIndex.header.entry_count))} of {fmtNum(globalIndex.header.entry_count)}
                      </span>
                      <button onClick={() => loadGlobalIndex(globalOffset + 500)} disabled={!globalIndex.has_more}
                        className="text-xs px-2 py-1 rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none cursor-pointer disabled:opacity-50 disabled:cursor-default">
                        Next
                      </button>
                    </div>
                  )}
                </>
              )}

              {!globalIndex && !globalLoading && (
                <div className="text-text-muted text-xs text-center py-8">Click "Global Index" to load.</div>
              )}
            </>
          )}

          {!selectedPack && packTab !== 'global' && (
            <div className="text-text-muted text-xs text-center py-8">Select a pack file to view its contents.</div>
          )}
        </div>
      </div>

      {/* Content viewer */}
      {viewerHash && (
        <ContentViewer connName={connName} repoPath={repoPath} hash={viewerHash}
          onClose={() => setViewerHash(null)} />
      )}
    </div>
  )
}
