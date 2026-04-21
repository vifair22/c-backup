import React, { useCallback, useEffect, useState } from 'react'
import { fmtSize, fmtNum } from './format'
import { ContentViewer } from './ContentViewer'

const api = window.cbackup

const PAGE_SIZE = 500

const OBJECT_TYPES: Record<number, string> = { 1: 'FILE', 2: 'XATTR', 3: 'ACL', 4: 'SPARSE' }
const COMPRESS_NAMES: Record<number, string> = { 0: 'none', 1: 'lz4-block', 2: 'lz4-frame' }

interface LooseStats {
  count: number
  total_uncomp: number
  per_type: { type: number; count: number; uncomp: number }[]
  skip: { count: number; uncomp: number }
}

interface LooseObject {
  hash: string
  type: number
  compression: number
  uncompressed_size: number
  compressed_size: number
  pack_skip_ver: number
  file_size?: number
}

interface LooseListResponse {
  objects: LooseObject[]
  count: number
  offset: number
  has_more: boolean
}

interface Props {
  connName: string
  repoPath: string
  onBack: () => void
}

export function LooseObjects({ connName, repoPath, onBack }: Props): React.ReactElement {
  const [stats, setStats] = useState<LooseStats | null>(null)
  const [objects, setObjects] = useState<LooseObject[]>([])
  const [offset, setOffset] = useState(0)
  const [hasMore, setHasMore] = useState(false)
  const [totalCount, setTotalCount] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selected, setSelected] = useState<LooseObject | null>(null)
  const [viewerHash, setViewerHash] = useState<string | null>(null)

  useEffect(() => {
    api.rpcCall<LooseStats>(connName, repoPath, 'loose_stats')
      .then(setStats)
      .catch(() => {})
  }, [connName, repoPath])

  const fetchPage = useCallback(async (pageOffset: number) => {
    setLoading(true)
    setError(null)
    try {
      const resp = await api.rpcCall<LooseListResponse>(connName, repoPath, 'loose_list', {
        offset: pageOffset, limit: PAGE_SIZE
      })
      setObjects(resp.objects)
      setHasMore(resp.has_more)
      setTotalCount(stats?.count ?? resp.count)
    } catch (err) {
      setError(String(err))
    }
    setLoading(false)
  }, [connName, repoPath, stats])

  useEffect(() => { fetchPage(0) }, [fetchPage])

  const goToPage = (newOffset: number) => {
    setOffset(newOffset)
    setSelected(null)
    fetchPage(newOffset)
  }

  const pageCount = Math.ceil((stats?.count ?? totalCount) / PAGE_SIZE)
  const currentPage = Math.floor(offset / PAGE_SIZE) + 1

  return (
    <div>
      {/* Header */}
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Dashboard
        </button>
        <h3 className="text-lg font-medium m-0">Loose Objects</h3>
        {stats && <span className="text-xs text-text-muted">{fmtNum(stats.count)} objects, {fmtSize(stats.total_uncomp)}</span>}
      </div>

      {error && <div className="text-status-error text-sm mb-3">{error}</div>}

      {/* Stats summary */}
      {stats && (
        <div className="flex flex-wrap gap-3 mb-4">
          {stats.per_type.filter(t => t.count > 0).map(t => (
            <div key={t.type} className="bg-surface-secondary border border-border-default rounded-lg px-3 py-2 text-xs">
              <span className="font-semibold">{OBJECT_TYPES[t.type] ?? `type-${t.type}`}</span>
              <span className="text-text-muted ml-2">{fmtNum(t.count)} ({fmtSize(t.uncomp)})</span>
            </div>
          ))}
          {stats.skip.count > 0 && (
            <div className="bg-surface-secondary border border-border-default rounded-lg px-3 py-2 text-xs">
              <span className="font-semibold text-status-warning">Skip-marked</span>
              <span className="text-text-muted ml-2">{fmtNum(stats.skip.count)} ({fmtSize(stats.skip.uncomp)})</span>
            </div>
          )}
        </div>
      )}

      {/* Object list */}
      {objects.length > 0 && (
        <div className="bg-surface-secondary border border-border-default rounded-lg overflow-hidden">
          <table className="w-full text-xs table-fixed">
            <thead>
              <tr className="text-text-muted text-left">
                <th className="font-medium pb-2 pt-4 pl-4">Hash</th>
                <th className="font-medium pb-2 pt-4 w-16">Type</th>
                <th className="font-medium pb-2 pt-4 w-20">Compress</th>
                <th className="font-medium pb-2 pt-4 w-20 text-right">Size</th>
                <th className="font-medium pb-2 pt-4 w-20 text-right">Compressed</th>
                <th className="font-medium pb-2 pt-4 w-16 text-right pr-4">Ratio</th>
              </tr>
            </thead>
            <tbody>
              {objects.map(obj => {
                const ratio = obj.uncompressed_size > 0
                  ? (obj.compressed_size / obj.uncompressed_size * 100).toFixed(0)
                  : '—'
                const isSelected = selected?.hash === obj.hash
                return (
                  <tr key={obj.hash}
                    onClick={() => setSelected(isSelected ? null : obj)}
                    className={`border-t border-border-default cursor-pointer hover:bg-surface-hover ${isSelected ? 'bg-accent/10' : ''}`}>
                    <td className="py-1.5 pl-4 font-mono truncate" title={obj.hash}>{obj.hash}</td>
                    <td className="py-1.5">{OBJECT_TYPES[obj.type] ?? obj.type}</td>
                    <td className="py-1.5 text-text-muted">{COMPRESS_NAMES[obj.compression] ?? obj.compression}</td>
                    <td className="py-1.5 text-right">{fmtSize(obj.uncompressed_size)}</td>
                    <td className="py-1.5 text-right text-text-muted">{fmtSize(obj.compressed_size)}</td>
                    <td className="py-1.5 text-right pr-4 text-text-muted">{ratio}%</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {loading && <div className="text-text-muted text-sm mt-3">Loading...</div>}

      {/* Pagination */}
      {pageCount > 1 && (
        <div className="flex items-center justify-center gap-2 mt-4">
          <button onClick={() => goToPage(offset - PAGE_SIZE)} disabled={offset === 0}
            className="text-xs px-2 py-1 rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none cursor-pointer disabled:opacity-50 disabled:cursor-default">
            Prev
          </button>
          <span className="text-xs text-text-muted">Page {currentPage} of {pageCount}</span>
          <button onClick={() => goToPage(offset + PAGE_SIZE)} disabled={!hasMore}
            className="text-xs px-2 py-1 rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none cursor-pointer disabled:opacity-50 disabled:cursor-default">
            Next
          </button>
        </div>
      )}

      {/* Selected object detail */}
      {selected && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
          <div className="bg-surface-primary rounded-lg p-5 w-[480px] shadow-xl border border-border-default">
            <div className="flex items-start justify-between mb-3">
              <div className="text-sm font-semibold">Object Detail</div>
              <button onClick={() => setSelected(null)}
                className="text-text-muted hover:text-text-primary bg-transparent border-none cursor-pointer text-lg px-1">&times;</button>
            </div>
            <div className="font-mono text-xs text-text-secondary break-all mb-3">{selected.hash}</div>
            <div className="grid grid-cols-2 gap-3 mb-3">
              <div>
                <div className="text-[10px] text-text-muted uppercase">Type</div>
                <div className="text-xs">{OBJECT_TYPES[selected.type] ?? selected.type}</div>
              </div>
              <div>
                <div className="text-[10px] text-text-muted uppercase">Compression</div>
                <div className="text-xs">{COMPRESS_NAMES[selected.compression] ?? selected.compression}</div>
              </div>
              <div>
                <div className="text-[10px] text-text-muted uppercase">Uncompressed</div>
                <div className="text-xs">{fmtSize(selected.uncompressed_size)} ({fmtNum(selected.uncompressed_size)} B)</div>
              </div>
              <div>
                <div className="text-[10px] text-text-muted uppercase">Compressed</div>
                <div className="text-xs">{fmtSize(selected.compressed_size)} ({fmtNum(selected.compressed_size)} B)</div>
              </div>
              {selected.file_size !== undefined && (
                <div>
                  <div className="text-[10px] text-text-muted uppercase">File on disk</div>
                  <div className="text-xs">{fmtSize(selected.file_size)}</div>
                </div>
              )}
              {selected.pack_skip_ver > 0 && (
                <div>
                  <div className="text-[10px] text-text-muted uppercase">Skip marked</div>
                  <div className="text-xs text-status-warning">v{selected.pack_skip_ver}</div>
                </div>
              )}
            </div>
            <div className="flex justify-end gap-2">
              <button onClick={() => setSelected(null)}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none">Close</button>
              <button onClick={() => { setViewerHash(selected.hash); setSelected(null) }}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text hover:bg-accent-hover border-none">View Content</button>
            </div>
          </div>
        </div>
      )}

      {/* Content viewer */}
      {viewerHash && (
        <ContentViewer connName={connName} repoPath={repoPath} hash={viewerHash}
          onClose={() => setViewerHash(null)} />
      )}
    </div>
  )
}
