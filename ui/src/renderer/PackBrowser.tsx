/*
 * What is inside the pack files.
 *
 * Rewritten rather than migrated, because the screen was built on a
 * repository layout that no longer exists. It listed packs from a 'scan'
 * action the bridge never mapped - so it threw the moment it opened - and
 * then read each pack's contents from a `.idx` sidecar file paired with the
 * `.dat`. There are no sidecars: pack contents are rows in pack_objects,
 * which is what makes an offset lookup an index seek instead of a binary
 * search through a file.
 *
 * The fanout chart is the part worth keeping. The `.idx` header stored a
 * 256-entry cumulative table because a sorted on-disk index needs somewhere
 * to start a binary search; the same shape is now a GROUP BY over hash
 * prefixes, so it is a picture of how evenly the hashes spread rather than a
 * lookup structure. It reads from /fanout, which had no consumer until now.
 */

import React, { useCallback, useEffect, useState } from 'react'
import { fmtSize, fmtNum, absoluteTime } from './format'
import { ContentViewer } from './ContentViewer'

const api = window.cbackup

const PAGE_SIZE = 200

/* Every integer arrives as a string. */
interface RawPack {
  id: string
  filename?: string
  size?: string
  sealed_at?: string
  version?: string
  entries?: string
}

interface Pack {
  id: number
  filename: string
  size: number
  sealedAt: number
  version: number
  entries: number
}

interface RawEntry {
  hash: string
  offset?: string
  comp_size?: string
  orig_size?: string
  entry_index?: string
}

interface Entry {
  hash: string
  offset: number
  compSize: number
  origSize: number
  entryIndex: number
}

interface Bucket { prefix: string; count: number }

const parsePack = (p: RawPack): Pack => ({
  id: Number(p.id),
  filename: p.filename ?? '',
  size: Number(p.size ?? '0'),
  sealedAt: Number(p.sealed_at ?? '0'),
  version: Number(p.version ?? '0'),
  entries: Number(p.entries ?? '0'),
})

const parseEntry = (e: RawEntry): Entry => ({
  hash: e.hash,
  offset: Number(e.offset ?? '0'),
  compSize: Number(e.comp_size ?? '0'),
  origSize: Number(e.orig_size ?? '0'),
  entryIndex: Number(e.entry_index ?? '0'),
})

interface Props {
  connName: string
  repoPath: string
  onBack: () => void
}

export function PackBrowser({ connName, repoPath, onBack }: Props): React.ReactElement {
  const repoId = Number(repoPath)

  const [packs, setPacks] = useState<Pack[]>([])
  const [selected, setSelected] = useState<Pack | null>(null)
  const [entries, setEntries] = useState<Entry[]>([])
  const [offset, setOffset] = useState(0)
  const [hasMore, setHasMore] = useState(false)
  const [loading, setLoading] = useState(true)
  const [entriesLoading, setEntriesLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [viewerHash, setViewerHash] = useState<string | null>(null)

  const [buckets, setBuckets] = useState<Bucket[]>([])
  const [hover, setHover] = useState<number | null>(null)
  const [showFanout, setShowFanout] = useState(false)
  const [fanoutLoading, setFanoutLoading] = useState(false)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    api.apiCall<{ packs?: RawPack[] }>(connName, { kind: 'packList', repoId })
      .then(r => { if (!cancelled) setPacks((r.packs ?? []).map(parsePack)) })
      .catch(err => { if (!cancelled) setError(String(err)) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [connName, repoId])

  const loadEntries = useCallback(async (pack: Pack, pageOffset: number) => {
    setEntriesLoading(true)
    try {
      const r = await api.apiCall<{ entries?: RawEntry[] }>(connName, {
        kind: 'packEntries', repoId, packId: pack.id,
        limit: PAGE_SIZE, offset: pageOffset,
      })
      const list = (r.entries ?? []).map(parseEntry)
      setEntries(list)
      setHasMore(list.length === PAGE_SIZE)
    } catch (err) {
      setError(String(err))
    }
    setEntriesLoading(false)
  }, [connName, repoId])

  const openPack = (pack: Pack) => {
    setSelected(pack)
    setOffset(0)
    void loadEntries(pack, 0)
  }

  const openFanout = async () => {
    setShowFanout(true)
    if (buckets.length > 0) return
    /*
     * This is a GROUP BY over every object in the repository, so it is the
     * one panel here that can take real time on a large store. Without this
     * flag the chart renders its empty state first and reads as "0 objects"
     * - a repository reporting itself empty, which is the worst possible
     * thing for it to say while it is simply still counting.
     */
    setFanoutLoading(true)
    try {
      const r = await api.apiCall<{ buckets?: { prefix: string; count: string }[] }>(
        connName, { kind: 'fanout', repoId })
      setBuckets((r.buckets ?? []).map(b => ({ prefix: b.prefix, count: Number(b.count) })))
    } catch (err) {
      setError(String(err))
    }
    setFanoutLoading(false)
  }

  /*
   * 256 slots, because a two-character hex prefix has 256 values. The route
   * only returns prefixes that occur, so the empty ones have to be filled in
   * here - a chart with the gaps closed up would hide exactly the unevenness
   * it exists to show.
   */
  const counts = new Array<number>(256).fill(0)
  for (const b of buckets) {
    const i = parseInt(b.prefix, 16)
    if (Number.isFinite(i) && i >= 0 && i < 256) counts[i] = b.count
  }
  const maxCount = Math.max(0, ...counts)
  const total = counts.reduce((a, b) => a + b, 0)
  const avg = total / 256

  const totalSize = packs.reduce((a, p) => a + p.size, 0)
  const totalEntries = packs.reduce((a, p) => a + p.entries, 0)

  return (
    <div>
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Back
        </button>
        <h3 className="text-lg font-medium m-0">Packs</h3>
        {!loading && packs.length > 0 && (
          <span className="text-xs text-text-muted">
            {fmtNum(packs.length)} pack{packs.length === 1 ? '' : 's'},{' '}
            {fmtNum(totalEntries)} objects, {fmtSize(totalSize)}
          </span>
        )}
        <button onClick={openFanout}
          className="text-[11px] px-2 py-0.5 rounded bg-surface-tertiary hover:bg-surface-hover
                     text-text-secondary cursor-pointer border-none ml-auto">
          Hash distribution
        </button>
      </div>

      {error && <div className="text-status-error text-sm mb-3">{error}</div>}
      {loading && <div className="text-text-muted text-sm">Loading...</div>}

      {!loading && packs.length === 0 && !error && (
        <div className="text-text-muted text-sm">
          Nothing is packed yet. Backup packs on completion, so this fills in
          after the first one finishes.
        </div>
      )}

      {!loading && packs.length > 0 && (
        <div className="flex gap-4 items-start">
          <div className="bg-surface-secondary border border-border-default rounded-lg overflow-hidden shrink-0">
            <table className="text-xs">
              <thead>
                <tr className="text-text-muted text-left">
                  <th className="font-medium pb-2 pt-4 pl-4">Pack</th>
                  <th className="font-medium pb-2 pt-4 text-right">Objects</th>
                  <th className="font-medium pb-2 pt-4 text-right pr-6">Size</th>
                  <th className="font-medium pb-2 pt-4 pr-4">Sealed</th>
                </tr>
              </thead>
              <tbody>
                {packs.map(p => (
                  <tr key={p.id} onClick={() => openPack(p)}
                    className={`border-t border-border-default cursor-pointer hover:bg-surface-hover
                                text-text-primary ${selected?.id === p.id ? 'bg-accent/10' : ''}`}>
                    <td className="py-1.5 pl-4 pr-4 font-mono">{p.filename || `#${p.id}`}</td>
                    <td className="py-1.5 pr-4 text-right">{fmtNum(p.entries)}</td>
                    <td className="py-1.5 pr-6 text-right">{fmtSize(p.size)}</td>
                    {/* A pack with no sealed_at is still being written to. */}
                    <td className="py-1.5 pr-4 text-text-muted whitespace-nowrap">
                      {p.sealedAt > 0 ? absoluteTime(p.sealedAt) : 'open'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {selected && (
            <div className="flex-1 min-w-0">
              <div className="text-xs text-text-secondary mb-2">
                {selected.filename || `Pack #${selected.id}`}
                <span className="text-text-muted ml-2">
                  format v{selected.version}, {fmtNum(selected.entries)} objects
                </span>
              </div>

              {entriesLoading ? (
                <div className="text-text-muted text-xs">Loading entries...</div>
              ) : (
                <div className="bg-surface-secondary border border-border-default rounded-lg overflow-hidden">
                  <table className="w-full text-xs table-fixed">
                    <thead>
                      <tr className="text-text-muted text-left">
                        <th className="font-medium pb-2 pt-4 pl-4">Hash</th>
                        <th className="font-medium pb-2 pt-4 w-24 text-right">Offset</th>
                        <th className="font-medium pb-2 pt-4 w-24 text-right">Content</th>
                        <th className="font-medium pb-2 pt-4 w-24 text-right">Packed</th>
                        <th className="font-medium pb-2 pt-4 w-16 text-right pr-4">Ratio</th>
                      </tr>
                    </thead>
                    <tbody>
                      {entries.map(e => (
                        <tr key={e.hash} onClick={() => setViewerHash(e.hash)}
                          className="border-t border-border-default cursor-pointer hover:bg-surface-hover text-text-primary">
                          <td className="py-1.5 pl-4 font-mono truncate" title={e.hash}>{e.hash}</td>
                          <td className="py-1.5 text-right font-mono text-text-muted">{fmtNum(e.offset)}</td>
                          <td className="py-1.5 text-right">{fmtSize(e.origSize)}</td>
                          <td className="py-1.5 text-right text-text-muted">{fmtSize(e.compSize)}</td>
                          <td className="py-1.5 text-right pr-4 text-text-muted">
                            {e.origSize > 0 ? `${(e.compSize / e.origSize * 100).toFixed(0)}%` : '-'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {(offset > 0 || hasMore) && (
                <div className="flex items-center justify-center gap-2 mt-3">
                  <button onClick={() => { const o = Math.max(0, offset - PAGE_SIZE); setOffset(o); void loadEntries(selected, o) }}
                    disabled={offset === 0}
                    className="text-xs px-2 py-1 rounded bg-surface-tertiary text-text-secondary
                               hover:bg-surface-hover border-none cursor-pointer disabled:opacity-50">
                    Prev
                  </button>
                  <span className="text-xs text-text-muted">
                    {fmtNum(offset + 1)}-{fmtNum(offset + entries.length)} of {fmtNum(selected.entries)}
                  </span>
                  <button onClick={() => { const o = offset + PAGE_SIZE; setOffset(o); void loadEntries(selected, o) }}
                    disabled={!hasMore}
                    className="text-xs px-2 py-1 rounded bg-surface-tertiary text-text-secondary
                               hover:bg-surface-hover border-none cursor-pointer disabled:opacity-50">
                    Next
                  </button>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {showFanout && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50"
             onClick={() => setShowFanout(false)}>
          <div className="bg-surface-primary rounded-lg p-5 shadow-xl border border-border-default"
               style={{ width: 'min(90vw, 900px)' }} onClick={e => e.stopPropagation()}>
            <div className="flex items-start justify-between mb-1">
              <div className="text-sm font-semibold">Hash distribution</div>
              <button onClick={() => setShowFanout(false)}
                className="text-text-muted hover:text-text-primary bg-transparent border-none cursor-pointer text-lg px-1">
                &times;
              </button>
            </div>
            <div className="text-xs text-text-muted mb-4" style={{ maxWidth: '46rem' }}>
              How many objects fall under each of the 256 first-byte hash
              prefixes. A flat chart is a healthy content-addressed store;
              a spike would mean the hash is not spreading, which is worth
              knowing because every lookup assumes it does.
            </div>

            <div className="text-xs text-text-secondary mb-2 h-4">
              {fanoutLoading
                ? 'Counting every object in the repository...'
                : hover !== null
                  ? `0x${hover.toString(16).padStart(2, '0')}: ${fmtNum(counts[hover])} objects`
                  : `${fmtNum(total)} objects, ${avg.toFixed(1)} per prefix on average`}
            </div>

            <div className="flex items-end gap-px relative" style={{ height: '8rem' }}
                 onMouseLeave={() => setHover(null)}>
              {!fanoutLoading && maxCount > 0 && (
                <div className="absolute left-0 right-0 border-t border-dashed border-status-warning"
                     style={{ bottom: `${(avg / maxCount) * 100}%` }}>
                  <span className="absolute right-0 -top-4 text-[9px] text-status-warning">
                    avg {avg.toFixed(1)}
                  </span>
                </div>
              )}
              {!fanoutLoading && counts.map((c, i) => (
                <div key={i} onMouseEnter={() => setHover(i)}
                  className={`flex-1 ${hover === i ? 'bg-accent' : 'bg-accent/50'}`}
                  style={{ height: maxCount > 0 ? `${Math.max(1, (c / maxCount) * 100)}%` : '1px' }} />
              ))}
            </div>
            <div className="flex justify-between text-[10px] text-text-muted mt-1">
              <span>0x00</span><span>0x7f</span><span>0xff</span>
            </div>
          </div>
        </div>
      )}

      {viewerHash && (
        <ContentViewer connName={connName} repoPath={repoPath} hash={viewerHash}
          onClose={() => setViewerHash(null)} />
      )}
    </div>
  )
}
