/*
 * The object store, by object.
 *
 * Migrated off the action bridge, and widened to match `c-backup objects`,
 * which takes a location: loose objects are the interesting case but they
 * are not a different kind of thing, and having no way to look at a packed
 * one from here meant dropping into the pack browser to answer a question
 * about a single hash.
 *
 * What the bridge fed this was a different response entirely. It read
 * `uncompressed_size`, `compressed_size`, `compression` and `pack_skip_ver`
 * from rows that carry `orig_size`, `phys_size` and `location`, so the size
 * columns were blank and the compression column showed a codec number that
 * was always undefined. The stats panel read a top-level `count` and a
 * `skip` block that the route does not send at all.
 *
 * Compression codec and skip markers are per-entry facts inside a pack, not
 * columns on the object row, so they are not shown here rather than shown
 * as blanks. The ratio between what an object holds and what it occupies is
 * the part that was actually wanted, and both halves of that are here.
 */

import React, { useCallback, useEffect, useState } from 'react'
import { fmtSize, fmtNum } from './format'
import { ContentViewer } from './ContentViewer'

const api = window.cbackup

const OBJECT_TYPES: Record<number, string> = {
  1: 'file', 2: 'xattr', 3: 'acl', 4: 'sparse',
}

const PAGE_SIZE = 200

type Location = 'loose' | 'packed' | 'all'

/* Every integer arrives as a string. */
interface RawObject {
  hash: string
  type?: string
  refcount?: string
  phys_size?: string
  orig_size?: string
  location?: string
}

interface StoreObject {
  hash: string
  type: number
  refcount: number
  physSize: number
  origSize: number
  location: string
}

const parse = (o: RawObject): StoreObject => ({
  hash: o.hash,
  type: Number(o.type ?? '0'),
  refcount: Number(o.refcount ?? '0'),
  physSize: Number(o.phys_size ?? '0'),
  origSize: Number(o.orig_size ?? '0'),
  location: o.location ?? '',
})

interface RawTypeStat {
  type?: string
  count?: string
  phys_size?: string
  orig_size?: string
}

interface Props {
  connName: string
  repoPath: string
  onBack: () => void
}

export function LooseObjects({ connName, repoPath, onBack }: Props): React.ReactElement {
  const repoId = Number(repoPath)

  const [location, setLocation] = useState<Location>('loose')
  const [perType, setPerType] = useState<RawTypeStat[]>([])
  const [objects, setObjects] = useState<StoreObject[]>([])
  const [offset, setOffset] = useState(0)
  const [hasMore, setHasMore] = useState(false)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selected, setSelected] = useState<StoreObject | null>(null)
  const [viewerHash, setViewerHash] = useState<string | null>(null)

  /* Totals come from a GROUP BY on the daemon side, so the header can say
   * how much is there without paging through all of it. */
  useEffect(() => {
    api.apiCall<{ per_type?: RawTypeStat[] }>(connName, {
      kind: 'stats', repoId, ...(location === 'loose' ? { scope: 'loose' } : {}),
    })
      .then(r => setPerType(r.per_type ?? []))
      .catch(() => setPerType([]))
  }, [connName, repoId, location])

  const fetchPage = useCallback(async (pageOffset: number, loc: Location) => {
    setLoading(true)
    setError(null)
    try {
      const resp = await api.apiCall<{ objects?: RawObject[] }>(connName, {
        kind: 'objectList', repoId,
        ...(loc === 'all' ? {} : { location: loc }),
        offset: pageOffset, limit: PAGE_SIZE,
      })
      const list = (resp.objects ?? []).map(parse)
      setObjects(list)
      /* The route reports no total and no "more" flag, so a full page is
       * the only evidence there is another one. */
      setHasMore(list.length === PAGE_SIZE)
    } catch (err) {
      setError(String(err))
    }
    setLoading(false)
  }, [connName, repoId])

  useEffect(() => { setOffset(0); void fetchPage(0, location) }, [fetchPage, location])

  const goToPage = (newOffset: number) => {
    setOffset(newOffset)
    setSelected(null)
    void fetchPage(newOffset, location)
  }

  const totalCount = perType.reduce((a, t) => a + Number(t.count ?? '0'), 0)
  const totalOrig  = perType.reduce((a, t) => a + Number(t.orig_size ?? '0'), 0)
  const totalPhys  = perType.reduce((a, t) => a + Number(t.phys_size ?? '0'), 0)

  const LOCATIONS: { key: Location; label: string }[] = [
    { key: 'loose', label: 'Loose' },
    { key: 'packed', label: 'Packed' },
    { key: 'all',   label: 'All' },
  ]

  return (
    <div>
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Back
        </button>
        <h3 className="text-lg font-medium m-0">Objects</h3>
        {totalCount > 0 && (
          <span className="text-xs text-text-muted">
            {fmtNum(totalCount)} objects, {fmtSize(totalOrig)} stored as {fmtSize(totalPhys)}
          </span>
        )}
      </div>

      <div className="flex gap-2 mb-4 items-center">
        {LOCATIONS.map(l => (
          <button key={l.key} onClick={() => setLocation(l.key)}
            className={`text-[11px] px-2 py-0.5 rounded cursor-pointer border-none ${
              location === l.key ? 'bg-accent text-accent-text'
                                 : 'bg-surface-tertiary text-text-secondary hover:bg-surface-hover'}`}>
            {l.label}
          </button>
        ))}
        {location === 'loose' && (
          <span className="text-xs text-text-muted ml-2">
            Loose objects are stored uncompressed - packing is what compresses
            them, so an unpacked repository holds more than its source.
          </span>
        )}
      </div>

      {error && <div className="text-status-error text-sm mb-3">{error}</div>}

      {perType.length > 0 && location === 'loose' && (
        <div className="flex flex-wrap gap-3 mb-4">
          {perType.filter(t => Number(t.count ?? '0') > 0).map(t => (
            <div key={t.type} className="bg-surface-secondary border border-border-default rounded-lg px-3 py-2 text-xs">
              <span className="font-semibold">{OBJECT_TYPES[Number(t.type)] ?? `type-${t.type}`}</span>
              <span className="text-text-muted ml-2">
                {fmtNum(t.count)} ({fmtSize(t.orig_size)})
              </span>
            </div>
          ))}
        </div>
      )}

      {objects.length > 0 && (
        <div className="bg-surface-secondary border border-border-default rounded-lg overflow-hidden">
          <table className="w-full text-xs table-fixed">
            <thead>
              <tr className="text-text-muted text-left">
                <th className="font-medium pb-2 pt-4 pl-4">Hash</th>
                <th className="font-medium pb-2 pt-4 w-16">Type</th>
                <th className="font-medium pb-2 pt-4 w-16 text-right">Refs</th>
                <th className="font-medium pb-2 pt-4 w-24 text-right">Content</th>
                <th className="font-medium pb-2 pt-4 w-24 text-right">On disk</th>
                <th className="font-medium pb-2 pt-4 w-16 text-right pr-4">Ratio</th>
              </tr>
            </thead>
            <tbody>
              {objects.map(obj => {
                /* Ratio of stored to original. Above 100% means the object
                 * costs more than it holds, which is what a small file plus
                 * its parity looks like. */
                const ratio = obj.origSize > 0
                  ? `${(obj.physSize / obj.origSize * 100).toFixed(0)}%`
                  : '-'
                const isSelected = selected?.hash === obj.hash
                return (
                  <tr key={obj.hash}
                    onClick={() => setSelected(isSelected ? null : obj)}
                    className={`border-t border-border-default cursor-pointer hover:bg-surface-hover ${isSelected ? 'bg-accent/10' : ''}`}>
                    <td className="py-1.5 pl-4 font-mono truncate" title={obj.hash}>{obj.hash}</td>
                    <td className="py-1.5">{OBJECT_TYPES[obj.type] ?? obj.type}</td>
                    <td className="py-1.5 text-right text-text-muted">{obj.refcount}</td>
                    <td className="py-1.5 text-right">{fmtSize(obj.origSize)}</td>
                    <td className="py-1.5 text-right text-text-muted">{fmtSize(obj.physSize)}</td>
                    <td className="py-1.5 text-right pr-4 text-text-muted">{ratio}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {loading && <div className="text-text-muted text-sm mt-3">Loading...</div>}
      {!loading && objects.length === 0 && !error && (
        <div className="text-text-muted text-sm">
          {location === 'loose'
            ? 'Nothing is loose. Everything here has been packed.'
            : 'No objects.'}
        </div>
      )}

      {(offset > 0 || hasMore) && (
        <div className="flex items-center justify-center gap-2 mt-4">
          <button onClick={() => goToPage(Math.max(0, offset - PAGE_SIZE))} disabled={offset === 0}
            className="text-xs px-2 py-1 rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none cursor-pointer disabled:opacity-50 disabled:cursor-default">
            Prev
          </button>
          <span className="text-xs text-text-muted">
            {fmtNum(offset + 1)}-{fmtNum(offset + objects.length)}
          </span>
          <button onClick={() => goToPage(offset + PAGE_SIZE)} disabled={!hasMore}
            className="text-xs px-2 py-1 rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none cursor-pointer disabled:opacity-50 disabled:cursor-default">
            Next
          </button>
        </div>
      )}

      {selected && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
          <div className="bg-surface-primary rounded-lg p-5 w-[480px] shadow-xl border border-border-default">
            <div className="flex items-start justify-between mb-3">
              <div className="text-sm font-semibold">Object</div>
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
                <div className="text-[10px] text-text-muted uppercase">Stored</div>
                <div className="text-xs">{selected.location || '-'}</div>
              </div>
              <div>
                <div className="text-[10px] text-text-muted uppercase">Content</div>
                <div className="text-xs">{fmtSize(selected.origSize)} ({fmtNum(selected.origSize)} B)</div>
              </div>
              <div>
                <div className="text-[10px] text-text-muted uppercase">On disk</div>
                <div className="text-xs">{fmtSize(selected.physSize)} ({fmtNum(selected.physSize)} B)</div>
              </div>
              <div>
                {/* Above one is dedup: the same bytes reached from more than
                  * one file, or from the same file in several snapshots. */}
                <div className="text-[10px] text-text-muted uppercase">Referenced by</div>
                <div className="text-xs">{fmtNum(selected.refcount)} node{selected.refcount === 1 ? '' : 's'}</div>
              </div>
            </div>
            <div className="flex justify-end gap-2">
              <button onClick={() => setSelected(null)}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none">Close</button>
              <button onClick={() => { setViewerHash(selected.hash); setSelected(null) }}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text hover:bg-accent-hover border-none">View content</button>
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
