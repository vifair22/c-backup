/*
 * Every snapshot in a repository, and why each one is still here.
 *
 * On the typed union rather than the action bridge. That is not a
 * housekeeping detail here: the bridge mapped 'list' to the snapshots route
 * and this component then read `created_sec`, `node_count`, `phys_new_bytes`
 * and `gfs_flags` out of a response that carries `ts`, `files`, `size` and
 * nothing else. Every column rendered as a dash and the sort comparator
 * subtracted undefined from undefined. It looked like an empty repository.
 *
 * The daemon renders every integer as a string so a 64-bit count cannot be
 * mangled by a JSON number, so everything numeric is converted on the way in
 * rather than trusted.
 */

import React, { useCallback, useEffect, useState } from 'react'
import { fmtSize, fmtNum, absoluteTime, gfsBadges, GFS_DAILY, GFS_WEEKLY, GFS_MONTHLY, GFS_YEARLY } from './format'

const api = window.cbackup

/* As the daemon sends it. */
interface RawSnapshot {
  id: string
  ts: string
  label?: string
  files?: string
  size?: string
  gfs_flags?: string
  materialized?: string
}

interface Snapshot {
  id: number
  ts: number
  label: string
  files: number
  size: number
  gfsFlags: number
  materialized: boolean
}

const parse = (r: RawSnapshot): Snapshot => ({
  id: Number(r.id),
  ts: Number(r.ts),
  label: r.label ?? '',
  files: Number(r.files ?? '0'),
  size: Number(r.size ?? '0'),
  gfsFlags: Number(r.gfs_flags ?? '0'),
  /* Absent means resident: the column postdates the first repositories. */
  materialized: r.materialized === undefined || r.materialized !== '0',
})

type SortKey = 'id' | 'ts' | 'files' | 'size'
type SortDir = 'asc' | 'desc'

const PAGE = 200

const TIER_FILTERS = [
  { label: 'Yearly', flag: GFS_YEARLY, color: 'bg-amber-600' },
  { label: 'Monthly', flag: GFS_MONTHLY, color: 'bg-blue-600' },
  { label: 'Weekly', flag: GFS_WEEKLY, color: 'bg-green-600' },
  { label: 'Daily', flag: GFS_DAILY, color: 'bg-teal-600' },
  { label: 'Untagged', flag: 0, color: 'bg-gray-500' },
] as const

interface Props {
  connName: string
  repoPath: string
  onSelectSnapshot: (snapId: number) => void
  onCompareSnapshots?: () => void
  onBack: () => void
}

export function SnapshotList({ connName, repoPath, onSelectSnapshot, onCompareSnapshots, onBack }: Props): React.ReactElement {
  const repoId = Number(repoPath)

  const [snaps, setSnaps] = useState<Snapshot[]>([])
  const [loading, setLoading] = useState(true)
  const [loadingMore, setLoadingMore] = useState(false)
  const [more, setMore] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [sortKey, setSortKey] = useState<SortKey>('ts')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [tierFilter, setTierFilter] = useState<number | null>(null) // null = show all

  /*
   * One page at a time. The route pages by design and a repository with a
   * year of hourly snapshots has thousands of them, so asking for all of
   * them would trade a fast first screen for a slow one nobody scrolls.
   */
  const fetchPage = useCallback(async (offset: number): Promise<Snapshot[]> => {
    const r = await api.apiCall<{ snapshots?: RawSnapshot[] }>(connName, {
      kind: 'snapshotList', repoId, limit: PAGE, offset,
    })
    const list = (r.snapshots ?? []).map(parse)
    setMore(list.length === PAGE)
    return list
  }, [connName, repoId])

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetchPage(0)
      .then(list => { if (!cancelled) setSnaps(list) })
      .catch(err => { if (!cancelled) setError(String(err)) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [fetchPage])

  const loadMore = async () => {
    setLoadingMore(true)
    try {
      const next = await fetchPage(snaps.length)
      setSnaps(prev => [...prev, ...next])
    } catch (err) {
      setError(String(err))
    }
    setLoadingMore(false)
  }

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setSortKey(key); setSortDir('desc') }
  }

  const sortIndicator = (key: SortKey) => {
    if (sortKey !== key) return ''
    return sortDir === 'asc' ? ' \u25B2' : ' \u25BC'
  }

  const filtered = tierFilter === null
    ? snaps
    : tierFilter === 0
      ? snaps.filter(s => !s.gfsFlags)
      : snaps.filter(s => s.gfsFlags & tierFilter)

  const sorted = [...filtered].sort((a, b) => {
    const av = a[sortKey]
    const bv = b[sortKey]
    return sortDir === 'asc' ? av - bv : bv - av
  })

  return (
    <div>
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Dashboard
        </button>
        <h3 className="text-lg font-medium m-0">Snapshots</h3>
        {!loading && (
          <span className="text-xs text-text-muted">
            {fmtNum(snaps.length)}{more ? '+' : ''} total
          </span>
        )}
      </div>

      {loading && <div className="text-text-muted text-sm">Loading snapshots...</div>}
      {error && <div className="text-status-error text-sm">{error}</div>}

      {!loading && !error && snaps.length === 0 && (
        <div className="text-text-muted text-sm">
          Nothing has been backed up into this repository yet.
        </div>
      )}

      {!loading && snaps.length > 0 && (
        <>
          <div className="flex gap-2 mb-3 items-center">
            <button onClick={() => setTierFilter(null)}
              className={`text-[11px] px-2 py-0.5 rounded cursor-pointer border-none ${
                tierFilter === null ? 'bg-accent text-accent-text' : 'bg-surface-tertiary text-text-secondary hover:bg-surface-hover'
              }`}>
              All
            </button>
            {TIER_FILTERS.map(t => {
              const active = tierFilter === t.flag
              return (
                <button key={t.label} onClick={() => setTierFilter(active ? null : t.flag)}
                  className={`text-[11px] px-2 py-0.5 rounded cursor-pointer border-none flex items-center gap-1 ${
                    active ? 'bg-accent text-accent-text' : 'bg-surface-tertiary text-text-secondary hover:bg-surface-hover'
                  }`}>
                  <span className={`inline-block w-2 h-2 rounded-full ${t.color}`} />
                  {t.label}
                </button>
              )
            })}
            {onCompareSnapshots && snaps.length >= 2 && (
              <button onClick={onCompareSnapshots}
                className="text-[11px] px-2 py-0.5 rounded bg-surface-tertiary hover:bg-surface-hover text-text-secondary cursor-pointer border-none ml-auto">
                Compare
              </button>
            )}
          </div>

          <div className="bg-surface-secondary border border-border-default rounded-lg p-4">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-text-muted text-left">
                  <th className="font-medium pb-2 w-16 cursor-pointer" onClick={() => toggleSort('id')}>
                    ID{sortIndicator('id')}
                  </th>
                  <th className="font-medium pb-2 cursor-pointer" onClick={() => toggleSort('ts')}>
                    Date{sortIndicator('ts')}
                  </th>
                  <th className="font-medium pb-2">Label</th>
                  <th className="font-medium pb-2 text-right cursor-pointer" onClick={() => toggleSort('files')}>
                    Files{sortIndicator('files')}
                  </th>
                  <th className="font-medium pb-2 text-right cursor-pointer" onClick={() => toggleSort('size')}>
                    Size{sortIndicator('size')}
                  </th>
                  <th className="font-medium pb-2 text-center">Retention</th>
                </tr>
              </thead>
              <tbody>
                {sorted.map(s => {
                  const badges = gfsBadges(s.gfsFlags)
                  return (
                    <tr key={s.id} onClick={() => onSelectSnapshot(s.id)}
                      className="text-text-primary border-t border-border-default cursor-pointer hover:bg-surface-hover">
                      <td className="py-1.5 font-mono">#{s.id}</td>
                      <td className="py-1.5">{absoluteTime(s.ts)}</td>
                      <td className="py-1.5 truncate max-w-[14rem]">
                        {s.label || <span className="text-text-muted">-</span>}
                        {/* A cold snapshot browses after a fault-in, which is
                          * slow enough to be worth warning about first. */}
                        {!s.materialized && (
                          <span className="text-text-muted ml-2">(archived)</span>
                        )}
                      </td>
                      <td className="py-1.5 text-right">{fmtNum(s.files)}</td>
                      <td className="py-1.5 text-right">{fmtSize(s.size)}</td>
                      <td className="py-1.5 text-center">
                        {badges.length > 0 ? (
                          <span className="inline-flex gap-1">
                            {badges.map(b => (
                              <span key={b.label} className={`inline-block px-1.5 py-0 rounded text-[10px] font-semibold ${b.color}`}>
                                {b.label}
                              </span>
                            ))}
                          </span>
                        ) : (
                          <span className="text-text-muted">-</span>
                        )}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
            {sorted.length === 0 && (
              <div className="text-text-muted text-xs text-center py-4">No snapshots match this filter.</div>
            )}
            {more && (
              <div className="text-center pt-3">
                <button onClick={loadMore} disabled={loadingMore}
                  className="text-[11px] px-2 py-1 rounded bg-surface-tertiary hover:bg-surface-hover
                             text-text-secondary cursor-pointer border-none">
                  {loadingMore ? 'Loading...' : 'Load more'}
                </button>
              </div>
            )}
          </div>
        </>
      )}
    </div>
  )
}
