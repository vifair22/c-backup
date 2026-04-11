import React, { useEffect, useState } from 'react'
import { fmtSize, fmtNum, absoluteTime, gfsBadges, GFS_DAILY, GFS_WEEKLY, GFS_MONTHLY, GFS_YEARLY } from './format'

const api = window.cbackup

interface Snapshot {
  id: number
  created_sec: number
  node_count: number
  dirent_count: number
  phys_new_bytes: number
  gfs_flags: number
  snap_flags: number
  logical_bytes: number
}

interface SnapList {
  head: number
  snapshots: Snapshot[]
}

type SortKey = 'id' | 'created_sec' | 'node_count' | 'phys_new_bytes'
type SortDir = 'asc' | 'desc'

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
  const [snapList, setSnapList] = useState<SnapList | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [sortKey, setSortKey] = useState<SortKey>('created_sec')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [tierFilter, setTierFilter] = useState<number | null>(null) // null = show all

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    api.rpcCall<SnapList>(connName, repoPath, 'list')
      .then(data => { if (!cancelled) setSnapList(data) })
      .catch(err => { if (!cancelled) setError(String(err)) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [connName, repoPath])

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setSortKey(key); setSortDir('desc') }
  }

  const sortIndicator = (key: SortKey) => {
    if (sortKey !== key) return ''
    return sortDir === 'asc' ? ' \u25B2' : ' \u25BC'
  }

  const snaps = snapList?.snapshots ?? []

  // Filter
  const filtered = tierFilter === null
    ? snaps
    : tierFilter === 0
      ? snaps.filter(s => !s.gfs_flags)
      : snaps.filter(s => s.gfs_flags & tierFilter)

  // Sort
  const sorted = [...filtered].sort((a, b) => {
    const av = a[sortKey]
    const bv = b[sortKey]
    return sortDir === 'asc' ? av - bv : bv - av
  })

  return (
    <div>
      {/* Header */}
      <div className="flex items-center gap-3 mb-4">
        <button onClick={onBack}
          className="text-xs text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
          &larr; Dashboard
        </button>
        <h3 className="text-lg font-medium m-0">Snapshots</h3>
        {snapList && (
          <span className="text-xs text-text-muted">{fmtNum(snaps.length)} total</span>
        )}
      </div>

      {loading && <div className="text-text-muted text-sm">Loading snapshots...</div>}
      {error && <div className="text-status-error text-sm">{error}</div>}

      {snapList && (
        <>
          {/* Tier filter + compare */}
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

          {/* Table */}
          <div className="bg-surface-secondary border border-border-default rounded-lg p-4">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-text-muted text-left">
                  <th className="font-medium pb-2 w-16 cursor-pointer" onClick={() => toggleSort('id')}>
                    ID{sortIndicator('id')}
                  </th>
                  <th className="font-medium pb-2 cursor-pointer" onClick={() => toggleSort('created_sec')}>
                    Date{sortIndicator('created_sec')}
                  </th>
                  <th className="font-medium pb-2 text-right cursor-pointer" onClick={() => toggleSort('node_count')}>
                    Nodes{sortIndicator('node_count')}
                  </th>
                  <th className="font-medium pb-2 text-right cursor-pointer" onClick={() => toggleSort('phys_new_bytes')}>
                    New Data{sortIndicator('phys_new_bytes')}
                  </th>
                  <th className="font-medium pb-2 text-center">Retention</th>
                </tr>
              </thead>
              <tbody>
                {sorted.map(s => {
                  const badges = gfsBadges(s.gfs_flags)
                  return (
                    <tr key={s.id} onClick={() => onSelectSnapshot(s.id)}
                      className="text-text-primary border-t border-border-default cursor-pointer hover:bg-surface-hover">
                      <td className="py-1.5 font-mono">#{s.id}</td>
                      <td className="py-1.5">{absoluteTime(s.created_sec)}</td>
                      <td className="py-1.5 text-right">{fmtNum(s.node_count)}</td>
                      <td className="py-1.5 text-right">{fmtSize(s.phys_new_bytes)}</td>
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
            {sorted.length === 0 && filtered.length === 0 && (
              <div className="text-text-muted text-xs text-center py-4">No snapshots match this filter.</div>
            )}
          </div>
        </>
      )}
    </div>
  )
}
