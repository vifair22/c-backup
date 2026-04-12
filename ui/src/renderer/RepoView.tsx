import React, { useEffect, useState } from 'react'
import { StatCard } from './StatCard'
import {
  fmtSize, fmtNum, relativeTime, absoluteTime, gfsBadges,
  GFS_DAILY, GFS_WEEKLY, GFS_MONTHLY, GFS_YEARLY,
} from './format'

const api = window.cbackup

/* ------------------------------------------------------------------ */
/* Types for RPC responses                                             */
/* ------------------------------------------------------------------ */

interface Stats {
  snap_count: number
  snap_total: number
  head_entries: number
  head_logical_bytes: number
  snap_bytes: number
  loose_objects: number
  loose_bytes: number
  pack_files: number
  pack_bytes: number
  total_bytes: number
}

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

interface TypeStat {
  type: number
  count: number
  uncomp: number
  comp: number
}

interface BucketStat {
  count: number
  uncomp: number
  comp: number
}

interface RepoStats {
  per_type: TypeStat[]
  pack: BucketStat
  loose: BucketStat
  skip: BucketStat
  hiratio: BucketStat
}

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

function lastBackupStatus(epochSec: number): 'ok' | 'warn' | 'error' {
  const ageSec = Date.now() / 1000 - epochSec
  if (ageSec < 86400) return 'ok'
  if (ageSec < 604800) return 'warn'
  return 'error'
}

const OBJECT_TYPE_NAMES: Record<number, string> = {
  1: 'File', 2: 'Xattr', 3: 'ACL', 4: 'Sparse',
}

/* ------------------------------------------------------------------ */
/* Component                                                           */
/* ------------------------------------------------------------------ */

interface Props {
  connName: string
  repoPath: string
  onSelectSnapshot?: (snapId: number) => void
  onViewAllSnapshots?: () => void
  onCompareSnapshots?: (a?: number, b?: number) => void
  onSearch?: () => void
  onEditPolicy?: () => void
  onViewJournal?: () => void
  onViewTags?: () => void
}

export function RepoView({ connName, repoPath, onSelectSnapshot, onViewAllSnapshots, onCompareSnapshots, onSearch, onEditPolicy, onViewJournal, onViewTags }: Props): React.ReactElement {
  const [stats, setStats] = useState<Stats | null>(null)
  const [snapList, setSnapList] = useState<SnapList | null>(null)
  const [repoStats, setRepoStats] = useState<RepoStats | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    setStats(null)
    setSnapList(null)
    setRepoStats(null)

    Promise.all([
      api.rpcCall<Stats>(connName, repoPath, 'stats'),
      api.rpcCall<SnapList>(connName, repoPath, 'list'),
      api.rpcCall<RepoStats>(connName, repoPath, 'repo_stats'),
    ]).then(([s, l, r]) => {
      if (cancelled) return
      setStats(s)
      setSnapList(l)
      setRepoStats(r)
    }).catch(err => {
      if (!cancelled) setError(String(err))
    }).finally(() => {
      if (!cancelled) setLoading(false)
    })

    return () => { cancelled = true }
  }, [connName, repoPath])

  if (loading) {
    return (
      <div className="p-4">
        <h3 className="text-lg font-medium m-0 mb-1">{repoPath}</h3>
        <div className="text-text-muted text-xs mb-4">via {connName}</div>
        <div className="text-text-muted">Loading dashboard...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="p-4">
        <h3 className="text-lg font-medium m-0 mb-1">{repoPath}</h3>
        <div className="text-text-muted text-xs mb-4">via {connName}</div>
        <div className="text-status-error text-sm">{error}</div>
      </div>
    )
  }

  const snaps = snapList?.snapshots ?? []
  const latest = snaps.length > 0 ? snaps[snaps.length - 1] : null
  const recentSnaps = [...snaps].reverse().slice(0, 10)

  // GFS counts
  const gfsCounts = {
    daily: snaps.filter(s => s.gfs_flags & GFS_DAILY).length,
    weekly: snaps.filter(s => s.gfs_flags & GFS_WEEKLY).length,
    monthly: snaps.filter(s => s.gfs_flags & GFS_MONTHLY).length,
    yearly: snaps.filter(s => s.gfs_flags & GFS_YEARLY).length,
    untagged: snaps.filter(s => !s.gfs_flags).length,
  }

  // Compression totals
  const totalUncomp = repoStats
    ? repoStats.per_type.reduce((a, t) => a + t.uncomp, 0)
    : 0
  const totalComp = repoStats
    ? repoStats.per_type.reduce((a, t) => a + t.comp, 0)
    : 0
  const compressionRatio = totalUncomp > 0 ? totalComp / totalUncomp : 1
  const spaceSaved = totalUncomp - totalComp

  // Storage bar percentages
  const looseBytes = stats?.loose_bytes ?? 0
  const packBytes = stats?.pack_bytes ?? 0
  const totalBytes = stats?.total_bytes ?? 1
  const loosePct = Math.round((looseBytes / totalBytes) * 100)
  const packPct = 100 - loosePct

  return (
    <div>
      <h3 className="text-lg font-medium m-0 mb-1">{repoPath}</h3>
      <div className="text-text-muted text-xs mb-4">via {connName}</div>

      {/* Row 1: Summary cards */}
      <div className="grid grid-cols-4 gap-3 mb-4">
        <StatCard
          label="Last Backup"
          value={latest ? relativeTime(latest.created_sec) : 'Never'}
          detail={latest ? absoluteTime(latest.created_sec) : undefined}
          status={latest ? lastBackupStatus(latest.created_sec) : 'error'}
        />
        <div onClick={() => onViewAllSnapshots?.()} className={`${onViewAllSnapshots ? 'cursor-pointer' : ''} h-full`}>
          <StatCard
            label="Snapshots"
            value={fmtNum(stats?.snap_count ?? 0)}
            detail={snapList?.head ? `HEAD: #${snapList.head}` : undefined}
          />
        </div>
        <StatCard
          label="Storage"
          value={fmtSize(stats?.total_bytes ?? 0)}
        >
          <div className="flex h-1.5 rounded-full overflow-hidden mt-2 bg-surface-tertiary">
            {packPct > 0 && <div className="bg-accent" style={{ width: `${packPct}%` }} />}
            {loosePct > 0 && <div className="bg-status-warning" style={{ width: `${loosePct}%` }} />}
          </div>
          <div className="flex justify-between text-[10px] text-text-muted mt-1">
            <span>Packed {fmtSize(packBytes)}</span>
            <span>Loose {fmtSize(looseBytes)}</span>
          </div>
        </StatCard>
        <StatCard
          label="Files Tracked"
          value={fmtNum(stats?.head_entries ?? 0)}
          detail={stats?.head_logical_bytes ? `${fmtSize(stats.head_logical_bytes)} logical` : undefined}
        />
      </div>

      {/* Quick actions */}
      <div className="flex gap-2 mb-4">
        {onSearch && (
          <button onClick={onSearch}
            className="text-xs px-3 py-1.5 rounded bg-surface-secondary border border-border-default text-text-secondary hover:bg-surface-hover cursor-pointer">
            Search files
          </button>
        )}
        {onCompareSnapshots && snaps.length >= 2 && (
          <button onClick={() => onCompareSnapshots()}
            className="text-xs px-3 py-1.5 rounded bg-surface-secondary border border-border-default text-text-secondary hover:bg-surface-hover cursor-pointer">
            Compare snapshots
          </button>
        )}
        {onViewAllSnapshots && (
          <button onClick={onViewAllSnapshots}
            className="text-xs px-3 py-1.5 rounded bg-surface-secondary border border-border-default text-text-secondary hover:bg-surface-hover cursor-pointer">
            All snapshots
          </button>
        )}
        {onEditPolicy && (
          <button onClick={onEditPolicy}
            className="text-xs px-3 py-1.5 rounded bg-surface-secondary border border-border-default text-text-secondary hover:bg-surface-hover cursor-pointer">
            Policy
          </button>
        )}
        {onViewTags && (
          <button onClick={onViewTags}
            className="text-xs px-3 py-1.5 rounded bg-surface-secondary border border-border-default text-text-secondary hover:bg-surface-hover cursor-pointer">
            Tags
          </button>
        )}
        {onViewJournal && (
          <button onClick={onViewJournal}
            className="text-xs px-3 py-1.5 rounded bg-surface-secondary border border-border-default text-text-secondary hover:bg-surface-hover cursor-pointer">
            Journal
          </button>
        )}
      </div>

      {/* Row 2: GFS Retention */}
      {snaps.length > 0 && (
        <div className="bg-surface-secondary border border-border-default rounded-lg p-4 mb-4">
          <div className="text-[11px] text-text-muted uppercase tracking-wide mb-2">Retention Coverage</div>
          <div className="flex gap-4">
            {([
              { label: 'Yearly', count: gfsCounts.yearly, color: 'bg-amber-600' },
              { label: 'Monthly', count: gfsCounts.monthly, color: 'bg-blue-600' },
              { label: 'Weekly', count: gfsCounts.weekly, color: 'bg-green-600' },
              { label: 'Daily', count: gfsCounts.daily, color: 'bg-teal-600' },
              { label: 'Untagged', count: gfsCounts.untagged, color: 'bg-gray-500' },
            ] as const).map(tier => (
              <div key={tier.label} className="flex items-center gap-2">
                <span className={`inline-block w-2.5 h-2.5 rounded-full ${tier.color}`} />
                <span className="text-xs text-text-primary">{tier.label}</span>
                <span className={`text-xs font-semibold ${tier.count === 0 && tier.label !== 'Untagged' ? 'text-status-error' : 'text-text-primary'}`}>
                  {tier.count}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Row 3: Storage breakdown */}
      {repoStats && (
        <div className="grid grid-cols-2 gap-3 mb-4">
          {/* Compression */}
          <div className="bg-surface-secondary border border-border-default rounded-lg p-4">
            <div className="text-[11px] text-text-muted uppercase tracking-wide mb-2">Compression</div>
            <div className="flex justify-between text-sm mb-2">
              <span className="text-text-secondary">{fmtSize(totalUncomp)} uncompressed</span>
              <span className="text-text-primary font-semibold">{fmtSize(totalComp)} on disk</span>
            </div>
            <div className="flex h-2 rounded-full overflow-hidden bg-surface-tertiary mb-2">
              <div className="bg-accent" style={{ width: `${Math.round(compressionRatio * 100)}%` }} />
            </div>
            <div className="flex justify-between text-[10px] text-text-muted">
              <span>{(compressionRatio * 100).toFixed(1)}% ratio</span>
              <span>{fmtSize(spaceSaved)} saved</span>
            </div>
            {repoStats.skip.count > 0 && (
              <div className="text-[10px] text-text-muted mt-1">
                {fmtNum(repoStats.skip.count)} incompressible objects ({fmtSize(repoStats.skip.uncomp)})
              </div>
            )}
          </div>

          {/* Object types */}
          <div className="bg-surface-secondary border border-border-default rounded-lg p-4">
            <div className="text-[11px] text-text-muted uppercase tracking-wide mb-2">Object Types</div>
            <table className="w-full text-xs">
              <thead>
                <tr className="text-text-muted text-left">
                  <th className="font-medium pb-1">Type</th>
                  <th className="font-medium pb-1 text-right">Count</th>
                  <th className="font-medium pb-1 text-right">Size</th>
                  <th className="font-medium pb-1 text-right">Compressed</th>
                </tr>
              </thead>
              <tbody>
                {repoStats.per_type.filter(t => t.count > 0).map(t => (
                  <tr key={t.type} className="text-text-primary">
                    <td className="py-0.5">{OBJECT_TYPE_NAMES[t.type] ?? `type-${t.type}`}</td>
                    <td className="py-0.5 text-right">{fmtNum(t.count)}</td>
                    <td className="py-0.5 text-right">{fmtSize(t.uncomp)}</td>
                    <td className="py-0.5 text-right">{fmtSize(t.comp)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Row 4: Recent snapshots */}
      {recentSnaps.length > 0 && (
        <div className="bg-surface-secondary border border-border-default rounded-lg p-4">
          <div className="flex items-center justify-between mb-2">
            <div className="text-[11px] text-text-muted uppercase tracking-wide">Recent Snapshots</div>
            <div className="flex gap-3">
              {onCompareSnapshots && snaps.length >= 2 && (
                <button onClick={() => onCompareSnapshots()}
                  className="text-[11px] text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
                  Compare snapshots
                </button>
              )}
              {onViewAllSnapshots && snaps.length > 10 && (
                <button onClick={onViewAllSnapshots}
                  className="text-[11px] text-accent hover:text-accent-hover bg-transparent border-none cursor-pointer">
                  View all {fmtNum(snaps.length)} snapshots
                </button>
              )}
            </div>
          </div>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-text-muted text-left">
                <th className="font-medium pb-1 w-16">ID</th>
                <th className="font-medium pb-1">Date</th>
                <th className="font-medium pb-1 text-right">Nodes</th>
                <th className="font-medium pb-1 text-right">New Data</th>
                <th className="font-medium pb-1 text-center">Retention</th>
              </tr>
            </thead>
            <tbody>
              {recentSnaps.map(s => {
                const badges = gfsBadges(s.gfs_flags)
                return (
                  <tr key={s.id} onClick={() => onSelectSnapshot?.(s.id)}
                    className={`text-text-primary border-t border-border-default ${onSelectSnapshot ? 'cursor-pointer hover:bg-surface-hover' : ''}`}>
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
        </div>
      )}
    </div>
  )
}
