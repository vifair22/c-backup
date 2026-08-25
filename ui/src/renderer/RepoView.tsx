import React, { useEffect, useState } from 'react'
import type { MaintenanceOp } from '../shared/api'
import { StatCard } from './StatCard'
import { ContextMenu } from './ContextMenu'
import {
  fmtSize, fmtNum, relativeTime, absoluteTime, gfsBadges,
  GFS_DAILY, GFS_WEEKLY, GFS_MONTHLY, GFS_YEARLY,
} from './format'

const api = window.cbackup

/* ------------------------------------------------------------------ */
/* Types for RPC responses                                             */
/* ------------------------------------------------------------------ */

/*
 * A snapshot as the list route reports it. The old shape had node counts,
 * byte totals and GFS flags per row; this one has what the daemon actually
 * sends, and every value is a string for the same reason the sizes are.
 */
interface Snapshot {
  id: string
  ts: string
  label?: string
  files?: string
  materialized?: string
}

interface SnapList {
  head: number
  snapshots: Snapshot[]
}

/*
 * The shapes the daemon returns, not the ones this screen was written
 * against.
 *
 * Every number arrives as a string: the daemon renders integers as text so a
 * 64-bit count cannot be mangled by a JSON number, and reading them as
 * numbers produced NaN throughout. The old shapes - pack/loose/skip/hiratio
 * buckets with comp and uncomp - belonged to the one-shot CLI and have no
 * successor, so the panels that needed them are gone rather than filled with
 * plausible arithmetic.
 */
interface TypeStat {
  type: string
  count: string
  phys_size: string
  orig_size: string
}

interface RepoStats {
  per_type: TypeStat[]
}

interface Summary {
  snapshots: number
  objects: number
  phys_size: number
  orig_size: number
  packs: number
  loose: number
  tags: number
  last_snapshot_ts: number
}

/* A backup from today is fine, from last week is not, and the gap between
 * says how worried to look. */
function lastBackupStatus(epochSec: number): 'ok' | 'warn' | 'error' {
  const ageSec = Date.now() / 1000 - epochSec
  if (ageSec < 86400) return 'ok'
  if (ageSec < 604800) return 'warn'
  return 'error'
}

interface Props {
  /* What to call this repository on screen. repoPath is the repo id the
   * daemon addresses it by, which is not a name and should never have been
   * the heading. */
  title?: string
  connName: string
  repoPath: string
  onSelectSnapshot?: (snapId: number) => void
  onViewAllSnapshots?: () => void
  onCompareSnapshots?: (a?: number, b?: number) => void
  onSearch?: () => void
  onEditPolicy?: () => void
  onViewJournal?: () => void
  onViewTags?: () => void
  onViewSchedules?: () => void
  onViewVerify?: () => void
  onViewEncryption?: () => void
  onViewGrep?: () => void
  onRunBackup?: () => void
  onRunOperation?: (command: MaintenanceOp) => void
  onViewTasks?: () => void
  onHashLookup?: () => void
  onLooseObjects?: () => void
  onPackBrowser?: () => void
}

export function RepoView({ connName, repoPath, title, onSelectSnapshot, onViewAllSnapshots, onCompareSnapshots, onSearch, onEditPolicy, onViewJournal, onViewTags, onViewSchedules, onViewVerify, onViewEncryption, onViewGrep, onRunBackup, onRunOperation, onViewTasks, onHashLookup, onLooseObjects, onPackBrowser }: Props): React.ReactElement {
  const [summary, setSummary] = useState<Summary | null>(null)
  const [opsMenu, setOpsMenu] = useState<{ x: number; y: number } | null>(null)
  const [snapList, setSnapList] = useState<SnapList | null>(null)
  const [repoStats, setRepoStats] = useState<RepoStats | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    setSummary(null)
    setSnapList(null)
    setRepoStats(null)

    const repoId = Number(repoPath)
    Promise.all([
      api.apiCall<Summary>(connName, { kind: 'summary', repoId }),
      api.rpcCall<SnapList>(connName, repoPath, 'list'),
      api.apiCall<RepoStats>(connName, { kind: 'stats', repoId }),
    ]).then(([s, l, r]) => {
      if (cancelled) return
      setSummary(s)
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
        <h3 className="text-lg font-medium m-0 mb-1">{title ?? repoPath}</h3>
        <div className="text-text-muted text-xs mb-4">via {connName}</div>
        <div className="text-text-muted">Loading dashboard...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="p-4">
        <h3 className="text-lg font-medium m-0 mb-1">{title ?? repoPath}</h3>
        <div className="text-text-muted text-xs mb-4">via {connName}</div>
        <div className="text-status-error text-sm">{error}</div>
      </div>
    )
  }

  const snaps = snapList?.snapshots ?? []
  /* Newest first, so the head of the list is the latest backup. */
  const latest = snaps.length > 0 ? snaps[0] : null
  const recentSnaps = snaps.slice(0, 10)

  /*
   * Retention tiers are not on the snapshot list route, so there is nothing
   * to count. They live in snapshots.gfs_flags and the daemon does not send
   * them here; showing zeroes would be a panel that says retention is doing
   * nothing, which is worse than a panel that is absent.
   */

  /* Compression totals, from the sizes the daemon reports as strings. */
  const totalUncomp = repoStats
    ? repoStats.per_type.reduce((a, t) => a + Number(t.orig_size ?? 0), 0)
    : 0
  const totalComp = repoStats
    ? repoStats.per_type.reduce((a, t) => a + Number(t.phys_size ?? 0), 0)
    : 0
  const compressionRatio = totalUncomp > 0 ? totalComp / totalUncomp : 1
  const spaceSaved = totalUncomp - totalComp

  // Storage bar percentages
  const looseBytes = 0 /* the daemon reports loose as a count, not bytes */
  const packBytes = summary?.phys_size ?? 0
  const totalBytes = summary?.phys_size ?? 1
  const loosePct = Math.round((looseBytes / totalBytes) * 100)
  const packPct = 100 - loosePct

  const QBtn = ({ onClick, children }: { onClick: () => void; children: React.ReactNode }) => (
    <button onClick={onClick}
      className="px-2.5 py-1 rounded bg-surface-secondary border border-border-default text-text-secondary hover:bg-surface-hover cursor-pointer">
      {children}
    </button>
  )

  return (
    <div>
      <h3 className="text-lg font-medium m-0 mb-1">{title ?? repoPath}</h3>
      <div className="text-text-muted text-xs mb-4">via {connName}</div>

      {/* Row 1: Summary cards */}
      <div className="grid grid-cols-4 gap-3 mb-4">
        <StatCard
          label="Last Backup"
          value={latest ? relativeTime(latest.ts) : 'Never'}
          detail={latest ? absoluteTime(latest.ts) : undefined}
          status={latest ? lastBackupStatus(Number(latest.ts)) : 'error'}
        />
        <div onClick={() => onViewAllSnapshots?.()} className={`${onViewAllSnapshots ? 'cursor-pointer' : ''} h-full`}>
          <StatCard
            label="Snapshots"
            value={fmtNum(summary?.snapshots ?? 0)}
            detail={snapList?.head ? `HEAD: #${snapList.head}` : undefined}
          />
        </div>
        <StatCard
          label="Storage"
          value={fmtSize(summary?.phys_size ?? 0)}
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
          value={fmtNum(summary?.objects ?? 0)}
          detail={summary?.orig_size ? `${fmtSize(summary.orig_size)} logical` : undefined}
        />
      </div>

      {/* Quick actions */}
      <div className="flex flex-wrap items-center gap-2 mb-4 text-xs">
        {/* Browse */}
        {onSearch && <QBtn onClick={onSearch}>Search</QBtn>}
        {onViewGrep && <QBtn onClick={onViewGrep}>Find in files</QBtn>}
        {onViewAllSnapshots && <QBtn onClick={onViewAllSnapshots}>Snapshots</QBtn>}
        {onCompareSnapshots && snaps.length >= 2 && <QBtn onClick={() => onCompareSnapshots()}>Compare</QBtn>}

        <div className="border-l border-border-default h-4 mx-0.5" />

        {/* Config */}
        {onEditPolicy && <QBtn onClick={onEditPolicy}>Policy</QBtn>}
        {onViewTags && <QBtn onClick={onViewTags}>Tags</QBtn>}
        {onViewSchedules && <QBtn onClick={onViewSchedules}>Schedules</QBtn>}
        {onViewVerify && <QBtn onClick={onViewVerify}>Integrity</QBtn>}
        {onViewEncryption && <QBtn onClick={onViewEncryption}>Encryption</QBtn>}

        <div className="border-l border-border-default h-4 mx-0.5" />

        {/* Debug */}
        {onLooseObjects && <QBtn onClick={onLooseObjects}>Loose</QBtn>}
        {onPackBrowser && <QBtn onClick={onPackBrowser}>Packs</QBtn>}
        {onHashLookup && <QBtn onClick={onHashLookup}>Lookup</QBtn>}
        {onViewJournal && <QBtn onClick={onViewJournal}>Journal</QBtn>}
        {onViewTasks && <QBtn onClick={onViewTasks}>Tasks</QBtn>}

        <div className="border-l border-border-default h-4 mx-0.5" />

        {/* Operations */}
        {onRunBackup && (
          <button onClick={onRunBackup}
            className="px-3 py-1 rounded bg-accent text-accent-text hover:bg-accent-hover cursor-pointer border-none">
            Run Backup
          </button>
        )}
        {onRunOperation && (
          <button onClick={(e) => {
            const rect = e.currentTarget.getBoundingClientRect()
            setOpsMenu({ x: rect.left, y: rect.bottom + 4 })
          }}
            className="px-3 py-1 rounded bg-surface-secondary border border-border-default text-text-secondary hover:bg-surface-hover cursor-pointer">
            Maintenance
          </button>
        )}
      </div>

      {/*
          The retention panel lived here. It counted daily / weekly / monthly
          / yearly snapshots from gfs_flags, which the snapshot list route
          does not send - so it can be rebuilt when there is an endpoint that
          reports tiers, rather than rendered as five zeroes that say
          retention is doing nothing.
      */}

      {/* Row 3: what the repository is made of */}
      {repoStats && summary && (() => {
        /*
         * Built from what the daemon reports. The panel here used to show
         * compressible / skipped / high-ratio object counts, which came from
         * a stats shape the one-shot CLI produced and this daemon does not:
         * there is no bucket breakdown to read, so the panel is the parts
         * that are real rather than the old layout with invented numbers.
         */
        const num = (v: string | number | undefined) => Number(v ?? 0)
        const packedCount = Math.max(0, num(summary.objects) - num(summary.loose))
        const looseCount = num(summary.loose)
        const totalObjCount = packedCount + looseCount
        const typeBytes = repoStats.per_type.reduce((a, t) => a + num(t.orig_size), 0)
        const typeColors: Record<string, string> = {
          '1': 'bg-blue-500', '2': 'bg-orange-500', '3': 'bg-green-500',
          '4': 'bg-red-500', '5': 'bg-purple-500',
        }
        const typeNames: Record<string, string> = {
          '1': 'File', '2': 'Symlink', '3': 'Xattr', '4': 'Sparse', '5': 'Tree',
        }
        const pct = (n: number, total: number) => total > 0 ? Math.round((n / total) * 100) : 0

        return (
        <div className="space-y-3 mb-4">
          <div className="grid grid-cols-2 gap-3">
            <div className="bg-surface-secondary rounded-lg p-3">
              <div className="text-xs text-text-secondary mb-2">Storage</div>
              <div className="flex items-baseline gap-2">
                <span className="text-lg font-semibold text-text-primary">
                  {fmtSize(num(summary.phys_size))}
                </span>
                <span className="text-xs text-text-muted">
                  from {fmtSize(num(summary.orig_size))} of files
                </span>
              </div>
              {num(summary.orig_size) > 0 && (
                <div className="text-xs text-text-muted mt-1">
                  {pct(num(summary.orig_size) - num(summary.phys_size), num(summary.orig_size))}% saved
                  by compression and dedup
                </div>
              )}
            </div>

            <div className="bg-surface-secondary rounded-lg p-3">
              <div className="text-xs text-text-secondary mb-2">Objects</div>
              <div className="h-2 rounded-full bg-surface-tertiary overflow-hidden flex mb-2">
                {packedCount > 0 && (
                  <div className="bg-accent" style={{ width: `${pct(packedCount, totalObjCount)}%` }} />
                )}
                {looseCount > 0 && (
                  <div className="bg-status-warning" style={{ width: `${pct(looseCount, totalObjCount)}%` }} />
                )}
              </div>
              <div className="flex gap-3 text-xs text-text-muted">
                <span>
                  <span className="inline-block w-2 h-2 rounded-full bg-accent mr-1" />
                  Packed {fmtNum(packedCount)}
                </span>
                <span>
                  <span className="inline-block w-2 h-2 rounded-full bg-status-warning mr-1" />
                  Loose {fmtNum(looseCount)}
                </span>
                <span className="ml-auto">{fmtNum(num(summary.packs))} pack files</span>
              </div>
            </div>
          </div>

          {repoStats.per_type.length > 0 && (
            <div className="bg-surface-secondary rounded-lg p-3">
              <div className="text-xs text-text-secondary mb-2">By object type</div>
              <div className="h-2 rounded-full bg-surface-tertiary overflow-hidden flex mb-2">
                {repoStats.per_type.filter(t => num(t.orig_size) > 0).map(t => (
                  <div key={t.type} className={typeColors[t.type] ?? 'bg-surface-tertiary'}
                    style={{ width: `${pct(num(t.orig_size), typeBytes)}%` }} />
                ))}
              </div>
              <div className="flex flex-wrap gap-3 text-xs text-text-muted">
                {repoStats.per_type.filter(t => num(t.count) > 0).map(t => (
                  <span key={t.type}>
                    <span className={`inline-block w-2 h-2 rounded-full mr-1 ${typeColors[t.type] ?? 'bg-surface-tertiary'}`} />
                    {typeNames[t.type] ?? `Type ${t.type}`} {fmtNum(num(t.count))}
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>
        )
      })()}

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
                <th className="font-medium pb-1 text-right pr-6">Files</th>
                <th className="font-medium pb-1">Label</th>
              </tr>
            </thead>
            <tbody>
              {recentSnaps.map(s => {
                return (
                  <tr key={s.id} onClick={() => onSelectSnapshot?.(Number(s.id))}
                    className={`text-text-primary border-t border-border-default ${onSelectSnapshot ? 'cursor-pointer hover:bg-surface-hover' : ''}`}>
                    <td className="py-1.5 font-mono">#{s.id}</td>
                    <td className="py-1.5">{absoluteTime(s.ts)}</td>
                    <td className="py-1.5 text-right pr-6">{fmtNum(s.files)}</td>
                    <td className="py-1.5">{s.label || ''}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
      {/* Operations menu */}
      {opsMenu && onRunOperation && (
        <ContextMenu x={opsMenu.x} y={opsMenu.y} onClose={() => setOpsMenu(null)}
          items={[
            { label: 'Verify', onClick: () => onRunOperation('verify') },
            { label: 'Pack', onClick: () => onRunOperation('pack') },
            { label: 'Garbage Collect', onClick: () => onRunOperation('gc') },
            { label: 'Apply Retention', onClick: () => onRunOperation('gfs') },
            { label: 'Reindex', onClick: () => onRunOperation('reindex') },
          ]}
        />
      )}
    </div>
  )
}
