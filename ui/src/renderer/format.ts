/* Shared formatting helpers */

export const GFS_DAILY = 1
export const GFS_WEEKLY = 2
export const GFS_MONTHLY = 4
export const GFS_YEARLY = 8

export function fmtSize(bytes: number | string | undefined | null): string {
  const n = Number(bytes ?? NaN)
  if (!Number.isFinite(n)) return '-'
  if (n === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
  const i = Math.min(Math.floor(Math.log(n) / Math.log(1024)), units.length - 1)
  const val = n / Math.pow(1024, i)
  return `${val < 10 ? val.toFixed(1) : Math.round(val)} ${units[i]}`
}

/*
 * Tolerant on purpose.
 *
 * These run inside render, so throwing does not produce a bad-looking cell -
 * it unmounts the tree and leaves a white window. fmtNum(undefined) blanked
 * the whole dashboard when one field of one row turned out not to exist.
 * A dash is a worse number and a much better failure.
 */
export function fmtNum(n: number | string | undefined | null): string {
  const v = Number(n ?? NaN)
  return Number.isFinite(v) ? v.toLocaleString() : '-'
}

export function relativeTime(epochSec: number | string | undefined | null): string {
  const t = Number(epochSec ?? NaN)
  if (!Number.isFinite(t) || t <= 0) return 'never'
  const now = Date.now() / 1000
  const diff = now - t
  if (diff < 60) return 'just now'
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  if (diff < 604800) return `${Math.floor(diff / 86400)}d ago`
  return `${Math.floor(diff / 604800)}w ago`
}

export function absoluteTime(epochSec: number | string | undefined | null): string {
  const t = Number(epochSec ?? NaN)
  if (!Number.isFinite(t) || t <= 0) return '-'
  return new Date(t * 1000).toLocaleString()
}

export function gfsBadges(flags: number): { label: string; color: string }[] {
  const badges: { label: string; color: string }[] = []
  if (flags & GFS_YEARLY) badges.push({ label: 'Y', color: 'bg-amber-600 text-white' })
  if (flags & GFS_MONTHLY) badges.push({ label: 'M', color: 'bg-blue-600 text-white' })
  if (flags & GFS_WEEKLY) badges.push({ label: 'W', color: 'bg-green-600 text-white' })
  if (flags & GFS_DAILY) badges.push({ label: 'D', color: 'bg-teal-600 text-white' })
  return badges
}

export function fmtMode(mode: number): string {
  return '0' + (mode & 0o7777).toString(8)
}

/*
 * Object types, from src/common/types.h.
 *
 * Three components each kept their own copy of this and all three stopped at
 * 4, so every manifest tree object - which is most of what a repository
 * holds - displayed as "Unknown (5)". One list, in the same file as the node
 * types it sits next to.
 */
export const OBJECT_TYPE_NAMES: Record<number, string> = {
  1: 'file',
  2: 'xattr',
  3: 'acl',
  4: 'sparse',
  5: 'tree',
}

export const NODE_TYPE_NAMES: Record<number, string> = {
  1: 'file',
  2: 'dir',
  3: 'symlink',
  4: 'hardlink',
  5: 'fifo',
  6: 'chardev',
  7: 'blkdev',
}
