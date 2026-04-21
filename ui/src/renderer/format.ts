/* Shared formatting helpers */

export const GFS_DAILY = 1
export const GFS_WEEKLY = 2
export const GFS_MONTHLY = 4
export const GFS_YEARLY = 8

export function fmtSize(bytes: number): string {
  if (bytes === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)
  const val = bytes / Math.pow(1024, i)
  return `${val < 10 ? val.toFixed(1) : Math.round(val)} ${units[i]}`
}

export function fmtNum(n: number): string {
  return n.toLocaleString()
}

export function relativeTime(epochSec: number): string {
  const now = Date.now() / 1000
  const diff = now - epochSec
  if (diff < 60) return 'just now'
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  if (diff < 604800) return `${Math.floor(diff / 86400)}d ago`
  return `${Math.floor(diff / 604800)}w ago`
}

export function absoluteTime(epochSec: number): string {
  return new Date(epochSec * 1000).toLocaleString()
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

export const NODE_TYPE_NAMES: Record<number, string> = {
  1: 'file',
  2: 'dir',
  3: 'symlink',
  4: 'hardlink',
  5: 'fifo',
  6: 'chardev',
  7: 'blkdev',
}
