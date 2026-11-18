/*
 * What the unit screens decide, kept apart from React.
 *
 * A unit is a VM or a container backed up through a source provider
 * (Manual 22). Two routes describe one: the provider's live enumeration
 * says what exists now and in what state, and the repository's unit list
 * says what has history here. The screens show both at once, so the merge
 * lives here where it can be tested, along with the words for levels and
 * item kinds, and the journal filter the unit page uses.
 */

import type { PauseScope, UnitLevel } from './api'
import type { WireJournalEntry, WireLiveUnit, WireRepoUnit } from './wire'

/* ---------------------------------------------------------------- */
/* Levels and scopes                                                 */
/* ---------------------------------------------------------------- */

/* The daemon's names, lowest first, as `require` takes them. */
export const UNIT_LEVELS: UnitLevel[] = ['inconsistent', 'crash', 'quiesced', 'application', 'offline']

/* The level a backup asks for when nothing is chosen (api.md, units/backup). */
export const DEFAULT_LEVEL: UnitLevel = 'crash'

export const PAUSE_SCOPES: PauseScope[] = ['unit', 'group', 'job']

/* One line each, from Manual 22.1 and 22.6.1, for the selects' titles. */
export const LEVEL_WORDS: Record<UnitLevel, string> = {
  inconsistent: 'read while running, with nothing to make it consistent',
  crash: 'as if the power had been cut at one instant',
  quiesced: 'filesystems frozen by the guest agent or a pause',
  application: 'the application itself flushed (a hook or dump)',
  offline: 'read while the unit was stopped',
}

export const SCOPE_WORDS: Record<PauseScope, string> = {
  unit: 'each unit its own capture',
  group: 'units sharing a capture.group captured together',
  job: 'every unit of the job in one capture',
}

/* ---------------------------------------------------------------- */
/* Items                                                             */
/* ---------------------------------------------------------------- */

/* A block item is a disk: the only kind an NBD export serves and the one
 * a VM restores as a sparse raw image. */
export function isDisk(kind: string): boolean {
  return kind === 'block'
}

export function itemKindLabel(kind: string): string {
  switch (kind) {
    case 'block': return 'disk'
    case 'tree': return 'data'
    case 'blob': return 'config'
    case 'stream': return 'stream'
    default: return kind
  }
}

/* ---------------------------------------------------------------- */
/* The list: live units and units with history, merged               */
/* ---------------------------------------------------------------- */

export interface UnitRow {
  key: string
  provider: string
  unitId: string
  name: string
  /* The provider's word for it now, or null when the provider does not
   * list it: removed, renamed, or outside units.allow. */
  state: string | null
  history: WireRepoUnit | null
}

/*
 * One row per unit key, live units first in the provider's order (by
 * name), then units with history here that the provider no longer lists,
 * by name. A live unit is matched to its history by key, which is what
 * snapshots carry; the name can change under a stable id. withGone false
 * leaves the gone ones out, for a page of the live list past the first,
 * which would otherwise repeat them.
 */
export function mergeUnits(provider: string, live: WireLiveUnit[], history: WireRepoUnit[],
                           withGone = true): UnitRow[] {
  const mine = history.filter(h => h.provider === provider)
  const byKey = new Map(mine.map(h => [h.unit_key, h]))
  const rows: UnitRow[] = live.map(u => ({
    key: u.unit_key, provider, unitId: u.unit_id, name: u.name, state: u.state,
    history: byKey.get(u.unit_key) ?? null,
  }))
  if (!withGone) return rows
  const seen = new Set(live.map(u => u.unit_key))
  const gone = mine.filter(h => !seen.has(h.unit_key))
    .sort((a, b) => a.name.localeCompare(b.name))
  for (const h of gone) {
    rows.push({ key: h.unit_key, provider, unitId: h.unit_id, name: h.name, state: null, history: h })
  }
  return rows
}

/* ---------------------------------------------------------------- */
/* Backup options                                                    */
/* ---------------------------------------------------------------- */

/*
 * Exclude globs as typed: one per line or comma separated, as many as the
 * CLI's repeatable --exclude. Blank entries are dropped, because the daemon
 * refuses an empty glob.
 */
export function parseGlobs(text: string): string[] {
  return text.split(/[\n,]/).map(s => s.trim()).filter(s => s.length > 0)
}

/* ---------------------------------------------------------------- */
/* Journal                                                           */
/* ---------------------------------------------------------------- */

/*
 * The journal operations a unit job writes: a backup (units), a restore
 * (unit-restore), the helper's warnings from a define (unit-define), an
 * NBD export opened or closed, an incrementals reset, and detection.
 */
export const UNIT_OPERATIONS = new Set([
  'units', 'unit-restore', 'unit-define', 'nbd-export', 'reset-incremental', 'detect',
])

export interface UnitJournalEntry extends WireJournalEntry {
  /* The message names this unit. */
  names: boolean
}

/*
 * The repository journal's unit entries, each marked when its message
 * names the unit. The journal records no unit key, so this cannot be
 * exact: a backup of several units says how many, not which, and a
 * restore's entry often has no message at all. An entry also counts as
 * naming the unit when it names one of the unit's snapshots. The screen
 * says all this rather than presenting a filtered list as complete.
 */
export function unitJournal(entries: WireJournalEntry[], name: string, key: string,
                            snapshots: ReadonlySet<number> = new Set()): UnitJournalEntry[] {
  return entries
    .filter(e => UNIT_OPERATIONS.has(e.operation))
    .map(e => {
      const m = e.message ?? ''
      /* An export's entry names its snapshot rather than the unit. */
      const snap = /\bsnapshot (\d+)\b/.exec(m)
      return { ...e, names: mentions(m, name) || mentions(m, key) ||
                            (snap !== null && snapshots.has(Number(snap[1]))) }
    })
}

/* word appears in text as a whole name: cbk-uat-vm is not named by a
 * message about cbk-uat-vm2. Unit names are made of letters, digits and
 * . _ -, so those are what a name cannot continue with; a colon can, since
 * the journal writes "unit <name>: ...". */
export function mentions(text: string, word: string): boolean {
  if (!word) return false
  const part = /[A-Za-z0-9._-]/
  for (let at = text.indexOf(word); at >= 0; at = text.indexOf(word, at + 1)) {
    const before = at > 0 ? text[at - 1] : ''
    const after = text[at + word.length] ?? ''
    if (!part.test(before) && !part.test(after)) return true
  }
  return false
}
