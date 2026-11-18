/*
 * What a finished job still needs an operator to hear (B241).
 *
 * A job that succeeded with something to say - files left out, a live
 * database captured uncovered, a source read live in warn mode - stays
 * `completed`, and the daemon puts the warnings in its result: `warnings`,
 * each one as it was raised, and `warning`, the same joined into the one
 * line the journal records. The task list showed neither, so such a job
 * read as a plain "Completed".
 */

import type { Job, JobState } from './api.ts'

/* The job's warnings, each one; [] when it has none. The joined line is
 * the fallback for a result written without the list. */
export function jobWarnings(t: Pick<Job, 'result'>): string[] {
  const r = (t.result ?? {}) as Record<string, unknown>
  if (Array.isArray(r.warnings)) {
    const list = r.warnings.filter((w): w is string => typeof w === 'string' && w.length > 0)
    if (list.length > 0) return list
  }
  return typeof r.warning === 'string' && r.warning.length > 0 ? [r.warning] : []
}

/* The state a card shows: a completed job with warnings is its own state,
 * "Completed with warnings". */
export type JobDisplayState = JobState | 'warning'

export function jobDisplayState(t: Pick<Job, 'state' | 'result'>): JobDisplayState {
  return t.state === 'completed' && jobWarnings(t).length > 0 ? 'warning' : t.state
}
