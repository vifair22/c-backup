/*
 * Follow one job to its end.
 *
 * The unit screens start jobs whose outcome is the point: a unit backup's
 * result lists each unit, and a refused restore's error is the daemon's
 * reason, which the operator has to read where they pressed the button
 * rather than in the task list. The job id is the only thing that names
 * this run's answer, so it is polled until the job leaves queued and
 * running, and the last state is returned whole.
 */

import type { Job } from '../shared/api'

const api = window.cbackup

export async function followJob(
  connName: string,
  jobId: number,
  onProgress?: (job: Job) => void,
  stopped?: () => boolean,
  intervalMs = 400,
): Promise<Job> {
  for (;;) {
    const job = await api.apiCall<Job>(connName, { kind: 'jobGet', jobId })
    if (job.state !== 'queued' && job.state !== 'running') return job
    onProgress?.(job)
    if (stopped?.()) return job
    await new Promise(r => setTimeout(r, intervalMs))
  }
}

/* The job id out of a 202, or an error saying the daemon sent none. */
export function jobIdOf(r: { job_id?: number } | null | undefined): number {
  const id = r?.job_id
  if (!id) throw new Error('the daemon did not return a job id')
  return id
}
