/*
 * What the last scrubs found.
 *
 * Verify has recorded its runs and its findings since it was written, and
 * neither was reachable from the app: the counters showed up in /metrics and
 * the findings nowhere at all. A scrub whose answer you cannot read is a
 * scrub you did not run, and putting Verify in the maintenance menu without
 * this would have made that worse rather than better - a button that starts
 * an integrity check and then has nowhere to show the result.
 *
 * The screen is built around one question: is anything wrong, and if so
 * what. A clean run is a single line, because a clean run is not news.
 */

import React, { useCallback, useEffect, useState } from 'react'
import { absoluteTime } from './format'

const api = window.cbackup

/* All numeric fields arrive as strings: the daemon renders integers as text
 * so that a 64-bit count cannot be mangled by a JSON number. */
interface VerifyRun {
  id: string
  started_at: string
  finished_at: string
  level: string
  completed: string
  checked: string
  ok: string
  repaired: string
  unrecoverable: string
  missing: string
  bytes_read: string
  affected_files: string
  affected_snapshots: string
}

interface Finding {
  hash?: string
  kind?: string
  detail?: string
  path?: string
}

interface Props {
  connName: string
  repoPath: string
  onBack: () => void
}

const n = (s?: string): number => Number(s ?? '0')

export function VerifyView({ connName, repoPath, onBack }: Props): React.ReactElement {
  const repoId = Number(repoPath)

  const [runs, setRuns] = useState<VerifyRun[]>([])
  const [selected, setSelected] = useState<VerifyRun | null>(null)
  const [findings, setFindings] = useState<Finding[]>([])
  const [loading, setLoading] = useState(true)
  const [loadingFindings, setLoadingFindings] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const r = await api.apiCall<{ runs?: VerifyRun[] }>(connName, {
        kind: 'verifyRuns', repoId, limit: 25,
      })
      const list = r.runs ?? []
      setRuns(list)
      /* Open on the newest run rather than an empty pane: the reason to be
       * here is almost always the scrub that just finished. */
      if (list.length > 0) void openRun(list[0])
    } catch (err) {
      setError(String(err))
    }
    setLoading(false)
  }, [connName, repoId])

  const openRun = async (run: VerifyRun) => {
    setSelected(run)
    setFindings([])
    /* A run with nothing wrong has nothing to fetch. Asking anyway would be
     * a round trip to be told what the counters already said. */
    if (n(run.unrecoverable) === 0 && n(run.missing) === 0 && n(run.repaired) === 0) return

    setLoadingFindings(true)
    try {
      const r = await api.apiCall<{ findings?: Finding[] }>(connName, {
        kind: 'verifyFindings', repoId, runId: Number(run.id),
      })
      setFindings(r.findings ?? [])
    } catch (err) {
      setError(String(err))
    }
    setLoadingFindings(false)
  }

  useEffect(() => { void load() }, [load])

  /*
   * What a run means, in one word.
   *
   * Repaired is deliberately not "ok". Reed-Solomon corrected something,
   * which means a disk is returning bytes it was not given - the data
   * survived and the medium is telling you something.
   */
  const verdict = (r: VerifyRun): { label: string; cls: string } => {
    if (n(r.completed) === 0) return { label: 'incomplete', cls: 'text-text-tertiary' }
    if (n(r.unrecoverable) > 0 || n(r.missing) > 0) return { label: 'damaged', cls: 'text-danger' }
    if (n(r.repaired) > 0) return { label: 'repaired', cls: 'text-warning' }
    return { label: 'clean', cls: 'text-success' }
  }

  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="flex items-center gap-2 mb-3">
        <button onClick={onBack}
          className="px-2 py-1 text-xs cursor-pointer rounded bg-surface-tertiary
                     text-text-secondary hover:bg-surface-hover border-none">
          Back
        </button>
        <h2 className="text-sm font-semibold m-0">Integrity checks</h2>
      </div>

      {error && (
        <div className="text-xs mb-3 p-2 rounded bg-danger-subtle text-danger border border-danger">
          {error}
        </div>
      )}

      {loading ? (
        <div className="text-xs text-text-tertiary">Loading...</div>
      ) : runs.length === 0 ? (
        <div className="text-xs text-text-tertiary">
          No scrub has run yet. Verify from the operations menu to prove the
          stored objects still read back.
        </div>
      ) : (
        <div className="flex gap-4 overflow-hidden flex-1">
          <div className="overflow-auto shrink-0" style={{ maxWidth: '22rem' }}>
            <table className="text-xs border-collapse">
              <thead>
                <tr className="text-text-secondary text-left">
                  <th className="py-1 pr-3 font-medium">When</th>
                  <th className="py-1 pr-3 font-medium">Level</th>
                  <th className="py-1 font-medium">Result</th>
                </tr>
              </thead>
              <tbody>
                {runs.map(r => {
                  const v = verdict(r)
                  const active = selected?.id === r.id
                  return (
                    <tr key={r.id}
                      onClick={() => void openRun(r)}
                      className={`border-t border-border-subtle cursor-pointer
                                  ${active ? 'bg-surface-secondary' : 'hover:bg-surface-hover'}`}>
                      <td className="py-1.5 pr-3 whitespace-nowrap">
                        {absoluteTime(Number(r.started_at))}
                      </td>
                      <td className="py-1.5 pr-3">{r.level}</td>
                      <td className={`py-1.5 ${v.cls}`}>{v.label}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>

          {selected && (
            <div className="flex-1 overflow-auto border-l border-border-subtle pl-4">
              <div className="grid grid-cols-2 gap-x-6 gap-y-1 text-xs mb-4"
                   style={{ maxWidth: '28rem' }}>
                <Stat label="Objects checked" value={n(selected.checked)} />
                <Stat label="Read back clean" value={n(selected.ok)} />
                <Stat label="Repaired"      value={n(selected.repaired)}
                      warn={n(selected.repaired) > 0} />
                <Stat label="Unrecoverable" value={n(selected.unrecoverable)}
                      bad={n(selected.unrecoverable) > 0} />
                <Stat label="Missing"       value={n(selected.missing)}
                      bad={n(selected.missing) > 0} />
                <Stat label="Files affected" value={n(selected.affected_files)}
                      bad={n(selected.affected_files) > 0} />
              </div>

              {n(selected.repaired) > 0 && (
                <div className="text-xs mb-3 p-2 rounded bg-warning-subtle text-warning
                                border border-warning">
                  Parity corrected {n(selected.repaired)} object(s). The data
                  survived, but a disk returned bytes it was not given - worth
                  looking at the drive.
                </div>
              )}

              {loadingFindings ? (
                <div className="text-xs text-text-tertiary">Loading findings...</div>
              ) : findings.length > 0 ? (
                <>
                  <div className="text-xs text-text-secondary mb-1">Findings</div>
                  <table className="w-full text-xs border-collapse">
                    <thead>
                      <tr className="text-text-secondary text-left">
                        <th className="py-1 pr-3 font-medium">Object</th>
                        <th className="py-1 pr-3 font-medium">Problem</th>
                        <th className="py-1 font-medium">Path</th>
                      </tr>
                    </thead>
                    <tbody>
                      {findings.map((f, i) => (
                        <tr key={`${f.hash ?? i}`} className="border-t border-border-subtle">
                          <td className="py-1.5 pr-3 font-mono">
                            {f.hash ? f.hash.slice(0, 12) : '-'}
                          </td>
                          <td className="py-1.5 pr-3">{f.kind ?? f.detail ?? '-'}</td>
                          <td className="py-1.5 truncate max-w-[20rem]">{f.path ?? '-'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </>
              ) : (
                <div className="text-xs text-text-tertiary">
                  {verdict(selected).label === 'clean'
                    ? 'Every object read back as stored.'
                    : 'No per-object detail was recorded for this run.'}
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function Stat({ label, value, warn, bad }: {
  label: string; value: number; warn?: boolean; bad?: boolean
}): React.ReactElement {
  const cls = bad ? 'text-danger' : warn ? 'text-warning' : 'text-text-primary'
  return (
    <>
      <div className="text-text-secondary">{label}</div>
      <div className={`text-right font-mono ${cls}`}>{value.toLocaleString()}</div>
    </>
  )
}
