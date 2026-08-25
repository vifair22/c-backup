/*
 * Searching inside the files, not just their names.
 *
 * The app has always had a file search - that matches paths. This is the
 * other one: it reads the contents of every file in a snapshot looking for a
 * pattern, which is how somebody finds the config they know they wrote and
 * cannot remember where.
 *
 * It is a job rather than a request, and that shapes the whole screen. The
 * daemon answers with a run id immediately and reads the snapshot in the
 * background, so a search is submitted, then watched, then read - and a
 * search over a large repository is genuinely slow, which is the one place
 * in this app where waiting is honest. What must not be slow is finding out
 * that it started.
 */

import React, { useCallback, useEffect, useRef, useState } from 'react'
import { absoluteTime } from './format'

const api = window.cbackup

interface GrepRun {
  id: string
  snapshot: string
  pattern: string
  started_at: string
  finished_at: string
  completed: string
  files_scanned: string
  files_matched: string
  hits: string
  truncated: string
}

interface Hit {
  path: string
  line: string
  excerpt: string
  node_id: string
}

interface Props {
  connName: string
  repoPath: string
  /* Set when arriving from a directory in the snapshot browser. */
  initialPrefix?: string
  onBack: () => void
}

const n = (v?: string): number => Number(v ?? '0')

export function GrepView({ connName, repoPath, initialPrefix, onBack }: Props): React.ReactElement {
  const repoId = Number(repoPath)

  const [pattern, setPattern] = useState('')
  /*
   * Empty unless somebody arrived here from a directory.
   *
   * Deliberately not defaulted to the snapshot root. A prefix decides which
   * files are read, so a narrow one is the difference between reading three
   * files and reading all of them - but a prefix covering everything
   * excludes nothing and still costs the walk to prove it. Pre-filling this
   * with "/" would make the feature a tax on every search.
   */
  const [prefix, setPrefix] = useState(initialPrefix ?? '')
  const [ignoreCase, setIgnoreCase] = useState(true)
  const [runs, setRuns] = useState<GrepRun[]>([])
  const [selected, setSelected] = useState<GrepRun | null>(null)
  const [hits, setHits] = useState<Hit[]>([])
  const [searching, setSearching] = useState(false)
  const [loadingHits, setLoadingHits] = useState(false)
  const [error, setError] = useState<string | null>(null)

  /* Cleared on unmount so a search that finishes after the screen closes
   * does not keep polling a component nobody is looking at. */
  const poll = useRef<ReturnType<typeof setInterval> | null>(null)
  useEffect(() => () => { if (poll.current) clearInterval(poll.current) }, [])

  const loadRuns = useCallback(async (): Promise<GrepRun[]> => {
    const r = await api.apiCall<{ runs?: GrepRun[] }>(connName, {
      kind: 'grepRuns', repoId, limit: 20,
    })
    const list = r.runs ?? []
    setRuns(list)
    return list
  }, [connName, repoId])

  useEffect(() => { void loadRuns().catch(err => setError(String(err))) }, [loadRuns])

  /* Arriving from a different directory rescopes rather than keeping the
   * path from the last visit. */
  useEffect(() => { setPrefix(initialPrefix ?? '') }, [initialPrefix])

  const openRun = async (run: GrepRun) => {
    setSelected(run)
    setHits([])
    if (n(run.hits) === 0) return
    setLoadingHits(true)
    try {
      const r = await api.apiCall<{ hits?: Hit[] }>(connName, {
        kind: 'grepHits', repoId, runId: Number(run.id), limit: 200,
      })
      setHits(r.hits ?? [])
    } catch (err) {
      setError(String(err))
    }
    setLoadingHits(false)
  }

  const search = async () => {
    const p = pattern.trim()
    if (!p) return
    setSearching(true)
    setError(null)
    setHits([])
    setSelected(null)
    try {
      await api.apiCall(connName, {
        kind: 'grepStart', repoId, pattern: p, ignoreCase,
        ...(prefix.trim() ? { pathPrefix: prefix.trim() } : {}),
      })

      /*
       * Watch for the run to finish rather than blocking on it. The daemon
       * has already accepted the search by this point, so the screen can say
       * so and fill in when there is something to show.
       */
      if (poll.current) clearInterval(poll.current)
      poll.current = setInterval(async () => {
        try {
          const list = await loadRuns()
          const newest = list[0]
          if (newest && n(newest.completed) === 1) {
            if (poll.current) { clearInterval(poll.current); poll.current = null }
            setSearching(false)
            void openRun(newest)
          }
        } catch {
          if (poll.current) { clearInterval(poll.current); poll.current = null }
          setSearching(false)
        }
      }, 400)
    } catch (err) {
      setError(String(err))
      setSearching(false)
    }
  }

  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="flex items-center gap-2 mb-3">
        <button onClick={onBack}
          className="px-2 py-1 text-xs cursor-pointer rounded bg-surface-tertiary
                     text-text-secondary hover:bg-surface-hover border-none">
          Back
        </button>
        <h2 className="text-sm font-semibold m-0">Search file contents</h2>
      </div>

      <div className="flex gap-2 items-center mb-3">
        <input
          value={pattern}
          onChange={e => setPattern(e.target.value)}
          onKeyDown={e => { if (e.key === 'Enter') void search() }}
          placeholder="Text to look for inside files"
          className="flex-1 text-xs p-2 rounded bg-surface-tertiary text-text-primary
                     border border-border-default"
          style={{ maxWidth: '28rem' }}
        />
        <label className="text-xs flex items-center gap-1.5 cursor-pointer text-text-secondary">
          <input type="checkbox" checked={ignoreCase}
            onChange={e => setIgnoreCase(e.target.checked)} />
          Ignore case
        </label>
        <button onClick={search} disabled={searching || pattern.trim().length === 0}
          className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text
                     border-none disabled:opacity-50">
          {searching ? 'Searching...' : 'Search'}
        </button>
      </div>

      <div className="flex gap-2 items-center mb-3">
        <label className="text-xs text-text-secondary whitespace-nowrap">Within</label>
        <input
          value={prefix}
          onChange={e => setPrefix(e.target.value)}
          onKeyDown={e => { if (e.key === 'Enter') void search() }}
          placeholder="the whole snapshot"
          className="flex-1 text-xs p-2 rounded bg-surface-tertiary text-text-primary
                     border border-border-default font-mono"
          style={{ maxWidth: '28rem' }}
        />
        {prefix && (
          <button onClick={() => setPrefix('')}
            className="text-[11px] underline cursor-pointer bg-transparent border-none
                       text-accent p-0">
            clear
          </button>
        )}
        <span className="text-xs text-text-muted">
          {prefix
            ? 'Only files under this path are opened'
            : 'Narrowing this is what makes a search fast'}
        </span>
      </div>

      {searching && (
        <div className="text-xs text-text-muted mb-3">
          {prefix
            ? `Reading every file under ${prefix}. The results appear when it finishes.`
            : 'Reading every file in the newest snapshot. This is the one thing here '
              + 'that takes a while - narrow it with a path above to read fewer.'}
        </div>
      )}

      {error && (
        <div className="text-xs mb-3 p-2 rounded bg-error-bg text-error-text border border-error-border">
          {error}
        </div>
      )}

      {runs.length === 0 && !searching ? (
        <div className="text-xs text-text-muted">
          Nothing searched yet. This looks inside the files rather than at
          their names - use Search for names.
        </div>
      ) : (
        <div className="flex gap-4 overflow-hidden flex-1">
          <div className="overflow-auto shrink-0" style={{ maxWidth: '20rem' }}>
            <table className="text-xs border-collapse">
              <thead>
                <tr className="text-text-secondary text-left">
                  <th className="py-1 pr-3 font-medium">Pattern</th>
                  <th className="py-1 pr-3 font-medium">Files</th>
                  <th className="py-1 pr-3 font-medium">Hits</th>
                  <th className="py-1 font-medium">When</th>
                </tr>
              </thead>
              <tbody>
                {runs.map(r => (
                  <tr key={r.id} onClick={() => void openRun(r)}
                    className={`border-t border-border-subtle cursor-pointer
                                ${selected?.id === r.id ? 'bg-surface-secondary' : 'hover:bg-surface-hover'}`}>
                    <td className="py-1.5 pr-3 font-mono truncate max-w-[9rem]">{r.pattern}</td>
                    {/* How many files the run opened. With a path set this is
                      * the number that shows the scoping worked. */}
                    <td className="py-1.5 pr-3 text-text-muted">{n(r.files_scanned)}</td>
                    <td className="py-1.5 pr-3">{n(r.hits)}</td>
                    <td className="py-1.5 whitespace-nowrap">{absoluteTime(r.started_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {selected && (
            <div className="flex-1 overflow-auto border-l border-border-subtle pl-4">
              {/*
                * The daemon counts every hit and keeps the first ten per
                * file, so a run reporting forty hits has ten rows to show.
                * Printing the count on its own reads as a list that lost
                * thirty of them.
                */}
              <div className="text-xs text-text-secondary mb-2">
                {n(selected.hits)} hit{n(selected.hits) === 1 ? '' : 's'} in{' '}
                {n(selected.files_matched)} of {n(selected.files_scanned)} files
                {!loadingHits && hits.length > 0 && hits.length < n(selected.hits) && (
                  <span className="text-text-muted">
                    {' '}- showing {hits.length}, at most ten per file
                  </span>
                )}
                {n(selected.truncated) > 0 && (
                  <span className="text-status-warning ml-2">
                    - stopped early, there may be more
                  </span>
                )}
              </div>

              {loadingHits ? (
                <div className="text-xs text-text-muted">Loading hits...</div>
              ) : hits.length === 0 ? (
                <div className="text-xs text-text-muted">
                  Nothing in this snapshot contains that.
                </div>
              ) : (
                <table className="w-full text-xs border-collapse">
                  <tbody>
                    {hits.map((hit, i) => (
                      <tr key={`${hit.path}:${hit.line}:${i}`}
                        className="border-t border-border-subtle align-top">
                        {/* width 1% plus nowrap shrinks the column to its
                          * content, so the excerpt starts beside the path
                          * rather than at the far edge of the pane. */}
                        <td className="py-1.5 pr-4 font-mono whitespace-nowrap text-text-secondary"
                            style={{ width: '1%' }}>
                          {hit.path}:{hit.line}
                        </td>
                        <td className="py-1.5 font-mono break-all">{hit.excerpt}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
