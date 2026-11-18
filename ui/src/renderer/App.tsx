import React, { useCallback, useEffect, useState } from 'react'
import type { Job } from '../shared/api'
import type { MaintenanceOp, SnapshotMode, SourceSnapshot } from '../shared/api'
import type { ConnectionState, ConnectionConfig, CredentialInfo } from '../shared/types'
import { sameDaemon } from '../shared/types'
import { AddConnectionDialog } from './AddConnectionDialog'
import { ContextMenu, type ContextMenuItem } from './ContextMenu'
import { ConfirmDialog } from './ConfirmDialog'
import { RepoView } from './RepoView'
import { SnapshotList } from './SnapshotList'
import { SnapshotBrowser } from './SnapshotBrowser'
import { SnapshotDiff } from './SnapshotDiff'
import { DaemonStatus } from './DaemonStatus'
import { FileSearch } from './FileSearch'
import { PolicyEditor } from './PolicyEditor'
import { HashLookup } from './HashLookup'
import { LooseObjects } from './LooseObjects'
import { PackBrowser } from './PackBrowser'
import { JournalView } from './JournalView'
import { TaskBar } from './TaskBar'
import { TaskListView } from './TaskListView'
import { TagsView } from './TagsView'
import { SchedulesView } from './SchedulesView'
import { VerifyView } from './VerifyView'
import { KeyView } from './KeyView'
import { GrepView } from './GrepView'
import { DatabasesView, useUncoveredDatabases } from './DatabasesView'
import { useTheme, type ThemeMode } from './useTheme'

const api = window.cbackup

if (!api) {
  console.error('window.cbackup is undefined — preload script may not have loaded')
}

export function App(): React.ReactElement {
  const { mode, setMode } = useTheme()

  if (!window.cbackup) {
    return (
      <div className="flex items-center justify-center h-screen bg-surface-primary text-text-primary">
        <div className="text-center">
          <div className="text-lg mb-2">Loading failed</div>
          <div className="text-sm text-text-muted">
            The preload bridge is not available.<br />
            Check DevTools console (Ctrl+Shift+I) for errors.
          </div>
        </div>
      </div>
    )
  }

  const [connections, setConnections] = useState<ConnectionState[]>([])
  /*
   * The open repository: which connection, which id, and where it lives.
   *
   * `id` is new and is the thing that was missing. Every screen was handed
   * `path` under a prop called repoPath, and the main process turned it into
   * a repo id with Number() - so a filesystem path became NaN and every call
   * from every repository screen came back "invalid repo id". The path stays
   * because a few things legitimately want it: the header, the context menus,
   * and matching against a repo being renamed or removed.
   */
  const [activeRepo, setActiveRepo] = useState<{ conn: string; id: number; path: string } | null>(null)

  /* The open repository's name, for headings. Falls back to the directory
   * when it has no label, the way the sidebar does. */
  const activeRepoLabel = activeRepo
    ? (connections.find(c => c.config.name === activeRepo.conn)?.repos
        ?.find(r => r.id === activeRepo.id)?.label
       || activeRepo.path.split('/').pop() || activeRepo.path)
    : ''
  // Navigation history
  type ViewState = { view: 'dashboard' } | { view: 'snapshots' } | { view: 'snapshot-browser'; snapId: number; path?: string } | { view: 'diff'; snapA?: number; snapB?: number } | { view: 'search' } | { view: 'policy' } | { view: 'journal' } | { view: 'tags' } | { view: 'tasks' } | { view: 'hash-lookup' } | { view: 'loose-objects' } | { view: 'pack-browser' } | { view: 'schedules' } | { view: 'verify' } | { view: 'encryption' } | { view: 'grep'; prefix?: string } | { view: 'databases'; scope?: string }
  const [navHistory, setNavHistory] = useState<ViewState[]>([{ view: 'dashboard' }])
  /*
   * The daemon status screen is scoped to a connection, not a repository, so
   * it sits beside the repo views rather than inside them: the question it
   * answers is about the daemon, and it has to be reachable when no
   * repository is open at all.
   */
  const [statusConn, setStatusConn] = useState<string | null>(null)
  const [navIndex, setNavIndex] = useState(0)
  const currentView = navHistory[navIndex]

  const navigateTo = (state: ViewState) => {
    setNavHistory(prev => [...prev.slice(0, navIndex + 1), state])
    setNavIndex(prev => prev + 1)
  }
  const navBack = () => { if (navIndex > 0) setNavIndex(prev => prev - 1) }
  const navForward = () => { if (navIndex < navHistory.length - 1) setNavIndex(prev => prev + 1) }

  const [error, setError] = useState<string | null>(null)

  // Shared task state — single poll shared by TaskBar + TaskListView
  const [taskList, setTaskList] = useState<Job[]>([])

  useEffect(() => {
    if (!activeRepo) { setTaskList([]); return }
    let cancelled = false
    const poll = async () => {
      try {
        /* `jobs`, not `tasks`. Reading the wrong key set the list to
         * undefined and TaskBar - which renders on every repository screen -
         * threw on it, so opening any repository blanked the window. */
        const resp = await api.apiCall<{ jobs?: Job[] }>(activeRepo.conn, { kind: 'jobList' })
        if (!cancelled) setTaskList(resp.jobs ?? [])
      } catch { /* ignore */ }
    }
    poll()
    const interval = setInterval(poll, 2000)
    return () => { cancelled = true; clearInterval(interval) }
  }, [activeRepo?.conn, activeRepo?.id])

  // Restore modal
  const [restoreModal, setRestoreModal] = useState<{ snapId: number; filePath?: string } | null>(null)
  const [restoreDest, setRestoreDest] = useState('')
  const [restoreVerify, setRestoreVerify] = useState(false)
  const [restorePreferCopy, setRestorePreferCopy] = useState(false)
  const [restoreStarting, setRestoreStarting] = useState(false)

  // Backup modal
  const [showBackupModal, setShowBackupModal] = useState(false)
  const [sources, setSources] = useState<string[]>([])
  const [sourceDraft, setSourceDraft] = useState('')
  /* Excludes travel with the sources: the daemon replaces both lists on
   * every save, so a save that left them out cleared them. */
  const [excludes, setExcludes] = useState<string[]>([])
  const [excludeDraft, setExcludeDraft] = useState('')
  /* Snapshot mode per source (Manual 6.6): off when a path is absent. */
  const [snapModes, setSnapModes] = useState<Record<string, SnapshotMode>>({})
  const [snapDraft, setSnapDraft] = useState<Record<string, SnapshotMode>>({})
  const [editingSources, setEditingSources] = useState(false)
  const [sourcesLoading, setSourcesLoading] = useState(false)
  const [backupVerify, setBackupVerify] = useState(false)
  const [backupStarting, setBackupStarting] = useState(false)
  /* Asked only while the dialog is open: the sources list is the "sources
   * page" db-detection.md section 12 means, and an uncovered database under
   * a source is worth seeing before pressing Start. */
  const sourceDbs = useUncoveredDatabases(
    showBackupModal && activeRepo ? activeRepo.conn : '', activeRepo?.id ?? 0)

  // Dialogs
  const [showAddConnection, setShowAddConnection] = useState(false)
  const [editingConnection, setEditingConnection] = useState<ConnectionConfig | null>(null)
  /* The remote connection the sign-in dialog is open for, and what it says. */
  const [signingIn, setSigningIn] = useState<string | null>(null)
  const [passwordInput, setPasswordInput] = useState('')
  const [rememberSession, setRememberSession] = useState(true)
  const [signInError, setSignInError] = useState<string | null>(null)
  const [signInBusy, setSignInBusy] = useState(false)
  const [credInfo, setCredInfo] = useState<CredentialInfo | null>(null)
  const [addRepoConn, setAddRepoConn] = useState<string | null>(null)
  const [repoPathInput, setRepoPathInput] = useState('')
  const [addRepoBusy, setAddRepoBusy] = useState(false)
  const [postCreateConn, setPostCreateConn] = useState<string | null>(null)

  // Collapsed connections
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set())
  const toggleCollapsed = (name: string) => {
    setCollapsed(prev => {
      const next = new Set(prev)
      if (next.has(name)) next.delete(name)
      else next.add(name)
      return next
    })
  }

  // Context menu
  const [contextMenu, setContextMenu] = useState<{
    x: number; y: number; items: ContextMenuItem[]
  } | null>(null)

  // Confirm dialog
  const [confirmDialog, setConfirmDialog] = useState<{
    title: string; message: string; confirmLabel?: string; cancelLabel?: string
    danger?: boolean; onConfirm: () => void; onCancel?: () => void
  } | null>(null)

  const refreshConnections = useCallback(async () => {
    try {
      const list = await api.connectionList()
      setConnections(list)
    } catch (err) {
      console.error('Failed to list connections:', err)
    }
  }, [])

  useEffect(() => {
    refreshConnections()
    const interval = setInterval(refreshConnections, 3000)
    return () => clearInterval(interval)
  }, [refreshConnections])

  // Mouse back/forward buttons
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (e.button === 3) { e.preventDefault(); navBack() }
      if (e.button === 4) { e.preventDefault(); navForward() }
    }
    window.addEventListener('mouseup', handler)
    return () => window.removeEventListener('mouseup', handler)
  }, [navIndex, navHistory.length])

  /* ---------------------------------------------------------------- */
  /* Handlers                                                          */
  /* ---------------------------------------------------------------- */

  const handleAddConnection = async (config: ConnectionConfig) => {
    setShowAddConnection(false)
    setError(null)
    const result = await api.connectionAdd(config)
    if (!result.ok) { setError(result.error ?? 'Failed to add connection'); return }
    await refreshConnections()
    /* Connect straight away, so a mistyped address or a daemon with no
     * password shows up now rather than at the first click. A remote one
     * asks for its password here; offering to add a repository only makes
     * sense once there is a daemon to add it to. */
    const conn = await api.connectionConnect(config.name)
    await refreshConnections()
    if (conn.needsCredentials) { openSignIn(config.name); return }
    if (!conn.ok) { setError(conn.error ?? 'Connection failed'); return }
    setPostCreateConn(config.name)
  }

  const handleEditConnection = async (config: ConnectionConfig) => {
    if (!editingConnection) return
    const originalName = editingConnection.name
    // No-op if nothing changed
    if (JSON.stringify(config) === JSON.stringify(editingConnection)) { setEditingConnection(null); return }
    setError(null)
    setEditingConnection(null)
    const result = await api.connectionEdit(originalName, config)
    if (result.ok || result.needsCredentials) {
      /* The view follows the connection. A rename is the same daemon, so the
       * open repository stays open under the new name; a new address is
       * another daemon, whose repository ids mean nothing here. */
      if (activeRepo?.conn === originalName) {
        if (sameDaemon(editingConnection, config)) {
          setActiveRepo({ ...activeRepo, conn: config.name })
        } else {
          setActiveRepo(null)
          setNavHistory([{ view: 'dashboard' }])
          setNavIndex(0)
          setTaskList([])
        }
      }
      if (config.name !== originalName && collapsed.has(originalName)) {
        setCollapsed(prev => {
          const next = new Set(prev)
          next.delete(originalName)
          next.add(config.name)
          return next
        })
      }
    }
    await refreshConnections()
    if (result.needsCredentials) { openSignIn(config.name); return }
    if (!result.ok) setError(result.error ?? 'Failed to edit connection')
  }

  /* The password prompt, for a remote daemon that needs one. */
  const openSignIn = (name: string) => {
    setSigningIn(name)
    setPasswordInput('')
    setRememberSession(true)
    setSignInError(null)
    if (!credInfo) void api.credentialInfo().then(setCredInfo).catch(() => {})
  }

  const handleConnect = async (name: string) => {
    setError(null)
    const result = await api.connectionConnect(name)
    await refreshConnections()
    if (result.needsCredentials) { openSignIn(name); return }
    if (!result.ok) setError(result.error ?? 'Connection failed')
  }

  /* Wrong passwords keep the dialog open with the reason, rather than
   * closing it and leaving a banner the user has to dismiss to retry. */
  const handleSignIn = async () => {
    if (!signingIn) return
    const name = signingIn
    setSignInBusy(true)
    setSignInError(null)
    const result = await api.connectionConnect(name, {
      password: passwordInput, remember: rememberSession,
    })
    setSignInBusy(false)
    await refreshConnections()
    if (!result.ok) { setSignInError(result.error ?? 'Sign-in failed'); return }
    setSigningIn(null)
    setPasswordInput('')
  }

  const handleSignOut = async (name: string) => {
    setError(null)
    await api.connectionSignOut(name)
    if (activeRepo?.conn === name) {
      setActiveRepo(null)
      setNavHistory([{ view: 'dashboard' }])
      setNavIndex(0)
      setTaskList([])
    }
    await refreshConnections()
  }

  const handleRemoveConnection = async (name: string) => {
    setConfirmDialog({
      title: 'Delete Connection',
      message: `Delete "${name}"? Its saved sign-in is forgotten. The daemon and its repositories are not touched.`,
      onConfirm: async () => {
        setConfirmDialog(null)
        await api.connectionRemove(name)
        if (activeRepo?.conn === name) {
          setActiveRepo(null)
          setNavHistory([{ view: 'dashboard' }])
          setNavIndex(0)
          setTaskList([])
        }
        await refreshConnections()
      }
    })
  }

  const handleRestartConnection = async (name: string) => {
    setError(null)
    const result = await api.connectionRestart(name)
    await refreshConnections()
    if (result.needsCredentials) { openSignIn(name); return }
    if (!result.ok) setError(result.error ?? 'Reconnect failed')
  }

  /* One operation for "adopt this repository" and "create a new one": the
   * daemon makes the directory and its marker when they do not exist. */
  const handleAddRepo = async () => {
    if (!addRepoConn || !repoPathInput.trim()) return
    const connName = addRepoConn
    const path = repoPathInput.trim()
    setError(null)
    setAddRepoBusy(true)
    const result = await api.repoAdd(connName, path)
    setAddRepoBusy(false)
    await refreshConnections()
    if (!result.ok) { setError(result.error ?? 'Failed to add repository'); return }
    setAddRepoConn(null)
    setRepoPathInput('')
  }

  const handleRemoveRepo = (connName: string, repo: { id: number; path: string; label?: string }) => {
    setConfirmDialog({
      title: 'Remove Repository',
      message: `Remove "${repo.label || repo.path.split('/').pop() || repo.path}" from this daemon? The data on disk is not deleted.`,
      confirmLabel: 'Remove',
      onConfirm: async () => {
        setConfirmDialog(null)
        const result = await api.repoRemove(connName, repo.id)
        if (!result.ok) { setError(result.error ?? 'Failed to remove repository'); return }
        if (activeRepo?.conn === connName && activeRepo?.id === repo.id) {
          setActiveRepo(null)
          setNavHistory([{ view: 'dashboard' }])
          setNavIndex(0)
          setTaskList([])
        }
        await refreshConnections()
      }
    })
  }

  /* Requests are stateless, so opening a repository is choosing it. There
   * used to be an "open" round trip here that did nothing. */
  const handleOpenRepo = (connName: string, repoPath: string, repoId: number) => {
    setError(null)
    /* Opening a repository leaves the daemon screen, which otherwise sits
     * in front of it. */
    setStatusConn(null)
    setActiveRepo({ conn: connName, id: repoId, path: repoPath })
    setNavHistory([{ view: 'dashboard' }])
    setNavIndex(0)
  }

  /* ---------------------------------------------------------------- */
  /* Task operations                                                   */
  /* ---------------------------------------------------------------- */

  /*
   * Start a backup of whatever this repository is configured to back up.
   *
   * No paths are sent. Sources belong to the repository, so the button means
   * "back up this repo" and the daemon reads the list - which is also what a
   * schedule does, so a scheduled run and a clicked one cannot disagree.
   *
   * This posted to /repos/<id>/run until now, a route the daemon has never
   * had, and failed silently every time it was pressed.
   */
  const handleStartBackup = async () => {
    if (!activeRepo) return
    setBackupStarting(true)
    setError(null)
    try {
      await api.apiCall(activeRepo.conn, {
        kind: 'backup',
        repoId: activeRepo.id,
      })
      setShowBackupModal(false)
      navigateTo({ view: 'tasks' })
    } catch (err) {
      setError(String(err))
    }
    setBackupStarting(false)
  }

  /* Load what the repository backs up, so the modal can show it rather than
   * asking for paths that are already recorded. */
  const loadSources = async () => {
    if (!activeRepo) return
    setSourcesLoading(true)
    try {
      const r = await api.apiCall<{ sources?: string[]; exclude?: string[]; snapshot?: SourceSnapshot[] }>(
        activeRepo.conn, { kind: 'sourcesGet', repoId: activeRepo.id })
      setSources(r.sources ?? [])
      setSourceDraft((r.sources ?? []).join('\n'))
      setExcludes(r.exclude ?? [])
      setExcludeDraft((r.exclude ?? []).join('\n'))
      const modes: Record<string, SnapshotMode> = {}
      for (const sn of r.snapshot ?? []) modes[sn.path] = sn.mode
      setSnapModes(modes)
      setSnapDraft(modes)
    } catch (err) {
      setError(String(err))
    }
    setSourcesLoading(false)
  }

  const handleSaveSources = async () => {
    if (!activeRepo) return
    const paths = sourceDraft.split('\n').map(s => s.trim()).filter(Boolean)
    const exclude = excludeDraft.split('\n').map(s => s.trim()).filter(Boolean)
    /* Every mode is sent, so a path set to off really is off; only paths
     * still in the list, which the daemon requires. */
    const snapshot: SourceSnapshot[] = paths
      .filter(p => snapDraft[p])
      .map(p => ({ path: p, mode: snapDraft[p] }))
    setSourcesLoading(true)
    setError(null)
    try {
      const r = await api.apiCall<{ warnings?: string[] } | null>(activeRepo.conn, {
        kind: 'sourcesSet', repoId: activeRepo.id, sources: paths, exclude, snapshot,
      })
      setSources(paths)
      setExcludes(exclude)
      const kept: Record<string, SnapshotMode> = {}
      for (const sn of snapshot) kept[sn.path] = sn.mode
      setSnapModes(kept)
      setSnapDraft(kept)
      setEditingSources(false)
      /* Saved, but a path the daemon cannot see yet is worth saying: it may
       * be an unmounted share, or a typo. */
      if (r?.warnings?.length) setError(`Saved. ${r.warnings.join('; ')}`)
    } catch (err) {
      setError(String(err))
    }
    setSourcesLoading(false)
  }

  const handleStartRestore = async () => {
    if (!activeRepo || !restoreModal || !restoreDest.trim()) return
    setRestoreStarting(true)
    setError(null)
    try {
      /* Under the snapshot. The old call posted to /repos/<id>/restore,
       * which the daemon has never served. */
      await api.apiCall(activeRepo.conn, {
        kind: 'restore',
        repoId: activeRepo.id,
        snapshotId: restoreModal.snapId,
        dest: restoreDest.trim(),
        subpath: restoreModal.filePath,
        preferEngineCopy: restorePreferCopy,
      })
      setRestoreModal(null)
      navigateTo({ view: 'tasks' })
    } catch (err) {
      setError(String(err))
    }
    setRestoreStarting(false)
  }

  /*
   * The maintenance operations, as a closed set.
   *
   * Typed rather than passed through as a command string. These three
   * happened to name real endpoints, unlike backup, but they were one typo
   * from the same silent 404 - and a union means a name that stops existing
   * fails the contract suite instead of a button.
   */
  const OP_LABELS: Record<MaintenanceOp, string> = {
    verify: 'Verify',
    gc: 'Garbage Collect',
    gfs: 'Apply Retention',
    reindex: 'Reindex',
  }

  const handleStartOperation = (command: MaintenanceOp) => {
    if (!activeRepo) return
    const label = OP_LABELS[command]
    setConfirmDialog({
      title: `Run ${label}`,
      message: `Start a background ${label} operation on this repository?`,
      confirmLabel: 'Start',
      danger: false,
      onConfirm: async () => {
        setConfirmDialog(null)
        setError(null)
        try {
          const repoId = activeRepo.id
          await api.apiCall(activeRepo.conn, { kind: command, repoId })
          navigateTo({ view: 'tasks' })
        } catch (err) {
          setError(String(err))
        }
      },
    })
  }

  /* ---------------------------------------------------------------- */
  /* Context menus                                                     */
  /* ---------------------------------------------------------------- */

  const leaveConnection = (name: string) => {
    if (activeRepo?.conn === name) {
      setActiveRepo(null)
      setNavHistory([{ view: 'dashboard' }])
      setNavIndex(0)
      setTaskList([])
    }
  }

  const showConnectionMenu = (e: React.MouseEvent, conn: ConnectionState) => {
    e.preventDefault()
    const name = conn.config.name
    const connected = conn.status === 'connected'
    const busy = conn.status === 'connecting'
    setContextMenu({
      x: e.clientX, y: e.clientY,
      items: [
        ...(!connected && !busy ? [
          conn.needsCredentials
            ? { label: 'Sign in', onClick: () => openSignIn(name) }
            : { label: 'Connect', onClick: () => { void handleConnect(name) } },
        ] : []),
        ...(connected ? [
          { label: 'Add Repository', onClick: () => { setAddRepoConn(name); setRepoPathInput('') } },
          { label: 'Daemon status', onClick: () => setStatusConn(name) },
          { label: 'Reconnect', onClick: () => handleRestartConnection(name) },
          { label: 'Disconnect', onClick: async () => {
            await api.connectionDisconnect(name)
            leaveConnection(name)
            await refreshConnections()
          }},
        ] : []),
        ...(conn.signedIn ? [
          { label: 'Sign out', onClick: () => { void handleSignOut(name) } },
        ] : []),
        { label: 'Edit', onClick: () => setEditingConnection(conn.config) },
        { label: 'Delete', danger: true, onClick: () => handleRemoveConnection(name) },
      ]
    })
  }

  const showRepoMenu = (e: React.MouseEvent, connName: string, repo: { id: number; path: string; label?: string }) => {
    e.preventDefault()
    e.stopPropagation()
    setContextMenu({
      x: e.clientX, y: e.clientY,
      items: [
        { label: 'Remove', danger: true, onClick: () => handleRemoveRepo(connName, repo) },
      ]
    })
  }

  /* Paths typed into the app are resolved by the daemon, on its host. For a
   * remote connection that is not this machine, and the forms say so. */
  const hostOf = (connName: string): string | null => {
    const c = connections.find(x => x.config.name === connName)?.config
    return c && c.type === 'remote' ? c.host : null
  }
  const pathLabel = (connName: string, what: string): string => {
    const host = hostOf(connName)
    return host ? `${what} on ${host}` : what
  }

  /* ---------------------------------------------------------------- */
  /* Status helpers                                                    */
  /* ---------------------------------------------------------------- */

  const statusColor = (status: string) => {
    switch (status) {
      case 'connected': return 'bg-status-connected'
      case 'connecting': return 'bg-status-warning'
      case 'error': return 'bg-status-error'
      default: return 'bg-status-disconnected'
    }
  }

  /* The status in words beside the dot, on the connection rather than on
   * every repository under it, where a "connected" tag used to sit on each
   * row whatever the connection was doing. */
  const connLabel = (conn: ConnectionState) => {
    const parts = [conn.config.type === 'remote' ? 'remote' : 'local']
    if (conn.status === 'connected') parts.push('connected')
    else if (conn.status === 'connecting') parts.push('connecting')
    else if (conn.needsCredentials) parts.push('signed out')
    return parts.join(', ')
  }

  const themeOptions: { value: ThemeMode; label: string }[] = [
    { value: 'system', label: 'Auto' },
    { value: 'light', label: 'Light' },
    { value: 'dark', label: 'Dark' },
  ]

  /* ---------------------------------------------------------------- */
  /* Render                                                            */
  /* ---------------------------------------------------------------- */

  return (
    <div className="flex h-screen">
      {/* Sidebar */}
      <div className="w-72 border-r border-border-default bg-surface-secondary flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-2 border-b border-border-default">
          <span className="font-semibold text-sm text-text-primary">Connections</span>
          <button onClick={() => setShowAddConnection(true)}
            className="text-xs px-2 py-0.5 rounded bg-surface-tertiary hover:bg-surface-hover text-text-secondary cursor-pointer">
            + Add
          </button>
        </div>

        {/* Connection list */}
        <div className="flex-1 overflow-auto p-2">
          {connections.map(conn => (
            <div key={conn.config.name} className="mb-3">
              <div className="flex items-center justify-between cursor-pointer rounded px-1 py-0.5 hover:bg-surface-hover"
                onClick={() => toggleCollapsed(conn.config.name)}
                onContextMenu={e => showConnectionMenu(e, conn)}>
                <span className="flex items-center gap-1.5">
                  <span className={`text-text-muted text-[10px] inline-flex items-center justify-center w-3 h-3 ${collapsed.has(conn.config.name) ? '' : 'rotate-90'} transition-transform origin-center`}>&#9654;</span>
                  <span className={`inline-block w-2 h-2 rounded-full ${statusColor(conn.status)}`} />
                  <strong className="text-sm">{conn.config.name}</strong>
                  <span className="text-text-muted text-[11px]">({connLabel(conn)})</span>
                </span>
              </div>

              {conn.error && (
                <div className="text-status-error text-[11px] ml-4 mt-0.5">{conn.error}</div>
              )}

              {/* A disconnected connection says how to bring it back, so the
                * only way in is not a right-click nobody knows about. */}
              {conn.status !== 'connected' && conn.status !== 'connecting' && (
                <div className="ml-4 mt-0.5">
                  <button
                    onClick={() => conn.needsCredentials ? openSignIn(conn.config.name) : void handleConnect(conn.config.name)}
                    className="text-[11px] cursor-pointer text-accent hover:text-accent-hover bg-transparent border-none p-0">
                    {conn.needsCredentials ? 'Sign in' : 'Connect'}
                  </button>
                </div>
              )}

              {/* Repos */}
              {!collapsed.has(conn.config.name) && <div className="ml-4 mt-1">
                {conn.repos.map(repo => (
                  <div
                    key={repo.id}
                    onClick={() => handleOpenRepo(conn.config.name, repo.path, repo.id)}
                    onContextMenu={e => showRepoMenu(e, conn.config.name, repo)}
                    className={`text-xs py-1 px-1.5 rounded mb-0.5 cursor-pointer hover:bg-surface-hover ${
                      activeRepo?.conn === conn.config.name && activeRepo?.id === repo.id ? 'bg-surface-active' : ''
                    }`}
                  >
                    {/*
                      * The label if there is one, the directory name if not.
                      * A repository is named when it is added and the name
                      * was shown nowhere: the sidebar used the last path
                      * segment, so "Photos" at a path ending in /encrepo
                      * appeared as encrepo.
                      */}
                    <span className="text-text-primary" title={repo.path}>
                      {repo.label || repo.path.split('/').pop() || repo.path}
                    </span>
                  </div>
                ))}
                {conn.status === 'connected' && (
                  <button onClick={() => { setAddRepoConn(conn.config.name); setRepoPathInput('') }}
                    className="text-[10px] mt-0.5 cursor-pointer text-accent hover:text-accent-hover bg-transparent border-none px-1.5">
                    + Add repo
                  </button>
                )}
              </div>}
            </div>
          ))}

          {connections.length === 0 && (
            <div className="text-text-muted text-xs text-center mt-8">
              No connections yet.<br />Click "+ Add" to create one.
            </div>
          )}
        </div>

        {/* Theme switcher */}
        <div className="border-t border-border-default p-2 flex items-center gap-2">
          <span className="text-[11px] text-text-muted">Theme:</span>
          {themeOptions.map(opt => (
            <button key={opt.value} onClick={() => setMode(opt.value)}
              className={`text-[11px] px-1.5 py-0.5 rounded cursor-pointer ${
                mode === opt.value
                  ? 'bg-accent text-accent-text'
                  : 'bg-surface-tertiary text-text-secondary hover:bg-surface-hover'
              }`}>
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      {/* Main content */}
      <div className="flex-1 flex flex-col bg-surface-primary">
      <div className="flex-1 p-4 overflow-auto">
        {error && (
          <div className="bg-error-bg border border-error-border text-error-text text-xs px-3 py-2 rounded mb-3 flex justify-between items-center">
            <span>{error}</span>
            <button onClick={() => setError(null)} className="cursor-pointer text-error-text hover:opacity-70 bg-transparent border-none text-[11px]">dismiss</button>
          </div>
        )}

        {statusConn ? (
          <DaemonStatus connName={statusConn} onBack={() => setStatusConn(null)} />
        ) : activeRepo ? (
          <>
            {currentView.view === 'dashboard' && (
              <RepoView connName={activeRepo.conn} repoPath={String(activeRepo.id)}
                title={activeRepoLabel}
                onSelectSnapshot={(id) => navigateTo({ view: 'snapshot-browser', snapId: id })}
                onViewAllSnapshots={() => navigateTo({ view: 'snapshots' })}
                onCompareSnapshots={(a, b) => navigateTo({ view: 'diff', snapA: a, snapB: b })}
                onSearch={() => navigateTo({ view: 'search' })}
                onEditPolicy={() => navigateTo({ view: 'policy' })}
                onViewJournal={() => navigateTo({ view: 'journal' })}
                onViewTags={() => navigateTo({ view: 'tags' })}
                onViewSchedules={() => navigateTo({ view: 'schedules' })}
                onViewVerify={() => navigateTo({ view: 'verify' })}
                onViewEncryption={() => navigateTo({ view: 'encryption' })}
                onViewDatabases={() => navigateTo({ view: 'databases' })}
                onViewGrep={() => navigateTo({ view: 'grep' })}
                onViewTasks={() => navigateTo({ view: 'tasks' })}
                onHashLookup={() => navigateTo({ view: 'hash-lookup' })}
                onLooseObjects={() => navigateTo({ view: 'loose-objects' })}
                onPackBrowser={() => navigateTo({ view: 'pack-browser' })}
                /* Read the sources as the modal opens, so it shows what
                 * will actually be backed up rather than a blank form. */
                onRunBackup={() => { setShowBackupModal(true); setEditingSources(false); void loadSources() }}
                onRunOperation={handleStartOperation}
              />
            )}
            {currentView.view === 'snapshots' && (
              <SnapshotList connName={activeRepo.conn} repoPath={String(activeRepo.id)}
                onSelectSnapshot={(id) => navigateTo({ view: 'snapshot-browser', snapId: id })}
                onCompareSnapshots={() => navigateTo({ view: 'diff' })}
                onBack={navBack}
              />
            )}
            {currentView.view === 'snapshot-browser' && (
              /* Keyed by snapshot so a different snapshot is a fresh browser:
               * its selection, highlight and open file card belong to the
               * snapshot they were made in, and a restore from a stale card
               * would act on the wrong one. */
              <SnapshotBrowser key={`${activeRepo.conn}/${activeRepo.id}/${currentView.snapId}`}
                connName={activeRepo.conn} repoPath={String(activeRepo.id)}
                snapId={currentView.snapId}
                initialPath={currentView.path}
                onBack={navBack}
                onRestore={(sid, fp) => { setRestoreModal({ snapId: sid, filePath: fp }); setRestoreDest(''); setRestoreVerify(false); setRestorePreferCopy(false) }}
                onSearchHere={(path) => navigateTo({ view: 'grep', prefix: path })}
              />
            )}
            {currentView.view === 'tasks' && (
              <TaskListView jobs={taskList} connName={activeRepo.conn} repoPath={String(activeRepo.id)}
                onBack={navBack}
              />
            )}
            {currentView.view === 'tags' && (
              <TagsView connName={activeRepo.conn} repoPath={String(activeRepo.id)}
                onSelectSnapshot={(id) => navigateTo({ view: 'snapshot-browser', snapId: id })}
                onBack={navBack}
              />
            )}
            {currentView.view === 'schedules' && (
              <SchedulesView connName={activeRepo.conn} repoPath={String(activeRepo.id)}
                onBack={navBack}
              />
            )}
            {currentView.view === 'verify' && (
              <VerifyView connName={activeRepo.conn} repoPath={String(activeRepo.id)}
                onBack={navBack}
              />
            )}
            {currentView.view === 'encryption' && (
              <KeyView connName={activeRepo.conn} repoPath={String(activeRepo.id)}
                onBack={navBack}
              />
            )}
            {currentView.view === 'grep' && (
              <GrepView connName={activeRepo.conn} repoPath={String(activeRepo.id)}
                initialPrefix={currentView.prefix}
                onBack={navBack}
              />
            )}
            {currentView.view === 'databases' && (
              <DatabasesView connName={activeRepo.conn} repoPath={String(activeRepo.id)}
                initialScope={currentView.scope}
                onBack={navBack} />
            )}
            {currentView.view === 'journal' && (
              <JournalView connName={activeRepo.conn} repoPath={String(activeRepo.id)}
                onBack={navBack}
              />
            )}
            {currentView.view === 'loose-objects' && (
              <LooseObjects connName={activeRepo.conn} repoPath={String(activeRepo.id)}
                onBack={navBack}
              />
            )}
            {currentView.view === 'pack-browser' && (
              <PackBrowser connName={activeRepo.conn} repoPath={String(activeRepo.id)}
                onBack={navBack}
              />
            )}
            {currentView.view === 'hash-lookup' && (
              <HashLookup connName={activeRepo.conn} repoPath={String(activeRepo.id)}
                onNavigateToSnapshot={(id) => navigateTo({ view: 'snapshot-browser', snapId: id })}
                onBack={navBack}
              />
            )}
            {currentView.view === 'policy' && (
              <PolicyEditor connName={activeRepo.conn} repoPath={String(activeRepo.id)}
                onBack={navBack}
              />
            )}
            {currentView.view === 'search' && (
              <FileSearch connName={activeRepo.conn} repoPath={String(activeRepo.id)}
                onNavigateToFile={(snapId, path) => navigateTo({ view: 'snapshot-browser', snapId, path })}
                onBack={navBack}
              />
            )}
            {currentView.view === 'diff' && (
              <SnapshotDiff connName={activeRepo.conn} repoPath={String(activeRepo.id)}
                initialSnapA={currentView.snapA}
                initialSnapB={currentView.snapB}
                onBack={navBack}
                onNavigateToFile={(snapId, path) => navigateTo({ view: 'snapshot-browser', snapId, path })}
              />
            )}
          </>
        ) : (
          <div className="text-text-muted text-center mt-16">
            <div className="text-xl mb-2 text-text-secondary">c-backup</div>
            <div className="text-sm">Add a connection and open a repository to get started.</div>
            {connections.some(c => c.status === 'connected') && (
              <button onClick={() => setStatusConn(connections.find(c => c.status === 'connected')!.config.name)}
                className="text-xs mt-4 underline cursor-pointer bg-transparent border-none text-accent p-0">
                Check the daemon
              </button>
            )}
          </div>
        )}
      </div>
      {/* Task status bar */}
      {activeRepo && (
        <TaskBar jobs={taskList} connName={activeRepo.conn} repoPath={String(activeRepo.id)}
          onViewTasks={() => navigateTo({ view: 'tasks' })}
        />
      )}
      </div>

      {/* Context menu */}
      {contextMenu && (
        <ContextMenu x={contextMenu.x} y={contextMenu.y} items={contextMenu.items}
          onClose={() => setContextMenu(null)} />
      )}

      {/* Confirm dialog */}
      {confirmDialog && (
        <ConfirmDialog
          title={confirmDialog.title}
          message={confirmDialog.message}
          confirmLabel={confirmDialog.confirmLabel}
          cancelLabel={confirmDialog.cancelLabel}
          danger={confirmDialog.danger}
          onConfirm={confirmDialog.onConfirm}
          onCancel={confirmDialog.onCancel ?? (() => setConfirmDialog(null))}
        />
      )}

      {/* Add connection dialog */}
      {showAddConnection && (
        <AddConnectionDialog
          onAdd={handleAddConnection}
          onCancel={() => setShowAddConnection(false)}
        />
      )}

      {/* Edit connection dialog */}
      {editingConnection && (
        <AddConnectionDialog
          initialConfig={editingConnection}
          onAdd={handleEditConnection}
          onCancel={() => setEditingConnection(null)}
        />
      )}

      {/* Post-create: offer to add a repo */}
      {postCreateConn && (
        <Overlay>
          <div className="bg-surface-primary rounded-lg p-5 w-80 shadow-xl border border-border-default">
            <h3 className="text-sm font-semibold m-0 mb-2">Connection Created</h3>
            <p className="text-xs text-text-secondary m-0 mb-4">
              "{postCreateConn}" was added successfully. Would you like to add a repository now?
            </p>
            <div className="flex justify-end gap-2">
              <button onClick={() => setPostCreateConn(null)}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none">
                Not Now
              </button>
              <button onClick={() => { setAddRepoConn(postCreateConn); setRepoPathInput(''); setPostCreateConn(null) }}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text hover:bg-accent-hover border-none">
                Add Repo
              </button>
            </div>
          </div>
        </Overlay>
      )}

      {/* Sign-in prompt, for a remote daemon */}
      {signingIn && (
        <Overlay>
          <form onSubmit={e => { e.preventDefault(); void handleSignIn() }} className="bg-surface-primary rounded-lg p-5 w-80 shadow-xl border border-border-default">
            <h3 className="text-sm font-semibold m-0 mb-1">Sign in: {signingIn}</h3>
            <div className="text-text-muted text-[11px] mb-3">
              The daemon's admin password, set on its host with <span className="font-mono">c-backup auth setup</span>.
            </div>
            <div className="mb-3">
              <label className="block text-xs font-medium text-text-secondary mb-1">
                Admin password
              </label>
              <input type="password" value={passwordInput} onChange={e => setPasswordInput(e.target.value)}
                className="w-full px-2 py-1 border border-border-default rounded text-sm bg-surface-primary text-text-primary focus:outline-none focus:border-accent"
                autoFocus required disabled={signInBusy} />
            </div>
            <div className="mb-3">
              <label className="text-xs flex items-center gap-1.5 cursor-pointer text-text-secondary">
                <input type="checkbox" checked={rememberSession} onChange={e => setRememberSession(e.target.checked)} />
                Stay signed in
              </label>
              {/* What staying signed in keeps, and how. Never the password:
                * the daemon issues a session token and that is stored. */}
              {rememberSession && credInfo && (
                <div className="text-[10px] text-text-muted mt-1 ml-5">
                  {credInfo.encrypted
                    ? 'Keeps a session token, encrypted with the system keyring. Not the password.'
                    : 'No system keyring is available, so the session token is stored unencrypted, in a file only your account can read. Not the password.'}
                </div>
              )}
            </div>
            {signInError && (
              <div className="text-status-error text-[11px] mb-3">{signInError}</div>
            )}
            <div className="flex justify-end gap-2">
              <button type="button" onClick={() => { setSigningIn(null); setPasswordInput('') }}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none"
                disabled={signInBusy}>Cancel</button>
              <button type="submit"
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text hover:bg-accent-hover border-none"
                disabled={signInBusy}>{signInBusy ? 'Signing in...' : 'Sign in'}</button>
            </div>
          </form>
        </Overlay>
      )}

      {/* Add repo dialog. Creates the repository when the path is new. */}
      {addRepoConn && (
        <Overlay>
          <form onSubmit={e => { e.preventDefault(); void handleAddRepo() }} className="bg-surface-primary rounded-lg p-5 w-96 shadow-xl border border-border-default">
            <h3 className="text-sm font-semibold m-0 mb-2">Add Repository</h3>
            <div className="text-text-muted text-xs mb-3">Connection: {addRepoConn}</div>
            <div className="mb-3">
              <label className="block text-xs font-medium text-text-secondary mb-1">{pathLabel(addRepoConn, 'Repository path')} (absolute)</label>
              <input value={repoPathInput} onChange={e => setRepoPathInput(e.target.value)}
                placeholder="/path/to/backup/repo"
                className="w-full px-2 py-1 border border-border-default rounded text-sm bg-surface-primary text-text-primary focus:outline-none focus:border-accent"
                autoFocus required disabled={addRepoBusy} />
              <div className="text-[10px] text-text-muted mt-1">An existing repository is added as it is. A new path is created and initialised.</div>
            </div>
            <div className="flex justify-end gap-2">
              <button type="button" onClick={() => setAddRepoConn(null)}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none"
                disabled={addRepoBusy}>Cancel</button>
              <button type="submit"
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text hover:bg-accent-hover border-none"
                disabled={addRepoBusy}>{addRepoBusy ? 'Adding...' : 'Add'}</button>
            </div>
          </form>
        </Overlay>
      )}

      {/* Restore modal */}
      {restoreModal && (
        <Overlay>
          <form onSubmit={e => { e.preventDefault(); handleStartRestore() }}
            className="bg-surface-primary rounded-lg p-5 w-96 shadow-xl border border-border-default">
            <h3 className="text-sm font-semibold m-0 mb-3">
              {restoreModal.filePath ? 'Restore File' : 'Restore Snapshot'}
            </h3>
            <div className="text-xs text-text-muted mb-3">
              Snapshot #{restoreModal.snapId}
              {restoreModal.filePath && <span className="font-mono ml-1">— {restoreModal.filePath}</span>}
            </div>
            <div className="mb-3">
              <label className="block text-xs font-medium text-text-secondary mb-1">{activeRepo ? pathLabel(activeRepo.conn, 'Destination path') : 'Destination path'}</label>
              <input value={restoreDest} onChange={e => setRestoreDest(e.target.value)}
                placeholder="/path/to/restore/destination"
                className="w-full px-2 py-1 border border-border-default rounded text-sm bg-surface-primary text-text-primary focus:outline-none focus:border-accent font-mono"
                autoFocus required />
            </div>
            <div className="mb-3">
              <label className="text-xs flex items-center gap-1.5 cursor-pointer text-text-secondary"
                title="A database with a consistent engine copy in this snapshot (.cbackup-captures) is restored from the copy, and its stale -wal, -shm and -journal are left out">
                <input type="checkbox" checked={restorePreferCopy}
                  onChange={e => setRestorePreferCopy(e.target.checked)} />
                Restore databases from their engine copies
              </label>
            </div>
            <div className="mb-4">
              <label className="text-xs flex items-center gap-1.5 cursor-pointer text-text-secondary hidden">
                <input type="checkbox" checked={restoreVerify} onChange={e => setRestoreVerify(e.target.checked)} />
                Verify after restore
              </label>
            </div>
            <div className="flex justify-end gap-2">
              <button type="button" onClick={() => setRestoreModal(null)}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none"
                disabled={restoreStarting}>Cancel</button>
              <button type="submit"
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text hover:bg-accent-hover border-none"
                disabled={restoreStarting}>{restoreStarting ? 'Starting...' : 'Start Restore'}</button>
            </div>
          </form>
        </Overlay>
      )}

      {/* Backup modal */}
      {showBackupModal && (
        <Overlay>
          <div className="bg-surface-primary rounded-lg p-5 w-96 shadow-xl border border-border-default">
            <h3 className="text-sm font-semibold m-0 mb-3">Run Backup</h3>

            {/*
              * What this repository backs up, read from the repository
              * rather than asked for again. The old modal offered a "verify
              * after" checkbox and no way to say what to back up at all,
              * which is why the button could never work: it had nothing to
              * send. The checkbox is gone because no endpoint takes it -
              * a control that quietly does nothing is worse than its
              * absence, and verifying on a schedule is the better shape.
              */}
            <div className="mb-3">
              <div className="text-xs text-text-secondary mb-1.5">{activeRepo && hostOf(activeRepo.conn) ? `Sources, on ${hostOf(activeRepo.conn)}` : 'Sources'}</div>
              {sourcesLoading ? (
                <div className="text-xs text-text-tertiary">Loading...</div>
              ) : editingSources ? (
                <>
                  <textarea
                    value={sourceDraft}
                    onChange={e => setSourceDraft(e.target.value)}
                    rows={4}
                    placeholder={'/home/me\n/etc'}
                    className="w-full text-xs font-mono p-2 rounded bg-surface-tertiary
                               text-text-primary border border-border-default resize-y"
                  />
                  <div className="text-xs text-text-tertiary mt-1">One absolute path per line.</div>
                  <div className="text-xs text-text-secondary mt-2 mb-1.5">Exclude</div>
                  <textarea
                    aria-label="Exclude patterns"
                    value={excludeDraft}
                    onChange={e => setExcludeDraft(e.target.value)}
                    rows={3}
                    placeholder={'*.tmp\n/home/me/.cache'}
                    className="w-full text-xs font-mono p-2 rounded bg-surface-tertiary
                               text-text-primary border border-border-default resize-y"
                  />
                  <div className="text-xs text-text-tertiary mt-1">One pattern per line (Manual 6.2).</div>
                  <SnapshotModes
                    paths={sourceDraft.split('\n').map(p => p.trim()).filter(Boolean)}
                    modes={snapDraft}
                    onChange={(p, m) => setSnapDraft(d => {
                      const next = { ...d }
                      if (m) next[p] = m
                      else delete next[p]
                      return next
                    })}
                  />
                  <div className="flex gap-2 mt-2">
                    <button onClick={handleSaveSources}
                      className="px-2 py-1 text-xs cursor-pointer rounded bg-accent text-accent-text border-none">
                      Save
                    </button>
                    <button onClick={() => {
                      setEditingSources(false)
                      setSourceDraft(sources.join('\n'))
                      setExcludeDraft(excludes.join('\n'))
                      setSnapDraft(snapModes)
                    }}
                      className="px-2 py-1 text-xs cursor-pointer rounded bg-surface-tertiary
                                 text-text-secondary border-none">
                      Cancel
                    </button>
                  </div>
                </>
              ) : sources.length === 0 ? (
                <div className="text-xs text-text-tertiary">
                  Nothing configured yet.{' '}
                  <button onClick={() => setEditingSources(true)}
                    className="underline cursor-pointer bg-transparent border-none text-accent p-0 text-xs">
                    Choose what to back up
                  </button>
                </div>
              ) : (
                <>
                  <ul className="text-xs font-mono m-0 pl-4 text-text-primary">
                    {sources.map(p => {
                      const n = sourceDbs?.byScope.get(`source:${p}`) ?? 0
                      return (
                        <li key={p} className="truncate">
                          {p}
                          {snapModes[p] && (
                            <span title={snapModes[p] === 'strict'
                                    ? 'Read from a ZFS or btrfs snapshot; the backup fails if it cannot be'
                                    : 'Read from a ZFS or btrfs snapshot; read live with a warning if it cannot be'}
                              className="ml-2 text-[10px] font-sans px-1 rounded bg-surface-tertiary text-text-secondary">
                              snapshot: {snapModes[p]}
                            </span>
                          )}
                          {n > 0 && (
                            <button onClick={() => {
                              setShowBackupModal(false)
                              navigateTo({ view: 'databases', scope: `source:${p}` })
                            }}
                              title="live databases under this source are captured uncovered"
                              className="ml-2 px-1.5 rounded text-[10px] font-sans font-semibold
                                         bg-status-error text-white border-none cursor-pointer">
                              {n} uncovered database{n === 1 ? '' : 's'}
                            </button>
                          )}
                        </li>
                      )
                    })}
                  </ul>
                  {excludes.length > 0 && (
                    <>
                      <div className="text-xs text-text-secondary mt-2 mb-1">Excluding</div>
                      <ul className="text-xs font-mono m-0 pl-4 text-text-primary">
                        {excludes.map(p => <li key={p} className="truncate">{p}</li>)}
                      </ul>
                    </>
                  )}
                  <button onClick={() => setEditingSources(true)}
                    className="underline cursor-pointer bg-transparent border-none text-accent p-0 text-xs mt-1.5">
                    Edit
                  </button>
                </>
              )}
            </div>
            <div className="flex justify-end gap-2">
              <button onClick={() => setShowBackupModal(false)}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none"
                disabled={backupStarting}>Cancel</button>
              <button onClick={handleStartBackup}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text
                           hover:bg-accent-hover border-none disabled:opacity-50"
                disabled={backupStarting || sources.length === 0 || editingSources}>
                {backupStarting ? 'Starting...' : 'Start Backup'}</button>
            </div>
          </div>
        </Overlay>
      )}
    </div>
  )
}

/*
 * Snapshot mode per source (db-detection.md 9.4, Manual 6.6): read the source
 * from a ZFS or btrfs snapshot the helper takes for the backup, so a live
 * database in it is captured at one instant. Strict fails the backup when the
 * source cannot be read that way; warn reads it live and the job says so.
 */
function SnapshotModes({ paths, modes, onChange }: {
  paths: string[]
  modes: Record<string, SnapshotMode>
  onChange: (path: string, mode: SnapshotMode | null) => void
}): React.ReactElement | null {
  if (paths.length === 0) return null
  return (
    <div className="mt-2">
      <div className="text-xs text-text-secondary mb-1.5">Read from a snapshot (ZFS or btrfs)</div>
      <ul className="m-0 p-0 list-none space-y-1">
        {paths.map(p => (
          <li key={p} className="flex items-center gap-2 text-xs">
            <span className="font-mono truncate flex-1 text-text-primary">{p}</span>
            <select
              aria-label={`Snapshot mode for ${p}`}
              value={modes[p] ?? 'off'}
              onChange={e => onChange(p, e.target.value === 'off' ? null : e.target.value as SnapshotMode)}
              className="text-xs rounded bg-surface-tertiary text-text-primary border border-border-default px-1 py-0.5">
              <option value="off">Off (read live)</option>
              <option value="strict">Strict (fail if no snapshot)</option>
              <option value="warn">Warn (read live, said)</option>
            </select>
          </li>
        ))}
      </ul>
      <div className="text-xs text-text-tertiary mt-1">
        For a live database: one consistent instant, no pause. Needs the helper and a dataset or pool its policy lists (Manual 6.6).
      </div>
    </div>
  )
}

function Overlay({ children }: { children: React.ReactNode }) {
  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
      {children}
    </div>
  )
}
