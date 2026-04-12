import React, { useCallback, useEffect, useState } from 'react'
import type { ConnectionState, ConnectionConfig } from '../shared/types'
import { AddConnectionDialog } from './AddConnectionDialog'
import { ContextMenu, type ContextMenuItem } from './ContextMenu'
import { ConfirmDialog } from './ConfirmDialog'
import { RepoView } from './RepoView'
import { SnapshotList } from './SnapshotList'
import { SnapshotBrowser } from './SnapshotBrowser'
import { SnapshotDiff } from './SnapshotDiff'
import { FileSearch } from './FileSearch'
import { PolicyEditor } from './PolicyEditor'
import { HashLookup } from './HashLookup'
import { LooseObjects } from './LooseObjects'
import { PackBrowser } from './PackBrowser'
import { JournalView } from './JournalView'
import { TaskBar, type TaskInfo } from './TaskBar'
import { TaskListView } from './TaskListView'
import { TagsView } from './TagsView'
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
  const [activeRepo, setActiveRepo] = useState<{ conn: string; path: string } | null>(null)
  // Navigation history
  type ViewState = { view: 'dashboard' } | { view: 'snapshots' } | { view: 'snapshot-browser'; snapId: number; path?: string } | { view: 'diff'; snapA?: number; snapB?: number } | { view: 'search' } | { view: 'policy' } | { view: 'journal' } | { view: 'tags' } | { view: 'tasks' } | { view: 'hash-lookup' } | { view: 'loose-objects' } | { view: 'pack-browser' }
  const [navHistory, setNavHistory] = useState<ViewState[]>([{ view: 'dashboard' }])
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
  const [taskList, setTaskList] = useState<TaskInfo[]>([])

  useEffect(() => {
    if (!activeRepo) { setTaskList([]); return }
    let cancelled = false
    const poll = async () => {
      try {
        const resp = await api.rpcCall<{ tasks: TaskInfo[] }>(activeRepo.conn, activeRepo.path, 'task_list')
        if (!cancelled) setTaskList(resp.tasks)
      } catch { /* ignore */ }
    }
    poll()
    const interval = setInterval(poll, 2000)
    return () => { cancelled = true; clearInterval(interval) }
  }, [activeRepo?.conn, activeRepo?.path])

  // Restore modal
  const [restoreModal, setRestoreModal] = useState<{ snapId: number; filePath?: string } | null>(null)
  const [restoreDest, setRestoreDest] = useState('')
  const [restoreVerify, setRestoreVerify] = useState(false)
  const [restoreStarting, setRestoreStarting] = useState(false)

  // Backup modal
  const [showBackupModal, setShowBackupModal] = useState(false)
  const [backupUsePolicy, setBackupUsePolicy] = useState(true)
  const [backupVerify, setBackupVerify] = useState(false)
  const [backupStarting, setBackupStarting] = useState(false)

  // Dialogs
  const [showAddConnection, setShowAddConnection] = useState(false)
  const [editingConnection, setEditingConnection] = useState<ConnectionConfig | null>(null)
  const [connectingName, setConnectingName] = useState<string | null>(null)
  const [pendingRepoOpen, setPendingRepoOpen] = useState<{ conn: string; path: string } | null>(null)
  const [sudoPasswordInput, setSudoPasswordInput] = useState('')
  const [savePassword, setSavePassword] = useState(true)
  const [addRepoConn, setAddRepoConn] = useState<string | null>(null)
  const [repoPathInput, setRepoPathInput] = useState('')
  const [postCreateConn, setPostCreateConn] = useState<string | null>(null)
  const [editingRepo, setEditingRepo] = useState<{ connName: string; oldPath: string } | null>(null)
  const [initRepoConn, setInitRepoConn] = useState<string | null>(null)
  const [initRepoPath, setInitRepoPath] = useState('')
  const [initRepoLoading, setInitRepoLoading] = useState(false)

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
    setPostCreateConn(config.name)
  }

  const handleEditConnection = async (config: ConnectionConfig) => {
    if (!editingConnection) return
    const originalName = editingConnection.name
    // No-op if nothing changed
    if (JSON.stringify(config) === JSON.stringify(editingConnection)) { setEditingConnection(null); return }
    setError(null)
    const result = await api.connectionEdit(originalName, config)
    if (!result.ok) setError(result.error ?? 'Failed to edit connection')
    await refreshConnections()
    setEditingConnection(null)
  }

  const handleConnectWithCredentials = async () => {
    if (!connectingName) return
    setError(null)
    const conn = connections.find(c => c.config.name === connectingName)
    const creds: { password?: string; sudoPassword?: string; save?: boolean } = { save: savePassword }
    if (conn?.config.type === 'ssh' && conn.config.authMethod === 'password') creds.password = sudoPasswordInput
    if (conn?.config.sudo) creds.sudoPassword = sudoPasswordInput

    setConnectingName(null)
    const result = await api.connectionConnect(connectingName, creds)
    if (!result.ok) { setError(result.error ?? 'Connection failed'); return }
    await refreshConnections()

    // If we were trying to do something, retry now
    if (pendingRepoOpen && pendingRepoOpen.conn === connectingName) {
      const pending = pendingRepoOpen
      setPendingRepoOpen(null)
      if (pending.path === '' && initRepoConn) {
        // Was trying to init a repo
        await handleInitRepo()
      } else {
        await handleOpenRepo(pending.conn, pending.path)
      }
    }
  }

  const handleRemoveConnection = async (name: string) => {
    setConfirmDialog({
      title: 'Delete Connection',
      message: `Delete "${name}"? All repo sessions will be closed and saved credentials will be removed.`,
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
    if (!result.ok) setError(result.error ?? 'Restart failed')
    await refreshConnections()
  }

  const handleAddRepo = async () => {
    if (!addRepoConn || !repoPathInput.trim()) return
    const connName = addRepoConn
    const path = repoPathInput.trim()
    setError(null)
    const result = await api.repoAdd(connName, path)
    if (!result.ok) { setError(result.error ?? 'Failed to add repo'); return }
    setAddRepoConn(null)
    setRepoPathInput('')
    await refreshConnections()

    // Probe the repo to verify it's reachable
    const probe = await api.repoOpen(connName, path)
    if (!probe.ok && !probe.needsCredentials) {
      setConfirmDialog({
        title: 'Repository Not Reachable',
        message: `Could not open "${path.split('/').pop() || path}": ${probe.error ?? 'unknown error'}. Keep it anyway?`,
        confirmLabel: 'Keep',
        cancelLabel: 'Remove',
        danger: false,
        onConfirm: () => setConfirmDialog(null),
        onCancel: async () => {
          setConfirmDialog(null)
          await api.repoRemove(connName, path)
          await refreshConnections()
        },
      })
    } else {
      await refreshConnections()
    }
  }

  const handleInitRepo = async () => {
    if (!initRepoConn || !initRepoPath.trim()) return
    const connName = initRepoConn
    const path = initRepoPath.trim()
    setError(null)
    setInitRepoLoading(true)
    const result = await api.repoInit(connName, path)
    setInitRepoLoading(false)
    if (!result.ok) {
      if (result.needsCredentials) {
        // Stash the init intent, show credential dialog
        setPendingRepoOpen({ conn: connName, path: '' }) // empty path signals init, not open
        setConnectingName(connName)
        setSudoPasswordInput('')
        setSavePassword(true)
        return
      }
      setError(result.error ?? 'Failed to create repo')
      return
    }
    setInitRepoConn(null)
    setInitRepoPath('')
    await refreshConnections()
  }

  const handleEditRepo = async () => {
    if (!editingRepo || !repoPathInput.trim()) return
    if (repoPathInput.trim() === editingRepo.oldPath) { setEditingRepo(null); return }
    setError(null)
    const result = await api.repoEdit(editingRepo.connName, editingRepo.oldPath, repoPathInput.trim())
    if (!result.ok) setError(result.error ?? 'Failed to edit repo')
    if (activeRepo?.conn === editingRepo.connName && activeRepo?.path === editingRepo.oldPath) {
      setActiveRepo({ conn: editingRepo.connName, path: repoPathInput.trim() })
    }
    await refreshConnections()
    setEditingRepo(null)
    setRepoPathInput('')
  }

  const handleRemoveRepo = (connName: string, repoPath: string) => {
    setConfirmDialog({
      title: 'Remove Repository',
      message: `Remove "${repoPath.split('/').pop() || repoPath}" from this connection? This does not delete the actual data.`,
      confirmLabel: 'Remove',
      onConfirm: async () => {
        setConfirmDialog(null)
        await api.repoRemove(connName, repoPath)
        if (activeRepo?.conn === connName && activeRepo?.path === repoPath) {
          setActiveRepo(null)
          setNavHistory([{ view: 'dashboard' }])
          setNavIndex(0)
          setTaskList([])
        }
        await refreshConnections()
      }
    })
  }

  const handleOpenRepo = async (connName: string, repoPath: string) => {
    setError(null)
    setActiveRepo({ conn: connName, path: repoPath })
    setNavHistory([{ view: 'dashboard' }])
    setNavIndex(0)
    try {
      const result = await api.repoOpen(connName, repoPath)
      if (!result.ok) {
        if (result.needsCredentials) {
          setPendingRepoOpen({ conn: connName, path: repoPath })
          setConnectingName(connName)
          setSudoPasswordInput('')
          setSavePassword(true)
          return
        }
        setError(result.error ?? 'Failed to open repo')
        return
      }
      await refreshConnections()
    } catch (err) {
      setError(`RPC error: ${err}`)
    }
  }

  /* ---------------------------------------------------------------- */
  /* Task operations                                                   */
  /* ---------------------------------------------------------------- */

  const handleStartBackup = async () => {
    if (!activeRepo) return
    setBackupStarting(true)
    setError(null)
    try {
      const params: Record<string, unknown> = { command: 'run' }
      if (backupVerify) params.verify_after = true
      await api.rpcCall(activeRepo.conn, activeRepo.path, 'task_start', params)
      setShowBackupModal(false)
      navigateTo({ view: 'tasks' })
    } catch (err) {
      setError(String(err))
    }
    setBackupStarting(false)
  }

  const handleStartRestore = async () => {
    if (!activeRepo || !restoreModal || !restoreDest.trim()) return
    setRestoreStarting(true)
    setError(null)
    try {
      const params: Record<string, unknown> = {
        command: 'restore',
        snapshot: String(restoreModal.snapId),
        dest: restoreDest.trim(),
      }
      if (restoreModal.filePath) params.file = restoreModal.filePath
      if (restoreVerify) params.verify = true
      await api.rpcCall(activeRepo.conn, activeRepo.path, 'task_start', params)
      setRestoreModal(null)
      navigateTo({ view: 'tasks' })
    } catch (err) {
      setError(String(err))
    }
    setRestoreStarting(false)
  }

  const handleStartOperation = (command: string) => {
    if (!activeRepo) return
    const labels: Record<string, string> = { verify: 'Verify', pack: 'Pack', gc: 'Garbage Collect' }
    setConfirmDialog({
      title: `Run ${labels[command] ?? command}`,
      message: `Start a background ${labels[command] ?? command} operation on this repository?`,
      confirmLabel: 'Start',
      danger: false,
      onConfirm: async () => {
        setConfirmDialog(null)
        setError(null)
        try {
          await api.rpcCall(activeRepo.conn, activeRepo.path, 'task_start', { command })
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

  const showConnectionMenu = (e: React.MouseEvent, conn: ConnectionState) => {
    e.preventDefault()
    setContextMenu({
      x: e.clientX, y: e.clientY,
      items: [
        { label: 'Edit', onClick: () => setEditingConnection(conn.config) },
        { label: 'Add Repo', onClick: () => { setAddRepoConn(conn.config.name); setRepoPathInput('') } },
        { label: 'Create New Repo', onClick: () => { setInitRepoConn(conn.config.name); setInitRepoPath('') } },
        ...(conn.status === 'connected' ? [
          { label: 'Disconnect', onClick: async () => {
            await api.connectionDisconnect(conn.config.name)
            if (activeRepo?.conn === conn.config.name) {
              setActiveRepo(null)
              setNavHistory([{ view: 'dashboard' }])
              setNavIndex(0)
              setTaskList([])
            }
            await refreshConnections()
          }},
          { label: 'Restart', onClick: () => handleRestartConnection(conn.config.name) },
        ] : []),
        { label: 'Delete', danger: true, onClick: () => handleRemoveConnection(conn.config.name) },
      ]
    })
  }

  const showRepoMenu = (e: React.MouseEvent, connName: string, repoPath: string) => {
    e.preventDefault()
    e.stopPropagation()
    const repo = connections.find(c => c.config.name === connName)?.repos.find(r => r.path === repoPath)
    setContextMenu({
      x: e.clientX, y: e.clientY,
      items: [
        ...(repo?.sessionActive ? [{
          label: 'Disconnect', onClick: async () => {
            await api.repoClose(connName, repoPath)
            if (activeRepo?.conn === connName && activeRepo?.path === repoPath) {
              setActiveRepo(null)
              setNavHistory([{ view: 'dashboard' }])
              setNavIndex(0)
              setTaskList([])
            }
            await refreshConnections()
          }
        }] : []),
        { label: 'Edit Path', onClick: () => { setEditingRepo({ connName, oldPath: repoPath }); setRepoPathInput(repoPath) } },
        { label: 'Remove', danger: true, onClick: () => handleRemoveRepo(connName, repoPath) },
      ]
    })
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

  const connLabel = (conn: ConnectionState) => {
    const parts = [conn.config.type === 'ssh' ? 'SSH' : 'local']
    if (conn.config.sudo) parts.push('sudo')
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

              {/* Repos */}
              {!collapsed.has(conn.config.name) && <div className="ml-4 mt-1">
                {conn.repos.map(repo => (
                  <div
                    key={repo.path}
                    onClick={() => handleOpenRepo(conn.config.name, repo.path)}
                    onContextMenu={e => showRepoMenu(e, conn.config.name, repo.path)}
                    className={`text-xs py-1 px-1.5 rounded mb-0.5 cursor-pointer hover:bg-surface-hover ${
                      activeRepo?.conn === conn.config.name && activeRepo?.path === repo.path ? 'bg-surface-active' : ''
                    }`}
                  >
                    <span className="text-text-primary">{repo.path.split('/').pop() || repo.path}</span>
                    {repo.sessionActive && (
                      <span className="text-status-connected text-[10px] ml-1.5">connected</span>
                    )}
                  </div>
                ))}
                <button onClick={() => { setAddRepoConn(conn.config.name); setRepoPathInput('') }}
                  className="text-[10px] mt-0.5 cursor-pointer text-accent hover:text-accent-hover bg-transparent border-none px-1.5">
                  + Add repo
                </button>
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

        {activeRepo ? (
          <>
            {currentView.view === 'dashboard' && (
              <RepoView connName={activeRepo.conn} repoPath={activeRepo.path}
                onSelectSnapshot={(id) => navigateTo({ view: 'snapshot-browser', snapId: id })}
                onViewAllSnapshots={() => navigateTo({ view: 'snapshots' })}
                onCompareSnapshots={(a, b) => navigateTo({ view: 'diff', snapA: a, snapB: b })}
                onSearch={() => navigateTo({ view: 'search' })}
                onEditPolicy={() => navigateTo({ view: 'policy' })}
                onViewJournal={() => navigateTo({ view: 'journal' })}
                onViewTags={() => navigateTo({ view: 'tags' })}
                onViewTasks={() => navigateTo({ view: 'tasks' })}
                onHashLookup={() => navigateTo({ view: 'hash-lookup' })}
                onLooseObjects={() => navigateTo({ view: 'loose-objects' })}
                onPackBrowser={() => navigateTo({ view: 'pack-browser' })}
                onRunBackup={() => { setShowBackupModal(true); setBackupUsePolicy(true); setBackupVerify(false) }}
                onRunOperation={handleStartOperation}
              />
            )}
            {currentView.view === 'snapshots' && (
              <SnapshotList connName={activeRepo.conn} repoPath={activeRepo.path}
                onSelectSnapshot={(id) => navigateTo({ view: 'snapshot-browser', snapId: id })}
                onCompareSnapshots={() => navigateTo({ view: 'diff' })}
                onBack={navBack}
              />
            )}
            {currentView.view === 'snapshot-browser' && (
              <SnapshotBrowser connName={activeRepo.conn} repoPath={activeRepo.path}
                snapId={currentView.snapId}
                initialPath={currentView.path}
                onBack={navBack}
                onRestore={(sid, fp) => { setRestoreModal({ snapId: sid, filePath: fp }); setRestoreDest(''); setRestoreVerify(false) }}
              />
            )}
            {currentView.view === 'tasks' && (
              <TaskListView tasks={taskList} connName={activeRepo.conn} repoPath={activeRepo.path}
                onBack={navBack}
              />
            )}
            {currentView.view === 'tags' && (
              <TagsView connName={activeRepo.conn} repoPath={activeRepo.path}
                onSelectSnapshot={(id) => navigateTo({ view: 'snapshot-browser', snapId: id })}
                onBack={navBack}
              />
            )}
            {currentView.view === 'journal' && (
              <JournalView connName={activeRepo.conn} repoPath={activeRepo.path}
                onBack={navBack}
              />
            )}
            {currentView.view === 'loose-objects' && (
              <LooseObjects connName={activeRepo.conn} repoPath={activeRepo.path}
                onBack={navBack}
              />
            )}
            {currentView.view === 'pack-browser' && (
              <PackBrowser connName={activeRepo.conn} repoPath={activeRepo.path}
                onBack={navBack}
              />
            )}
            {currentView.view === 'hash-lookup' && (
              <HashLookup connName={activeRepo.conn} repoPath={activeRepo.path}
                onNavigateToSnapshot={(id) => navigateTo({ view: 'snapshot-browser', snapId: id })}
                onBack={navBack}
              />
            )}
            {currentView.view === 'policy' && (
              <PolicyEditor connName={activeRepo.conn} repoPath={activeRepo.path}
                onBack={navBack}
              />
            )}
            {currentView.view === 'search' && (
              <FileSearch connName={activeRepo.conn} repoPath={activeRepo.path}
                onNavigateToFile={(snapId, path) => navigateTo({ view: 'snapshot-browser', snapId, path })}
                onBack={navBack}
              />
            )}
            {currentView.view === 'diff' && (
              <SnapshotDiff connName={activeRepo.conn} repoPath={activeRepo.path}
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
          </div>
        )}
      </div>
      {/* Task status bar */}
      {activeRepo && (
        <TaskBar tasks={taskList} connName={activeRepo.conn} repoPath={activeRepo.path}
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

      {/* Credential prompt */}
      {connectingName && (
        <Overlay>
          <form onSubmit={e => { e.preventDefault(); handleConnectWithCredentials() }} className="bg-surface-primary rounded-lg p-5 w-80 shadow-xl border border-border-default">
            <h3 className="text-sm font-semibold m-0 mb-3">Connect: {connectingName}</h3>
            <div className="mb-3">
              <label className="block text-xs font-medium text-text-secondary mb-1">
                {connections.find(c => c.config.name === connectingName)?.config.sudo ? 'Sudo Password' : 'Password'}
              </label>
              <input type="password" value={sudoPasswordInput} onChange={e => setSudoPasswordInput(e.target.value)}
                className="w-full px-2 py-1 border border-border-default rounded text-sm bg-surface-primary text-text-primary focus:outline-none focus:border-accent"
                autoFocus required />
            </div>
            <div className="mb-3">
              <label className="text-xs flex items-center gap-1.5 cursor-pointer text-text-secondary">
                <input type="checkbox" checked={savePassword} onChange={e => setSavePassword(e.target.checked)} />
                Save password (encrypted in OS keychain)
              </label>
            </div>
            <div className="flex justify-end gap-2">
              <button type="button" onClick={() => { setConnectingName(null); setPendingRepoOpen(null) }}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none">Cancel</button>
              <button type="submit"
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text hover:bg-accent-hover border-none">Connect</button>
            </div>
          </form>
        </Overlay>
      )}

      {/* Add repo dialog */}
      {addRepoConn && (
        <Overlay>
          <form onSubmit={e => { e.preventDefault(); handleAddRepo() }} className="bg-surface-primary rounded-lg p-5 w-96 shadow-xl border border-border-default">
            <h3 className="text-sm font-semibold m-0 mb-2">Add Repository</h3>
            <div className="text-text-muted text-xs mb-3">Connection: {addRepoConn}</div>
            <div className="mb-3">
              <label className="block text-xs font-medium text-text-secondary mb-1">Repository Path (absolute)</label>
              <input value={repoPathInput} onChange={e => setRepoPathInput(e.target.value)}
                placeholder="/path/to/backup/repo"
                className="w-full px-2 py-1 border border-border-default rounded text-sm bg-surface-primary text-text-primary focus:outline-none focus:border-accent"
                autoFocus required />
            </div>
            <div className="flex justify-end gap-2">
              <button type="button" onClick={() => setAddRepoConn(null)}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none">Cancel</button>
              <button type="submit"
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text hover:bg-accent-hover border-none">Add</button>
            </div>
          </form>
        </Overlay>
      )}

      {/* Edit repo dialog */}
      {/* Create new repo dialog */}
      {initRepoConn && (
        <Overlay>
          <form onSubmit={e => { e.preventDefault(); handleInitRepo() }} className="bg-surface-primary rounded-lg p-5 w-96 shadow-xl border border-border-default">
            <h3 className="text-sm font-semibold m-0 mb-2">Create New Repository</h3>
            <div className="text-text-muted text-xs mb-3">Connection: {initRepoConn}</div>
            <div className="mb-3">
              <label className="block text-xs font-medium text-text-secondary mb-1">Repository Path (absolute)</label>
              <input value={initRepoPath} onChange={e => setInitRepoPath(e.target.value)}
                placeholder="/path/to/new/repo"
                className="w-full px-2 py-1 border border-border-default rounded text-sm bg-surface-primary text-text-primary focus:outline-none focus:border-accent"
                autoFocus required />
              <div className="text-[10px] text-text-muted mt-1">The directory will be created and initialized as a c-backup repository.</div>
            </div>
            <div className="flex justify-end gap-2">
              <button type="button" onClick={() => setInitRepoConn(null)}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none"
                disabled={initRepoLoading}>Cancel</button>
              <button type="submit"
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text hover:bg-accent-hover border-none"
                disabled={initRepoLoading}>{initRepoLoading ? 'Creating...' : 'Create'}</button>
            </div>
          </form>
        </Overlay>
      )}

      {editingRepo && (
        <Overlay>
          <form onSubmit={e => { e.preventDefault(); handleEditRepo() }} className="bg-surface-primary rounded-lg p-5 w-96 shadow-xl border border-border-default">
            <h3 className="text-sm font-semibold m-0 mb-2">Edit Repository</h3>
            <div className="text-text-muted text-xs mb-3">Connection: {editingRepo.connName}</div>
            <div className="mb-3">
              <label className="block text-xs font-medium text-text-secondary mb-1">Repository Path (absolute)</label>
              <input value={repoPathInput} onChange={e => setRepoPathInput(e.target.value)}
                placeholder="/path/to/backup/repo"
                className="w-full px-2 py-1 border border-border-default rounded text-sm bg-surface-primary text-text-primary focus:outline-none focus:border-accent"
                autoFocus required />
            </div>
            <div className="flex justify-end gap-2">
              <button type="button" onClick={() => setEditingRepo(null)}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none">Cancel</button>
              <button type="submit"
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text hover:bg-accent-hover border-none">Save</button>
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
              <label className="block text-xs font-medium text-text-secondary mb-1">Destination Path</label>
              <input value={restoreDest} onChange={e => setRestoreDest(e.target.value)}
                placeholder="/path/to/restore/destination"
                className="w-full px-2 py-1 border border-border-default rounded text-sm bg-surface-primary text-text-primary focus:outline-none focus:border-accent font-mono"
                autoFocus required />
            </div>
            <div className="mb-4">
              <label className="text-xs flex items-center gap-1.5 cursor-pointer text-text-secondary">
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
            <div className="mb-3">
              <label className="text-xs flex items-center gap-1.5 cursor-pointer text-text-secondary">
                <input type="checkbox" checked={backupVerify} onChange={e => setBackupVerify(e.target.checked)} />
                Verify after backup
              </label>
            </div>
            <div className="flex justify-end gap-2">
              <button onClick={() => setShowBackupModal(false)}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none"
                disabled={backupStarting}>Cancel</button>
              <button onClick={handleStartBackup}
                className="px-3 py-1.5 text-xs cursor-pointer rounded bg-accent text-accent-text hover:bg-accent-hover border-none"
                disabled={backupStarting}>{backupStarting ? 'Starting...' : 'Start Backup'}</button>
            </div>
          </div>
        </Overlay>
      )}
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
