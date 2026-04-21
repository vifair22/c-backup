import { app, BrowserWindow, ipcMain } from 'electron'
import { join } from 'path'
import { ConnectionManager } from './connection-manager'
import { loadConfig, loadCredentials, updateWindowBounds } from './config-store'
import { IPC } from '../shared/types'
import type { ConnectionConfig } from '../shared/types'

let mainWindow: BrowserWindow | null = null
const connectionManager = new ConnectionManager()

function createWindow(): void {
  const config = loadConfig()
  const bounds = config.windowBounds ?? { width: 1200, height: 800 }

  mainWindow = new BrowserWindow({
    ...bounds,
    minWidth: 800,
    minHeight: 600,
    webPreferences: {
      preload: join(__dirname, '../preload/index.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
    title: 'c-backup',
  })

  // Save window bounds on move/resize
  const saveBounds = () => {
    if (mainWindow && !mainWindow.isDestroyed()) {
      const b = mainWindow.getBounds()
      updateWindowBounds(b)
    }
  }
  mainWindow.on('resized', saveBounds)
  mainWindow.on('moved', saveBounds)

  // Log renderer console to main process stdout
  mainWindow.webContents.on('console-message', (_e, level, message, line, sourceId) => {
    const levelStr = ['verbose', 'info', 'warn', 'error'][level] ?? 'log'
    console.log(`[renderer:${levelStr}] ${message} (${sourceId}:${line})`)
  })

  // Dev: load from vite dev server. Prod: load built file.
  if (process.env.ELECTRON_RENDERER_URL) {
    mainWindow.loadURL(process.env.ELECTRON_RENDERER_URL)
  } else {
    mainWindow.loadFile(join(__dirname, '../renderer/index.html'))
  }
}

app.whenReady().then(() => {
  // Load persisted config and credentials
  const config = loadConfig()
  loadCredentials()

  // Restore connections and repos from config
  connectionManager.restoreFromConfig(config)

  registerIpcHandlers()
  createWindow()

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow()
  })
})

app.on('window-all-closed', () => {
  connectionManager.disconnectAll()
  if (process.platform !== 'darwin') app.quit()
})

/* ------------------------------------------------------------------ */
/* IPC handlers                                                        */
/* ------------------------------------------------------------------ */

function registerIpcHandlers(): void {
  ipcMain.handle(IPC.CONNECTION_ADD, async (_e, config: ConnectionConfig) => {
    return connectionManager.addConnection(config)
  })

  ipcMain.handle(IPC.CONNECTION_REMOVE, async (_e, name: string) => {
    return connectionManager.removeConnection(name)
  })

  ipcMain.handle(IPC.CONNECTION_CONNECT, async (_e, name: string, credentials?: { password?: string; sudoPassword?: string; save?: boolean }) => {
    return connectionManager.connect(name, credentials)
  })

  ipcMain.handle(IPC.CREDENTIAL_HAS, async (_e, name: string) => {
    return connectionManager.canAutoConnect(name)
  })

  ipcMain.handle(IPC.CREDENTIAL_DELETE, async (_e, name: string) => {
    const { deleteCredentials: delCreds } = await import('./config-store')
    delCreds(name)
    return { ok: true }
  })

  ipcMain.handle(IPC.CONNECTION_DISCONNECT, async (_e, name: string) => {
    return connectionManager.disconnect(name)
  })

  ipcMain.handle(IPC.CONNECTION_LIST, async () => {
    return connectionManager.listConnections()
  })

  ipcMain.handle(IPC.CONNECTION_STATUS, async (_e, name: string) => {
    return connectionManager.getStatus(name)
  })

  ipcMain.handle(IPC.CONNECTION_EDIT, async (_e, name: string, config: ConnectionConfig) => {
    return connectionManager.editConnection(name, config)
  })

  ipcMain.handle(IPC.CONNECTION_RESTART, async (_e, name: string) => {
    return connectionManager.restartConnection(name)
  })

  ipcMain.handle(IPC.REPO_INIT, async (_e, connectionName: string, repoPath: string) => {
    return connectionManager.initRepo(connectionName, repoPath)
  })

  ipcMain.handle(IPC.REPO_EDIT, async (_e, connectionName: string, oldPath: string, newPath: string) => {
    return connectionManager.editRepo(connectionName, oldPath, newPath)
  })

  ipcMain.handle(IPC.REPO_ADD, async (_e, connectionName: string, repoPath: string) => {
    return connectionManager.addRepo(connectionName, repoPath)
  })

  ipcMain.handle(IPC.REPO_REMOVE, async (_e, connectionName: string, repoPath: string) => {
    return connectionManager.removeRepo(connectionName, repoPath)
  })

  ipcMain.handle(IPC.REPO_OPEN, async (_e, connectionName: string, repoPath: string) => {
    return connectionManager.openRepo(connectionName, repoPath)
  })

  ipcMain.handle(IPC.REPO_CLOSE, async (_e, connectionName: string, repoPath: string) => {
    return connectionManager.closeRepo(connectionName, repoPath)
  })

  ipcMain.handle(IPC.RPC_CALL, async (_e, connectionName: string, repoPath: string, action: string, params?: Record<string, unknown>) => {
    return connectionManager.rpcCall(connectionName, repoPath, action, params)
  })
}
