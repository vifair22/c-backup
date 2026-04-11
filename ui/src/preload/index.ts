import { contextBridge, ipcRenderer } from 'electron'
import { IPC } from '../shared/types'
import type { ConnectionConfig, ConnectionState } from '../shared/types'

export interface CBackupApi {
  // Connection management
  connectionAdd(config: ConnectionConfig): Promise<{ ok: boolean; error?: string }>
  connectionRemove(name: string): Promise<{ ok: boolean; error?: string }>
  connectionConnect(name: string, credentials?: { password?: string; sudoPassword?: string }): Promise<{ ok: boolean; error?: string }>
  connectionEdit(name: string, config: ConnectionConfig): Promise<{ ok: boolean; error?: string }>
  connectionRestart(name: string): Promise<{ ok: boolean; error?: string }>
  connectionDisconnect(name: string): Promise<{ ok: boolean }>
  connectionList(): Promise<ConnectionState[]>
  connectionStatus(name: string): Promise<ConnectionState | null>

  // Repo management
  repoInit(connectionName: string, repoPath: string): Promise<{ ok: boolean; error?: string; needsCredentials?: boolean }>
  repoAdd(connectionName: string, repoPath: string): Promise<{ ok: boolean; error?: string }>
  repoRemove(connectionName: string, repoPath: string): Promise<{ ok: boolean; error?: string }>
  repoEdit(connectionName: string, oldPath: string, newPath: string): Promise<{ ok: boolean; error?: string }>
  repoOpen(connectionName: string, repoPath: string): Promise<{ ok: boolean; error?: string; version?: string; needsCredentials?: boolean }>
  repoClose(connectionName: string, repoPath: string): Promise<{ ok: boolean }>

  // RPC passthrough
  rpcCall<T = unknown>(connectionName: string, repoPath: string, action: string, params?: Record<string, unknown>): Promise<T>

  // Credentials
  credentialHas(connectionName: string): Promise<boolean>
  credentialDelete(connectionName: string): Promise<{ ok: boolean }>
}

const api: CBackupApi = {
  connectionAdd: (config) => ipcRenderer.invoke(IPC.CONNECTION_ADD, config),
  connectionRemove: (name) => ipcRenderer.invoke(IPC.CONNECTION_REMOVE, name),
  connectionConnect: (name, creds) => ipcRenderer.invoke(IPC.CONNECTION_CONNECT, name, creds),
  connectionEdit: (name, config) => ipcRenderer.invoke(IPC.CONNECTION_EDIT, name, config),
  connectionRestart: (name) => ipcRenderer.invoke(IPC.CONNECTION_RESTART, name),
  connectionDisconnect: (name) => ipcRenderer.invoke(IPC.CONNECTION_DISCONNECT, name),
  connectionList: () => ipcRenderer.invoke(IPC.CONNECTION_LIST),
  connectionStatus: (name) => ipcRenderer.invoke(IPC.CONNECTION_STATUS, name),

  repoInit: (conn, path) => ipcRenderer.invoke(IPC.REPO_INIT, conn, path),
  repoAdd: (conn, path) => ipcRenderer.invoke(IPC.REPO_ADD, conn, path),
  repoRemove: (conn, path) => ipcRenderer.invoke(IPC.REPO_REMOVE, conn, path),
  repoEdit: (conn, oldPath, newPath) => ipcRenderer.invoke(IPC.REPO_EDIT, conn, oldPath, newPath),
  repoOpen: (conn, path) => ipcRenderer.invoke(IPC.REPO_OPEN, conn, path),
  repoClose: (conn, path) => ipcRenderer.invoke(IPC.REPO_CLOSE, conn, path),

  rpcCall: (conn, path, action, params) => ipcRenderer.invoke(IPC.RPC_CALL, conn, path, action, params),

  credentialHas: (name) => ipcRenderer.invoke(IPC.CREDENTIAL_HAS, name),
  credentialDelete: (name) => ipcRenderer.invoke(IPC.CREDENTIAL_DELETE, name),
}

contextBridge.exposeInMainWorld('cbackup', api)
