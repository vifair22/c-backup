import { contextBridge, ipcRenderer } from 'electron'
import { IPC } from '../shared/types'
import type { ConnectionConfig, ConnectionState, ConnectResult, CredentialInfo } from '../shared/types'
import type { ApiCallResult, ApiRequest } from '../shared/api'

export interface CBackupApi {
  // Connection management
  connectionAdd(config: ConnectionConfig): Promise<{ ok: boolean; error?: string }>
  connectionRemove(name: string): Promise<{ ok: boolean; error?: string }>
  /* A password is only for a remote daemon; remember keeps the session
   * token it is exchanged for, never the password. */
  connectionConnect(name: string, credentials?: { password?: string; remember?: boolean }): Promise<ConnectResult>
  /* A rename keeps the connection as it is; a new address reconnects a live
   * one, which may need the new daemon's password. */
  connectionEdit(name: string, config: ConnectionConfig): Promise<ConnectResult>
  connectionRestart(name: string): Promise<ConnectResult>
  connectionDisconnect(name: string): Promise<{ ok: boolean }>
  /* Revoke the session on the daemon and forget the stored token. */
  connectionSignOut(name: string): Promise<{ ok: boolean; error?: string }>
  connectionList(): Promise<ConnectionState[]>
  connectionStatus(name: string): Promise<ConnectionState | null>

  // Repo management. Adding creates the repository when it does not exist.
  repoAdd(connectionName: string, repoPath: string): Promise<{ ok: boolean; error?: string }>
  repoRemove(connectionName: string, repoId: number): Promise<{ ok: boolean; error?: string }>

  /*
   * The typed call. A request the union does not describe will not compile,
   * and every request it does describe is checked against a live daemon by
   * the contract suite.
   */
  apiCall<T = unknown>(connectionName: string, req: ApiRequest): Promise<T>
  /* The same, with a refusal returned rather than thrown, so its fields
   * survive the IPC boundary that keeps only an error's message. */
  apiCallResult<T = unknown>(connectionName: string, req: ApiRequest): Promise<ApiCallResult<T>>


  // Whether stored tokens are encrypted, so the sign-in dialog can say so.
  credentialInfo(): Promise<CredentialInfo>
}

/*
 * Electron rewrites an error thrown in the main process into
 *   Error invoking remote method 'api:call': SomeError: the real message
 * and every screen printed the whole thing. The useful half is the tail, and
 * the prefix names an IPC channel that means nothing to the person reading
 * it - it turned "no daemon at /run/c-backup.sock" into a line about remote
 * method invocation that reads like the app itself is broken.
 */
function unwrap(err: unknown): never {
  const raw = err instanceof Error ? err.message : String(err)
  const m = raw.match(/^Error invoking remote method '[^']*':\s*(?:\w*Error:\s*)?(.*)$/s)
  throw new Error(m ? m[1] : raw)
}

const api: CBackupApi = {
  connectionAdd: (config) => ipcRenderer.invoke(IPC.CONNECTION_ADD, config),
  connectionRemove: (name) => ipcRenderer.invoke(IPC.CONNECTION_REMOVE, name),
  connectionConnect: (name, creds) => ipcRenderer.invoke(IPC.CONNECTION_CONNECT, name, creds),
  connectionEdit: (name, config) => ipcRenderer.invoke(IPC.CONNECTION_EDIT, name, config),
  connectionRestart: (name) => ipcRenderer.invoke(IPC.CONNECTION_RESTART, name),
  connectionDisconnect: (name) => ipcRenderer.invoke(IPC.CONNECTION_DISCONNECT, name),
  connectionSignOut: (name) => ipcRenderer.invoke(IPC.CONNECTION_SIGN_OUT, name),
  connectionList: () => ipcRenderer.invoke(IPC.CONNECTION_LIST),
  connectionStatus: (name) => ipcRenderer.invoke(IPC.CONNECTION_STATUS, name),

  repoAdd: (conn, path) => ipcRenderer.invoke(IPC.REPO_ADD, conn, path),
  repoRemove: (conn, repoId) => ipcRenderer.invoke(IPC.REPO_REMOVE, conn, repoId),

  apiCall: (conn, req) => ipcRenderer.invoke(IPC.API_CALL, conn, req).catch(unwrap),
  apiCallResult: (conn, req) => ipcRenderer.invoke(IPC.API_CALL_RESULT, conn, req).catch(unwrap),

  credentialInfo: () => ipcRenderer.invoke(IPC.CREDENTIAL_INFO),
}

contextBridge.exposeInMainWorld('cbackup', api)
