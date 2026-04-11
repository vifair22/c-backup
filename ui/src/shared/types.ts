/* ------------------------------------------------------------------ */
/* Connection types                                                    */
/* ------------------------------------------------------------------ */

export type ConnectionType = 'local' | 'ssh'

export interface LocalConnectionConfig {
  type: 'local'
  name: string
  binaryPath: string       // defaults to 'backup' in PATH
  sudo: boolean            // run via sudo -S
  repos: string[]          // absolute repo paths on this connection
}

export interface SshConnectionConfig {
  type: 'ssh'
  name: string
  host: string
  port: number             // defaults to 22
  username: string
  authMethod: 'key' | 'password' | 'agent'
  keyFilePath?: string     // for key auth
  remoteBinaryPath: string // defaults to 'backup'
  sudo: boolean            // run via sudo -S on remote host
  repos: string[]          // absolute repo paths on this connection
}

export type ConnectionConfig = LocalConnectionConfig | SshConnectionConfig

export type ConnectionStatus = 'disconnected' | 'connecting' | 'connected' | 'error'

export interface ConnectionState {
  config: ConnectionConfig
  status: ConnectionStatus
  error?: string
  repos: RepoState[]
}

/* ------------------------------------------------------------------ */
/* Repository types                                                    */
/* ------------------------------------------------------------------ */

export interface RepoState {
  path: string
  sessionActive: boolean
}

/* ------------------------------------------------------------------ */
/* RPC types                                                           */
/* ------------------------------------------------------------------ */

export interface RpcReadyBanner {
  status: 'ready'
  protocol: number
  compression: string
  lock: boolean
  version: string
}

export interface RpcResponse<T = unknown> {
  status: 'ok' | 'error'
  data?: T
  message?: string
}

/* ------------------------------------------------------------------ */
/* App config (persisted to disk)                                      */
/* ------------------------------------------------------------------ */

export interface AppConfig {
  connections: ConnectionConfig[]
  windowBounds?: { x: number; y: number; width: number; height: number }
}

/* ------------------------------------------------------------------ */
/* IPC channel names                                                   */
/* ------------------------------------------------------------------ */

export const IPC = {
  // Connection management
  CONNECTION_LIST: 'connection:list',
  CONNECTION_ADD: 'connection:add',
  CONNECTION_REMOVE: 'connection:remove',
  CONNECTION_CONNECT: 'connection:connect',
  CONNECTION_DISCONNECT: 'connection:disconnect',
  CONNECTION_STATUS: 'connection:status',

  CONNECTION_EDIT: 'connection:edit',
  CONNECTION_RESTART: 'connection:restart',

  // Repo management
  REPO_ADD: 'repo:add',
  REPO_REMOVE: 'repo:remove',
  REPO_INIT: 'repo:init',
  REPO_EDIT: 'repo:edit',
  REPO_OPEN: 'repo:open',
  REPO_CLOSE: 'repo:close',

  // RPC passthrough
  RPC_CALL: 'rpc:call',

  // Credential management
  CREDENTIAL_HAS: 'credential:has',
  CREDENTIAL_DELETE: 'credential:delete',
} as const
