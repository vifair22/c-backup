/* ------------------------------------------------------------------ */
/* Connection types                                                    */
/* ------------------------------------------------------------------ */

export type ConnectionType = 'local' | 'ssh'

/*
 * A connection is a c-backupd instance, not a machine to run a binary on.
 *
 * The daemon owns every repo on its host and is the only writer, so the app no
 * longer spawns processes, escalates with sudo, or manages SSH children. Local
 * means the daemon's unix socket; remote means its TCP port with a bearer
 * token. Everything that made the old transport complicated lived on the
 * client side and is simply gone.
 */
export interface LocalConnectionConfig {
  type: 'local'
  name: string
  /** Unix socket path. Access is gated by filesystem permissions, so there is
   *  no token: if you can open the socket, you are authorised. */
  socketPath: string
}

export interface RemoteConnectionConfig {
  type: 'remote'
  name: string
  host: string
  port: number
  /** Identifier for the bearer token in Electron safeStorage. The token
   *  itself is never written to the config file. */
  tokenId?: string
}

export type ConnectionConfig = LocalConnectionConfig | RemoteConnectionConfig

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

/*
 * Repos are registered with the daemon and addressed by id. The path still
 * matters to a human choosing one, but it is no longer the handle: a repo can
 * be moved on disk and re-registered while keeping its identity, which is what
 * the .cbackup marker's uuid is for.
 */
export interface RepoState {
  id: number
  path: string
  label?: string
  uuid?: string
}

/* ------------------------------------------------------------------ */
/* RPC types                                                           */
/* ------------------------------------------------------------------ */

/* What /readyz reports. Replaces the session banner the old subprocess
 * transport printed on stdout before accepting commands. */
export interface DaemonInfo {
  status: string
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
