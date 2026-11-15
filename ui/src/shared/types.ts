/* ------------------------------------------------------------------ */
/* Connection types                                                    */
/* ------------------------------------------------------------------ */

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
}

export type ConnectionConfig = LocalConnectionConfig | RemoteConnectionConfig

/** Whether two configs reach the same daemon, whatever they are called. An
 *  edit that keeps the daemon is a rename and keeps the connection live; one
 *  that changes it drops the session, which belongs to the old daemon. */
export function sameDaemon(a: ConnectionConfig, b: ConnectionConfig): boolean {
  if (a.type === 'local') return b.type === 'local' && a.socketPath === b.socketPath
  return b.type === 'remote' && a.host === b.host && a.port === b.port
}

export type ConnectionStatus = 'disconnected' | 'connecting' | 'connected' | 'error'

export interface ConnectionState {
  config: ConnectionConfig
  status: ConnectionStatus
  error?: string
  repos: RepoState[]
  /** A remote daemon is waiting for the admin password: none is stored, or
   *  the stored session was refused. The UI prompts rather than retrying. */
  needsCredentials?: boolean
  /** A session token is stored for this connection, so it can reconnect at
   *  startup without asking and "Sign out" has something to revoke. */
  signedIn?: boolean
}

/** Outcome of a connect attempt, for the renderer to act on. */
export interface ConnectResult {
  ok: boolean
  error?: string
  needsCredentials?: boolean
}

/** What the credential store can promise, so the sign-in dialog can say it. */
export interface CredentialInfo {
  /** Tokens are encrypted with the OS keyring. When false they are still
   *  kept, base64 in a file only this account can read, and the UI says so. */
  encrypted: boolean
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

  // Repo management. Registration creates the directory and marker when it
  // does not exist, so adding and creating are one operation.
  REPO_ADD: 'repo:add',
  REPO_REMOVE: 'repo:remove',

  /*
   * The typed entry point. One channel rather than one per operation,
   * because the request is a discriminated union and the main process
   * switches on its `kind` with an exhaustiveness check - forty channels
   * would buy the same safety for forty times the wiring.
   */
  API_CALL: 'api:call',

  // Credentials. Sign-out revokes the session on the daemon and forgets it.
  CREDENTIAL_INFO: 'credential:info',
  CONNECTION_SIGN_OUT: 'connection:signOut',
} as const
