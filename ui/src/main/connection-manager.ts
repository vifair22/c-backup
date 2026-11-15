/*
 * Connection manager.
 *
 * A connection is a c-backupd instance. Connecting means checking that the
 * daemon answers, signing in if it is remote, and listing the repos it owns;
 * it does not mean spawning anything, so there are no child processes to
 * supervise and no SSH lifecycle to coordinate.
 *
 * Repos are not configured per connection. The daemon knows which repos it
 * owns; the app asks.
 *
 * Persistence comes in through ConnectionStore rather than an import of the
 * Electron-backed config store, so this file runs under plain node and is
 * tested against a real daemon in tests/connections.test.ts. It used to be
 * reachable only through the window, and the remote path it carried had never
 * once been exercised.
 */

import { EventEmitter } from 'node:events'
/* With extensions, so tests/connections.test.ts can load this under node's
 * type stripping, which resolves import paths literally. */
import { DaemonClient, DaemonUnreachableError, notConfiguredMessage } from './rpc-session.ts'
import { toEndpoint } from './client.ts'
import { sameDaemon } from '../shared/types.ts'
import type { ApiRequest } from '../shared/api'
import type { WireRepo } from '../shared/wire'
import type {
  ConnectionConfig,
  ConnectionState,
  ConnectionStatus,
  ConnectResult,
  RepoState,
} from '../shared/types'

/*
 * What the manager needs kept between runs. Connections go to the config
 * file and session tokens to the credential file; the password itself is
 * never stored anywhere.
 */
export interface ConnectionStore {
  saveConnections(configs: ConnectionConfig[]): void
  getToken(name: string): string | undefined
  setToken(name: string, token: string): void
  deleteToken(name: string): void
}

/* For tests and for anything that should not touch disk. */
export class MemoryConnectionStore implements ConnectionStore {
  saved: ConnectionConfig[] = []
  tokens = new Map<string, string>()
  saveConnections(configs: ConnectionConfig[]): void { this.saved = configs.map((c) => ({ ...c })) }
  getToken(name: string): string | undefined { return this.tokens.get(name) }
  setToken(name: string, token: string): void { this.tokens.set(name, token) }
  deleteToken(name: string): void { this.tokens.delete(name) }
}

interface Entry {
  config: ConnectionConfig
  client: DaemonClient
  status: ConnectionStatus
  error?: string
  repos: RepoState[]
  version?: string
  needsCredentials?: boolean
}

function describe(config: ConnectionConfig): string {
  return config.type === 'local' ? config.socketPath : `${config.host}:${config.port}`
}

/* What to tell someone whose daemon did not answer. The remedy differs: a
 * local daemon is a systemctl away, a remote one could be down, firewalled,
 * or listening on loopback only, which is the default. */
function unreachableMessage(config: ConnectionConfig, err: DaemonUnreachableError): string {
  if (config.type === 'local') {
    return `${err.message}. Start it with 'systemctl start c-backupd'.`
  }
  return `${err.message}. Check that the host is up and that c-backupd there ` +
    `listens on the network: it binds 127.0.0.1 unless http.bind says otherwise.`
}

export class ConnectionManager extends EventEmitter {
  private entries = new Map<string, Entry>()
  private store: ConnectionStore

  constructor(store: ConnectionStore = new MemoryConnectionStore()) {
    super()
    this.store = store
  }

  /* ---------------------------------------------------------------- */
  /* Connection list, persisted                                         */
  /* ---------------------------------------------------------------- */

  /*
   * Every change to the list is saved. The save functions existed and nothing
   * called them, so a connection added in the window was gone on the next
   * launch; the e2e suites never noticed because they write config.json
   * themselves.
   */
  private persist(): void {
    this.store.saveConnections(Array.from(this.entries.values()).map((e) => e.config))
  }

  private newEntry(config: ConnectionConfig): Entry {
    const token = config.type === 'remote' ? this.store.getToken(config.name) : undefined
    return {
      config,
      client: new DaemonClient(config, token),
      status: 'disconnected',
      repos: [],
    }
  }

  addConnection(config: ConnectionConfig, opts: { persist?: boolean } = {}): { ok: boolean; error?: string } {
    if (this.entries.has(config.name)) {
      return { ok: false, error: `connection '${config.name}' already exists` }
    }
    this.entries.set(config.name, this.newEntry(config))
    if (opts.persist !== false) this.persist()
    this.emitChange()
    return { ok: true }
  }

  removeConnection(name: string): { ok: boolean; error?: string } {
    if (!this.entries.delete(name)) {
      return { ok: false, error: `no connection '${name}'` }
    }
    this.store.deleteToken(name)
    this.persist()
    this.emitChange()
    return { ok: true }
  }

  /*
   * Edit a connection: a new name, a new address, or both.
   *
   * A rename is a label change on the same daemon, so it keeps the entry -
   * client, in-memory token, status and repo list - and only rekeys it. It
   * used to rebuild the entry, which left a signed-in connection showing
   * "remote" and disconnected until somebody clicked Connect, and lost a
   * session that was not stored at all.
   *
   * A new address is a different daemon. The token goes, because a token
   * belongs to the daemon that issued it, and a connection that was live
   * reconnects to the new one: for a remote daemon that means asking for
   * its password, which the result says.
   */
  async editConnection(name: string, config: ConnectionConfig): Promise<ConnectResult> {
    const entry = this.entries.get(name)
    if (!entry) return { ok: false, error: `no connection '${name}'` }
    if (config.name !== name && this.entries.has(config.name)) {
      return { ok: false, error: `connection '${config.name}' already exists` }
    }

    if (sameDaemon(entry.config, config)) {
      if (config.name !== name) {
        const token = this.store.getToken(name)
        this.store.deleteToken(name)
        if (token) this.store.setToken(config.name, token)
      }
      /* The client keeps the config it was made with; only the name
       * differs, and the client never reads the name. */
      entry.config = config
      this.rekey(name, config.name, entry)
      this.persist()
      this.emitChange()
      return { ok: true }
    }

    const wasLive = entry.status === 'connected' || entry.status === 'connecting'
    this.store.deleteToken(name)
    this.rekey(name, config.name, this.newEntry(config))
    this.persist()
    this.emitChange()
    return wasLive ? this.connect(config.name) : { ok: true }
  }

  /* Replace a key in place, so an edit does not move the connection to the
   * bottom of the sidebar. */
  private rekey(from: string, to: string, entry: Entry): void {
    const next = new Map<string, Entry>()
    for (const [k, v] of this.entries) next.set(k === from ? to : k, k === from ? entry : v)
    this.entries = next
  }

  /* ---------------------------------------------------------------- */
  /* Connect and sign in                                                */
  /* ---------------------------------------------------------------- */

  /*
   * Connect: confirm the daemon is ready, sign in if it is remote, and load
   * its repo list.
   *
   * For a remote daemon the order is: a password if one was just typed;
   * otherwise the stored session token, if any; otherwise ask. A token the
   * daemon refuses (expired, revoked by a sign-out elsewhere, or the daemon's
   * database replaced) is forgotten and the user is asked, rather than
   * failing the connection with "unauthorized".
   */
  async connect(
    name: string,
    credentials?: { password?: string; remember?: boolean }
  ): Promise<ConnectResult> {
    const entry = this.entries.get(name)
    if (!entry) return { ok: false, error: `no connection '${name}'` }

    entry.status = 'connecting'
    entry.error = undefined
    entry.needsCredentials = false
    this.emitChange()

    try {
      const info = await entry.client.ready()
      entry.version = info.version

      if (entry.config.type === 'remote') {
        if (!(await entry.client.isSetup())) {
          return this.fail(entry, notConfiguredMessage(describe(entry.config)))
        }
        if (credentials?.password) {
          const token = await entry.client.login(credentials.password)
          /* By the entry's name now, not the one this was called with: a
           * rename while the login was in flight rekeys the entry. */
          if (credentials.remember) this.store.setToken(entry.config.name, token)
          else this.store.deleteToken(entry.config.name)
        } else if (!entry.client.hasToken()) {
          return this.askForPassword(entry)
        }
      }

      const repos = await this.loadRepos(entry)
      if (repos === 'unauthorized') {
        this.store.deleteToken(entry.config.name)
        entry.client.setToken(undefined)
        return this.askForPassword(entry)
      }
      entry.repos = repos
      entry.status = 'connected'
      this.emitChange()
      return { ok: true }
    } catch (err) {
      const message =
        err instanceof DaemonUnreachableError
          ? unreachableMessage(entry.config, err)
          : err instanceof Error
            ? err.message
            : String(err)
      return this.fail(entry, message)
    }
  }

  private fail(entry: Entry, message: string): ConnectResult {
    entry.status = 'error'
    entry.error = message
    entry.repos = []
    this.emitChange()
    return { ok: false, error: message }
  }

  private askForPassword(entry: Entry): ConnectResult {
    entry.status = 'disconnected'
    entry.needsCredentials = true
    entry.repos = []
    this.emitChange()
    return { ok: false, needsCredentials: true, error: 'password required' }
  }

  disconnect(name: string): { ok: boolean } {
    const entry = this.entries.get(name)
    if (!entry) return { ok: false }
    /* Nothing to tear down: there is no session, only requests. The stored
     * token stays, so connecting again does not ask. */
    entry.status = 'disconnected'
    entry.repos = []
    entry.needsCredentials = false
    this.emitChange()
    return { ok: true }
  }

  /*
   * Sign out: revoke the session on the daemon and forget the token. The
   * next connect asks for the password. Revocation is best effort, because a
   * daemon that cannot be reached still should not leave a token behind here.
   */
  async signOut(name: string): Promise<{ ok: boolean; error?: string }> {
    const entry = this.entries.get(name)
    if (!entry) return { ok: false, error: `no connection '${name}'` }
    try {
      await entry.client.logout()
    } catch {
      /* forgotten locally either way */
    }
    entry.client.setToken(undefined)
    this.store.deleteToken(name)
    this.disconnect(name)
    /* Signed out, and shown as that: the way back is the password. */
    entry.needsCredentials = true
    this.emitChange()
    return { ok: true }
  }

  /* ---------------------------------------------------------------- */
  /* Requests                                                          */
  /* ---------------------------------------------------------------- */

  /*
   * Through toEndpoint like everything else, so the set of paths this can
   * produce is fixed at compile time and checked against a live daemon by
   * tests/contract.test.ts.
   */
  private async requestVia<T>(entry: Entry, req: ApiRequest) {
    const ep = toEndpoint(req)
    return entry.client.request<T>(ep.method, ep.path, ep.body)
  }

  private async loadRepos(entry: Entry): Promise<RepoState[] | 'unauthorized'> {
    const res = await this.requestVia<{ repos?: WireRepo[] }>(
      entry, { kind: 'repoList' })
    if (res.status === 401) return 'unauthorized'
    if (res.status !== 200) throw new Error(`cannot list repos (HTTP ${res.status})`)
    return (res.body?.repos ?? []).map((r) => ({
      id: r.id,
      path: r.path,
      label: r.label ?? undefined,
      uuid: r.uuid,
    }))
  }

  /* A 401 after connecting means the session ended under us: the token
   * expired, or was revoked by a sign-out elsewhere. Say so and ask again,
   * rather than leave the sidebar on "connected" while every request fails. */
  private sessionEnded(entry: Entry): Error {
    this.store.deleteToken(entry.config.name)
    entry.client.setToken(undefined)
    this.askForPassword(entry)
    return new Error('the session with this daemon has ended: sign in again')
  }

  private async refreshRepos(entry: Entry): Promise<void> {
    const repos = await this.loadRepos(entry)
    if (repos === 'unauthorized') throw this.sessionEnded(entry)
    entry.repos = repos
    this.emitChange()
  }

  /* Registration creates the directory and marker if they do not exist, so
   * "add an existing repository" and "create a new one" are one operation. */
  async addRepo(
    name: string,
    repoPath: string,
    label?: string
  ): Promise<{ ok: boolean; error?: string }> {
    const entry = this.entries.get(name)
    if (!entry) return { ok: false, error: `no connection '${name}'` }
    try {
      const res = await this.requestVia<{ error?: string }>(entry, {
        kind: 'repoAdd', path: repoPath, label: label ?? '',
      })
      if (res.status === 401) throw this.sessionEnded(entry)
      if (res.status !== 200 && res.status !== 201) {
        return { ok: false, error: res.body?.error ?? `HTTP ${res.status}` }
      }
      await this.refreshRepos(entry)
      return { ok: true }
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : String(err) }
    }
  }

  /* By id. The renderer used to pass the path here, which became
   * /repos/NaN, and removing a repository from the sidebar did nothing. */
  async removeRepo(name: string, repoId: number): Promise<{ ok: boolean; error?: string }> {
    const entry = this.entries.get(name)
    if (!entry) return { ok: false, error: `no connection '${name}'` }
    if (!Number.isInteger(repoId) || repoId <= 0) {
      return { ok: false, error: `not a repository id: ${repoId}` }
    }
    try {
      const res = await this.requestVia<{ error?: string }>(entry, { kind: 'repoRemove', repoId })
      if (res.status === 401) throw this.sessionEnded(entry)
      if (res.status !== 204 && res.status !== 200) {
        return { ok: false, error: res.body?.error ?? `HTTP ${res.status}` }
      }
      await this.refreshRepos(entry)
      return { ok: true }
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : String(err) }
    }
  }

  /*
   * The renderer's only way to reach the daemon.
   *
   * A daemon that dies after a connection is established used to leave the
   * sidebar saying "connected" while every request failed, and a remote
   * session that expired did the same with "unauthorized". The status is what
   * somebody looks at to decide whether the problem is theirs, so it follows
   * the daemon rather than remembering the last time it was asked.
   */
  async apiCall<T = unknown>(name: string, req: ApiRequest): Promise<T> {
    const entry = this.entries.get(name)
    if (!entry) throw new Error(`no connection '${name}'`)
    if (entry.status !== 'connected') throw new Error(`'${name}' is not connected`)

    let res
    try {
      res = await this.requestVia<T>(entry, req)
    } catch (err) {
      if (err instanceof DaemonUnreachableError) {
        this.fail(entry, unreachableMessage(entry.config, err))
      }
      throw err
    }
    if (res.status === 401 && entry.config.type === 'remote') throw this.sessionEnded(entry)
    if (res.status >= 400) {
      const body = res.body as { error?: string } | undefined
      throw new Error(body?.error ?? `HTTP ${res.status}`)
    }
    return res.body
  }

  /* ---------------------------------------------------------------- */
  /* State                                                             */
  /* ---------------------------------------------------------------- */

  private stateOf(e: Entry): ConnectionState {
    return {
      config: e.config,
      status: e.status,
      error: e.error,
      repos: e.repos,
      needsCredentials: e.needsCredentials,
      signedIn: e.config.type === 'remote' && e.client.hasToken(),
    }
  }

  listConnections(): ConnectionState[] {
    return Array.from(this.entries.values()).map((e) => this.stateOf(e))
  }

  getStatus(name: string): ConnectionState | null {
    const e = this.entries.get(name)
    return e ? this.stateOf(e) : null
  }

  /*
   * Bring back what the config file remembers, at startup. Local daemons
   * need no credential and a remote one with a stored session needs none
   * either, so both connect unattended; a remote with nothing stored waits
   * for the user to choose Connect.
   */
  async restoreFromConfig(configs: ConnectionConfig[]): Promise<void> {
    for (const config of configs) {
      if (!this.entries.has(config.name)) this.addConnection(config, { persist: false })
    }
    await Promise.all(configs.map((config) => {
      const entry = this.entries.get(config.name)
      if (!entry) return undefined
      if (config.type === 'local' || entry.client.hasToken()) return this.connect(config.name)
      return undefined
    }))
  }

  async disconnectAll(): Promise<void> {
    for (const name of this.entries.keys()) this.disconnect(name)
  }

  /* Reconnecting is the whole of "restart": the daemon's lifetime is
   * systemd's business, not the app's. */
  async restartConnection(name: string): Promise<ConnectResult> {
    this.disconnect(name)
    return this.connect(name)
  }

  private emitChange(): void {
    this.emit('change', this.listConnections())
  }
}
