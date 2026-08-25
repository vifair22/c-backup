/*
 * Connection manager.
 *
 * A connection is a c-backupd instance. Connecting means checking that the
 * daemon answers and listing the repos it owns; it does not mean spawning
 * anything, so there are no child processes to supervise, no sudo prompts to
 * detect on stderr, and no SSH lifecycle to coordinate. That machinery made up
 * most of the previous version of this file and all of it is gone.
 *
 * Repos are no longer configured per connection either. The daemon knows which
 * repos it owns; the app asks.
 */

import { EventEmitter } from 'node:events'
import { DaemonClient, DaemonUnreachableError } from './rpc-session'
import { mapAction } from './api-map'
import { toEndpoint } from './client'
import type { ApiRequest } from '../shared/api'
import type {
  ConnectionConfig,
  ConnectionState,
  ConnectionStatus,
  RepoState,
} from '../shared/types'

interface Entry {
  config: ConnectionConfig
  client: DaemonClient
  status: ConnectionStatus
  error?: string
  repos: RepoState[]
  version?: string
}

export class ConnectionManager extends EventEmitter {
  private entries = new Map<string, Entry>()

  addConnection(config: ConnectionConfig): { ok: boolean; error?: string } {
    if (this.entries.has(config.name)) {
      return { ok: false, error: `connection '${config.name}' already exists` }
    }
    this.entries.set(config.name, {
      config,
      client: new DaemonClient(config),
      status: 'disconnected',
      repos: [],
    })
    this.emitChange()
    return { ok: true }
  }

  removeConnection(name: string): { ok: boolean; error?: string } {
    if (!this.entries.delete(name)) {
      return { ok: false, error: `no connection '${name}'` }
    }
    this.emitChange()
    return { ok: true }
  }

  editConnection(name: string, config: ConnectionConfig): { ok: boolean; error?: string } {
    const entry = this.entries.get(name)
    if (!entry) return { ok: false, error: `no connection '${name}'` }
    entry.config = config
    entry.client = new DaemonClient(config)
    entry.status = 'disconnected'
    entry.repos = []
    if (config.name !== name) {
      this.entries.delete(name)
      this.entries.set(config.name, entry)
    }
    this.emitChange()
    return { ok: true }
  }

  /*
   * Connect: confirm the daemon is ready, authenticate if remote, and load its
   * repo list. A password is only needed for a remote daemon that has been set
   * up; the unix socket is gated by filesystem permissions instead.
   */
  async connect(
    name: string,
    credentials?: { password?: string }
  ): Promise<{ ok: boolean; error?: string; needsCredentials?: boolean }> {
    const entry = this.entries.get(name)
    if (!entry) return { ok: false, error: `no connection '${name}'` }

    entry.status = 'connecting'
    entry.error = undefined
    this.emitChange()

    try {
      const info = await entry.client.ready()
      entry.version = info.version

      if (entry.config.type === 'remote') {
        if (credentials?.password) {
          await entry.client.login(credentials.password)
        } else if (await entry.client.isSetup()) {
          /* Configured but no token: the UI must prompt rather than fail, or
           * the user has no way to supply one. */
          entry.status = 'disconnected'
          this.emitChange()
          return { ok: false, needsCredentials: true, error: 'password required' }
        }
      }

      entry.repos = await this.loadRepos(entry)
      entry.status = 'connected'
      this.emitChange()
      return { ok: true }
    } catch (err) {
      const message =
        err instanceof DaemonUnreachableError
          ? `${err.message}. Start it with 'systemctl start c-backupd'.`
          : err instanceof Error
            ? err.message
            : String(err)
      entry.status = 'error'
      entry.error = message
      this.emitChange()
      return { ok: false, error: message }
    }
  }

  disconnect(name: string): { ok: boolean } {
    const entry = this.entries.get(name)
    if (!entry) return { ok: false }
    /* Nothing to tear down: there is no session, only requests. */
    entry.status = 'disconnected'
    entry.repos = []
    this.emitChange()
    return { ok: true }
  }

  private async loadRepos(entry: Entry): Promise<RepoState[]> {
    const res = await entry.client.request<{ repos?: Array<Record<string, string>> }>(
      'GET',
      '/repos'
    )
    if (res.status !== 200) throw new Error(`cannot list repos (HTTP ${res.status})`)
    return (res.body?.repos ?? []).map((r) => ({
      id: Number(r.id),
      path: r.path,
      label: r.label,
      uuid: r.uuid,
    }))
  }

  /* Registration replaces the old init/add/open trio: the daemon creates the
   * directory if needed, writes the marker, and records the row, so there is
   * one operation rather than three. */
  async addRepo(
    name: string,
    repoPath: string,
    label?: string
  ): Promise<{ ok: boolean; error?: string }> {
    const entry = this.entries.get(name)
    if (!entry) return { ok: false, error: `no connection '${name}'` }
    try {
      const res = await entry.client.request<{ error?: string }>('POST', '/repos', {
        path: repoPath,
        label: label ?? '',
      })
      if (res.status !== 200 && res.status !== 201) {
        return { ok: false, error: res.body?.error ?? `HTTP ${res.status}` }
      }
      entry.repos = await this.loadRepos(entry)
      this.emitChange()
      return { ok: true }
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : String(err) }
    }
  }

  async removeRepo(name: string, repoId: number): Promise<{ ok: boolean; error?: string }> {
    const entry = this.entries.get(name)
    if (!entry) return { ok: false, error: `no connection '${name}'` }
    try {
      const res = await entry.client.request('DELETE', `/repos/${repoId}`)
      if (res.status !== 204 && res.status !== 200) {
        return { ok: false, error: `HTTP ${res.status}` }
      }
      entry.repos = await this.loadRepos(entry)
      this.emitChange()
      return { ok: true }
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : String(err) }
    }
  }

  /*
   * The renderer's action-based entry point, preserved so components did not
   * all have to change at once. Actions are translated to endpoints in
   * api-map.ts, which also names the ones that no longer exist.
   */
  /*
   * The typed entry point.
   *
   * Identical plumbing to rpcCall below, with the one difference that
   * matters: the endpoint comes from client.ts rather than from
   * interpolating a caller's string, so the set of paths this can produce is
   * fixed at compile time and checked against a live daemon by
   * tests/contract.test.ts.
   */
  async apiCall<T = unknown>(name: string, req: ApiRequest): Promise<T> {
    const entry = this.entries.get(name)
    if (!entry) throw new Error(`no connection '${name}'`)
    if (entry.status !== 'connected') throw new Error(`'${name}' is not connected`)

    const ep = toEndpoint(req)
    const res = await entry.client.request<T>(ep.method, ep.path, ep.body)
    if (res.status >= 400) {
      const body = res.body as { error?: string } | undefined
      throw new Error(body?.error ?? `HTTP ${res.status}`)
    }
    return res.body
  }

  async rpcCall<T = unknown>(
    name: string,
    repoId: number,
    action: string,
    params?: Record<string, unknown>
  ): Promise<T> {
    const entry = this.entries.get(name)
    if (!entry) throw new Error(`no connection '${name}'`)
    if (entry.status !== 'connected') throw new Error(`'${name}' is not connected`)

    const ep = mapAction(repoId, action, params)
    const res = await entry.client.request<T>(ep.method, ep.path, ep.body)
    if (res.status >= 400) {
      const body = res.body as { error?: string } | undefined
      throw new Error(body?.error ?? `HTTP ${res.status}`)
    }
    return res.body
  }

  listConnections(): ConnectionState[] {
    return Array.from(this.entries.values()).map((e) => ({
      config: e.config,
      status: e.status,
      error: e.error,
      repos: e.repos,
    }))
  }

  getStatus(name: string): ConnectionState | null {
    const e = this.entries.get(name)
    if (!e) return null
    return { config: e.config, status: e.status, error: e.error, repos: e.repos }
  }

  /* Reconnect everything the config file remembers, at startup. */
  async restoreFromConfig(configs: ConnectionConfig[]): Promise<void> {
    for (const config of configs) {
      if (!this.entries.has(config.name)) this.addConnection(config)
      /* Local daemons need no credential, so they can come up on their own.
       * Remote ones wait for the user, since a token may be needed. */
      if (config.type === 'local') await this.connect(config.name)
    }
  }

  /* A local connection can connect unattended; a remote one may need a
   * password the app does not have. */
  canAutoConnect(name: string): boolean {
    const e = this.entries.get(name)
    return !!e && e.config.type === 'local'
  }

  async disconnectAll(): Promise<void> {
    for (const name of this.entries.keys()) this.disconnect(name)
  }

  /* Reconnecting is the whole of "restart" now. There is no child process to
   * kill and respawn: the daemon's lifetime is systemd's business, not the
   * app's. */
  async restartConnection(name: string): Promise<{ ok: boolean; error?: string }> {
    this.disconnect(name)
    return this.connect(name)
  }

  /*
   * Registration creates the repo if it does not exist, so there is no longer
   * a separate init step. Kept as a distinct entry point because the UI still
   * distinguishes "make me a new one" from "adopt this one", and the daemon
   * treats both the same way.
   */
  async initRepo(name: string, repoPath: string): Promise<{ ok: boolean; error?: string }> {
    return this.addRepo(name, repoPath)
  }

  /*
   * There is no session to open or close. The old transport had to spawn a
   * process per repo and hold it; requests are stateless now, so these exist
   * only so the renderer's existing calls remain valid.
   */
  async openRepo(_name: string, _repoId: number): Promise<{ ok: boolean }> {
    return { ok: true }
  }

  async closeRepo(_name: string, _repoId: number): Promise<{ ok: boolean }> {
    return { ok: true }
  }

  /*
   * Moving a repo is not an edit the daemon can make on the app's behalf: the
   * data has to move on disk first, after which registering the new path
   * re-links it by uuid and keeps its history.
   */
  async editRepo(): Promise<{ ok: boolean; error?: string }> {
    return {
      ok: false,
      error:
        'move the repository directory, then register the new path; ' +
        'it is re-linked by its .cbackup uuid',
    }
  }

  private emitChange(): void {
    this.emit('change', this.listConnections())
  }
}
