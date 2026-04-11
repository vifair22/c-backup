/**
 * Connection manager — orchestrates connections, repos, and RPC sessions.
 *
 * Repos are stored as string paths inside each ConnectionConfig.
 * Each open repo gets its own RPC session.
 */

import { RpcSession } from './rpc-session'
import type { SessionOptions } from './rpc-session'
import {
  addConnectionConfig, removeConnectionConfig, updateConnectionConfig,
  addRepoToConnection, removeRepoFromConnection,
  setCredential, getCredential, deleteCredentials,
  hasStoredCredentials,
} from './config-store'
import type {
  AppConfig,
  ConnectionConfig,
  ConnectionState,
  ConnectionStatus,
  LocalConnectionConfig,
  SshConnectionConfig,
} from '../shared/types'

interface ManagedRepo {
  path: string
  session: RpcSession | null
  opening?: Promise<{ ok: boolean; error?: string; version?: string; needsCredentials?: boolean }>
  callQueue: Promise<unknown>
}

interface ManagedConnection {
  config: ConnectionConfig
  status: ConnectionStatus
  error?: string
  repos: Map<string, ManagedRepo>      // keyed by repo path
  credentials?: {
    password?: string
    sudoPassword?: string
  }
}

export class ConnectionManager {
  private connections = new Map<string, ManagedConnection>()

  /* ---------------------------------------------------------------- */
  /* Restore from persisted config                                     */
  /* ---------------------------------------------------------------- */

  restoreFromConfig(config: AppConfig): void {
    for (const connConfig of config.connections) {
      const repos = new Map<string, ManagedRepo>()
      for (const repoPath of connConfig.repos ?? []) {
        repos.set(repoPath, { path: repoPath, session: null, callQueue: Promise.resolve() })
      }
      this.connections.set(connConfig.name, {
        config: connConfig,
        status: 'disconnected',
        repos,
      })
    }
    const totalRepos = config.connections.reduce((n, c) => n + (c.repos?.length ?? 0), 0)
    console.log(`[ConnectionManager] restored ${config.connections.length} connection(s), ${totalRepos} repo(s)`)
  }

  /* ---------------------------------------------------------------- */
  /* Connection CRUD                                                   */
  /* ---------------------------------------------------------------- */

  addConnection(config: ConnectionConfig): { ok: boolean; error?: string } {
    if (this.connections.has(config.name)) {
      return { ok: false, error: `connection '${config.name}' already exists` }
    }
    if (!Array.isArray(config.repos)) config.repos = []
    this.connections.set(config.name, {
      config,
      status: 'disconnected',
      repos: new Map(),
    })
    addConnectionConfig(config)
    return { ok: true }
  }

  removeConnection(name: string): { ok: boolean; error?: string } {
    const conn = this.connections.get(name)
    if (!conn) return { ok: false, error: `connection '${name}' not found` }

    for (const repo of conn.repos.values()) {
      repo.session?.close()
    }
    this.connections.delete(name)
    removeConnectionConfig(name)
    deleteCredentials(name)
    return { ok: true }
  }

  async editConnection(name: string, newConfig: ConnectionConfig): Promise<{ ok: boolean; error?: string }> {
    const conn = this.connections.get(name)
    if (!conn) return { ok: false, error: `connection '${name}' not found` }

    // Disconnect if currently connected
    if (conn.status === 'connected' || conn.status === 'connecting') {
      await this.disconnect(name)
    }

    // If name changed, re-key the map
    if (newConfig.name !== name) {
      if (this.connections.has(newConfig.name)) {
        return { ok: false, error: `connection '${newConfig.name}' already exists` }
      }
      this.connections.delete(name)
    }

    // Preserve repos from old config
    if (!Array.isArray(newConfig.repos) || newConfig.repos.length === 0) {
      newConfig.repos = conn.config.repos
    }

    conn.config = newConfig
    this.connections.set(newConfig.name, conn)
    updateConnectionConfig(name, newConfig)
    return { ok: true }
  }

  editRepo(connectionName: string, oldPath: string, newPath: string): { ok: boolean; error?: string } {
    const conn = this.connections.get(connectionName)
    if (!conn) return { ok: false, error: `connection '${connectionName}' not found` }

    const repo = conn.repos.get(oldPath)
    if (!repo) return { ok: false, error: `repo '${oldPath}' not found` }

    if (conn.repos.has(newPath)) {
      return { ok: false, error: `repo '${newPath}' already exists` }
    }

    repo.session?.close()
    repo.session = null
    conn.repos.delete(oldPath)
    conn.repos.set(newPath, { path: newPath, session: null, callQueue: Promise.resolve() })
    removeRepoFromConnection(connectionName, oldPath)
    addRepoToConnection(connectionName, newPath)
    return { ok: true }
  }

  async restartConnection(name: string): Promise<{ ok: boolean; error?: string }> {
    const conn = this.connections.get(name)
    if (!conn) return { ok: false, error: `connection '${name}' not found` }

    await this.disconnect(name)
    return this.connect(name)
  }

  listConnections(): ConnectionState[] {
    return Array.from(this.connections.values()).map(c => ({
      config: c.config,
      status: c.status,
      error: c.error,
      repos: Array.from(c.repos.values()).map(r => ({
        path: r.path,
        sessionActive: r.session?.alive ?? false,
      })),
    }))
  }

  getStatus(name: string): ConnectionState | null {
    const conn = this.connections.get(name)
    if (!conn) return null
    return {
      config: conn.config,
      status: conn.status,
      error: conn.error,
      repos: Array.from(conn.repos.values()).map(r => ({
        path: r.path,
        sessionActive: r.session?.alive ?? false,
      })),
    }
  }

  /* ---------------------------------------------------------------- */
  /* Connect / disconnect                                              */
  /* ---------------------------------------------------------------- */

  async connect(
    name: string,
    credentials?: { password?: string; sudoPassword?: string; save?: boolean }
  ): Promise<{ ok: boolean; error?: string }> {
    const conn = this.connections.get(name)
    if (!conn) return { ok: false, error: `connection '${name}' not found` }

    conn.status = 'connecting'
    const shouldSave = credentials?.save !== false // default true

    // Merge provided credentials with stored ones
    const effectiveCreds: { password?: string; sudoPassword?: string } = {}

    if (credentials?.password) {
      effectiveCreds.password = credentials.password
      if (shouldSave) setCredential(name, 'password', credentials.password)
    } else {
      effectiveCreds.password = getCredential(name, 'password')
    }

    if (credentials?.sudoPassword) {
      effectiveCreds.sudoPassword = credentials.sudoPassword
      if (shouldSave) setCredential(name, 'sudoPassword', credentials.sudoPassword)
    } else {
      effectiveCreds.sudoPassword = getCredential(name, 'sudoPassword')
    }

    conn.credentials = effectiveCreds

    if (conn.config.type === 'ssh') {
      try {
        await this.testSshConnectivity(conn.config, effectiveCreds.password)
        conn.status = 'connected'
      } catch (err) {
        conn.status = 'error'
        conn.error = `SSH connection failed: ${err}`
        return { ok: false, error: conn.error }
      }
    } else {
      conn.status = 'connected'
    }

    return { ok: true }
  }

  canAutoConnect(name: string): boolean {
    const conn = this.connections.get(name)
    if (!conn) return false
    if (conn.config.type === 'local' && !conn.config.sudo) return true
    return hasStoredCredentials(name)
  }

  async disconnect(name: string): Promise<{ ok: boolean }> {
    const conn = this.connections.get(name)
    if (!conn) return { ok: false }

    for (const repo of conn.repos.values()) {
      if (repo.session) {
        await repo.session.close()
        repo.session = null
      }
    }

    conn.status = 'disconnected'
    conn.credentials = undefined
    return { ok: true }
  }

  disconnectAll(): void {
    for (const name of this.connections.keys()) {
      this.disconnect(name)
    }
  }

  /* ---------------------------------------------------------------- */
  /* Repo management                                                   */
  /* ---------------------------------------------------------------- */

  addRepo(connectionName: string, repoPath: string): { ok: boolean; error?: string } {
    const conn = this.connections.get(connectionName)
    if (!conn) return { ok: false, error: `connection '${connectionName}' not found` }

    if (conn.repos.has(repoPath)) {
      return { ok: false, error: `repo '${repoPath}' already added` }
    }

    conn.repos.set(repoPath, { path: repoPath, session: null, callQueue: Promise.resolve() })
    addRepoToConnection(connectionName, repoPath)
    return { ok: true }
  }

  removeRepo(connectionName: string, repoPath: string): { ok: boolean; error?: string } {
    const conn = this.connections.get(connectionName)
    if (!conn) return { ok: false, error: `connection '${connectionName}' not found` }

    const repo = conn.repos.get(repoPath)
    if (!repo) return { ok: false, error: `repo '${repoPath}' not found` }

    repo.session?.close()
    conn.repos.delete(repoPath)
    removeRepoFromConnection(connectionName, repoPath)
    return { ok: true }
  }

  /* ---------------------------------------------------------------- */
  /* Open / close repo sessions                                        */
  /* ---------------------------------------------------------------- */

  async openRepo(
    connectionName: string,
    repoPath: string
  ): Promise<{ ok: boolean; error?: string; version?: string; needsCredentials?: boolean }> {
    const conn = this.connections.get(connectionName)
    if (!conn) return { ok: false, error: `connection '${connectionName}' not found` }

    // Auto-connect if not connected
    if (conn.status !== 'connected') {
      const needsCreds = conn.config.sudo || (conn.config.type === 'ssh' && conn.config.authMethod === 'password')
      if (needsCreds && !hasStoredCredentials(connectionName) && !conn.credentials) {
        return { ok: false, error: 'credentials_required', needsCredentials: true }
      }
      const connectResult = await this.connect(connectionName)
      if (!connectResult.ok) return { ok: false, error: connectResult.error, needsCredentials: needsCreds }
    }
    console.log(`[openRepo] ${connectionName}:${repoPath}`)

    let repo = conn.repos.get(repoPath)
    if (!repo) {
      conn.repos.set(repoPath, { path: repoPath, session: null, callQueue: Promise.resolve() })
      addRepoToConnection(connectionName, repoPath)
      repo = conn.repos.get(repoPath)!
    }

    if (repo.session?.alive) {
      return { ok: true, version: repo.session.banner?.version }
    }

    // Deduplicate concurrent open attempts
    if (repo.opening) {
      return repo.opening
    }

    const openPromise = this.doOpenRepo(conn, repo, connectionName, repoPath)
    repo.opening = openPromise
    openPromise.finally(() => { repo!.opening = undefined })
    return openPromise
  }

  private async doOpenRepo(
    conn: ManagedConnection,
    repo: ManagedRepo,
    connectionName: string,
    repoPath: string
  ): Promise<{ ok: boolean; error?: string; version?: string; needsCredentials?: boolean }> {
    const sessionOpts = this.buildSessionOpts(conn, repoPath)
    console.log(`[openRepo] spawning:`, sessionOpts.command.join(' '))
    const session = new RpcSession()

    session.on('exit', (code) => {
      console.log(`[${connectionName}:${repoPath}] session exited with code ${code}`)
      repo!.session = null
    })

    session.on('stderr', (text: string) => {
      console.log(`[${connectionName}:${repoPath}] stderr: ${text.trim()}`)
    })

    session.on('error', (err: Error) => {
      console.error(`[${connectionName}:${repoPath}] error:`, err.message)
    })

    try {
      const banner = await session.start(sessionOpts)
      repo.session = session

      if (!banner.lock) {
        console.warn(`[${connectionName}:${repoPath}] repo locked by another process, data may be stale`)
      }

      return { ok: true, version: banner.version }
    } catch (err) {
      session.kill()
      return { ok: false, error: `failed to open repo: ${err}` }
    }
  }

  async closeRepo(connectionName: string, repoPath: string): Promise<{ ok: boolean }> {
    const conn = this.connections.get(connectionName)
    if (!conn) return { ok: false }

    const repo = conn.repos.get(repoPath)
    if (!repo) return { ok: false }

    if (repo.session) {
      await repo.session.close()
      repo.session = null
    }

    // Auto-disconnect if no repo sessions remain active
    const hasActiveSessions = Array.from(conn.repos.values()).some(r => r.session?.alive)
    if (!hasActiveSessions && conn.status === 'connected') {
      await this.disconnect(connectionName)
    }

    return { ok: true }
  }

  /* ---------------------------------------------------------------- */
  /* Repo init (one-shot command)                                      */
  /* ---------------------------------------------------------------- */

  async initRepo(connectionName: string, repoPath: string): Promise<{ ok: boolean; error?: string; needsCredentials?: boolean }> {
    const conn = this.connections.get(connectionName)
    if (!conn) return { ok: false, error: `connection '${connectionName}' not found` }

    // Ensure connection is up (auto-connect)
    if (conn.status !== 'connected') {
      const needsCreds = conn.config.sudo || (conn.config.type === 'ssh' && conn.config.authMethod === 'password')
      if (needsCreds && !hasStoredCredentials(connectionName) && !conn.credentials) {
        return { ok: false, error: 'credentials_required', needsCredentials: true }
      }
      const connectResult = await this.connect(connectionName)
      if (!connectResult.ok) return { ok: false, error: connectResult.error, needsCredentials: needsCreds }
    }

    const config = conn.config
    const cmd = this.buildInitCommand(config, conn.credentials, repoPath)

    const { spawn } = await import('child_process')
    return new Promise((resolve) => {
      const proc = spawn(cmd[0], cmd.slice(1), { stdio: ['pipe', 'pipe', 'pipe'] })
      let stderr = ''

      // Feed sudo password if needed
      if (config.sudo && conn.credentials?.sudoPassword) {
        proc.stdin!.write(conn.credentials.sudoPassword + '\n')
      }

      proc.stderr!.on('data', (chunk: Buffer) => { stderr += chunk.toString() })

      const timer = setTimeout(() => {
        proc.kill()
        resolve({ ok: false, error: 'init timed out' })
      }, 30_000)

      proc.on('exit', (code) => {
        clearTimeout(timer)
        if (code === 0) {
          // Auto-add the repo to the connection
          this.addRepo(connectionName, repoPath)
          resolve({ ok: true })
        } else {
          const msg = stderr.trim().split('\n').pop() || `exit code ${code}`
          resolve({ ok: false, error: msg })
        }
      })

      proc.on('error', (err) => {
        clearTimeout(timer)
        resolve({ ok: false, error: err.message })
      })
    })
  }

  private buildInitCommand(config: ConnectionConfig, credentials: ManagedConnection['credentials'], repoPath: string): string[] {
    if (config.type === 'local') {
      const bin = (config as LocalConnectionConfig).binaryPath || 'backup'
      return config.sudo
        ? ['sudo', '-S', bin, 'init', '--repo', repoPath]
        : [bin, 'init', '--repo', repoPath]
    }

    const sshConfig = config as SshConnectionConfig
    const remoteBin = sshConfig.remoteBinaryPath || 'backup'

    const args: string[] = ['ssh']
    if (sshConfig.port !== 22) args.push('-p', String(sshConfig.port))
    if (sshConfig.username) args.push('-l', sshConfig.username)
    if (sshConfig.authMethod === 'key' && sshConfig.keyFilePath) {
      args.push('-i', sshConfig.keyFilePath)
    }
    args.push(
      '-o', 'StrictHostKeyChecking=accept-new',
      '-o', 'BatchMode=yes',
      '-o', 'ConnectTimeout=10',
      sshConfig.host,
    )

    if (config.sudo) {
      args.push('sudo', '-S', remoteBin, 'init', '--repo', repoPath)
    } else {
      args.push(remoteBin, 'init', '--repo', repoPath)
    }

    return args
  }

  /* ---------------------------------------------------------------- */
  /* RPC call passthrough                                              */
  /* ---------------------------------------------------------------- */

  async rpcCall(
    connectionName: string,
    repoPath: string,
    action: string,
    params?: Record<string, unknown>
  ): Promise<unknown> {
    const conn = this.connections.get(connectionName)
    if (!conn) throw new Error(`connection '${connectionName}' not found`)

    let repo = conn.repos.get(repoPath)
    if (!repo) {
      // Ensure repo entry exists
      const openResult = await this.openRepo(connectionName, repoPath)
      if (!openResult.ok) throw new Error(openResult.error ?? 'failed to open repo')
      repo = conn.repos.get(repoPath)!
    }

    if (!repo.session?.alive) {
      const result = await this.openRepo(connectionName, repoPath)
      if (!result.ok) throw new Error(result.error ?? 'failed to open repo')
    }

    // Serialize calls through the queue to avoid "another call in progress"
    const callPromise = repo.callQueue.then(() => {
      if (!repo!.session?.alive) throw new Error('session not alive')
      return repo!.session.call(action, params)
    })
    repo.callQueue = callPromise.catch(() => {}) // prevent unhandled rejection on queue
    return callPromise
  }

  /* ---------------------------------------------------------------- */
  /* Internal helpers                                                  */
  /* ---------------------------------------------------------------- */

  private buildSessionOpts(conn: ManagedConnection, repoPath: string): SessionOptions {
    const config = conn.config
    const sudoPassword = config.sudo ? conn.credentials?.sudoPassword : undefined

    if (config.type === 'local') {
      const bin = (config as LocalConnectionConfig).binaryPath || 'backup'
      const command = config.sudo
        ? ['sudo', '-S', bin, '--json-session', repoPath]
        : [bin, '--json-session', repoPath]
      return { command, sudoPassword }
    }

    const sshConfig = config as SshConnectionConfig
    const remoteBin = sshConfig.remoteBinaryPath || 'backup'

    const sshArgs: string[] = ['ssh']
    if (sshConfig.port !== 22) sshArgs.push('-p', String(sshConfig.port))
    if (sshConfig.username) sshArgs.push('-l', sshConfig.username)
    if (sshConfig.authMethod === 'key' && sshConfig.keyFilePath) {
      sshArgs.push('-i', sshConfig.keyFilePath)
    }
    sshArgs.push(
      '-o', 'StrictHostKeyChecking=accept-new',
      '-o', 'BatchMode=yes',
      '-o', 'ConnectTimeout=10',
      sshConfig.host,
    )

    if (config.sudo) {
      sshArgs.push('sudo', '-S', remoteBin, '--json-session', repoPath)
    } else {
      sshArgs.push(remoteBin, '--json-session', repoPath)
    }

    return { command: sshArgs, sudoPassword }
  }

  private async testSshConnectivity(
    config: SshConnectionConfig,
    _password?: string
  ): Promise<void> {
    const args: string[] = []
    if (config.port !== 22) args.push('-p', String(config.port))
    if (config.username) args.push('-l', config.username)
    if (config.authMethod === 'key' && config.keyFilePath) {
      args.push('-i', config.keyFilePath)
    }
    args.push(
      '-o', 'StrictHostKeyChecking=accept-new',
      '-o', 'BatchMode=yes',
      '-o', 'ConnectTimeout=10',
      config.host,
      'true',
    )

    const { spawn } = await import('child_process')
    return new Promise((resolve, reject) => {
      const proc = spawn('ssh', args, { stdio: 'pipe' })
      const timer = setTimeout(() => {
        proc.kill()
        reject(new Error('SSH connection timed out'))
      }, 15_000)

      proc.on('exit', (code) => {
        clearTimeout(timer)
        if (code === 0) resolve()
        else reject(new Error(`SSH connection failed (exit ${code})`))
      })
      proc.on('error', (err) => {
        clearTimeout(timer)
        reject(err)
      })
    })
  }
}
