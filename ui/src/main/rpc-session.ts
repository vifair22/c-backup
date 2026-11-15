/*
 * HTTP client for c-backupd.
 *
 * Replaces the subprocess transport entirely. The old session spawned a binary
 * per (host, repo), fed sudo a password on stdin, managed an SSH child, and
 * parsed a ready banner before it could send anything. All of that existed
 * because the CLI was the only way in; the daemon has an HTTP API, so none of
 * it is needed.
 *
 * Local connections use node:http with socketPath, which speaks HTTP over a
 * unix socket with no extra dependency. Remote connections are ordinary TCP
 * with a bearer token.
 */

import http from 'node:http'
import type { ConnectionConfig, DaemonInfo } from '../shared/types'

export interface HttpResult<T = unknown> {
  status: number
  body: T
}

export class DaemonUnreachableError extends Error {
  constructor(target: string, cause: string) {
    super(`no daemon at ${target}: ${cause}`)
    this.name = 'DaemonUnreachableError'
  }
}

/*
 * Network failures that mean "nothing answered there", as opposed to a
 * request that reached a daemon and failed. Only ENOENT and ECONNREFUSED
 * counted before, which is all a unix socket produces; a remote host that is
 * down, unroutable or misspelled surfaced as a raw transport error instead.
 */
const UNREACHABLE = new Set([
  'ENOENT', 'ECONNREFUSED', 'EHOSTUNREACH', 'ENETUNREACH', 'EHOSTDOWN',
  'ETIMEDOUT', 'ENOTFOUND', 'EAI_AGAIN', 'ECONNRESET',
])

/*
 * How long a remote request may sit idle before it is abandoned. Without one
 * a host that drops packets holds the request for as long as the kernel keeps
 * retrying, which leaves the sidebar on "connecting" for minutes. Every route
 * answers well inside this; long work runs as a job and is polled.
 */
const REMOTE_IDLE_TIMEOUT_MS = 30_000

/* Said when a remote daemon has no admin password yet. Setup is accepted only
 * over the daemon's own socket, so the fix is on that host, not in this app. */
export function notConfiguredMessage(target: string): string {
  return `the daemon at ${target} has no admin password yet. ` +
    `Set one on that host with 'c-backup auth setup', then connect again.`
}

export class DaemonClient {
  private config: ConnectionConfig
  private token?: string

  constructor(config: ConnectionConfig, token?: string) {
    this.config = config
    this.token = token
  }

  setToken(token: string | undefined): void {
    this.token = token
  }

  private target(): string {
    return this.config.type === 'local'
      ? this.config.socketPath
      : `${this.config.host}:${this.config.port}`
  }

  async request<T = unknown>(
    method: string,
    path: string,
    body?: unknown
  ): Promise<HttpResult<T>> {
    const payload = body === undefined ? undefined : JSON.stringify(body)

    const options: http.RequestOptions = {
      method,
      path,
      headers: {
        'Content-Type': 'application/json',
        ...(payload ? { 'Content-Length': Buffer.byteLength(payload) } : {}),
        // The unix socket bypasses auth entirely, so a token is only ever
        // sent to a remote daemon.
        ...(this.token && this.config.type === 'remote'
          ? { Authorization: `Bearer ${this.token}` }
          : {}),
      },
    }

    if (this.config.type === 'local') {
      options.socketPath = this.config.socketPath
    } else {
      options.host = this.config.host
      options.port = this.config.port
    }

    return new Promise((resolve, reject) => {
      const req = http.request(options, (res) => {
        const chunks: Buffer[] = []
        res.on('data', (c) => chunks.push(c))
        res.on('end', () => {
          const text = Buffer.concat(chunks).toString('utf8')
          let parsed: unknown = undefined
          if (text.length > 0) {
            try {
              parsed = JSON.parse(text)
            } catch {
              /*
               * Almost every route returns JSON, so unparseable text
               * alongside a failure status is an upstream problem and the
               * text is the most useful thing to surface.
               *
               * /metrics is the exception: Prometheus exposition format is
               * text by definition, and a successful response was being
               * reported as an error because of where it landed. A 2xx that
               * is not JSON is a body, not a fault.
               */
              parsed = (res.statusCode ?? 0) >= 400 ? { error: text } : { text }
            }
          }
          resolve({ status: res.statusCode ?? 0, body: parsed as T })
        })
      })

      req.on('error', (err: NodeJS.ErrnoException) => {
        // Nothing answering is a different problem from a request that
        // failed, and the UI should say so rather than show a transport error.
        if (err.code && UNREACHABLE.has(err.code)) {
          reject(new DaemonUnreachableError(this.target(), err.code))
        } else {
          reject(err)
        }
      })

      if (this.config.type === 'remote') {
        req.setTimeout(REMOTE_IDLE_TIMEOUT_MS, () => {
          const e: NodeJS.ErrnoException = new Error('timed out')
          e.code = 'ETIMEDOUT'
          req.destroy(e)
        })
      }

      if (payload) req.write(payload)
      req.end()
    })
  }

  /* Readiness doubles as the connection check: it proves the daemon is up and
   * its database is open, which is what "connected" should mean. */
  async ready(): Promise<DaemonInfo> {
    const res = await this.request<DaemonInfo>('GET', '/readyz')
    if (res.status !== 200) throw new Error(`daemon not ready (HTTP ${res.status})`)
    return res.body
  }

  /* Exchange a password for a bearer token. Remote connections only: the
   * socket needs no credential. */
  async login(password: string): Promise<string> {
    const res = await this.request<{ token?: string }>('POST', '/api/auth/login', {
      password,
    })
    if (res.status === 403) {
      throw new Error(notConfiguredMessage(this.target()))
    }
    if (res.status !== 200 || !res.body?.token) {
      throw new Error('login failed: check the admin password')
    }
    this.token = res.body.token
    return res.body.token
  }

  /* Revoke the current session on the daemon. Best effort: a token the daemon
   * no longer knows is as signed out as it gets. */
  async logout(): Promise<void> {
    if (!this.token) return
    try {
      await this.request('POST', '/api/auth/logout')
    } finally {
      this.token = undefined
    }
  }

  hasToken(): boolean {
    return !!this.token
  }

  async isSetup(): Promise<boolean> {
    const res = await this.request<{ configured?: boolean }>('GET', '/api/auth/setup')
    return res.status === 200 && res.body?.configured === true
  }
}
