/**
 * RPC session client for the c-backup binary.
 *
 * Session mode only (--json-session): persistent subprocess reads
 * newline-delimited JSON on stdin, writes responses on stdout.
 *
 * Handles:
 * - LZ4 compressed binary frames (0x00 magic + lengths + payload)
 * - Plain JSON lines (small responses)
 * - sudo -S password feeding (detects password prompt before banner)
 * - Ready banner parsing
 *
 * All four connection permutations:
 *   local          -> spawn: backup --json-session <repo>
 *   local + sudo   -> spawn: sudo -S backup --json-session <repo>
 *   remote         -> spawn: ssh host backup --json-session <repo>
 *   remote + sudo  -> spawn: ssh host sudo -S backup --json-session <repo>
 */

import { ChildProcess, spawn } from 'child_process'
import { EventEmitter } from 'events'
import type { RpcReadyBanner } from '../shared/types'

// LZ4 decompression — loaded lazily
let lz4Uncompress: ((input: Buffer) => Buffer) | null = null
async function ensureLz4(): Promise<void> {
  if (lz4Uncompress) return
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const lz4 = require('lz4-napi') as typeof import('lz4-napi')
    lz4Uncompress = (input: Buffer) => lz4.uncompressSync(input)
  } catch {
    console.warn('lz4-napi not available — compressed responses will fail')
  }
}

export interface SessionOptions {
  /** Command + args to spawn (e.g. ['backup', '--json-session', '/path/to/repo']) */
  command: string[]
  /** Sudo password — if set, we expect a password prompt before the ready banner */
  sudoPassword?: string
  /** Timeout for initial connection (ms) */
  connectTimeout?: number
  /** Timeout for individual RPC calls (ms) */
  callTimeout?: number
}

interface PendingCall {
  resolve: (data: unknown) => void
  reject: (err: Error) => void
  timer: ReturnType<typeof setTimeout>
}

export class RpcSession extends EventEmitter {
  private proc: ChildProcess | null = null
  private buffer: Buffer = Buffer.alloc(0)
  private pending: PendingCall | null = null
  private bannerResolve: ((banner: RpcReadyBanner) => void) | null = null
  private bannerReject: ((err: Error) => void) | null = null
  private sudoPassword: string | undefined
  private sudoFed = false
  private _alive = false
  private callTimeout: number

  banner: RpcReadyBanner | null = null

  constructor() {
    super()
    this.callTimeout = 120_000
  }

  get alive(): boolean {
    return this._alive && this.proc !== null && this.proc.exitCode === null
  }

  /**
   * Start the session subprocess and wait for the ready banner.
   */
  async start(opts: SessionOptions): Promise<RpcReadyBanner> {
    await ensureLz4()

    this.sudoPassword = opts.sudoPassword
    this.sudoFed = false
    this.callTimeout = opts.callTimeout ?? 120_000
    const connectTimeout = opts.connectTimeout ?? 15_000

    const [cmd, ...args] = opts.command
    this.proc = spawn(cmd, args, {
      stdio: ['pipe', 'pipe', 'pipe'],
    })

    this.proc.stdout!.on('data', (chunk: Buffer) => this.onData(chunk))
    this.proc.stderr!.on('data', (chunk: Buffer) => this.onStderr(chunk))
    this.proc.on('exit', (code) => {
      this._alive = false
      this.emit('exit', code)
      // Reject any pending call
      if (this.pending) {
        this.pending.reject(new Error(`session process exited with code ${code}`))
        clearTimeout(this.pending.timer)
        this.pending = null
      }
      if (this.bannerReject) {
        this.bannerReject(new Error(`session process exited before ready banner (code ${code})`))
        this.bannerResolve = null
        this.bannerReject = null
      }
    })

    return new Promise<RpcReadyBanner>((resolve, reject) => {
      this.bannerResolve = resolve
      this.bannerReject = reject

      const timer = setTimeout(() => {
        reject(new Error('session start timed out'))
        this.kill()
      }, connectTimeout)

      // Store timer ref so we can clear it when banner arrives
      const origResolve = this.bannerResolve
      this.bannerResolve = (banner: RpcReadyBanner) => {
        clearTimeout(timer)
        this.banner = banner
        this._alive = true
        origResolve(banner)
      }
      const origReject = this.bannerReject
      this.bannerReject = (err: Error) => {
        clearTimeout(timer)
        origReject(err)
      }
    })
  }

  /**
   * Send an RPC request and wait for the response.
   */
  async call<T = unknown>(action: string, params?: Record<string, unknown>): Promise<T> {
    if (!this.alive) {
      throw new Error('session not alive')
    }
    if (this.pending) {
      throw new Error('another call is already in progress')
    }

    const req: Record<string, unknown> = { action }
    if (params && Object.keys(params).length > 0) {
      req.params = params
    }

    const line = JSON.stringify(req) + '\n'

    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending = null
        reject(new Error(`RPC call '${action}' timed out after ${this.callTimeout}ms`))
      }, this.callTimeout)

      this.pending = {
        resolve: resolve as (data: unknown) => void,
        reject,
        timer,
      }

      try {
        this.proc!.stdin!.write(line)
      } catch (err) {
        clearTimeout(timer)
        this.pending = null
        reject(new Error(`failed to write to session: ${err}`))
      }
    })
  }

  /**
   * Gracefully close the session.
   */
  async close(): Promise<void> {
    if (!this.proc) return

    if (this.alive) {
      try {
        this.proc.stdin!.write('{"action":"quit"}\n')
      } catch { /* ignore */ }

      // Wait up to 5s for graceful exit
      await new Promise<void>((resolve) => {
        const timer = setTimeout(() => {
          this.kill()
          resolve()
        }, 5000)
        this.proc!.once('exit', () => {
          clearTimeout(timer)
          resolve()
        })
      })
    }

    this._alive = false
    this.proc = null
  }

  kill(): void {
    if (this.proc) {
      try { this.proc.kill() } catch { /* ignore */ }
      this._alive = false
      this.proc = null
    }
  }

  /* ---------------------------------------------------------------- */
  /* Internal: data processing                                         */
  /* ---------------------------------------------------------------- */

  private onStderr(chunk: Buffer): void {
    const text = chunk.toString()
    // Detect sudo password prompt on stderr
    if (this.sudoPassword && !this.sudoFed) {
      const lower = text.toLowerCase()
      if (lower.includes('password') || lower.includes('passphrase')) {
        this.proc!.stdin!.write(this.sudoPassword + '\n')
        this.sudoFed = true
        return
      }
    }
    this.emit('stderr', text)
  }

  private onData(chunk: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, chunk])

    // Also check stdout for sudo password prompt (some sudo configs use stdout)
    if (this.sudoPassword && !this.sudoFed && !this.banner) {
      const text = this.buffer.toString('utf-8', 0, Math.min(this.buffer.length, 256))
      const lower = text.toLowerCase()
      if (lower.includes('[sudo] password') || lower.includes('password:')) {
        this.proc!.stdin!.write(this.sudoPassword + '\n')
        this.sudoFed = true
        // Clear the prompt from our buffer
        const nlIdx = this.buffer.indexOf(0x0a)
        if (nlIdx >= 0) {
          this.buffer = this.buffer.subarray(nlIdx + 1)
        } else {
          this.buffer = Buffer.alloc(0)
        }
        return
      }
    }

    // Try to extract complete messages
    this.processBuffer()
  }

  private processBuffer(): void {
    while (this.buffer.length > 0) {
      if (this.buffer[0] === 0x00) {
        // LZ4 binary frame: 0x00 + uint32le(uncomp) + uint32le(comp) + data + \n
        if (this.buffer.length < 9) return  // need at least header

        const uncompLen = this.buffer.readUInt32LE(1)
        const compLen = this.buffer.readUInt32LE(5)
        const totalLen = 9 + compLen + 1  // header + payload + newline

        if (this.buffer.length < totalLen) return  // incomplete frame

        const compData = this.buffer.subarray(9, 9 + compLen)
        let json: string

        if (lz4Uncompress) {
          try {
            const raw = lz4Uncompress(compData)
            json = raw.toString('utf-8')
          } catch (err) {
            this.deliverError(new Error(`LZ4 decompression failed: ${err}`))
            this.buffer = this.buffer.subarray(totalLen)
            continue
          }
        } else {
          this.deliverError(new Error('LZ4 not available for compressed response'))
          this.buffer = this.buffer.subarray(totalLen)
          continue
        }

        this.buffer = this.buffer.subarray(totalLen)
        this.deliverMessage(json)
      } else {
        // Plain text line — find newline
        const nlIdx = this.buffer.indexOf(0x0a)
        if (nlIdx < 0) return  // incomplete line

        const line = this.buffer.subarray(0, nlIdx).toString('utf-8')
        this.buffer = this.buffer.subarray(nlIdx + 1)

        if (line.length > 0) {
          this.deliverMessage(line)
        }
      }
    }
  }

  private deliverMessage(json: string): void {
    let parsed: Record<string, unknown>
    try {
      parsed = JSON.parse(json)
    } catch (err) {
      this.deliverError(new Error(`invalid JSON from session: ${json.substring(0, 200)}`))
      return
    }

    // Is this the ready banner?
    if (this.bannerResolve && parsed.status === 'ready') {
      this.bannerResolve(parsed as unknown as RpcReadyBanner)
      this.bannerResolve = null
      this.bannerReject = null
      return
    }

    // Is this a response to a pending call?
    if (this.pending) {
      const { resolve, reject, timer } = this.pending
      this.pending = null
      clearTimeout(timer)

      if (parsed.status === 'error') {
        reject(new Error((parsed.message as string) || 'unknown RPC error'))
      } else {
        resolve(parsed.data ?? parsed)
      }
      return
    }

    // Unsolicited message
    this.emit('message', parsed)
  }

  private deliverError(err: Error): void {
    if (this.bannerReject) {
      this.bannerReject(err)
      this.bannerResolve = null
      this.bannerReject = null
      return
    }
    if (this.pending) {
      const { reject, timer } = this.pending
      this.pending = null
      clearTimeout(timer)
      reject(err)
      return
    }
    this.emit('error', err)
  }
}
