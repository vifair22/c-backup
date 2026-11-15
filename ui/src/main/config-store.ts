/**
 * Config store: what the app keeps on disk.
 *
 * Non-secret config: <userData>/config.json
 *   - connections and window bounds
 *
 * Session tokens: <userData>/credentials.json, mode 0600
 *   - one per remote connection, encrypted with Electron safeStorage when the
 *     OS keyring is available. Passwords are never stored: a password is
 *     exchanged for a token at sign-in and then forgotten.
 */

import { app, safeStorage } from 'electron'
import { readFileSync, writeFileSync, mkdirSync, chmodSync } from 'fs'
import { join } from 'path'
import type { AppConfig, ConnectionConfig, CredentialInfo } from '../shared/types'
import type { ConnectionStore } from './connection-manager'

const CONFIG_FILE = 'config.json'
const CREDENTIALS_FILE = 'credentials.json'

function configPath(): string {
  return join(app.getPath('userData'), CONFIG_FILE)
}

function credentialsPath(): string {
  return join(app.getPath('userData'), CREDENTIALS_FILE)
}

/* ------------------------------------------------------------------ */
/* Non-secret config                                                   */
/* ------------------------------------------------------------------ */

const defaultConfig: AppConfig = {
  connections: [],
}

let _config: AppConfig = { ...defaultConfig }

export function loadConfig(): AppConfig {
  try {
    const raw = readFileSync(configPath(), 'utf-8')
    const parsed = JSON.parse(raw)
    _config = { ...defaultConfig, ...parsed }
    /* Only the two shapes a connection can have. A config written by the
     * old SSH transport has entries of type 'ssh', with no host or port a
     * daemon could be reached at; carrying them would make them remote
     * connections to nowhere. */
    _config.connections = (_config.connections ?? []).filter(
      (c) => c.type === 'local' || c.type === 'remote')
  } catch {
    _config = { ...defaultConfig }
  }
  console.log(`[config] loaded from ${configPath()}: ${_config.connections.length} connection(s)`)
  return _config
}

export function saveConfig(): void {
  try {
    mkdirSync(app.getPath('userData'), { recursive: true })
    writeFileSync(configPath(), JSON.stringify(_config, null, 2), 'utf-8')
  } catch (err) {
    console.error('[config] save failed:', err)
  }
}

export function getConfig(): AppConfig {
  return _config
}

export function saveConnections(configs: ConnectionConfig[]): void {
  _config.connections = configs
  saveConfig()
}

export function updateWindowBounds(bounds: { x: number; y: number; width: number; height: number }): void {
  _config.windowBounds = bounds
  saveConfig()
}

/* ------------------------------------------------------------------ */
/* Session tokens                                                      */
/* ------------------------------------------------------------------ */

/*
 * One bearer token per remote connection. A token is a 90-day credential for
 * the daemon, so the file is created 0600 and kept that way: written with
 * defaults it was 0644, readable by every account on the machine.
 */
interface TokenStore {
  [connectionName: string]: { token?: string }   // 'enc:' or 'b64:' + base64
}

let _tokens: TokenStore = {}

export function loadCredentials(): void {
  try {
    const raw = readFileSync(credentialsPath(), 'utf-8')
    const parsed = JSON.parse(raw) as Record<string, { token?: string }>
    /* Only tokens. Earlier versions kept password and sudoPassword slots,
     * which nothing reads any more and nothing should keep. */
    _tokens = {}
    for (const [name, entry] of Object.entries(parsed ?? {})) {
      if (entry && typeof entry.token === 'string') _tokens[name] = { token: entry.token }
    }
  } catch {
    _tokens = {}
  }
}

function saveCredentials(): void {
  try {
    mkdirSync(app.getPath('userData'), { recursive: true })
    const path = credentialsPath()
    writeFileSync(path, JSON.stringify(_tokens, null, 2), { encoding: 'utf-8', mode: 0o600 })
    /* mode only applies when the file is created; one written by an earlier
     * version keeps whatever it had. */
    chmodSync(path, 0o600)
  } catch (err) {
    console.error('[credentials] save failed:', err)
  }
}

/*
 * Encrypted with the OS keyring when there is one. Without one (a Linux
 * session with no secret service running, typically) the token is kept as
 * base64 in the 0600 file: anyone who can read this account's files can use
 * it, which the sign-in dialog says, so the choice is a visible one.
 */
export function credentialInfo(): CredentialInfo {
  return { encrypted: safeStorage.isEncryptionAvailable() }
}

function encrypt(plaintext: string): string {
  if (!safeStorage.isEncryptionAvailable()) {
    return 'b64:' + Buffer.from(plaintext, 'utf-8').toString('base64')
  }
  return 'enc:' + safeStorage.encryptString(plaintext).toString('base64')
}

function decrypt(stored: string): string | undefined {
  if (stored.startsWith('b64:')) {
    return Buffer.from(stored.slice(4), 'base64').toString('utf-8')
  }
  if (stored.startsWith('enc:')) {
    if (!safeStorage.isEncryptionAvailable()) {
      console.error('[credentials] cannot decrypt: the OS keyring is not available')
      return undefined
    }
    return safeStorage.decryptString(Buffer.from(stored.slice(4), 'base64'))
  }
  return undefined
}

export function getToken(name: string): string | undefined {
  const stored = _tokens[name]?.token
  if (!stored) return undefined
  try {
    return decrypt(stored)
  } catch (err) {
    console.error(`[credentials] decrypt failed for ${name}:`, err)
    return undefined
  }
}

export function setToken(name: string, token: string): void {
  _tokens[name] = { token: encrypt(token) }
  saveCredentials()
}

export function deleteToken(name: string): void {
  if (!(name in _tokens)) return
  delete _tokens[name]
  saveCredentials()
}

/* The store the connection manager is given in the app. */
export const electronConnectionStore: ConnectionStore = {
  saveConnections,
  getToken,
  setToken,
  deleteToken,
}
