/**
 * Config store — persists app configuration to disk.
 *
 * Non-secret config: <userData>/config.json
 *   - connections (with repos list inside each connection)
 *   - window bounds
 *
 * Secrets (passwords): <userData>/credentials.json
 *   - encrypted via Electron safeStorage (OS keychain)
 */

import { app, safeStorage } from 'electron'
import { readFileSync, writeFileSync, mkdirSync } from 'fs'
import { join } from 'path'
import type { AppConfig, ConnectionConfig } from '../shared/types'

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
    // Ensure every connection has a repos array
    for (const conn of _config.connections) {
      if (!Array.isArray(conn.repos)) conn.repos = []
    }
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

export function addConnectionConfig(config: ConnectionConfig): void {
  _config.connections = _config.connections.filter(c => c.name !== config.name)
  if (!Array.isArray(config.repos)) config.repos = []
  _config.connections.push(config)
  saveConfig()
}

export function removeConnectionConfig(name: string): void {
  _config.connections = _config.connections.filter(c => c.name !== name)
  saveConfig()
}

export function updateConnectionConfig(name: string, newConfig: ConnectionConfig): void {
  const idx = _config.connections.findIndex(c => c.name === name)
  if (idx === -1) return
  // Preserve existing repos list unless the new config carries one
  if (!Array.isArray(newConfig.repos) || newConfig.repos.length === 0) {
    newConfig.repos = _config.connections[idx].repos
  }
  _config.connections[idx] = newConfig
  saveConfig()
}

export function addRepoToConnection(connectionName: string, repoPath: string): void {
  const conn = _config.connections.find(c => c.name === connectionName)
  if (!conn) return
  if (!conn.repos.includes(repoPath)) {
    conn.repos.push(repoPath)
    saveConfig()
  }
}

export function removeRepoFromConnection(connectionName: string, repoPath: string): void {
  const conn = _config.connections.find(c => c.name === connectionName)
  if (!conn) return
  conn.repos = conn.repos.filter(r => r !== repoPath)
  saveConfig()
}

export function updateWindowBounds(bounds: { x: number; y: number; width: number; height: number }): void {
  _config.windowBounds = bounds
  saveConfig()
}

/* ------------------------------------------------------------------ */
/* Secrets (passwords encrypted via safeStorage)                       */
/* ------------------------------------------------------------------ */

interface CredentialStore {
  [connectionName: string]: {
    password?: string      // encrypted base64
    sudoPassword?: string  // encrypted base64
  }
}

let _credentials: CredentialStore = {}

export function loadCredentials(): void {
  try {
    const raw = readFileSync(credentialsPath(), 'utf-8')
    _credentials = JSON.parse(raw)
  } catch {
    _credentials = {}
  }
}

function saveCredentials(): void {
  try {
    mkdirSync(app.getPath('userData'), { recursive: true })
    writeFileSync(credentialsPath(), JSON.stringify(_credentials, null, 2), 'utf-8')
  } catch (err) {
    console.error('[credentials] save failed:', err)
  }
}

function encrypt(plaintext: string): string {
  if (!safeStorage.isEncryptionAvailable()) {
    console.warn('[credentials] safeStorage not available — using base64 (insecure)')
    return 'b64:' + Buffer.from(plaintext, 'utf-8').toString('base64')
  }
  const encrypted = safeStorage.encryptString(plaintext)
  return 'enc:' + encrypted.toString('base64')
}

function decrypt(stored: string): string {
  if (stored.startsWith('b64:')) {
    return Buffer.from(stored.slice(4), 'base64').toString('utf-8')
  }
  if (stored.startsWith('enc:')) {
    if (!safeStorage.isEncryptionAvailable()) {
      console.error('[credentials] cannot decrypt: safeStorage not available')
      return ''
    }
    const buf = Buffer.from(stored.slice(4), 'base64')
    return safeStorage.decryptString(buf)
  }
  return stored
}

export function setCredential(
  connectionName: string,
  type: 'password' | 'sudoPassword',
  value: string
): void {
  if (!_credentials[connectionName]) _credentials[connectionName] = {}
  _credentials[connectionName][type] = encrypt(value)
  saveCredentials()
}

export function getCredential(
  connectionName: string,
  type: 'password' | 'sudoPassword'
): string | undefined {
  const entry = _credentials[connectionName]
  if (!entry) return undefined
  const stored = entry[type]
  if (!stored) return undefined
  try {
    return decrypt(stored)
  } catch (err) {
    console.error(`[credentials] decrypt failed for ${connectionName}:`, err)
    return undefined
  }
}

export function deleteCredentials(connectionName: string): void {
  delete _credentials[connectionName]
  saveCredentials()
}

export function hasStoredCredentials(connectionName: string): boolean {
  const entry = _credentials[connectionName]
  if (!entry) return false
  return !!(entry.password || entry.sudoPassword)
}
