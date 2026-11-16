/*
 * The connection manager, against a real daemon over both transports.
 *
 * This is the main-process half of remote access: connecting, signing in,
 * keeping a session, noticing when it ends. None of it had ever run. The
 * manager was reachable only through the Electron window, the Playwright
 * fixtures all use the unix socket, and the remote path it carried could not
 * have worked: /readyz answered 401 to a configured daemon before the login
 * that would have fixed it was ever attempted.
 *
 * The daemon starts with no admin password, so the first tests see a remote
 * daemon that has not been set up; the password is then set the only way it
 * can be, with the CLI over the socket, and the rest run against a
 * configured one. Order matters, so the tests are sequential.
 */

import { test, before, after } from 'node:test'
import assert from 'node:assert/strict'
import http from 'node:http'
import { createServer } from 'node:net'
import { spawn, type ChildProcess } from 'node:child_process'
import { mkdtempSync, rmSync, writeFileSync, mkdirSync, existsSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

import { ConnectionManager, MemoryConnectionStore } from '../src/main/connection-manager.ts'
import type { ConnectionConfig } from '../src/shared/types.ts'

const REPO_ROOT = resolve(import.meta.dirname, '..', '..')
const DAEMON = join(REPO_ROOT, 'build', 'bins', 'c-backupd')
const CLI = join(REPO_ROOT, 'build', 'bins', 'c-backup')
const PASSWORD = 'correcthorsebattery'

/* A port per run, so a suite left running elsewhere cannot answer for this
 * one. Chosen by the kernel in before() (FLAKE-13): it was derived from the
 * pid, which another run on the host could share. */
let PORT = 0

function freePort(): Promise<number> {
  return new Promise((done, fail) => {
    const srv = createServer()
    srv.once('error', fail)
    srv.listen(0, () => {
      const addr = srv.address()
      const port = typeof addr === 'object' && addr ? addr.port : 0
      srv.close(() => done(port))
    })
  })
}

let work: string
let sock: string
let daemon: ChildProcess

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

const local = (): ConnectionConfig => ({ type: 'local', name: 'here', socketPath: sock })
const remote = (name = 'there', port = PORT): ConnectionConfig =>
  ({ type: 'remote', name, host: '127.0.0.1', port })

/* Run the CLI over the socket, feeding stdin. Resolves with its exit code. */
function cli(args: string[], stdin = ''): Promise<number> {
  return new Promise((done) => {
    const p = spawn(CLI, ['--socket', sock, ...args], { stdio: ['pipe', 'ignore', 'ignore'] })
    p.on('exit', (code) => done(code ?? -1))
    p.stdin.end(stdin)
  })
}

/* A raw TCP request, outside the manager, for checking what the daemon
 * thinks of a token. */
function tcp(method: string, path: string, token?: string): Promise<number> {
  return new Promise((done, reject) => {
    const req = http.request({
      host: '127.0.0.1', port: PORT, method, path,
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    }, (res) => { res.resume(); res.on('end', () => done(res.statusCode ?? 0)) })
    req.on('error', reject)
    req.end()
  })
}

before(async () => {
  assert.ok(existsSync(DAEMON), `build the daemon first: ${DAEMON}`)
  PORT = await freePort()
  work = mkdtempSync(join(tmpdir(), 'cbackup-connections-'))
  sock = join(work, 'sock')
  mkdirSync(join(work, 'keys'), { recursive: true })
  writeFileSync(join(work, 'config.yaml'),
    `db:\n  path: ${work}/state.db\n` +
    `http:\n  socket: ${sock}\n  port: ${PORT}\n  bind: 127.0.0.1\n` +
    `security:\n  key_dir: ${work}/keys\n` +
    `log:\n  level: warn\n`)
  daemon = spawn(DAEMON, [], {
    env: { ...process.env, C_BACKUP_CONFIG_PATH: join(work, 'config.yaml') },
    stdio: 'ignore',
  })
  for (let i = 0; i < 100 && !existsSync(sock); i++) await sleep(50)
  assert.ok(existsSync(sock), 'daemon did not start')
})

after(async () => {
  daemon?.kill()
  await sleep(200)
  if (work) rmSync(work, { recursive: true, force: true })
})

test('a local connection connects with no credential, and is saved', async () => {
  const store = new MemoryConnectionStore()
  const m = new ConnectionManager(store)
  assert.deepEqual(m.addConnection(local()), { ok: true })
  assert.equal(store.saved.length, 1, 'adding a connection saves the list')

  const r = await m.connect('here')
  assert.equal(r.ok, true, r.error)
  assert.equal(m.getStatus('here')?.status, 'connected')
  assert.equal(m.getStatus('here')?.signedIn, false)
})

test('a remote daemon with no password says how to set one, and does not prompt', async () => {
  const m = new ConnectionManager(new MemoryConnectionStore())
  m.addConnection(remote())
  const r = await m.connect('there')
  assert.equal(r.ok, false)
  assert.notEqual(r.needsCredentials, true, 'there is no password it could take')
  assert.match(r.error ?? '', /c-backup auth setup/)
  assert.equal(m.getStatus('there')?.status, 'error')
})

test('the password is set on the host, over the socket', async () => {
  assert.equal(await cli(['auth', 'setup'], `${PASSWORD}\n`), 0)
})

test('a configured remote with nothing stored asks for the password', async () => {
  const m = new ConnectionManager(new MemoryConnectionStore())
  m.addConnection(remote())
  const r = await m.connect('there')
  assert.equal(r.ok, false)
  assert.equal(r.needsCredentials, true)
  const s = m.getStatus('there')
  assert.equal(s?.status, 'disconnected')
  assert.equal(s?.needsCredentials, true)
})

test('a wrong password is refused and nothing is stored', async () => {
  const store = new MemoryConnectionStore()
  const m = new ConnectionManager(store)
  m.addConnection(remote())
  const r = await m.connect('there', { password: 'not the password', remember: true })
  assert.equal(r.ok, false)
  assert.match(r.error ?? '', /login failed/)
  assert.equal(store.getToken('there'), undefined)
})

test('signing in stores the session token, never the password, and lists repos', async () => {
  const store = new MemoryConnectionStore()
  const m = new ConnectionManager(store)
  m.addConnection(remote())
  const r = await m.connect('there', { password: PASSWORD, remember: true })
  assert.equal(r.ok, true, r.error)

  const token = store.getToken('there')
  assert.ok(token && token !== PASSWORD, 'a token is stored, and it is not the password')
  assert.equal(await tcp('GET', '/repos', token), 200, 'and the daemon accepts it')
  assert.equal(m.getStatus('there')?.signedIn, true)
  assert.ok(!JSON.stringify(store.saved).includes(PASSWORD), 'the config holds no password')
})

test('not staying signed in keeps the session for this run only', async () => {
  const store = new MemoryConnectionStore()
  const m = new ConnectionManager(store)
  m.addConnection(remote())
  const r = await m.connect('there', { password: PASSWORD, remember: false })
  assert.equal(r.ok, true, r.error)
  assert.equal(store.getToken('there'), undefined)
  assert.equal(m.getStatus('there')?.status, 'connected')
})

test('a stored session reconnects at startup without asking', async () => {
  const store = new MemoryConnectionStore()
  const first = new ConnectionManager(store)
  first.addConnection(remote())
  assert.equal((await first.connect('there', { password: PASSWORD, remember: true })).ok, true)

  /* A new process: same store, nothing in memory. */
  const second = new ConnectionManager(store)
  await second.restoreFromConfig(store.saved)
  assert.equal(second.getStatus('there')?.status, 'connected')
})

test('a remote with nothing stored waits at startup rather than prompting', async () => {
  const m = new ConnectionManager(new MemoryConnectionStore())
  await m.restoreFromConfig([remote()])
  assert.equal(m.getStatus('there')?.status, 'disconnected')
  assert.notEqual(m.getStatus('there')?.needsCredentials, true)
})

test('a stored token the daemon refuses is forgotten, and the password is asked for', async () => {
  const store = new MemoryConnectionStore()
  store.setToken('there', 'f'.repeat(64))
  const m = new ConnectionManager(store)
  m.addConnection(remote())
  const r = await m.connect('there')
  assert.equal(r.needsCredentials, true)
  assert.equal(store.getToken('there'), undefined)
})

test('a session that ends mid-use says so and asks again', async () => {
  const store = new MemoryConnectionStore()
  const m = new ConnectionManager(store)
  m.addConnection(remote())
  assert.equal((await m.connect('there', { password: PASSWORD, remember: true })).ok, true)

  /* Revoked behind the app's back: a sign-out from another machine, say. */
  const token = store.getToken('there')
  assert.equal(await tcp('POST', '/api/auth/logout', token), 204)

  await assert.rejects(m.apiCall('there', { kind: 'repoList' }), /sign in again/)
  const s = m.getStatus('there')
  assert.equal(s?.status, 'disconnected', 'not left saying connected')
  assert.equal(s?.needsCredentials, true)
  assert.equal(store.getToken('there'), undefined)
})

test('signing out revokes the session on the daemon and forgets it here', async () => {
  const store = new MemoryConnectionStore()
  const m = new ConnectionManager(store)
  m.addConnection(remote())
  assert.equal((await m.connect('there', { password: PASSWORD, remember: true })).ok, true)
  const token = store.getToken('there')

  assert.deepEqual(await m.signOut('there'), { ok: true })
  assert.equal(store.getToken('there'), undefined)
  assert.equal(m.getStatus('there')?.signedIn, false)
  assert.equal(m.getStatus('there')?.needsCredentials, true, 'shown as signed out')
  assert.equal(await tcp('GET', '/repos', token), 401, 'the daemon no longer accepts it')
})

test('an unreachable remote says where it looked and what to check', async () => {
  const m = new ConnectionManager(new MemoryConnectionStore())
  /* Port 1 on loopback: nothing listens there. */
  m.addConnection(remote('nowhere', 1))
  const r = await m.connect('nowhere')
  assert.equal(r.ok, false)
  assert.match(r.error ?? '', /no daemon at 127\.0\.0\.1:1/)
  assert.match(r.error ?? '', /http\.bind/)
  assert.doesNotMatch(r.error ?? '', /systemctl/, 'that remedy is for a local daemon')
})

test('repositories are added and removed by id', async () => {
  const m = new ConnectionManager(new MemoryConnectionStore())
  m.addConnection(local())
  assert.equal((await m.connect('here')).ok, true)

  const path = join(work, 'repo-one')
  assert.deepEqual(await m.addRepo('here', path), { ok: true })
  const repo = m.getStatus('here')?.repos.find((r) => r.path === path)
  assert.ok(repo, 'listed after adding')

  /* The renderer used to send the path here, which became /repos/NaN. */
  const bad = await m.removeRepo('here', Number(path))
  assert.equal(bad.ok, false)
  assert.match(bad.error ?? '', /not a repository id/)

  assert.deepEqual(await m.removeRepo('here', repo.id), { ok: true })
  assert.equal(m.getStatus('here')?.repos.some((r) => r.id === repo.id), false)
})

test('editing a connection saves it, keeps the session across a rename, drops it on a new address', async () => {
  const store = new MemoryConnectionStore()
  const m = new ConnectionManager(store)
  m.addConnection(remote('a'))
  store.setToken('a', 'token-for-a')

  assert.deepEqual(await m.editConnection('a', remote('b')), { ok: true })
  assert.deepEqual(store.saved.map((c) => c.name), ['b'])
  assert.equal(store.getToken('a'), undefined)
  assert.equal(store.getToken('b'), 'token-for-a', 'same daemon, renamed')

  assert.deepEqual(await m.editConnection('b', remote('b', PORT + 1)), { ok: true })
  assert.equal(store.getToken('b'), undefined, 'a token belongs to the daemon that issued it')

  m.addConnection(remote('c'))
  assert.equal((await m.editConnection('b', remote('c'))).ok, false, 'names stay unique')

  assert.deepEqual(m.removeConnection('b'), { ok: true })
  assert.deepEqual(store.saved.map((c) => c.name), ['c'])
})

/* B105: renaming a signed-in connection dropped it to disconnected, and it
 * stayed there until somebody clicked Connect. Session-only on purpose: that
 * token lives nowhere but the client, so a rename that rebuilt the entry
 * lost it outright. */
test('renaming a signed-in connection keeps it connected under the new name', async () => {
  const store = new MemoryConnectionStore()
  const m = new ConnectionManager(store)
  m.addConnection(local())
  m.addConnection(remote('there'))
  assert.equal((await m.connect('there', { password: PASSWORD, remember: false })).ok, true)
  const repos = m.getStatus('there')?.repos

  assert.deepEqual(await m.editConnection('there', remote('renamed')), { ok: true })
  assert.equal(m.getStatus('there'), null)
  const s = m.getStatus('renamed')
  assert.equal(s?.status, 'connected')
  assert.equal(s?.signedIn, true)
  assert.equal(s?.needsCredentials, false)
  assert.deepEqual(s?.repos, repos)
  await m.apiCall('renamed', { kind: 'repoList' })
  assert.deepEqual(m.listConnections().map((c) => c.config.name), ['here', 'renamed'],
                   'in the same place in the list')
  assert.deepEqual(store.saved.map((c) => c.name), ['here', 'renamed'])

  /* A sign-in that remembers lands on the new name. */
  assert.equal((await m.connect('renamed', { password: PASSWORD, remember: true })).ok, true)
  assert.ok(store.getToken('renamed'))
  assert.equal(store.getToken('there'), undefined)
})

test('a new address reconnects a live connection there, and leaves an idle one idle', async () => {
  const store = new MemoryConnectionStore()
  const m = new ConnectionManager(store)
  m.addConnection(remote('there', 1))
  assert.equal((await m.connect('there')).ok, false)

  /* Not live (it failed): the edit only saves. */
  assert.deepEqual(await m.editConnection('there', remote('there')), { ok: true })
  assert.equal(m.getStatus('there')?.status, 'disconnected')

  assert.equal((await m.connect('there', { password: PASSWORD, remember: true })).ok, true)
  /* Live, and the address changes: the old token is dropped rather than
   * offered to another daemon, and the connection goes there at once. */
  const r = await m.editConnection('there', remote('there', 1))
  assert.equal(r.ok, false)
  assert.match(r.error ?? '', /no daemon at 127\.0\.0\.1:1/)
  assert.equal(m.getStatus('there')?.status, 'error')
  assert.equal(store.getToken('there'), undefined)
  assert.equal(m.getStatus('there')?.signedIn, false)
})
