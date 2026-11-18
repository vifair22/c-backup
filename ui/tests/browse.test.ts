/*
 * Re-resolving a path after the daemon's index was rebuilt.
 *
 * A reindex renumbers a snapshot's nodes, and the daemon answers a node id
 * sent with a stale index generation with 409 "index rebuilt; re-resolve by
 * path" (docs/api.md, "Node ids and index generations"). The browser's
 * answer is shared/browse.ts: walk the path again from the root and carry on
 * with the new ids. This drives it against a fake daemon that numbers its
 * nodes the way the real one does (from a counter that only grows, so a
 * rebuild gives every node a new id) and refuses stale generations the way
 * the real one does.
 *
 * node:test rather than Vitest for the reason contract.test.ts gives: there
 * is no component to render, and the standard library costs no dependency.
 */

import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  INDEX_REBUILT, isIndexRebuilt, pathSegments, resolvePath,
  type DirEntry, type ListDir,
} from '../src/shared/browse.ts'

const DIR = 2
const FILE = 1

interface Spec { [name: string]: Spec | string }

class FakeIndex {
  private nextId = 1
  private nextGen = 1
  gen = 0
  calls = 0
  /* Called before each list, to land a rebuild mid-walk. */
  beforeList: ((call: number) => void) | null = null
  private children = new Map<number, DirEntry[]>()
  private ids = new Set<number>()
  /* Plain fields, not parameter properties: Node strips types, it does not
   * transform, and a parameter property needs transforming. */
  private readonly spec: Spec
  private readonly withGen: boolean

  constructor(spec: Spec, withGen = true) {
    this.spec = spec
    this.withGen = withGen
    this.rebuild()
  }

  /* What a reindex does: every row numbered afresh, a new generation. */
  rebuild(): void {
    this.children.clear()
    this.ids.clear()
    this.gen = this.nextGen++
    const add = (parent: number, s: Spec): void => {
      const rows: DirEntry[] = []
      for (const name of Object.keys(s).sort()) {
        const id = this.nextId++
        this.ids.add(id)
        const v = s[name]
        rows.push({ id, name, type: typeof v === 'string' ? FILE : DIR })
        if (typeof v !== 'string') add(id, v)
      }
      this.children.set(parent, rows)
    }
    add(0, this.spec)
  }

  list(pageSize: number): ListDir<DirEntry> {
    return async (parent, gen, offset) => {
      this.calls++
      this.beforeList?.(this.calls)
      if (gen !== undefined && gen !== this.gen) throw new Error(INDEX_REBUILT)
      if (parent !== 0 && !this.ids.has(parent)) throw new Error('no such directory in this snapshot')
      const rows = this.children.get(parent) ?? []
      return {
        entries: rows.slice(offset, offset + pageSize),
        gen: this.withGen ? this.gen : undefined,
      }
    }
  }
}

const SPEC: Spec = {
  etc: { 'hosts': 'x', 'fstab': 'x' },
  srv: { data: { 'a.txt': 'x', 'b.txt': 'x', deep: { 'z.bin': 'x' } } },
  'readme': 'x',
}

const names = (rows: DirEntry[] | null): string[] => (rows ?? []).map(r => r.name)

test('a path resolves to its node ids and the generation they belong to', async () => {
  const d = new FakeIndex(SPEC)
  const r = await resolvePath(d.list(500), ['srv', 'data'], 500)
  assert.equal(r.gen, d.gen)
  assert.deepEqual(names(r.root), ['etc', 'readme', 'srv'])
  assert.deepEqual(r.steps.map(s => s.name), ['srv', 'data'])
  assert.deepEqual(names(r.steps[1].entries), ['a.txt', 'b.txt', 'deep'])
})

test('after a rebuild, the old generation is refused and re-resolving finds the same directory', async () => {
  const d = new FakeIndex(SPEC)
  const list = d.list(500)
  const before = await resolvePath(list, ['srv', 'data'], 500)
  const dataId = before.steps[1].nodeId

  d.rebuild()

  /* What the browser does next with the id it kept: refused, not misread. */
  await assert.rejects(list(dataId, before.gen, 0), (err: unknown) => isIndexRebuilt(err))

  const after = await resolvePath(list, ['srv', 'data'], 500)
  assert.notEqual(after.gen, before.gen)
  assert.notEqual(after.steps[1].nodeId, dataId)
  assert.deepEqual(names(after.steps[1].entries), names(before.steps[1].entries))
  /* And the new pair is served. */
  const page = await list(after.steps[1].nodeId, after.gen, 0)
  assert.deepEqual(names(page.entries), ['a.txt', 'b.txt', 'deep'])
})

test('a rebuild landing part way through the walk starts it again', async () => {
  const d = new FakeIndex(SPEC)
  /* The root read is call 1; rebuild just before the second read. */
  d.beforeList = (call) => { if (call === 2) d.rebuild() }
  const r = await resolvePath(d.list(500), ['srv', 'data', 'deep'], 500)
  assert.equal(r.gen, d.gen)
  assert.deepEqual(r.steps.map(s => s.name), ['srv', 'data', 'deep'])
  assert.deepEqual(names(r.steps[2].entries), ['z.bin'])
  /* Every id it returned is one the current numbering has. */
  const page = await d.list(500)(r.steps[2].nodeId, r.gen, 0)
  assert.deepEqual(names(page.entries), ['z.bin'])
})

test('rebuilds that never stop give up with the 409 rather than loop', async () => {
  const d = new FakeIndex(SPEC)
  d.beforeList = (call) => { if (call % 2 === 0) d.rebuild() }
  await assert.rejects(resolvePath(d.list(500), ['srv', 'data'], 500, true, 3),
    (err: unknown) => isIndexRebuilt(err))
})

test('a component past the first page of its directory is found', async () => {
  const wide: Spec = {}
  for (let i = 0; i < 7; i++) wide[`f${i}`] = 'x'
  wide['zz'] = { 'inner': 'x' }
  const d = new FakeIndex({ top: wide })
  const r = await resolvePath(d.list(2), ['top', 'zz'], 2)
  assert.deepEqual(r.steps.map(s => s.name), ['top', 'zz'])
  assert.deepEqual(names(r.steps[1].entries), ['inner'])
})

test('a path that no longer exists resolves as far as it goes', async () => {
  const d = new FakeIndex(SPEC)
  const r = await resolvePath(d.list(500), ['srv', 'gone', 'x'], 500)
  assert.deepEqual(r.steps.map(s => s.name), ['srv'])
})

test('a file at the end of the path is found and not listed', async () => {
  const d = new FakeIndex(SPEC)
  const r = await resolvePath(d.list(500), pathSegments('/etc/hosts'), 500)
  assert.deepEqual(r.steps.map(s => s.name), ['etc', 'hosts'])
  assert.equal(r.steps[1].entries, null)
})

test('a daemon older than generations still browses', async () => {
  const d = new FakeIndex(SPEC, false)
  const r = await resolvePath(d.list(500), ['srv'], 500)
  assert.equal(r.gen, undefined)
  assert.deepEqual(names(r.steps[0].entries), ['data'])
})

test('only the stale-generation message counts as a rebuild', () => {
  assert.equal(isIndexRebuilt(new Error(INDEX_REBUILT)), true)
  assert.equal(isIndexRebuilt(`Error: ${INDEX_REBUILT}`), true)
  assert.equal(isIndexRebuilt(new Error('no such directory in this snapshot')), false)
})
