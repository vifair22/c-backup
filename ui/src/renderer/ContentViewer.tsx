import React, { useEffect, useState } from 'react'
import { fmtSize } from './format'

const api = window.cbackup

const TEXT_DISPLAY_CHARS = 4096
const HEX_DISPLAY_BYTES = 4096

const OBJECT_TYPES: Record<number, string> = {
  1: 'FILE', 2: 'XATTR', 3: 'ACL', 4: 'SPARSE',
}

/*
 * As the daemon sends them. Both were wrong here: locate carries orig_size
 * and phys_size, not uncompressed_size and compressed_size, and content
 * carries base64 rather than content_base64 - so decoding threw on
 * undefined and the panel showed an error for every object. There is no
 * truncation either: an object over the limit comes back as too_large with
 * no bytes at all, which is a different thing to show.
 */
interface ObjectLocateResult {
  found?: boolean
  type: number | string
  refcount?: string
  orig_size?: string
  phys_size?: string
  location?: string
  pack_id?: string
}

interface ObjectContentResult {
  type?: number
  size?: number
  base64?: string
  too_large?: boolean
  limit?: number
}

function isBinary(data: Uint8Array): boolean {
  const check = Math.min(data.length, 4096)
  for (let i = 0; i < check; i++) {
    const b = data[i]
    if (b < 0x09 || (b > 0x0d && b < 0x20 && b !== 0x1b)) return true
  }
  return false
}

function hexDump(data: Uint8Array, maxBytes: number): string {
  const lines: string[] = []
  const len = Math.min(data.length, maxBytes)
  for (let off = 0; off < len; off += 16) {
    const hex: string[] = []
    const ascii: string[] = []
    for (let j = 0; j < 16; j++) {
      if (off + j < len) {
        const b = data[off + j]
        hex.push(b.toString(16).padStart(2, '0'))
        ascii.push(b >= 0x20 && b < 0x7f ? String.fromCharCode(b) : '.')
      } else {
        hex.push('  ')
        ascii.push(' ')
      }
    }
    const offsetStr = off.toString(16).padStart(8, '0')
    lines.push(`${offsetStr}  ${hex.slice(0, 8).join(' ')}  ${hex.slice(8).join(' ')}  |${ascii.join('')}|`)
  }
  if (data.length > maxBytes) {
    lines.push(`... ${data.length - maxBytes} more bytes not shown`)
  }
  return lines.join('\n')
}

type Tab = 'info' | 'text' | 'hex'

interface Props {
  connName: string
  repoPath: string
  hash: string
  filename?: string
  onClose: () => void
}

export function ContentViewer({ connName, repoPath, hash, filename, onClose }: Props): React.ReactElement {
  const repoId = Number(repoPath)

  const [tab, setTab] = useState<Tab>('info')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [locate, setLocate] = useState<ObjectLocateResult | null>(null)
  const [rawData, setRawData] = useState<Uint8Array | null>(null)
  const [tooLarge, setTooLarge] = useState<{ size: number; limit: number } | null>(null)
  const [binary, setBinary] = useState(false)
  const [copied, setCopied] = useState(false)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)

    ;(async () => {
      try {
        const loc = await api.apiCall<ObjectLocateResult>(connName, {
          kind: 'objectInfo', repoId, hash,
        })
        if (cancelled) return
        setLocate(loc)

        const content = await api.apiCall<ObjectContentResult>(connName, {
          kind: 'objectContent', repoId, hash,
        })
        if (cancelled) return

        /* The daemon refuses to send an object over its limit rather than
         * cutting one short, so this is a whole file or none of it. */
        if (content.too_large) {
          setTooLarge({ size: Number(content.size ?? 0), limit: Number(content.limit ?? 0) })
          setRawData(null)
        } else {
          const decoded = Uint8Array.from(atob(content.base64 ?? ''), c => c.charCodeAt(0))
          setRawData(decoded)
          setBinary(isBinary(decoded))
        }
      } catch (err) {
        if (!cancelled) setError(String(err))
      }
      if (!cancelled) setLoading(false)
    })()

    return () => { cancelled = true }
  }, [connName, repoId, hash])

  const copyHash = () => {
    navigator.clipboard.writeText(hash)
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }

  const textContent = (): string => {
    if (!rawData || binary) return '(binary data - not valid UTF-8)'
    try {
      const text = new TextDecoder('utf-8', { fatal: true }).decode(rawData)
      if (text.length > TEXT_DISPLAY_CHARS) {
        return text.slice(0, TEXT_DISPLAY_CHARS) + `\n\n... ${(text.length - TEXT_DISPLAY_CHARS).toLocaleString()} more chars not shown`
      }
      return text
    } catch {
      return '(binary data - not valid UTF-8)'
    }
  }

  const tabs: { key: Tab; label: string }[] = [
    { key: 'info', label: 'Info' },
    { key: 'text', label: 'Text' },
    { key: 'hex', label: 'Hex' },
  ]

  return (
    <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50">
      <div className="bg-surface-primary rounded-lg shadow-xl border border-border-default flex flex-col"
        style={{ width: 'min(90vw, 800px)', height: 'min(80vh, 600px)' }}>

        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-border-default shrink-0">
          <div className="min-w-0">
            <div className="text-sm font-semibold truncate">{filename || hash.slice(0, 16) + '...'}</div>
            {filename && <div className="text-[10px] text-text-muted font-mono truncate">{hash}</div>}
          </div>
          <button onClick={onClose}
            className="text-text-muted hover:text-text-primary bg-transparent border-none cursor-pointer text-lg px-2 shrink-0">
            &times;
          </button>
        </div>

        {/* Tabs */}
        <div className="flex gap-0 border-b border-border-default shrink-0">
          {tabs.map(t => (
            <button key={t.key} onClick={() => setTab(t.key)}
              className={`px-4 py-2 text-xs cursor-pointer border-none bg-transparent ${
                tab === t.key
                  ? 'text-accent border-b-2 border-accent font-medium'
                  : 'text-text-muted hover:text-text-primary'
              }`}>
              {t.label}
            </button>
          ))}
        </div>

        {/* Content */}
        <div className="flex-1 overflow-auto p-4 min-h-0">
          {loading && <div className="text-text-muted text-sm">Loading object...</div>}
          {error && <div className="text-status-error text-sm">{error}</div>}

          {!loading && !error && (
            <>
              {tab === 'info' && locate && (
                <div className="space-y-3">
                  {filename && (
                    <div>
                      <div className="text-[10px] text-text-muted uppercase tracking-wide">Filename</div>
                      <div className="text-sm text-text-primary">{filename}</div>
                    </div>
                  )}
                  <div>
                    <div className="text-[10px] text-text-muted uppercase tracking-wide">Hash</div>
                    <div className="text-sm font-mono text-text-primary flex items-center gap-2">
                      <span className="break-all">{hash}</span>
                      <button onClick={copyHash}
                        className="text-[10px] px-1.5 py-0.5 rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none cursor-pointer shrink-0">
                        {copied ? 'Copied' : 'Copy'}
                      </button>
                    </div>
                  </div>
                  <div className="grid grid-cols-4 gap-4">
                    <div>
                      <div className="text-[10px] text-text-muted uppercase tracking-wide">Type</div>
                      <div className="text-sm text-text-primary">
                        {OBJECT_TYPES[Number(locate.type)] ?? `Unknown (${locate.type})`}
                      </div>
                    </div>
                    <div>
                      <div className="text-[10px] text-text-muted uppercase tracking-wide">Size</div>
                      <div className="text-sm text-text-primary">{fmtSize(locate.orig_size)}</div>
                      <div className="text-[10px] text-text-muted">
                        {Number(locate.orig_size ?? 0).toLocaleString()} bytes
                      </div>
                    </div>
                    <div>
                      <div className="text-[10px] text-text-muted uppercase tracking-wide">On disk</div>
                      <div className="text-sm text-text-primary">{fmtSize(locate.phys_size)}</div>
                    </div>
                    <div>
                      <div className="text-[10px] text-text-muted uppercase tracking-wide">Stored</div>
                      <div className="text-sm text-text-primary">{locate.location ?? '-'}</div>
                      {locate.pack_id !== undefined && Number(locate.pack_id) >= 0 && (
                        <div className="text-[10px] text-text-muted">pack #{locate.pack_id}</div>
                      )}
                    </div>
                  </div>
                  {binary && (
                    <div className="text-xs text-status-warning bg-status-warning/10 rounded px-3 py-2">
                      Binary content detected. Text tab will show hex representation.
                    </div>
                  )}
                  {tooLarge && (
                    <div className="text-xs text-status-warning bg-status-warning/10 rounded px-3 py-2">
                      {fmtSize(tooLarge.size)} is over the {fmtSize(tooLarge.limit)} the
                      daemon will send in one response, so none of it was
                      loaded. Restore the file to read it.
                    </div>
                  )}
                </div>
              )}

              {(tab === 'text' || tab === 'hex') && !rawData && tooLarge && (
                <div className="text-xs text-text-muted">
                  Nothing was loaded: the object is larger than the daemon
                  sends in one response.
                </div>
              )}

              {tab === 'text' && rawData && (
                <div>
                  <div className="text-[10px] text-text-muted mb-2">
                    {binary ? 'Binary content' : `UTF-8 text - ${rawData.length.toLocaleString()} bytes`}
                  </div>
                  <pre className="text-xs font-mono text-text-primary whitespace-pre-wrap break-all leading-relaxed m-0">
                    {textContent()}
                  </pre>
                </div>
              )}

              {tab === 'hex' && rawData && (
                <div>
                  <div className="text-[10px] text-text-muted mb-2">
                    {/* Bare JSX text is not a string literal, so the \u2014
                      * that used to be here rendered as those six characters
                      * on screen rather than as a dash. */}
                    Hex dump - showing {Math.min(rawData.length, HEX_DISPLAY_BYTES).toLocaleString()} of {rawData.length.toLocaleString()} bytes
                  </div>
                  <pre className="text-[11px] font-mono text-text-primary whitespace-pre leading-relaxed m-0">
                    {hexDump(rawData, HEX_DISPLAY_BYTES)}
                  </pre>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  )
}
