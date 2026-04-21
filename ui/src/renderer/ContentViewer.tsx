import React, { useEffect, useState } from 'react'
import { fmtSize } from './format'

const api = window.cbackup

const CONTENT_MAX_BYTES = 2 * 1024 * 1024
const TEXT_DISPLAY_CHARS = 4096
const HEX_DISPLAY_BYTES = 4096

const OBJECT_TYPES: Record<number, string> = {
  1: 'FILE', 2: 'XATTR', 3: 'ACL', 4: 'SPARSE',
}

interface ObjectLocateResult {
  type: number
  uncompressed_size: number
  compressed_size?: number
  location?: string
}

interface ObjectContentResult {
  type: number
  size: number
  truncated: boolean
  content_base64: string
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
  const [tab, setTab] = useState<Tab>('info')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [locate, setLocate] = useState<ObjectLocateResult | null>(null)
  const [rawData, setRawData] = useState<Uint8Array | null>(null)
  const [truncated, setTruncated] = useState(false)
  const [binary, setBinary] = useState(false)
  const [copied, setCopied] = useState(false)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)

    ;(async () => {
      try {
        const loc = await api.rpcCall<ObjectLocateResult>(connName, repoPath, 'object_locate', { hash })
        if (cancelled) return
        setLocate(loc)

        const content = await api.rpcCall<ObjectContentResult>(connName, repoPath, 'object_content', { hash, max_bytes: CONTENT_MAX_BYTES })
        if (cancelled) return

        const decoded = Uint8Array.from(atob(content.content_base64), c => c.charCodeAt(0))
        setRawData(decoded)
        setTruncated(content.truncated)
        setBinary(isBinary(decoded))
      } catch (err) {
        if (!cancelled) setError(String(err))
      }
      if (!cancelled) setLoading(false)
    })()

    return () => { cancelled = true }
  }, [connName, repoPath, hash])

  const copyHash = () => {
    navigator.clipboard.writeText(hash)
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }

  const textContent = (): string => {
    if (!rawData || binary) return '(binary data \u2014 not valid UTF-8)'
    try {
      const text = new TextDecoder('utf-8', { fatal: true }).decode(rawData)
      if (text.length > TEXT_DISPLAY_CHARS) {
        return text.slice(0, TEXT_DISPLAY_CHARS) + `\n\n\u2026 ${(text.length - TEXT_DISPLAY_CHARS).toLocaleString()} more chars not shown`
      }
      return text
    } catch {
      return '(binary data \u2014 not valid UTF-8)'
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
                  <div className="grid grid-cols-3 gap-4">
                    <div>
                      <div className="text-[10px] text-text-muted uppercase tracking-wide">Type</div>
                      <div className="text-sm text-text-primary">{OBJECT_TYPES[locate.type] ?? `Unknown (${locate.type})`}</div>
                    </div>
                    <div>
                      <div className="text-[10px] text-text-muted uppercase tracking-wide">Size</div>
                      <div className="text-sm text-text-primary">{fmtSize(locate.uncompressed_size)}</div>
                      <div className="text-[10px] text-text-muted">{locate.uncompressed_size.toLocaleString()} bytes</div>
                    </div>
                    {locate.compressed_size !== undefined && (
                      <div>
                        <div className="text-[10px] text-text-muted uppercase tracking-wide">Compressed</div>
                        <div className="text-sm text-text-primary">{fmtSize(locate.compressed_size)}</div>
                      </div>
                    )}
                  </div>
                  {binary && (
                    <div className="text-xs text-status-warning bg-status-warning/10 rounded px-3 py-2">
                      Binary content detected. Text tab will show hex representation.
                    </div>
                  )}
                  {truncated && (
                    <div className="text-xs text-status-warning bg-status-warning/10 rounded px-3 py-2">
                      Content truncated to {fmtSize(CONTENT_MAX_BYTES)} (full size: {fmtSize(locate.uncompressed_size)})
                    </div>
                  )}
                </div>
              )}

              {tab === 'text' && rawData && (
                <div>
                  <div className="text-[10px] text-text-muted mb-2">
                    {binary ? 'Binary content' : `UTF-8 text \u2014 ${rawData.length.toLocaleString()} bytes loaded`}
                    {truncated && ' (truncated)'}
                  </div>
                  <pre className="text-xs font-mono text-text-primary whitespace-pre-wrap break-all leading-relaxed m-0">
                    {textContent()}
                  </pre>
                </div>
              )}

              {tab === 'hex' && rawData && (
                <div>
                  <div className="text-[10px] text-text-muted mb-2">
                    Hex dump \u2014 showing {Math.min(rawData.length, HEX_DISPLAY_BYTES).toLocaleString()} of {rawData.length.toLocaleString()} bytes
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
