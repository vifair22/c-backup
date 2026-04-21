import React from 'react'

interface Props {
  label: string
  value: string
  detail?: string
  status?: 'ok' | 'warn' | 'error'
  children?: React.ReactNode
}

const borderColor = {
  ok: 'border-l-status-connected',
  warn: 'border-l-status-warning',
  error: 'border-l-status-error',
}

export function StatCard({ label, value, detail, status, children }: Props): React.ReactElement {
  return (
    <div className={`bg-surface-secondary border border-border-default rounded-lg p-4 border-l-4 h-full ${
      status ? borderColor[status] : 'border-l-border-default'
    }`}>
      <div className="text-[11px] text-text-muted uppercase tracking-wide mb-1">{label}</div>
      <div className="text-xl font-semibold text-text-primary">{value}</div>
      {detail && <div className="text-xs text-text-secondary mt-1">{detail}</div>}
      {children}
    </div>
  )
}
