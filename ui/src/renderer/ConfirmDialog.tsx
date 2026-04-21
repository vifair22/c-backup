import React from 'react'

interface Props {
  title: string
  message: string
  confirmLabel?: string
  cancelLabel?: string
  danger?: boolean
  onConfirm: () => void
  onCancel: () => void
}

export function ConfirmDialog({ title, message, confirmLabel = 'Delete', cancelLabel = 'Cancel', danger = true, onConfirm, onCancel }: Props): React.ReactElement {
  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
      <div className="bg-surface-primary rounded-lg p-5 w-80 shadow-xl border border-border-default">
        <h3 className="text-sm font-semibold m-0 mb-2">{title}</h3>
        <p className="text-xs text-text-secondary m-0 mb-4">{message}</p>
        <div className="flex justify-end gap-2">
          <button onClick={onCancel}
            className="px-3 py-1.5 text-xs cursor-pointer rounded bg-surface-tertiary text-text-secondary hover:bg-surface-hover border-none">
            {cancelLabel}
          </button>
          <button onClick={onConfirm}
            className={`px-3 py-1.5 text-xs cursor-pointer rounded border-none ${
              danger
                ? 'bg-status-error text-white hover:opacity-90'
                : 'bg-accent text-accent-text hover:bg-accent-hover'
            }`}>
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  )
}
