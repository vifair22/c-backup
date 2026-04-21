import React, { useEffect, useRef } from 'react'

export interface ContextMenuItem {
  label: string
  onClick: () => void
  danger?: boolean
}

interface Props {
  x: number
  y: number
  items: ContextMenuItem[]
  onClose: () => void
}

export function ContextMenu({ x, y, items, onClose }: Props): React.ReactElement {
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose()
    }
    const keyHandler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    document.addEventListener('mousedown', handler)
    document.addEventListener('keydown', keyHandler)
    return () => {
      document.removeEventListener('mousedown', handler)
      document.removeEventListener('keydown', keyHandler)
    }
  }, [onClose])

  // Clamp to viewport
  const style: React.CSSProperties = {
    position: 'fixed',
    left: Math.min(x, window.innerWidth - 160),
    top: Math.min(y, window.innerHeight - items.length * 32 - 8),
    zIndex: 100,
  }

  return (
    <div ref={ref} style={style}
      className="bg-surface-primary border border-border-default rounded shadow-lg py-1 min-w-[140px]">
      {items.map((item, i) => (
        <button key={i}
          onClick={() => { item.onClick(); onClose() }}
          className={`block w-full text-left px-3 py-1.5 text-xs cursor-pointer border-none bg-transparent hover:bg-surface-hover ${
            item.danger ? 'text-status-error' : 'text-text-primary'
          }`}>
          {item.label}
        </button>
      ))}
    </div>
  )
}
