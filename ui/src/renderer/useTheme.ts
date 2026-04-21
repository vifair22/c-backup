import { useEffect, useState } from 'react'

export type ThemeMode = 'system' | 'light' | 'dark'

const STORAGE_KEY = 'cbackup-theme'

function getSystemPreference(): 'light' | 'dark' {
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

function applyTheme(mode: ThemeMode): void {
  const resolved = mode === 'system' ? getSystemPreference() : mode
  document.documentElement.classList.toggle('dark', resolved === 'dark')
}

export function useTheme(): {
  mode: ThemeMode
  resolved: 'light' | 'dark'
  setMode: (mode: ThemeMode) => void
} {
  const [mode, setModeState] = useState<ThemeMode>(() => {
    const stored = localStorage.getItem(STORAGE_KEY)
    return (stored === 'light' || stored === 'dark' || stored === 'system')
      ? stored
      : 'system'
  })

  const resolved = mode === 'system' ? getSystemPreference() : mode

  const setMode = (newMode: ThemeMode) => {
    setModeState(newMode)
    localStorage.setItem(STORAGE_KEY, newMode)
    applyTheme(newMode)
  }

  // Apply on mount and listen for OS changes
  useEffect(() => {
    applyTheme(mode)

    const mq = window.matchMedia('(prefers-color-scheme: dark)')
    const handler = () => {
      if (mode === 'system') applyTheme('system')
    }
    mq.addEventListener('change', handler)
    return () => mq.removeEventListener('change', handler)
  }, [mode])

  return { mode, resolved, setMode }
}
