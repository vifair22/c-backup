import type { CBackupApi } from '../preload/index'

declare global {
  interface Window {
    cbackup: CBackupApi
  }
}
