import { FileHandle } from 'fs/promises'
import { Segment } from './segmentPolicy'

export interface DownloadCheckpoint {
  segments: Segment[]
  url: string
}

export interface CheckpointHandler {
  popCheckpoint(url: URL, handle: FileHandle, destination: string): Promise<DownloadCheckpoint | undefined>
  pushCheckpoint(url: URL, handle: FileHandle, destination: string, checkpoint: DownloadCheckpoint): Promise<void>
}

export function createInMemoryCheckpointHandler(): CheckpointHandler {
  const storage: Record<string, DownloadCheckpoint | undefined> = {}
  return {
    async popCheckpoint(url: URL, handle: FileHandle, destination: string) {
      const result = storage[destination]
      delete storage[destination]
      return result
    },
    async pushCheckpoint(url: URL, handle: FileHandle, destination: string, checkpoint: DownloadCheckpoint) {
      storage[destination] = checkpoint
    },
  }
}
