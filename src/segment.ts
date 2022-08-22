import { FileHandle } from 'fs/promises'
import { Writable } from 'stream'
import { Dispatcher, stream } from 'undici'
import { Segment } from './segmentPolicy'
import { StatusController } from './status'

export async function* downloadRange(
  url: URL,
  segment: Segment,
  headers: Record<string, string>,
  handle: FileHandle,
  statusController: StatusController | undefined,
  abortSignal: AbortSignal | undefined,
  dispatcher: Dispatcher | undefined,
): AsyncGenerator<any, void, void> {
  if (segment.start >= segment.end) {
    // the segment is finished, just ignore it
    return
  }
  const fileStream = new Writable({
    write(chunk, en, cb) {
      handle.write(chunk, 0, chunk.length, segment.start).then(({ bytesWritten }) => {
        // track the progress
        segment.start += bytesWritten
        statusController?.onProgress(url, bytesWritten, statusController.progress + bytesWritten)
        cb()
      }, cb)
    },
  })

  while (true) {
    try {
      await stream(url, {
        method: 'GET',
        dispatcher,
        headers: {
          ...headers,
          Range: `bytes=${segment.start}-${(segment.end) ?? ''}`,
        },
        throwOnError: true,
        maxRedirections: 2,
        signal: abortSignal,
        opaque: fileStream,
        bodyTimeout: 10_000,
        headersTimeout: 10_000,
      }, ({ opaque }) => opaque as Writable)
    } catch (e) {
      yield e
    }
  }
}
