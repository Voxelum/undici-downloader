import { FileHandle } from 'fs/promises'
import CachePolicy from 'http-cache-semantics'
import { Dispatcher, getGlobalDispatcher } from 'undici'
import { downloadRange } from './segment'
import { getMetadata, parseMetadata } from './metadata'
import { createDefaultRetryHandler, RetryHandler } from './retry'
import { DefaultSegmentPolicy, Segment, SegmentPolicy } from './segmentPolicy'
import { StatusController } from './status'
import { CheckpointHandler, createInMemoryCheckpointHandler } from './checkpoint'

export interface CacheStorage {
  get(key: string): Promise<string | undefined>
  set(key: string, val: any): Promise<void>
  del(key: string): Promise<void>
}

export interface DownloadAgentOptions {
  retryHandler?: RetryHandler
  segmentPolicy?: SegmentPolicy
  dispatcher: Dispatcher
  cache?: CacheStorage
  checkpointHandler?: CheckpointHandler
}

export function resolveAgent(agent?: DownloadAgent | DownloadAgentOptions) {
  return agent instanceof DownloadAgent
    ? agent
    : new DownloadAgent(
      agent?.retryHandler ?? createDefaultRetryHandler(3),
      agent?.segmentPolicy ?? new DefaultSegmentPolicy(2 * 1024 * 1024, 4),
      agent?.dispatcher ?? getGlobalDispatcher(),
      agent?.cache,
      agent?.checkpointHandler ?? createInMemoryCheckpointHandler(),
    )
}

export class DownloadAgent {
  constructor(
    readonly retryHandler: RetryHandler,
    readonly segmentPolicy: SegmentPolicy,
    readonly dispatcher: Dispatcher,
    readonly cache: CacheStorage | undefined,
    readonly checkpointHandler: CheckpointHandler | undefined,
  ) { }

  async dispatch(url: URL, method: string, headers: Record<string, string>, destination: string, handle: FileHandle, statusController: StatusController | undefined, abortSignal: AbortSignal | undefined) {
    const key = `${method}${url}${JSON.stringify(headers)}`

    let targetUrl: URL = url
    let segments: Segment[] | undefined

    const cache = await this.cache?.get(key)
    if (cache) {
      const policy = CachePolicy.fromObject(JSON.parse(cache))
      if (policy.satisfiesWithoutRevalidation({
        url: url.toString(),
        method,
        headers,
      })) {
        if (Date.now() >= policy.timeToLive()) {
          await this.cache?.del(key)
        } else {
          // use saved checkpoint
          const checkpoint = await this.checkpointHandler?.popCheckpoint(url, handle, destination)

          if (checkpoint) {
            segments = checkpoint.segments
            targetUrl = new URL(checkpoint.url)
          } else {
            const o = policy.toObject()
            const metadata = parseMetadata(o.resh)

            const contentLength = metadata.contentLength
            segments = contentLength && metadata.isAcceptRanges
              ? this.segmentPolicy.computeSegments(contentLength)
              : [{ start: 0, end: contentLength }]
            if (o.u) {
              targetUrl = new URL(o.u)
            }
          }
        }
      }
    }

    if (!segments) {
      const metadata = await getMetadata(url, headers, false, this.dispatcher, abortSignal)
      if (metadata.policy.storable()) {
        await this.cache?.set(key, metadata.policy.toObject())
      }

      const contentLength = metadata.contentLength
      segments = contentLength && metadata.isAcceptRanges
        ? this.segmentPolicy.computeSegments(contentLength)
        : [{ start: 0, end: contentLength }]
      targetUrl = metadata.url
    }

    const results = await Promise.all(segments.map(async (segment) => {
      const generator = downloadRange(targetUrl, segment, headers, handle, statusController, abortSignal, this.dispatcher)

      let attempt = 0
      for (let current = await generator.next(); !current.done; current = await generator.next(), attempt++) {
        const err = current.value
        if (!await this.retryHandler.retry(url.toString(), attempt, err)) {
          // won't retry anymore
          await generator.return(err)
          return err
        }
        if (attempt > 3) {

        }
      }
    }))

    const errors = results.filter(r => !!r)

    if (errors.length > 0) {
      await this.checkpointHandler?.pushCheckpoint(url, handle, destination, {
        segments,
        url: targetUrl.toString(),
      }).catch(() => { })

      throw errors
    }
  }
}
