import CachePolicy from 'http-cache-semantics'
import { Dispatcher, request } from 'undici'
import { URL } from 'url'

export interface ResourceMetadata {
  url: URL
  isAcceptRanges: boolean
  contentLength: number
  lastModified: string | undefined
  eTag: string | undefined
}

export class FetchMetadataError extends Error {
  constructor(
    readonly error: 'FetchResourceNotFound' | 'BadResourceRequest' | 'FetchResourceServerUnavailable',
    readonly statusCode: number,
    readonly url: string,
    message: string,
  ) {
    super(message)
    this.name = error
  }
}

export function parseMetadata(headers: Record<string, any>) {
  const isAcceptRanges = headers['accept-ranges'] === 'bytes'
  const contentLength = headers['content-length'] ? Number.parseInt(headers['content-length']) : -1
  const lastModified = headers['last-modified'] ?? undefined
  const eTag = headers.etag
  return {
    isAcceptRanges,
    contentLength,
    lastModified,
    eTag,
  }
}

export async function getMetadata(srcUrl: URL, _headers: Record<string, any>, useGet = false, dispatcher?: Dispatcher, abortSignal?: AbortSignal): Promise<ResourceMetadata & { policy: CachePolicy }> {
  const headersCache: Record<string, string> = {}
  const method = useGet ? 'GET' : 'HEAD'
  const response = await request(srcUrl, {
    method,
    ..._headers,
    signal: abortSignal,
    maxRedirections: 2,
    dispatcher,
    onInfo({ headers }) { Object.assign(headersCache, headers) },
  })

  const resultUrl = headersCache.location

  response.body.destroy()
  response.body.once('error', () => { })

  let { headers, statusCode } = response
  if (statusCode === 405 && !useGet) {
    return getMetadata(srcUrl, _headers, true, dispatcher, abortSignal)
  }
  statusCode = statusCode ?? 500
  if (statusCode !== 200 && statusCode !== 201) {
    throw new FetchMetadataError(
      statusCode === 404
        ? 'FetchResourceNotFound'
        : statusCode >= 500
          ? 'FetchResourceServerUnavailable'
          : 'BadResourceRequest',
      statusCode,
      resultUrl || srcUrl.toString(),
      `Fetch download metadata failed due to http error. Status code: ${statusCode} on ${resultUrl}`)
  }
  const policy = new CachePolicy({
    method,
    url: srcUrl.toString(),
    headers: _headers,
  }, {
    status: response.statusCode,
    headers: response.headers,
  })

  const url = resultUrl ? new URL(resultUrl) : srcUrl
  const isAcceptRanges = headers['accept-ranges'] === 'bytes'
  const contentLength = headers['content-length'] ? Number.parseInt(headers['content-length']) : -1
  const lastModified = headers['last-modified'] ?? undefined
  const eTag = headers.etag

  return {
    policy,
    url,
    isAcceptRanges,
    contentLength,
    lastModified,
    eTag,
  }
}
