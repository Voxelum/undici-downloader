import { FileHandle, mkdir, open, unlink } from 'fs/promises'
import { constants } from 'fs'
import { dirname } from 'path'
import { errors } from 'undici'
import { DownloadAgent, resolveAgent } from './agent'
import { resolveNetworkErrorType } from './error'
import { Segment } from './segmentPolicy'
import { resolveStatusController, StatusController } from './status'
import { ChecksumValidatorOptions, resolveValidator, ValidationError, Validator } from './validator'

export interface DownloadBaseOptions {
  /**
   * The header of the request
   */
  headers?: Record<string, any>
}

export interface DownloadOptions extends DownloadBaseOptions {
  /**
   * The url or urls (fallback) of the resource
   */
  url: string | string[]
  /**
   * The header of the request
   */
  headers?: Record<string, any>
  /**
   * If the download is aborted, and want to recover, you can use this option to recover the download
   */
  segments?: Segment[]
  /**
   * Where the file will be downloaded to
   */
  destination: string
  /**
   * The status controller. If you want to track download progress, you should use this.
   */
  statusController?: StatusController
  /**
   * The validator, or the options to create a validator based on checksum.
   */
  validator?: Validator | ChecksumValidatorOptions
  /**
   * The user abort signal to abort the download
   */
  abortSignal?: AbortSignal
  /**
   * The download agent
   */
  agent?: DownloadAgent
}

/**
 * Download url or urls to a file path. This process is abortable, it's compatible with the dom like `AbortSignal`.
 */
export async function download(options: DownloadOptions) {
  const urls = typeof options.url === 'string' ? [options.url] : options.url
  const headers = options.headers || {}
  const destination = options.destination
  const statusController = resolveStatusController(options.statusController)
  const validator = resolveValidator(options.validator)
  const abortSignal = options.abortSignal
  const agent = resolveAgent(options.agent)

  let fd = undefined as FileHandle | undefined

  try {
    try {
      await mkdir(dirname(destination), { recursive: true })
    } catch (e) {
      debugger
      console.log(e)
    }
    // use O_RDWR for read write which won't be truncated
    fd = await open(destination, constants.O_RDWR | constants.O_CREAT)

    // pre-validate the file
    const size = (await fd.stat()).size
    if (size !== 0) {
      const error = await validator.validate(fd, destination, urls[0]).catch((e) => e)
      // if the file size is not 0 and checksum matched, we just don't process the file
      if (!error) {
        return
      }
    }

    let succeed = false
    const aggregatedErrors: any[] = []
    for (const url of urls) {
      try {
        await agent.dispatch(new URL(url), 'GET', headers, destination, fd, statusController, abortSignal)
        await fd.datasync()
        await validator.validate(fd, destination, url)
        succeed = true
        break
      } catch (e) {
        const [err, attempt] = e as any[]
        // user abort should throw anyway
        if (err instanceof errors.RequestAbortedError) {
          throw [e]
        }

        if (err instanceof errors.SocketError) {

        }

        const networkError = resolveNetworkErrorType(e)
        // if (networkError) {
        //     throw new DownloadError(networkError,
        //         metadata,
        //         headers,
        //         destination,
        //         segments,
        //         [e],
        //     );
        // }

        aggregatedErrors.push(e)
      }
    }
    if (!succeed && aggregatedErrors.length > 0) {
      throw aggregatedErrors
    }
  } catch (e) {
    const errs: any[] = e instanceof Array ? e : [e]

    const lastError = errs[0]
    if (!(lastError instanceof ValidationError)) {
      await unlink(destination).catch(() => { })
    }

    throw e
  } finally {
    if (fd !== undefined) {
      await fd.close().catch(() => { })
    }
  }
}
