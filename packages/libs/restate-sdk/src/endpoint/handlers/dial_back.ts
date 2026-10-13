/*
 * Copyright (c) 2023-2024 - Restate Software, Inc., Restate GmbH
 *
 * This file is part of the Restate SDK for Node.js/TypeScript,
 * which is released under the MIT license.
 *
 * You can find a copy of the license in file LICENSE in the root
 * directory of this repository or package, or at
 * https://github.com/restatedev/sdk-typescript/blob/main/LICENSE
 */

import * as http2 from "node:http2";
import type { IncomingMessage, ServerResponse } from "node:http";
import type { Http2ServerRequest, Http2ServerResponse } from "node:http2";
import { ensureError } from "../../types/errors.js";
import type {
  Headers,
  InputReader,
  OutputWriter,
  RestateHandler,
  ResponseHeaders,
} from "./types.js";
import { tryCreateContextualLogger } from "./utils.js";

export const ATTACH_URL_HEADER = "x-restate-attach-url";
export const ATTACH_TOKEN_HEADER = "x-restate-attach-token";

type LeaseRequest = IncomingMessage | Http2ServerRequest;
type LeaseResponse = ServerResponse | Http2ServerResponse;

// Both ServerResponse and Http2ServerResponse satisfy this interface.
interface NodeLeaseResponse {
  writeHead(statusCode: number, headers?: Record<string, string>): void;
  end(chunk?: Uint8Array): void;
  once(event: "close", listener: () => void): void;
  readonly writableEnded: boolean;
}

/**
 * Handles an invocation delivered as a lease: a short, complete request that
 * carries `x-restate-attach-url` and `x-restate-attach-token`.
 *
 * The service protocol does not run on the lease. For `/invoke/*` requests
 * whose head is a 2xx, this adapter dials back to the attach URL over HTTP/2
 * and runs the protocol on that outbound stream: the request body of the
 * attach is the SDK -> Restate direction and its response body the Restate ->
 * SDK direction. Error and discovery responses are served on the lease
 * response, as for any other request.
 *
 * Ordering: the lease is answered only after `process()` resolved, i.e. after
 * the input of the attach was consumed to its end and the attach was closed.
 * Answering earlier tells Restate that the environment died.
 */
export function handleDialBackLease(
  handler: RestateHandler,
  request: LeaseRequest,
  response: LeaseResponse,
  attachUrl: string,
  attachToken: string
): void {
  const url = request.url!;
  const headers = request.headers as Headers;
  const lease = response as unknown as NodeLeaseResponse;
  const isInvoke = url.startsWith("/invoke/");

  const abortController = new AbortController();
  const abort = () => abortController.abort();

  // The lease request is empty, but must be consumed to complete the exchange.
  const leaseInput = (request as AsyncIterable<Uint8Array>)[
    Symbol.asyncIterator
  ]();

  let attach: AttachChannel | undefined;
  let leaseHead: { statusCode: number; headers: ResponseHeaders } | undefined;
  const leaseChunks: Uint8Array[] = [];

  // The lease connection going away before we answered means the attempt is over.
  lease.once("close", () => {
    if (!lease.writableEnded) {
      abort();
    }
  });

  const writeHead = (statusCode: number, responseHeaders: ResponseHeaders) => {
    if (isInvoke && statusCode >= 200 && statusCode < 300) {
      attach = openAttach(attachUrl, attachToken, responseHeaders, abort);
    } else {
      leaseHead = { statusCode, headers: { ...responseHeaders } };
    }
  };

  const inputReader: InputReader = {
    next: () => (attach ? attach.input.next() : leaseInput.next()),
  };

  const outputWriter: OutputWriter = {
    write: (value) =>
      attach
        ? attach.write(value)
        : Promise.resolve(void leaseChunks.push(value)),
    close: () =>
      attach
        ? attach.end()
        : Promise.resolve().then(() => {
            // Error / discovery response: served on the lease itself.
            lease.writeHead(leaseHead!.statusCode, leaseHead!.headers);
            lease.end(Buffer.concat(leaseChunks));
          }),
  };

  // handle should never throw
  const restateResponse = handler.handle({ url, headers, extraArgs: [] });

  restateResponse
    .process({
      inputReader,
      outputWriter,
      writeHead,
      abortSignal: abortController.signal,
    })
    .then(
      () => {
        if (attach) {
          // The attach is authoritative; the lease just reports whether it worked.
          lease.writeHead(attach.failed() ? 500 : 200);
          lease.end();
        }
      },
      (e) => {
        const error = ensureError(e);
        const logger =
          tryCreateContextualLogger(
            handler.endpoint.loggerTransport,
            url,
            headers
          ) ?? handler.endpoint.rlog;
        logger.error("Unexpected error: " + (error.stack ?? error.message));
        if (!lease.writableEnded) {
          lease.writeHead(500);
          lease.end();
        }
      }
    )
    .finally(() => {
      attach?.dispose();
      abort();
    });
}

interface AttachChannel {
  input: InputReader;
  write(value: Uint8Array): Promise<void>;
  end(): Promise<void>;
  failed(): boolean;
  dispose(): void;
}

/** One HTTP/2 session per attach; no reuse. */
function openAttach(
  attachUrl: string,
  attachToken: string,
  responseHeaders: ResponseHeaders,
  abort: () => void
): AttachChannel {
  const target = new URL(attachUrl);
  let failed = false;
  const fail = () => {
    failed = true;
    abort();
  };

  const session = http2.connect(target.origin);
  session.on("error", fail);

  const requestHeaders: Record<string, string> = {
    ":method": "POST",
    ":path": target.pathname + target.search,
    [ATTACH_TOKEN_HEADER]: attachToken,
  };
  for (const name of ["content-type", "x-restate-server"]) {
    const value = responseHeaders[name];
    if (value !== undefined) {
      requestHeaders[name] = value;
    }
  }

  const stream = session.request(requestHeaders, { endStream: false });
  stream.on("response", (head) => {
    if (head[":status"] !== 200) {
      // e.g. 410: the attempt already ended. Fails the input iterator.
      stream.destroy(new Error(`Attach rejected with ${head[":status"]}`));
    }
  });
  stream.on("error", fail);
  stream.on("close", () => {
    if (stream.rstCode !== http2.constants.NGHTTP2_NO_ERROR) {
      fail();
    }
    session.close();
  });

  return {
    input: stream[Symbol.asyncIterator]() as AsyncIterator<Uint8Array>,
    write: (value) =>
      new Promise((resolve, reject) => {
        stream.write(value, (err) => (err ? reject(err) : resolve()));
      }),
    end: () =>
      new Promise((resolve) => {
        stream.end(() => resolve());
      }),
    failed: () => failed,
    dispose: () => {
      if (!stream.destroyed) {
        stream.close();
      }
      session.close();
    },
  };
}
