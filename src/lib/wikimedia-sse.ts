import { createParser } from "eventsource-parser";
import { EnvHttpProxyAgent, setGlobalDispatcher } from "undici";
import { logger } from "./logger";
import { WikimediaRecentChange } from "../types/wikimedia";

type EventHandler = (event: WikimediaRecentChange) => Promise<void>;

const httpProxy = process.env.HTTP_PROXY ?? process.env.http_proxy;
const httpsProxy = process.env.HTTPS_PROXY ?? process.env.https_proxy;
const noProxy = process.env.NO_PROXY ?? process.env.no_proxy;

if (httpProxy || httpsProxy) {
  setGlobalDispatcher(
    new EnvHttpProxyAgent({
      httpProxy,
      httpsProxy,
      noProxy
    })
  );
}

export async function streamWikimediaRecentChanges(url: string, onEvent: EventHandler): Promise<void> {
  const response = await fetch(url, {
    headers: {
      Accept: "text/event-stream"
    }
  });

  if (!response.ok || !response.body) {
    throw new Error(`Failed to connect to Wikimedia stream: ${response.status} ${response.statusText}`);
  }

  const decoder = new TextDecoder();
  const reader = response.body.getReader();
  const pending: Promise<void>[] = [];

  const parser = createParser({
    onEvent(parsedEvent) {
      if (!parsedEvent.data) {
        return;
      }

      try {
        const payload = JSON.parse(parsedEvent.data) as WikimediaRecentChange;
        pending.push(onEvent(payload));
      } catch (error) {
        logger.warn({ error }, "Ignoring malformed Wikimedia event");
      }
    },
    onError(error) {
      logger.warn({ error }, "SSE parsing warning");
    }
  });

  while (true) {
    const { done, value } = await reader.read();

    if (done) {
      break;
    }

    parser.feed(decoder.decode(value, { stream: true }));

    if (pending.length >= 512) {
      await Promise.allSettled(pending.splice(0, pending.length));
    }
  }

  if (pending.length > 0) {
    await Promise.allSettled(pending);
  }
}