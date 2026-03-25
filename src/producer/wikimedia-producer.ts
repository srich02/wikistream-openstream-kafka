import { ProducerRecord } from "kafkajs";
import { appConfig } from "../config";
import { createProducer, defaultCompression } from "../lib/kafka";
import { logger } from "../lib/logger";
import { streamWikimediaRecentChanges } from "../lib/wikimedia-sse";
import { WikimediaRecentChange } from "../types/wikimedia";
import { getProducerPreset } from "./config-presets";

interface BufferedMessage {
  key: string;
  value: string;
}

async function main(): Promise<void> {
  const preset = getProducerPreset(appConfig.producer.profile);
  const producer = createProducer(preset.mode);
  const buffer: BufferedMessage[] = [];
  let flushTimer: NodeJS.Timeout | undefined;
  let totalProduced = 0;

  async function flush(reason: string): Promise<void> {
    if (buffer.length === 0) {
      return;
    }

    const batch = buffer.splice(0, buffer.length);
    const record: ProducerRecord = {
      topic: appConfig.kafka.topicRaw,
      compression: defaultCompression,
      acks: preset.acks,
      messages: batch
    };

    await producer.send(record);
    totalProduced += batch.length;

    logger.info(
      {
        reason,
        batchSize: batch.length,
        totalProduced,
        profile: preset.mode
      },
      "Produced Wikimedia events to Kafka"
    );
  }

  function scheduleFlush(): void {
    if (flushTimer) {
      return;
    }

    flushTimer = setTimeout(async () => {
      flushTimer = undefined;
      await flush("linger-ms");
    }, preset.lingerMs);
  }

  async function enqueue(event: WikimediaRecentChange): Promise<void> {
    buffer.push({
      key: String(event.id ?? Date.now()),
      value: JSON.stringify(event)
    });

    if (buffer.length >= preset.maxBatchSize) {
      if (flushTimer) {
        clearTimeout(flushTimer);
        flushTimer = undefined;
      }

      await flush("batch-size");
      return;
    }

    scheduleFlush();
  }

  await producer.connect();

  logger.info(
    {
      streamUrl: appConfig.wikimedia.streamUrl,
      topic: appConfig.kafka.topicRaw,
      preset
    },
    "Starting Wikimedia Kafka producer"
  );

  const shutdown = async (signal: string, exitCode = 0) => {
    logger.info({ signal }, "Shutting down producer");
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = undefined;
    }

    await flush("shutdown");
    await producer.disconnect();
    process.exit(exitCode);
  };

  process.once("SIGINT", () => void shutdown("SIGINT", 0));
  process.once("SIGTERM", () => void shutdown("SIGTERM", 0));

  try {
    await streamWikimediaRecentChanges(appConfig.wikimedia.streamUrl, enqueue);
  } catch (error) {
    logger.error({ err: error }, "Producer stopped with an unrecoverable error");
    await shutdown("failure", 1);
  }
}

void main();