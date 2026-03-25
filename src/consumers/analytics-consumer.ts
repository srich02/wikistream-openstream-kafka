import { appConfig } from "../config";
import { kafka } from "../lib/kafka";
import { logger } from "../lib/logger";
import { AnalyticsSnapshot, WikimediaRecentChange } from "../types/wikimedia";

function topEntries(values: Map<string, number>, size: number): Array<{ key: string; count: number }> {
  return [...values.entries()]
    .sort((left, right) => right[1] - left[1])
    .slice(0, size)
    .map(([key, count]) => ({ key, count }));
}

async function main(): Promise<void> {
  const consumer = kafka.consumer({ groupId: appConfig.kafka.groupAnalytics });
  const producer = kafka.producer({ allowAutoTopicCreation: false });

  const eventTypes = new Map<string, number>();
  const wikis = new Map<string, number>();
  let processedMessages = 0;
  let botEvents = 0;
  let nonBotEvents = 0;

  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: appConfig.kafka.topicRaw, fromBeginning: false });

  const publishSnapshot = async () => {
    if (processedMessages === 0) {
      return;
    }

    const snapshot: AnalyticsSnapshot = {
      processedMessages,
      botEvents,
      nonBotEvents,
      topWikis: topEntries(wikis, 5),
      topEventTypes: topEntries(eventTypes, 5)
    };

    await producer.send({
      topic: appConfig.kafka.topicAnalytics,
      acks: -1,
      messages: [
        {
          key: "latest",
          value: JSON.stringify(snapshot)
        }
      ]
    });

    logger.info(snapshot, "Published analytics snapshot");
  };

  const interval = setInterval(() => {
    void publishSnapshot();
  }, 10000);

  await consumer.run({
    autoCommit: true,
    partitionsConsumedConcurrently: 3,
    eachBatchAutoResolve: true,
    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
      for (const message of batch.messages) {
        if (!message.value) {
          resolveOffset(message.offset);
          continue;
        }

        const payload = JSON.parse(message.value.toString()) as WikimediaRecentChange;
        processedMessages += 1;

        const typeKey = payload.type ?? "unknown";
        const wikiKey = payload.wiki ?? payload.server_name ?? "unknown";

        eventTypes.set(typeKey, (eventTypes.get(typeKey) ?? 0) + 1);
        wikis.set(wikiKey, (wikis.get(wikiKey) ?? 0) + 1);

        if (payload.bot) {
          botEvents += 1;
        } else {
          nonBotEvents += 1;
        }

        resolveOffset(message.offset);
      }

      await commitOffsetsIfNecessary();
      await heartbeat();
    }
  });

  const shutdown = async (signal: string) => {
    clearInterval(interval);
    logger.info({ signal }, "Shutting down analytics consumer");
    await publishSnapshot();
    await consumer.disconnect();
    await producer.disconnect();
    process.exit(0);
  };

  process.once("SIGINT", () => void shutdown("SIGINT"));
  process.once("SIGTERM", () => void shutdown("SIGTERM"));
}

void main();