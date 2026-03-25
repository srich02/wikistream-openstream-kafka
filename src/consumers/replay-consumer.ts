import { appConfig } from "../config";
import { kafka } from "../lib/kafka";
import { logger } from "../lib/logger";
import { WikimediaRecentChange } from "../types/wikimedia";

async function main(): Promise<void> {
  const consumer = kafka.consumer({ groupId: `${appConfig.kafka.groupReplay}-${Date.now()}` });
  let processed = 0;

  await consumer.connect();
  await consumer.subscribe({ topic: appConfig.kafka.topicRaw, fromBeginning: true });

  logger.info({ topic: appConfig.kafka.topicRaw }, "Starting replay consumer from the beginning of the topic");

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ partition, message }) => {
      if (!message.value) {
        return;
      }

      const payload = JSON.parse(message.value.toString()) as WikimediaRecentChange;
      processed += 1;

      if (processed % 100 === 0) {
        logger.info(
          {
            processed,
            partition,
            offset: message.offset,
            wiki: payload.wiki,
            type: payload.type
          },
          "Replay progress"
        );
      }
    }
  });
}

void main();