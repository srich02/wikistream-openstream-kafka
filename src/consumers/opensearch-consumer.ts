import { Client } from "@opensearch-project/opensearch";
import { appConfig } from "../config";
import { kafka } from "../lib/kafka";
import { logger } from "../lib/logger";
import { WikimediaRecentChange } from "../types/wikimedia";

async function ensureIndex(client: Client): Promise<void> {
  const exists = await client.indices.exists({ index: appConfig.opensearch.index });

  if (exists.body) {
    return;
  }

  await client.indices.create({
    index: appConfig.opensearch.index,
    body: {
      mappings: {
        properties: {
          id: { type: "long" },
          type: { type: "keyword" },
          bot: { type: "boolean" },
          wiki: { type: "keyword" },
          server_name: { type: "keyword" },
          title: { type: "text" },
          user: { type: "keyword" },
          timestamp: { type: "date", format: "epoch_second" }
        }
      }
    }
  });
}

async function main(): Promise<void> {
  const consumer = kafka.consumer({ groupId: appConfig.kafka.groupOpensearch });
  const client = new Client({
    node: appConfig.opensearch.node,
    auth: {
      username: appConfig.opensearch.username,
      password: appConfig.opensearch.password
    },
    ssl: {
      rejectUnauthorized: false
    }
  });

  await ensureIndex(client);
  await consumer.connect();
  await consumer.subscribe({ topic: appConfig.kafka.topicRaw, fromBeginning: false });

  await consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
      const operations: Array<Record<string, unknown>> = [];

      for (const message of batch.messages) {
        if (!message.value) {
          resolveOffset(message.offset);
          continue;
        }

        const payload = JSON.parse(message.value.toString()) as WikimediaRecentChange;
        operations.push({ index: { _index: appConfig.opensearch.index, _id: String(payload.id) } });
        operations.push(payload as unknown as Record<string, unknown>);
        resolveOffset(message.offset);
      }

      if (operations.length > 0) {
        const response = await client.bulk({ refresh: false, body: operations });
        if (response.body.errors) {
          logger.warn({ errors: response.body.items }, "OpenSearch bulk request completed with partial failures");
        }
      }

      await commitOffsetsIfNecessary();
      await heartbeat();
      logger.info({ records: operations.length / 2 }, "Indexed records into OpenSearch");
    }
  });
}

void main();