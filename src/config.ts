import { config as loadEnv } from "dotenv";
import { z } from "zod";

loadEnv();

const envSchema = z.object({
  KAFKA_CLIENT_ID: z.string().default("wikimedia-analytics-app"),
  KAFKA_BROKERS: z.string().default("localhost:9092"),
  KAFKA_TOPIC_RAW: z.string().default("wikimedia.recentchange.raw"),
  KAFKA_TOPIC_ANALYTICS: z.string().default("wikimedia.recentchange.analytics"),
  KAFKA_TOPIC_DEAD_LETTER: z.string().default("wikimedia.recentchange.dlq"),
  KAFKA_GROUP_ANALYTICS: z.string().default("wikimedia-analytics-consumer"),
  KAFKA_GROUP_OPENSEARCH: z.string().default("wikimedia-opensearch-consumer"),
  KAFKA_GROUP_REPLAY: z.string().default("wikimedia-replay-consumer"),
  KAFKA_TOPIC_PARTITIONS: z.coerce.number().int().positive().default(6),
  KAFKA_TOPIC_REPLICATION_FACTOR: z.coerce.number().int().positive().default(1),
  WIKIMEDIA_STREAM_URL: z.string().url().default("https://stream.wikimedia.org/v2/stream/recentchange"),
  PRODUCER_PROFILE: z.enum(["safe", "throughput"]).default("throughput"),
  PRODUCER_LINGER_MS: z.coerce.number().int().nonnegative().default(250),
  PRODUCER_MAX_BATCH_SIZE: z.coerce.number().int().positive().default(200),
  PRODUCER_RETRIES: z.coerce.number().int().nonnegative().default(8),
  PRODUCER_ACKS: z.union([z.literal("-1"), z.literal("0"), z.literal("1")]).default("-1"),
  PRODUCER_IDEMPOTENT: z.enum(["true", "false"]).default("true"),
  OPENSEARCH_NODE: z.string().url().default("https://localhost:9200"),
  OPENSEARCH_INDEX: z.string().default("wikimedia_recentchange"),
  OPENSEARCH_USERNAME: z.string().default("admin"),
  OPENSEARCH_PASSWORD: z.string().default("Str0ng!WikiSearch#2026")
});

const env = envSchema.parse(process.env);

export const appConfig = {
  kafka: {
    clientId: env.KAFKA_CLIENT_ID,
    brokers: env.KAFKA_BROKERS.split(",").map((broker) => broker.trim()).filter(Boolean),
    topicRaw: env.KAFKA_TOPIC_RAW,
    topicAnalytics: env.KAFKA_TOPIC_ANALYTICS,
    topicDeadLetter: env.KAFKA_TOPIC_DEAD_LETTER,
    groupAnalytics: env.KAFKA_GROUP_ANALYTICS,
    groupOpensearch: env.KAFKA_GROUP_OPENSEARCH,
    groupReplay: env.KAFKA_GROUP_REPLAY,
    topicPartitions: env.KAFKA_TOPIC_PARTITIONS,
    replicationFactor: env.KAFKA_TOPIC_REPLICATION_FACTOR
  },
  wikimedia: {
    streamUrl: env.WIKIMEDIA_STREAM_URL
  },
  producer: {
    profile: env.PRODUCER_PROFILE,
    lingerMs: env.PRODUCER_LINGER_MS,
    maxBatchSize: env.PRODUCER_MAX_BATCH_SIZE,
    retries: env.PRODUCER_RETRIES,
    acks: Number(env.PRODUCER_ACKS) as -1 | 0 | 1,
    idempotent: env.PRODUCER_IDEMPOTENT === "true"
  },
  opensearch: {
    node: env.OPENSEARCH_NODE,
    index: env.OPENSEARCH_INDEX,
    username: env.OPENSEARCH_USERNAME,
    password: env.OPENSEARCH_PASSWORD
  }
} as const;