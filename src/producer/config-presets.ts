import { appConfig } from "../config";

export interface ProducerPreset {
  mode: "safe" | "throughput";
  acks: -1 | 0 | 1;
  retries: number;
  idempotent: boolean;
  compression: "gzip";
  lingerMs: number;
  maxBatchSize: number;
  notes: string[];
}

export function getProducerPreset(mode: "safe" | "throughput"): ProducerPreset {
  if (mode === "safe") {
    return {
      mode,
      acks: -1,
      retries: Math.max(appConfig.producer.retries, 8),
      idempotent: true,
      compression: "gzip",
      lingerMs: 100,
      maxBatchSize: 100,
      notes: [
        "Maps Kafka safe producer guidance to KafkaJS idempotence and all-replicas acknowledgments.",
        "Keeps smaller batches to prioritize stronger durability and faster flush cadence."
      ]
    };
  }

  return {
    mode,
    acks: appConfig.producer.acks,
    retries: appConfig.producer.retries,
    idempotent: appConfig.producer.idempotent,
    compression: "gzip",
    lingerMs: appConfig.producer.lingerMs,
    maxBatchSize: appConfig.producer.maxBatchSize,
    notes: [
      "Uses application-side batching to illustrate linger.ms and batch.size semantics in KafkaJS.",
      "Compression and larger batches improve throughput at the cost of additional latency."
    ]
  };
}