import { CompressionTypes, Kafka, Partitioners, Producer, logLevel } from "kafkajs";
import { appConfig } from "../config";

export const kafka = new Kafka({
  clientId: appConfig.kafka.clientId,
  brokers: appConfig.kafka.brokers,
  logLevel: logLevel.NOTHING
});

export function createProducer(mode: "safe" | "throughput"): Producer {
  const idempotent = mode === "safe" ? true : appConfig.producer.idempotent;

  return kafka.producer({
    allowAutoTopicCreation: false,
    idempotent,
    maxInFlightRequests: idempotent ? 5 : undefined,
    createPartitioner: Partitioners.DefaultPartitioner,
    retry: {
      retries: appConfig.producer.retries
    }
  });
}

export const defaultCompression = CompressionTypes.GZIP;