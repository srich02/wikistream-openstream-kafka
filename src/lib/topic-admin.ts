import { appConfig } from "../config";
import { kafka } from "./kafka";

export async function createProjectTopics(): Promise<void> {
  const admin = kafka.admin();

  await admin.connect();

  try {
    await admin.createTopics({
      waitForLeaders: true,
      topics: [
        {
          topic: appConfig.kafka.topicRaw,
          numPartitions: appConfig.kafka.topicPartitions,
          replicationFactor: appConfig.kafka.replicationFactor,
          configEntries: [
            { name: "cleanup.policy", value: "delete" },
            { name: "retention.ms", value: String(7 * 24 * 60 * 60 * 1000) }
          ]
        },
        {
          topic: appConfig.kafka.topicAnalytics,
          numPartitions: 3,
          replicationFactor: appConfig.kafka.replicationFactor,
          configEntries: [
            { name: "cleanup.policy", value: "compact,delete" },
            { name: "retention.ms", value: String(14 * 24 * 60 * 60 * 1000) }
          ]
        },
        {
          topic: appConfig.kafka.topicDeadLetter,
          numPartitions: 2,
          replicationFactor: appConfig.kafka.replicationFactor,
          configEntries: [{ name: "cleanup.policy", value: "delete" }]
        }
      ]
    });
  } finally {
    await admin.disconnect();
  }
}

export async function describeProjectTopics() {
  const admin = kafka.admin();
  await admin.connect();

  try {
    return admin.fetchTopicMetadata({
      topics: [
        appConfig.kafka.topicRaw,
        appConfig.kafka.topicAnalytics,
        appConfig.kafka.topicDeadLetter
      ]
    });
  } finally {
    await admin.disconnect();
  }
}