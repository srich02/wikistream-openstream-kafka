import { createProjectTopics } from "../lib/topic-admin";
import { logger } from "../lib/logger";

async function main(): Promise<void> {
  await createProjectTopics();
  logger.info("Project topics are ready");
}

void main();