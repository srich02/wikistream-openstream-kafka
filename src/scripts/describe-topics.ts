import { describeProjectTopics } from "../lib/topic-admin";

async function main(): Promise<void> {
  const metadata = await describeProjectTopics();
  console.log(JSON.stringify(metadata, null, 2));
}

void main();