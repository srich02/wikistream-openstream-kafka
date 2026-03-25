# 02 Consumer Concepts

## Consumer delivery semantics

### At-most-once

Commit offsets before processing. If the application crashes after the commit, messages are lost from the consumer point of view.

### At-least-once

Process first, then commit. If the application crashes after processing but before committing, messages can be re-read. This is the most common baseline.

### Exactly-once

Exactly-once requires coordinated producer, broker, and consumer behavior. It usually means transactions, idempotence, and a sink that cooperates correctly. In pure Node.js KafkaJS pipelines, treat exactly-once as a system design goal, not a default switch.

## Consumer offsets

Offsets are the durable read position for each topic partition. The replay consumer demonstrates how reading from the beginning works when you use a fresh group id.

## Offset commit strategies

- Auto commit: simplest, but least explicit.
- Manual commit after processing: more control.
- Batch commit after a successful sink write: common in analytics and indexing pipelines.

This project uses:

- auto commit in `analytics-consumer.ts`
- manual commit in `opensearch-consumer.ts`
- fresh group replay in `replay-consumer.ts`

## Offset reset behavior

- `earliest`: read from the oldest retained records.
- `latest`: only new records after subscription.

The replay consumer uses `fromBeginning: true` so you can study backfills and reprocessing.

## Advanced consumer patterns

### Idempotent consumer logic

Consumers should tolerate duplicate reads. A sink consumer can use natural keys, upserts, or deduplication tables. The OpenSearch consumer uses the Wikimedia event `id` as the document `_id`, which makes repeated indexing idempotent.

### Batching data in consumers

Batch consumption reduces overhead per record and usually increases throughput. The analytics consumer uses `eachBatch`; the OpenSearch consumer builds one bulk request per Kafka batch.

### Replaying data

Replay is useful for:

- debugging a new consumer
- recomputing analytics
- rebuilding a search index
- validating a schema migration

The replay consumer is the simplest place to practice this pattern.

### OpenSearch consumer integrations

The OpenSearch sink in `src/consumers/opensearch-consumer.ts` shows a standard pattern:

1. Read a Kafka batch.
2. Convert records into bulk indexing operations.
3. Send one bulk request.
4. Commit Kafka offsets only after the sink call succeeds.

That is a common real-world at-least-once sink integration.

## Experiments

1. Start only the producer and let the raw topic grow.
2. Start the replay consumer with a fresh group.
3. Start the OpenSearch consumer and observe manual offset commits.
4. Change the consumer group id and see how Kafka treats the process as a new reader.