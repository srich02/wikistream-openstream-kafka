# 06 Advanced Kafka Topic and Broker Internals

## Changing topic configs

Topic-level configs let you tune retention, compaction, segment size, min ISR, and more. Use them carefully and document why each deviation exists.

This project creates topics programmatically in `src/lib/topic-admin.ts`.

## Segment and index files

Kafka stores each partition as a sequence of log segments on disk. Index files help the broker jump quickly to offsets within those segments. Smaller segments improve cleanup agility but increase file counts.

## Log cleanup policies

- `delete`: keep data for a retention window or until disk thresholds are hit
- `compact`: keep the latest value per key
- `compact,delete`: combine both behaviors

The analytics topic uses `compact,delete` to illustrate a derived-state topic.

## Log deletion

Deletion is retention-based. Kafka does not remove records one by one. It deletes whole segments once the policy allows it.

## Log compaction theory

Compaction turns a topic into a durable key-value history where the latest value for each key eventually survives. It is useful for materialized views, account profiles, and reference data.

## Log compaction practice

To practice compaction, publish analytics snapshots with the same key such as `latest`. Over time the topic will retain the newest value for that key after compaction catches up.

## Unclean leader election

Unclean leader election allows an out-of-sync replica to become leader. It can reduce downtime but risks data loss. Most production systems disable it unless availability is valued more than durability for that workload.

## Large messages in Kafka

Large messages create pressure on:

- producer memory
- broker network buffers
- replica fetchers
- consumer fetch size

Preferred strategy:

- keep messages modest
- put blobs in object storage
- send references through Kafka

Wikimedia events are a good fit for Kafka because they are relatively small and numerous.