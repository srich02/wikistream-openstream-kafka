# 04 Real-World Kafka Architecture and Best Practices

## Choosing partition count

Partition count is a tradeoff between parallelism and operational cost.

Think about:

- expected throughput
- number of consumer instances
- ordering requirements
- future growth
- rebalance cost

This project starts with six partitions on the raw topic to support parallel consumer experiments.

## Choosing replication factor

- `1`: local development only
- `3`: common production baseline
- more than `3`: only when you have clear durability or geo-redundancy reasons

This repo uses replication factor `1` locally because the Docker environment is single-node.

## Topic naming conventions

Useful naming rules:

- lowercase only
- dot-separated domains
- stable business meaning
- avoid team-specific abbreviations

Examples in this project:

- `wikimedia.recentchange.raw`
- `wikimedia.recentchange.analytics`
- `wikimedia.recentchange.dlq`

## Big-data ingestion patterns

Common patterns:

- raw immutable topic first
- downstream derived topics for aggregations
- sink connectors or sink consumers for indexes and warehouses
- dead-letter topic for poison messages

That is the exact pattern used in this repo.

## Logging and metrics aggregation

Kafka is often the backbone for:

- application logs
- clickstream events
- audit trails
- metrics pipelines

The same design rules apply to Wikimedia event ingestion.

## Case studies

### MovieFlix

Use Kafka to stream watch events, recommendation updates, and billing signals. Partition by `userId` or `subscriptionId` when order matters.

### GetTaxi

Partition by `rideId` or `driverId` depending on the workflow. Use replay to rebuild surge-pricing models.

### MySocialMedia

Keep a raw activity topic, derive feed-ranking features downstream, and index posts into search separately.

### MyBank

Bias toward safe producer settings, stronger replication, schema governance, and stricter security. Avoid large unbounded messages and treat exactly-once claims carefully.