# 01 Producer Concepts

## Producer configuration basics

Kafka producers must answer four basic questions:

- Which brokers do I connect to?
- Which topic do I write to?
- What key decides partition placement?
- What delivery guarantee do I need?

In this project these decisions live in `src/config.ts` and `src/producer/config-presets.ts`.

## Producer acknowledgments (`acks`)

- `acks=0`: fastest, but the broker does not confirm persistence.
- `acks=1`: leader confirms receipt, followers may still lag.
- `acks=-1` or `all`: strongest durability from replicas in sync.

Use `safe` mode when learning strong durability. Use `throughput` mode when exploring performance tradeoffs.

## Producer retries

Retries handle transient broker, network, or leader-election failures. Retries without idempotence can produce duplicates. Retries with idempotence are much safer.

This project sets retries in `src/config.ts` and applies them in `src/lib/kafka.ts`.

## Idempotent producer

Idempotence prevents duplicates caused by retrying the same record sequence. In KafkaJS that maps to the producer `idempotent` option. Safe producer mode enables it.

## Safe producer settings

The usual safe producer baseline is:

- `acks=all`
- retries enabled
- idempotence enabled
- `max.in.flight.requests.per.connection<=5`

That exact strategy is what `createProducer("safe")` aims to represent.

## High-throughput producer settings

Throughput comes from batching, compression, and accepting a little more latency.

- `linger.ms`: wait briefly to accumulate a larger batch.
- `batch.size`: flush when enough records are queued.
- `compression.type`: reduce network and disk load.
- `buffer.memory`: allow more records to wait in client memory.
- `max.block.ms`: define how long the client can stall while the buffer is full.

KafkaJS does not expose all of those settings directly. This project demonstrates the same ideas with an in-memory batch buffer and timed flushes in `src/producer/wikimedia-producer.ts`.

## Producer architecture and behavior

### Default partitioner

The default partitioner typically behaves like this:

- If a key exists, hash the key and keep ordering for that key.
- If no key exists, spread records across partitions.

This project uses the Wikimedia event `id` as the Kafka key to keep deterministic placement.

### Sticky partitioner

A sticky partitioner sends keyless records to the same partition for a short window before switching. The goal is better batching and compression.

KafkaJS exposes the default partitioner, not the full Java sticky partitioner behavior. The throughput profile still illustrates why sticky-style batching improves throughput.

### Kafka message compression

Compression lowers broker I/O, network traffic, and storage. The tradeoff is CPU. This project uses GZIP to show the pattern clearly.

### Wikimedia producer implementation

Read `src/producer/wikimedia-producer.ts` to see:

- SSE ingestion from Wikimedia
- application-side batching
- timed flushes to mimic `linger.ms`
- size-based flushes to mimic `batch.size`
- safe shutdown with final flush

### Safe producer implementation

Switch `.env` to `PRODUCER_PROFILE=safe` to bias the producer toward stronger delivery guarantees.

## Experiments

1. Set `PRODUCER_PROFILE=throughput` and `PRODUCER_LINGER_MS=500`.
2. Observe larger batch logs and slower end-to-end latency.
3. Switch to `safe` and compare throughput.
4. Turn off OpenSearch and compare broker load.