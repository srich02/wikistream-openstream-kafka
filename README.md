# Kafka Wikimedia Analytics with Node.js

This project ingests the Wikimedia recent change stream, pushes the events into Kafka, and runs multiple consumers to analyze and index the data. It is designed to do two jobs at once:

1. Show a realistic high-throughput Kafka pipeline in Node.js.
2. Teach both foundational and advanced Kafka concepts with runnable code and focused notes.

## What this project includes

- A Wikimedia producer that reads Server-Sent Events and publishes them to Kafka.
- Two producer modes:
  - `safe` for stronger delivery guarantees.
  - `throughput` for larger batches and better ingestion speed.
- An analytics consumer that aggregates event types and wikis.
- An OpenSearch sink consumer for search-oriented exploration.
- A replay consumer that starts from the beginning of the topic to demonstrate historical reads.
- Docker Compose for local Kafka, Kafka UI, OpenSearch, and OpenSearch Dashboards.
- A documentation track under `docs/` covering the concept list you requested.

## Architecture

```text
Wikimedia SSE -> Node.js producer -> Kafka topic: wikimedia.recentchange.raw
                                      -> analytics consumer -> wikimedia.recentchange.analytics
                                      -> OpenSearch consumer -> OpenSearch index
                                      -> replay consumer -> console / experiments
```

## Quick start

1. Copy `.env.example` to `.env`.
2. Make sure `KAFKA_BROKERS` is set to `localhost:9094` when running the Node app from your machine.
3. Make sure `OPENSEARCH_NODE` is set to `https://localhost:9200` because the local OpenSearch container uses TLS.
4. Start infrastructure:

   ```powershell
   docker compose up -d
   ```

5. Create topics:

   ```powershell
   npm run dev:topics:create
   ```

6. Start the producer:

   ```powershell
   npm run dev:producer
   ```

7. In separate terminals, start one or more consumers:

   ```powershell
   npm run dev:consumer:analytics
   npm run dev:consumer:opensearch
   npm run dev:consumer:replay
   ```

## Key scripts

- `npm run build`: compile TypeScript into `dist/`
- `npm run check`: typecheck only
- `npm run dev:topics:create`: create Kafka topics
- `npm run dev:topics:describe`: inspect topic metadata
- `npm run dev:producer`: run the Wikimedia producer
- `npm run dev:consumer:analytics`: run the analytics consumer
- `npm run dev:consumer:opensearch`: run the OpenSearch sink consumer
- `npm run dev:consumer:replay`: replay data from the beginning of the raw topic

## Concept coverage map

- Producer concepts: `src/producer/`, `docs/01-producer-concepts.md`
- Consumer concepts: `src/consumers/`, `docs/02-consumer-concepts.md`
- Extended APIs: `docs/03-extended-apis.md`, `examples/`
- Architecture and best practices: `docs/04-architecture-best-practices.md`
- Administration and enterprise topics: `docs/05-admin-enterprise.md`
- Broker internals and topic internals: `docs/06-broker-internals.md`

## Important implementation note

This project uses KafkaJS. KafkaJS supports the most important delivery and retry behaviors, but it does not expose every JVM producer knob with a one-to-one API. In particular, concepts such as `linger.ms`, `batch.size`, `buffer.memory`, and `max.block.ms` are explained in the docs and approximated here with application-side batching. That is still useful for learning the tradeoffs, but it is not identical to the Java producer internals.

## Suggested learning path

1. Start the stack and watch the producer fill the raw topic.
2. Run the analytics consumer and observe offset commits and group behavior.
3. Run the replay consumer to see how historical reads differ from live processing.
4. Add the OpenSearch consumer and inspect indexed documents.
5. Read the docs in order from `01` through `06`.