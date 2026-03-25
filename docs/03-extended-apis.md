# 03 Extended APIs

## Kafka Connect basics

Kafka Connect is the integration framework around Kafka. Use it when you want managed source and sink connectors instead of custom producer and consumer code.

Good fit:

- databases
- object stores
- search engines
- SaaS integrations

Poor fit:

- custom low-latency business logic
- highly specialized transforms that are easier in code

## Kafka Connect with Wikimedia and Elasticsearch / OpenSearch

This project implements the sink path in Node.js so you can understand the mechanics. After that, compare it with a managed connector approach.

An example connector config is provided in `examples/kafka-connect/opensearch-sink.json`.

Suggested exercise:

1. Run the Node.js producer.
2. Inspect the raw topic.
3. Compare the current Node.js OpenSearch sink with the connector configuration.
4. Decide whether your real system should keep custom code or move to Kafka Connect.

## Kafka Streams introduction

Kafka Streams is the JVM-native stream-processing library built around Kafka topics as both input and state backbone.

Core ideas:

- stateless transforms
- stateful aggregations
- joins
- windowing
- repartitioning
- changelog topics and state stores

## Kafka Streams hands-on

Kafka Streams itself is a Java API, not a Node.js library. Because this repo is intentionally Node.js-first, the hands-on path here is conceptual:

1. Use `analytics-consumer.ts` as the mental model for stateful aggregation.
2. Read how the in-memory maps represent a temporary state store.
3. Imagine replacing that custom consumer with a Kafka Streams topology in a JVM service when you need windows, joins, or fault-tolerant state stores.

If you want a next step, add a small JVM submodule later and implement the same aggregation as a Kafka Streams topology.

## Schema Registry introduction

Schema Registry manages data contracts for Avro, Protobuf, or JSON Schema payloads.

Benefits:

- producer and consumer compatibility control
- versioned schemas
- less fragile downstream parsing
- clearer evolution strategy

## Schema Registry hands-on

This starter project keeps raw Wikimedia events as JSON strings so the pipeline stays easy to understand. The next step is to introduce a governed event contract.

An example schema is provided in `examples/schema-registry/wikimedia-change.avsc`.

Suggested evolution path:

1. Define the subset of Wikimedia fields you care about.
2. Register that Avro schema.
3. Serialize in the producer.
4. Deserialize in each consumer.
5. Practice forward and backward compatibility checks.