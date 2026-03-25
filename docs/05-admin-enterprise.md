# 05 Kafka Administration and Enterprise Concepts

## Kafka cluster setup

This repo uses one local broker in KRaft mode. Production clusters usually separate brokers across hosts and availability zones.

High-level production concerns:

- rack awareness
- storage sizing
- broker count
- controller quorum sizing
- replication strategy
- monitoring and alerting

## Kafka monitoring and operations

Watch at least these signals:

- under-replicated partitions
- consumer lag
- request latency
- disk usage
- broker CPU and network saturation
- rebalance frequency

Kafka UI gives a lightweight local view. In production you would add Prometheus and Grafana.

## Kafka security

Main controls:

- TLS encryption
- SASL authentication
- ACL authorization
- secret rotation
- network segmentation

The local stack is plaintext for simplicity. That is acceptable only for development.

## Multi-cluster Kafka

Teams add clusters for:

- regional isolation
- disaster recovery
- data residency
- workload separation

Once you have multiple clusters, topic naming, replication strategy, and observability become much more important.

## MirrorMaker

MirrorMaker 2 replicates topics across clusters. Use it for:

- disaster recovery
- migration
- regional fan-out
- aggregate analytics

Be careful with loops, offset sync expectations, and topic filters.

## Advertised listeners

Listeners define where brokers accept traffic. Advertised listeners define what address clients should use when the broker returns metadata.

This is one of the most common local Kafka issues. In this repo:

- containers use `kafka:9092`
- tools on the host use `localhost:9094`

## Kafka client and server protocol

Clients bootstrap with one broker, fetch metadata, then connect directly to leaders for the partitions they need. That is why bad advertised listeners break clients even when the bootstrap address looks correct.