# StoreMesh Analytics Service

This service is the first Kafka consumer boundary for StoreMesh. It consumes
`OrderCreated` events from `storemesh.order.events` and maintains a small
PostgreSQL `daily_sales` projection for operational analytics. It also consumes
Inventory adjustment, reservation, and release events from
`storemesh.inventory.events` and maintains idempotent `inventory_event_counts`.

The consumer is deliberately opt-in. It requires an externally operated Kafka
broker and PostgreSQL; the local Argo bootstrap does not install or start it.
Apply `migrations/001_analytics.sql` before running the consumer.

At-least-once delivery is expected. `processed_events.event_id` is written in
the same transaction as the projection update, so replayed Kafka messages are
acknowledged without double-counting sales. If projection fails, the Kafka
offset is not committed and the message can be retried.

## Local development

Run the unit tests without Kafka, PostgreSQL, Docker, or Kubernetes:

```sh
go test ./...
```

Run the consumer only when both dependencies are intentionally available:

```sh
DATABASE_URL='postgres://...' \
KAFKA_BROKERS='broker:9092' \
go run ./cmd/consumer
```
