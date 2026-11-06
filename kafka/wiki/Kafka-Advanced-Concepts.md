# Kafka Advanced Concepts

This document records the reliability patterns and advanced Kafka behaviour explored in the Spring Boot Kafka POC. It distinguishes implemented features from concepts and experiments that remain.

## 1. Delivery semantics

Kafka applications commonly discuss three delivery guarantees.

### 1.1 At-most-once

A record is processed zero or one time.

The consumer commits its offset before processing. If processing fails after the commit, the record will not normally be delivered again.

This avoids duplicate processing but can lose work.

### 1.2 At-least-once

A record is processed one or more times.

The consumer commits its offset after processing. If the application crashes after performing the work but before committing the offset, Kafka can deliver the record again.

This avoids losing work due to that failure window, but duplicates are possible.

### 1.3 Exactly-once

Exactly-once processing means that the relevant effects are committed atomically within the scope of the system providing the guarantee.

Kafka transactions can atomically write Kafka records and commit consumed Kafka offsets. They do not automatically include an external database such as PostgreSQL.

**Important:** Kafka exactly-once semantics do not make arbitrary database operations exactly once. For database consumers, at-least-once delivery combined with idempotent database operations is a common approach.

## 2. Producer reliability

The POC configures the producer with:

```yaml
spring:
  kafka:
    producer:
      acks: all
      retries: 10
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
```

### 2.1 Acknowledgements (`acks`)

This setting controls how much acknowledgement the producer waits for.

| Value | Behaviour |
|---|---|
| `0` | The producer does not wait for a broker acknowledgement. |
| `1` | The leader acknowledges the record. |
| `all` | The leader waits for the required in-sync replicas to acknowledge the record. |

`acks=all` should be considered together with the topic replication factor and broker `min.insync.replicas` setting.

The POC uses replication factor one because it has a single broker. Therefore, `acks=all` does not provide broker-failure protection in this environment.

### 2.2 Producer retries

Retries allow the producer to retry certain failed sends.

Without appropriate reliability settings, a temporary broker or network problem could cause a message to fail even though retrying might have succeeded.

### 2.3 Producer idempotence

With idempotence enabled, Kafka uses producer IDs and sequence numbers to prevent duplicate records caused by producer retries within the supported producer session.

This is important when a broker receives a record but the acknowledgement is lost. The producer may retry because it does not know whether the original attempt succeeded.

Idempotence helps Kafka recognize that the retry belongs to the same producer sequence.

**Important:** Producer idempotence does not prevent an application from deliberately sending the same business event again. It also does not make database operations idempotent.

**POC status:** Producer reliability settings are implemented and discussed.

## 3. Consumer error handling, retries and DLT

A consumer can fail while processing a record.

Examples include:
- Temporary database outages
- Invalid data
- Unexpected application exceptions
- External service failures

Retrying can help when the problem is temporary.

### 3.1 Blocking retries

The POC uses Spring Kafka's `DefaultErrorHandler` with a `FixedBackOff`:

```java
@Bean
public DefaultErrorHandler errorHandler(
        KafkaTemplate<String, OrderEvent> kafkaTemplate
) {
    DeadLetterPublishingRecoverer recoverer =
            new DeadLetterPublishingRecoverer(
                    kafkaTemplate,
                    (record, exception) ->
                            new TopicPartition(
                                    record.topic() + ".DLT",
                                    record.partition()
                            )
            );

    FixedBackOff backOff = new FixedBackOff(5_000L, 3L);

    return new DefaultErrorHandler(recoverer, backOff);
}
```

The configuration:

```java
new FixedBackOff(5_000L, 3L);
```

means:
- Wait 5 seconds between attempts.
- Retry three times after the initial failed attempt.
- Perform up to four processing attempts in total.

These are blocking retries: the listener container waits and retries the failed record before moving on.

### 3.2 Dead-letter topics (DLT)

After retries are exhausted, the `DeadLetterPublishingRecoverer` publishes the failed record to a dead-letter topic.

In this POC:

```text
poc.order-events -> poc.order-events.DLT
```

A DLT is an ordinary Kafka topic. It stores records that could not be processed successfully by the normal listener.

The original record is not deleted from its original topic just because it is sent to the DLT. The consumer can advance its offset after recovery, while the failed record remains subject to the original topic's retention policy.

DLT records can include diagnostic headers, such as exception details and information about the original topic and partition.

### 3.3 Important DLT considerations

- A DLT does not fix the underlying problem.
- DLT records need monitoring and an operational process.
- Replaying a DLT record creates another processing attempt; it should not be assumed to be safe automatically.
- Avoid replay loops where a record repeatedly fails and is sent back to the DLT.
- The destination DLT topic must exist and have a suitable partition count for the configured routing strategy.

**POC status:** Basic blocking retries and DLT publishing are implemented. Retry topics are not yet implemented.

## 4. Kafka transactions

Kafka transactions allow a producer to write multiple records atomically.

If the transaction commits, all its records become visible to `read_committed` consumers. If it aborts, those records are not visible to `read_committed` consumers.

### 4.1 Configuration

The POC configures a transaction ID prefix:

```yaml
spring:
  kafka:
    producer:
      transaction-id-prefix: kafka-poc-${random.uuid}-
```

This enables transactional producer support.

### 4.2 Successful transaction

The POC uses `KafkaTemplate.executeInTransaction()` to send multiple order events in one transaction.

Conceptually:

```java
kafkaTemplate.executeInTransaction(operations -> {
    operations.send(topic, key, firstEvent);
    operations.send(topic, key, secondEvent);
    return true;
});
```

Both records are committed together.

### 4.3 Aborted transaction

The POC also has an endpoint that deliberately throws an exception inside the transaction callback.

```java
kafkaTemplate.executeInTransaction(operations -> {
    operations.send(topic, key, firstEvent);
    operations.send(topic, key, secondEvent);

    throw new IllegalStateException("Simulated transaction failure");
});
```

The transaction is aborted, so its records are not visible to `read_committed` consumers.

### 4.4 Isolation level

The POC uses:

```yaml
spring:
  kafka:
    consumer:
      properties:
        isolation.level: read_committed
```

There are two relevant isolation levels:

| Isolation level | Behaviour |
|---|---|
| `read_uncommitted` | Can read records from committed and aborted transactions. |
| `read_committed` | Reads only committed transactional records. |

For this POC, `read_committed` is used so aborted transactional records are not processed by the consumer.

### 4.5 Kafka transactions are not database transactions

A Kafka transaction can atomically write Kafka records and, when used in a transactional listener, commit consumed Kafka offsets as part of the Kafka transaction.

It cannot atomically commit a PostgreSQL transaction.

For workflows involving both Kafka and a relational database, another pattern is needed.

**POC status:** Kafka producer transactions and the `read_committed` behaviour are implemented and tested.

The POC also uses separate transactional and non-transactional producer configurations for different use cases.

## 5. Database idempotency

With at-least-once delivery, a consumer may receive the same event more than once.

If processing changes database state, duplicate delivery must not repeat the business operation.

This is where **idempotency** becomes important.

An operation is idempotent when performing it multiple times has the same effect as performing it once.

### 5.1 Processed events table

The POC uses a `processed_events` table to track event IDs that have already been processed.

The important distinction is:

- `eventId` identifies a particular event.
- `orderId` identifies the business order and can appear in multiple events.

For example, the same order might generate:

```text
eventId: evt-1001, orderId: order-1001, status: CREATED
eventId: evt-1002, orderId: order-1001, status: PAID
eventId: evt-1003, orderId: order-1001, status: SHIPPED
```

Each event has a different event ID, even though they belong to the same order.

### 5.2 Atomic duplicate detection

The database enforces uniqueness on the event ID.

The POC uses PostgreSQL's atomic insert:

```sql
INSERT INTO processed_events (
    event_id,
    order_id,
    customer_id,
    amount,
    status,
    processed_at
)
VALUES (
    :eventId,
    :orderId,
    :customerId,
    :amount,
    :status,
    :processedAt
)
ON CONFLICT (event_id) DO NOTHING;
```

The repository method returns:
- `1` if the event was inserted for the first time.
- `0` if the event ID already existed.

This avoids a race-prone `SELECT` followed by `INSERT`.

Two concurrent consumers can both attempt to register the same event, but the database uniqueness constraint ensures that only one insert succeeds.

### 5.3 Processing flow

The service performs the following work in one PostgreSQL transaction:

1. Register the event ID in `processed_events`.
2. If it is a duplicate, stop processing.
3. Update the order in `orders`.
4. Insert the corresponding notification event into `outbox_events`.

The processed-event marker and business update must be committed together.

Otherwise, a marker could be saved without the business operation, or vice versa.

### 5.4 Database transaction and Kafka offset are separate

The PostgreSQL transaction does not include the Kafka consumer offset.

For example:

1. The consumer receives an event.
2. PostgreSQL commits the order update and processed-event marker.
3. The application crashes before the Kafka offset is committed.
4. Kafka delivers the event again.
5. The unique event ID causes the second database attempt to be ignored.

This is how the POC safely handles that duplicate-delivery window.

**POC status:** Implemented using PostgreSQL, Spring Data JPA, a unique event ID, and a native `INSERT ... ON CONFLICT DO NOTHING` query.

## 6. Transactional Outbox pattern

### 6.1 The dual-write problem

Suppose a service needs to update PostgreSQL and publish an event to Kafka.

These are two separate systems. A failure between the database commit and Kafka publication can leave the systems inconsistent.

For example:

**Scenario A:**

1. The database transaction commits successfully.
2. The application tries to publish the event to Kafka.
3. Kafka is unavailable.
4. The database contains the change, but Kafka never receives the event.

**Scenario B:**

1. The application publishes the event to Kafka.
2. The database transaction fails.
3. Kafka contains an event describing a change that was never committed to the database.

A normal PostgreSQL transaction cannot atomically commit a Kafka message.

This is known as the **dual-write problem**.

### 6.2 How the Outbox pattern solves this

The Outbox pattern stores the event in the same database transaction as the business update.

In this POC, processing an order event performs these operations in one PostgreSQL transaction:

1. Register the event in `processed_events`.
2. Update the order in `orders`.
3. Insert a notification event into `outbox_events`.

A separate scheduled publisher (the Outbox relay) polls unpublished rows, publishes them to Kafka, and marks them as published after Kafka acknowledges the send.

The flow is:

```text
Kafka: poc.order-events
          |
          v
    Order consumer
          |
          v
  PostgreSQL transaction
    - processed_events
    - orders
    - outbox_events
          |
          v
    Outbox publisher
          |
          v
  Kafka: poc.notifications
          |
          v
 Notification consumer
```

The important guarantee is that the business update and the intention to publish the event are committed together.

If the database transaction rolls back, the outbox event is rolled back too.

### 6.3 Outbox table

The POC's `outbox_events` entity contains:

| Field | Purpose |
|---|---|
| `id` | Unique event identifier |
| `eventType` | Business meaning/type of the event |
| `aggregateId` | Business entity identifier; used as the Kafka key |
| `topic` | Destination Kafka topic |
| `payload` | Serialized event payload |
| `createdAt` | When the outbox row was created |
| `publishedAt` | When successful publication was recorded |

The `eventType` and `topic` serve different purposes: one describes what the event means, while the other identifies where it should be published.

### 6.4 Outbox publisher

The current publisher:

1. Runs periodically using Spring's `@Scheduled`.
2. Reads a limited batch of unpublished records.
3. Deserializes each payload into a `NotificationEvent`.
4. Sends the event to the topic stored in the outbox record.
5. Uses the aggregate ID as the Kafka key.
6. Waits for Kafka acknowledgement.
7. Updates `publishedAt` only after successful acknowledgement.

The publisher uses a non-transactional `KafkaTemplate` for this operation.

### 6.5 Why can the notification arrive before the outbox update log?

This is expected.

The publisher performs:

```java
kafkaTemplate.send(
        event.getTopic(),
        event.getAggregateId(),
        notification
).get();

outboxEventRepository.markAsPublished(
        event.getId(),
        LocalDateTime.now(Clock.systemUTC())
);
```

The `.get()` waits for Kafka's acknowledgement.

Once Kafka acknowledges the message, it can become available to consumers. The notification listener can receive and log it before the publisher updates PostgreSQL.

Therefore, the log order can be:

```text
Notification event received
Hibernate: update outbox_events ...
Outbox event published
```

This does not indicate a problem.

`publishedAt` means the publisher successfully received Kafka's acknowledgement and recorded that fact in PostgreSQL. It does not mean that a consumer has received or processed the notification.

### 6.6 Publication and duplicate delivery

The publisher waits for Kafka acknowledgement before updating `publishedAt`.

However, this does not provide exactly-once publication across PostgreSQL and Kafka.

Consider this failure:

1. Kafka acknowledges the event.
2. The application crashes before updating `publishedAt`.
3. The row still appears unpublished after restart.
4. The publisher sends it again.

Therefore, the Outbox pattern provides reliable, eventual publication with **at-least-once delivery**, assuming the relay continues running and failures are handled.

It can publish duplicates.

The downstream notification consumer should therefore be idempotent too, using the notification event ID to prevent duplicate business effects.

### 6.7 Current POC limitations

The current relay:

- Runs on a scheduled interval.
- Reads a limited batch of pending records.
- Publishes records sequentially.
- Waits for Kafka acknowledgement.
- Marks a row as published only after acknowledgement.
- Stops processing the current batch when a publication fails.

This is a useful single-instance learning implementation.

It is not yet safe against multiple application instances publishing the same pending row concurrently. A claiming/locking strategy, such as PostgreSQL `FOR UPDATE SKIP LOCKED`, is a future improvement.

**POC status:** The basic Transactional Outbox is implemented and the happy path has been verified. Failure scenarios, downstream deduplication, and multi-instance concurrency still need to be tested or improved.

## 7. Transactional vs. non-transactional producers in the POC

The POC uses both transactional and non-transactional producers.

This distinction became important after configuring:

```yaml
spring:
  kafka:
    producer:
      transaction-id-prefix: kafka-poc-${random.uuid}-
```

With a transaction-capable producer factory, sends through the transactional template must happen within an appropriate transaction scope.

However, not every producer operation in the application needs a Kafka transaction.

For example:

- The transaction demonstration endpoints use a transactional producer.
- The regular order event producer uses a non-transactional producer.
- The Outbox publisher uses a non-transactional notification producer.

The configuration defines separate typed `ProducerFactory` and `KafkaTemplate` beans.

Conceptually:

| Producer | Transactional? | Purpose |
|---|---|---|
| Order event transactional template | Yes | Demonstrate Kafka transactions |
| Regular order event template | No | Normal event publication |
| Notification event template | No | Outbox relay publication |

The transactional factory has a transaction ID prefix. The non-transactional factories do not.

Explicit `@Qualifier` annotations are used to inject the intended template. A `@Primary` transactional producer factory resolves the default producer-factory dependency used by Spring Boot's Kafka transaction manager.

This setup is more explicit than relying entirely on Spring Boot's default Kafka bean configuration.

## 8. POC implementation overview

The main components are:

| Component | Responsibility |
|---|---|
| `KafkaMessageProducer` | Sends order events and demonstrates producer transactions |
| `KafkaMessageListener` | Consumes order events and handles DLT records |
| `OrderEventService` | Performs idempotent database processing |
| `ProcessedEventRepository` | Registers unique processed event IDs |
| `OrderRepository` | Persists order state |
| `OutboxEventRepository` | Reads and updates outbox records |
| `OutboxPublisher` | Publishes pending outbox events to Kafka |
| `NotificationEventListener` | Consumes notification events from `poc.notifications` |
| `KafkaErrorHandlerConfig` | Configures retries and DLT recovery |
| `KafkaProducerConfig` | Configures transactional and non-transactional producer factories/templates |

### Event models

`OrderEvent`:

```java
public record OrderEvent(
        String eventId,
        String orderId,
        String customerId,
        BigDecimal amount,
        String status
) {}
```

`NotificationEvent`:

```java
public record NotificationEvent(
        String eventId,
        String orderId,
        String message
) {}
```

The order ID is used as the Kafka key for order-related events.

Notification event IDs are stable and derived from the original order event ID:

```text
notification-<original-event-id>
```

## 9. Remaining learning and implementation work

The following items are not yet complete. They are deliberately kept separate from the concepts already implemented.

### 9.1 Outbox failure scenarios — next priority

The happy path has been verified. The next step is to test the failure cases that demonstrate the Outbox pattern's guarantees.

**A. Database rollback**

Force an exception during order processing.

Verify that:
- The order update is rolled back.
- The processed-event marker is rolled back.
- The outbox row is rolled back.

All three operations should belong to the same PostgreSQL transaction.

**B. Kafka unavailable**

1. Stop Kafka.
2. Let the Outbox publisher attempt to send a pending event.
3. Verify that `publishedAt` remains null.
4. Restart Kafka.
5. Verify that the event is eventually published and marked as published.

**C. Crash after Kafka acknowledgement**

Simulate a crash after Kafka acknowledges the message but before the database update.

Verify that the event can be published again after restart.

This demonstrates why the Outbox relay provides at-least-once publication rather than exactly-once publication.

**D. Notification idempotency**

Make the notification consumer handle duplicate event IDs without repeating its business operation.

The current listener logs received notifications; it does not yet demonstrate database-backed notification idempotency.

### 9.2 Outbox concurrency

Make the relay safe for multiple application instances.

Currently, two application instances could fetch and publish the same pending rows.

Explore row claiming/locking, for example with PostgreSQL:

```sql
SELECT *
FROM outbox_events
WHERE published_at IS NULL
ORDER BY created_at
FOR UPDATE SKIP LOCKED;
```

This needs to be designed carefully so that claiming and publishing work correctly across transactions and instances.

### 9.3 Consumer rebalancing

Explore partition reassignment when consumers join, leave, or fail.

Understand:
- How Kafka assigns partitions.
- What happens to in-flight processing.
- How offset commits interact with rebalancing.
- What happens when a consumer exceeds its configured poll interval.
- How duplicate processing can occur around failures and rebalances.

### 9.4 Retry topics

Explore non-blocking retry topics and compare them with the current blocking retry and DLT approach.

Important questions:
- How are retry topics configured in Spring Kafka?
- How are retry delays implemented?
- What happens to ordering?
- When is blocking retry preferable?
- When is a retry-topic strategy preferable?

### 9.5 Testcontainers

Add automated integration tests using real Kafka and PostgreSQL containers.

Potential scenarios:
- Producer sends an event and consumer receives it.
- Kafka transaction commits successfully.
- Aborted transaction is invisible to `read_committed` consumers.
- Failed processing is retried and eventually sent to DLT.
- Duplicate events do not repeat database operations.
- Outbox records are published and marked as published.
- Failed publication leaves an outbox row pending.

### 9.6 Monitoring and production readiness

Explore:

- Consumer lag
- Producer send failures and latency
- Retry and DLT metrics
- Broker and consumer health
- Important consumer and producer configuration
- Operational handling of failed and replayed messages

## 10. Learning status at the end of the current implementation phase

| Area | Status |
|---|---|
| Producer reliability settings | Implemented and discussed |
| Kafka producer transactions | Implemented and tested |
| `read_committed` consumers | Implemented and tested |
| Blocking retries and DLT | Implemented |
| Database idempotency | Implemented |
| Transactional Outbox happy path | Implemented and verified |
| Separate transactional/non-transactional producers | Implemented |
| Outbox failure scenarios | To test |
| Notification idempotency | To implement |
| Outbox multi-instance concurrency | To implement |
| Consumer rebalancing | To explore |
| Retry topics | To explore |
| Testcontainers integration tests | To implement |
| Monitoring | To explore |

## Key takeaways

- Producer idempotence protects against duplicates caused by producer retries; it does not replace business-level idempotency.
- At-least-once delivery means consumers must be prepared for duplicate records.
- Kafka transactions provide atomicity within Kafka, not across Kafka and PostgreSQL.
- Database idempotency protects business effects when a record is delivered more than once.
- The Transactional Outbox atomically stores business changes and the event to be published.
- The Outbox relay can publish duplicates if it crashes after Kafka acknowledgement but before recording publication.
- The Outbox pattern solves the dual-write problem, but it does not remove the need for downstream idempotency.
- Reliable event-driven systems require both correct implementation and deliberate testing of failure scenarios.
