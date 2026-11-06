# Kafka Advanced Concepts

This document records the reliability patterns, transaction behavior, failure handling, and operational concepts explored in the Spring Boot Kafka POC.

It serves as a technical reference for both the concepts behind the implementation and the decisions made while building the POC.

Each section describes the relevant Kafka behavior, explains why it matters, and documents how it is applied in this project. Implemented features and concepts that have only been explored theoretically are identified separately.

## 1. Delivery semantics

Delivery semantics describe the guarantees an application has regarding whether a message will be processed, how many times it might be processed, and how failures affect that guarantee.

Kafka applications commonly discuss three delivery guarantees: at-most-once, at-least-once, and exactly-once.

### 1.1 At-most-once

A record is processed zero or one time.

The consumer commits its offset before processing the record.

For example:

1. The consumer receives a record.
2. The consumer commits its offset.
3. The application attempts to process the record.
4. Processing fails.

Kafka considers the record consumed because its offset was already committed. It will not normally be delivered again to that consumer group.

This avoids duplicate processing but introduces the possibility of losing work.

At-most-once can be appropriate when occasional message loss is acceptable and duplicate processing must be avoided.

### 1.2 At-least-once

A record is processed one or more times.

The consumer commits its offset after successful processing.

For example:

1. The consumer receives a record.
2. The application processes it successfully.
3. The application commits its offset.

However, there is a failure window:

1. The consumer receives a record.
2. The application processes it successfully.
3. The application crashes before committing the offset.
4. After restart or reassignment, Kafka delivers the record again.

The business operation may therefore be executed more than once.

At-least-once avoids losing work due to this particular failure window, but duplicate delivery is possible.

For applications that modify database state, this normally requires idempotent processing.

### 1.3 Exactly-once

Exactly-once semantics mean that the relevant operations are committed atomically within the scope of the system providing the guarantee.

Kafka transactions can atomically:
- Write multiple records to Kafka.
- Commit consumed Kafka offsets as part of a Kafka transaction when using transactional consumption.

For example, a Kafka Streams application can consume records, produce new records, and commit the corresponding input offsets as one Kafka transaction.

However, Kafka transactions do not automatically include an external database such as PostgreSQL.

If an application updates PostgreSQL and publishes to Kafka, those operations are not automatically part of one distributed transaction.

**Important distinction:** Exactly-once delivery or processing within Kafka does not mean arbitrary external business operations happen exactly once.

For database consumers, at-least-once delivery combined with database-backed idempotency is a common approach.

**POC status:** Delivery semantics explored through producer reliability, Kafka transactions, database idempotency, and Outbox failure scenarios.

## 2. Producer reliability

The producer is responsible for sending records to Kafka. Network problems, broker failures, timeouts, and other transient errors can interrupt publication.

Producer reliability settings control how much acknowledgement is required, whether failed sends are retried, and whether retries can create duplicate records.

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

The `acks` setting controls how much acknowledgement the producer waits for before considering a send successful.

| Value | Behaviour |
|---|---|
| `0` | The producer does not wait for a broker acknowledgement. |
| `1` | The partition leader acknowledges the record. |
| `all` | The leader waits for the required in-sync replicas to acknowledge the record. |

With `acks=0`, the producer cannot reliably know whether the broker received the record.

With `acks=1`, the leader acknowledges the record without waiting for all in-sync replicas. If the leader fails before replication, the record may be lost.

With `acks=all`, the leader waits for the current in-sync replicas to acknowledge the record. This provides stronger durability, provided replication is configured appropriately.

`acks=all` should be considered together with:

- Topic replication factor.
- Broker `min.insync.replicas`.
- The number of available in-sync replicas.

For example, a topic with replication factor 3 and `min.insync.replicas=2` requires at least two in-sync replicas for a successful `acks=all` write.

The POC uses replication factor one because it has a single broker. Therefore, `acks=all` does not provide protection against that broker failing.

### 2.2 Producer retries

The `retries` setting allows the producer to retry certain failed sends.

This is useful when a failure is temporary. For example, a network interruption or a broker becoming temporarily unavailable does not necessarily mean that a record should permanently fail.

Retries are not guaranteed to run the configured number of times. The producer can stop retrying when delivery succeeds or when the overall delivery deadline is reached.

The relevant settings include:

- `retries`: maximum retry attempts.
- `request.timeout.ms`: how long the producer waits for an individual broker request.
- `delivery.timeout.ms`: maximum total time allowed for delivering a record, including retries and waiting for acknowledgements.

A request timeout does not necessarily mean that the entire send has failed. The producer may still retry within its delivery deadline.

### 2.3 Producer idempotence

With producer idempotence enabled, Kafka uses producer IDs and sequence numbers to prevent duplicate records caused by producer retries within the supported producer session.

Consider this scenario:

1. The producer sends a record.
2. The broker stores it successfully.
3. The acknowledgement is lost due to a network problem.
4. The producer does not know whether the original send succeeded.
5. The producer retries the record.

Without idempotence, the broker could store the record twice.

With idempotence, Kafka recognizes that the retry belongs to the same producer sequence and avoids storing a duplicate.

**Important:** Producer idempotence does not prevent an application from independently sending the same business event multiple times. It also does not make database operations idempotent.

For example, if the Outbox publisher sends an event, receives an acknowledgement, and crashes before updating PostgreSQL, it can send the same event again after restarting. That is an application-level resend, not simply a producer retry.

**POC status:** Producer reliability settings are implemented. Producer idempotence, acknowledgements, and retry behavior have been explored.

## 3. Consumer error handling, retries and DLT

A consumer can fail while processing a record for many reasons:

- Temporary database outages.
- External service failures.
- Invalid data.
- Unexpected application exceptions.
- Bugs in business logic.

The handling strategy depends on whether the error is temporary or permanent.

Retrying can help when the problem is temporary. However, repeatedly retrying a permanently invalid record will not make it valid.

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

- Wait five seconds between attempts.
- Retry three times after the initial failed attempt.
- Perform up to four processing attempts in total.

These are blocking retries.

The listener container waits and retries the failed record before moving on to subsequent records from that partition.

This has an important consequence: a repeatedly failing record can delay other records in the same partition.

Blocking retries are relatively straightforward and preserve the normal partition processing flow, but they can reduce throughput when failures take a long time to recover.

### 3.2 Dead-letter topics (DLT)

After retries are exhausted, the `DeadLetterPublishingRecoverer` publishes the failed record to a dead-letter topic.

In this POC:

```text
poc.order-events -> poc.order-events.DLT
```

A DLT is an ordinary Kafka topic used to store records that could not be processed successfully by the normal consumer.

Moving a record to the DLT allows the original consumer to recover from the failure and continue processing subsequent records, assuming recovery succeeds.

The original record is not deleted from its original topic just because it is published to the DLT. The original record remains subject to the original topic's retention policy.

The consumer can advance its offset after successful recovery, while the DLT contains another copy of the failed record.

DLT records can include diagnostic headers such as:
- Exception class and message.
- Original topic.
- Original partition.
- Original offset.
- Original timestamp.

These details help identify the cause of the failure and trace the record back to its original location.

### 3.3 Retry monitoring and recovery failures

Spring Kafka provides a `RetryListener` interface that can observe retry and recovery activity.

Important callbacks include:

- `failedDelivery()`: called when a delivery attempt fails.
- `recovered()`: called when the record is successfully recovered.
- `recoveryFailed()`: called when recovery itself fails.

These callbacks can be used to record custom metrics, such as retry counts and successful or failed recovery attempts.

A DLT publishing failure is different from a normal processing failure. If the recoverer cannot publish the record, recovery has not succeeded. The error handler must handle that failure rather than assuming the record was safely moved.

### 3.4 Important DLT considerations

- A DLT does not fix the underlying problem.
- DLT records need monitoring and an operational recovery process.
- Replaying a DLT record creates another processing attempt; it should not be assumed to be safe automatically.
- Avoid replay loops where a record repeatedly fails and is sent back to the DLT.
- The destination DLT topic must exist and have a suitable partition count for the configured routing strategy.
- DLT retention must be long enough for investigation and recovery.

**POC status:** Blocking retries and DLT publishing are implemented. The POC uses a five-second backoff with three retries after the initial attempt. Non-blocking retry topics have not yet been explored.

## 4. Kafka transactions

Kafka transactions allow multiple Kafka operations to be committed atomically.

Without transactions, sending multiple records involves separate producer operations. Some records might be successfully published while others fail.

Transactions allow the producer to group those operations into one atomic unit.

If the transaction commits, all its records become visible to `read_committed` consumers. If it aborts, its records are not visible to those consumers.

### 4.1 Configuration

The POC configures a transaction ID prefix:

```yaml
spring:
  kafka:
    producer:
      transaction-id-prefix: kafka-poc-${random.uuid}-
```

Spring Boot uses this prefix when configuring transactional producer support.

The prefix must be unique for concurrently running application instances. A unique suffix is used in this POC.

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

A consumer using `read_committed` will only see them after the transaction commits.

### 4.3 Aborted transaction

The POC also has an endpoint that deliberately throws an exception inside the transaction callback:

```java
kafkaTemplate.executeInTransaction(operations -> {
    operations.send(topic, key, firstEvent);
    operations.send(topic, key, secondEvent);

    throw new IllegalStateException("Simulated transaction failure");
});
```

The exception causes the transaction to abort.

Although the producer may have sent the records to the broker, they are not visible to `read_committed` consumers.

This demonstrates an important distinction: a record can exist in the Kafka log without being part of a committed transaction.

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

For this POC, `read_committed` ensures that aborted transactional records are not processed by the consumer.

A `read_committed` consumer can also be held back by an open transaction in a partition. It cannot read beyond the last stable offset until the transaction is resolved.

### 4.5 Kafka transactions are not database transactions

Kafka transactions can atomically write Kafka records and, when used with transactional consumption, commit consumed Kafka offsets as part of the Kafka transaction.

They cannot atomically commit a PostgreSQL transaction.

Consider an application that:
1. Updates an order in PostgreSQL.
2. Publishes an event to Kafka.

A Kafka transaction can protect the Kafka operation, but it cannot automatically roll back the PostgreSQL update if the Kafka transaction aborts.

Likewise, a PostgreSQL rollback cannot automatically abort a Kafka transaction.

This is why workflows involving both Kafka and a relational database need another approach, such as the Transactional Outbox pattern.

**POC status:** Kafka producer transactions and `read_committed` behavior are implemented and tested. The POC also uses separate transactional and non-transactional producer configurations.

## 5. Database idempotency

At-least-once delivery means that a consumer may receive the same event more than once.

If processing changes database state, duplicate delivery must not repeat the business operation.

An operation is idempotent when performing it multiple times has the same effect as performing it once.

For example, processing an order event twice should not create two orders, apply the same payment twice, or generate duplicate business notifications.

### 5.1 Event identity versus business identity

The POC uses a `processed_events` table to track event IDs that have already been processed.

Two identifiers have different meanings:

- `eventId` identifies a particular event.
- `orderId` identifies the business order and can appear in multiple events.

For example, the same order might generate:

```text
eventId: evt-1001, orderId: order-1001, status: CREATED
eventId: evt-1002, orderId: order-1001, status: PAID
eventId: evt-1003, orderId: order-1001, status: SHIPPED
```

Each event has a different event ID, even though all three events belong to the same order.

The event ID must remain stable when the same business event is redelivered or replayed. A replay is another delivery attempt for the same event, not a new business event.

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

A separate existence check is not sufficient for concurrent processing:

1. Consumer A checks whether the event exists and finds nothing.
2. Consumer B checks the same event and also finds nothing.
3. Both attempt to process it.

The database uniqueness constraint and atomic insert ensure that only one registration succeeds.

This makes the database the authority for duplicate detection rather than relying on application-level checks.

### 5.3 Processing flow and transaction boundaries

The `OrderEventService` performs the following work in one PostgreSQL transaction:

1. Register the event ID in `processed_events`.
2. If the insert returns zero, stop processing because the event is a duplicate.
3. Update or create the order in `orders`.
4. Insert the corresponding notification event into `outbox_events`.

The processed-event marker and business update must be committed together.

Otherwise, an event could be marked as processed without its business operation being completed, or the business operation could be committed without the corresponding marker.

The PostgreSQL transaction ensures that either all these changes commit or all of them roll back.

### 5.4 Database transaction and Kafka offset are separate

The PostgreSQL transaction does not include the Kafka consumer offset.

For example:

1. The consumer receives an event.
2. PostgreSQL commits the order update and processed-event marker.
3. The application crashes before the Kafka offset is committed.
4. Kafka delivers the event again.
5. The unique event ID causes the second database attempt to be ignored.

This is the main duplicate-delivery window handled by the POC.

The reverse situation is also important: if the PostgreSQL transaction rolls back, the processed-event marker is rolled back too. When the event is delivered again, it can be processed normally.

### 5.5 Notification idempotency

The POC applies the same principle to notification events using a separate `processed_notifications` table.

The notification event has its own stable event ID, derived from the original order event ID:

```text
notification-<original-event-id>
```

The notification consumer registers this ID atomically before performing its processing.

This prevents duplicate notification events from repeating the same business operation.

**POC status:** Implemented using PostgreSQL, Spring Data JPA, unique event IDs, and native `INSERT ... ON CONFLICT DO NOTHING` queries for both order and notification processing.

## 6. Transactional Outbox pattern

### 6.1 The dual-write problem

Suppose a service needs to update PostgreSQL and publish an event to Kafka.

These are two separate systems. A normal PostgreSQL transaction cannot atomically commit both operations.

This creates the dual-write problem.

There are two possible failure scenarios.

**Scenario A: Database commits, Kafka publication fails**

1. The database transaction commits successfully.
2. The application tries to publish the event to Kafka.
3. Kafka is unavailable.
4. The database contains the change, but Kafka never receives the event.

**Scenario B: Kafka publication succeeds, database transaction fails**

1. The application publishes the event to Kafka.
2. The database transaction fails.
3. Kafka contains an event describing a change that was never committed to the database.

Either scenario can leave the systems inconsistent.

Simply putting both operations inside a Spring `@Transactional` method does not solve this problem. A normal database transaction does not automatically include Kafka.

### 6.2 How the Outbox pattern solves this

The Transactional Outbox pattern stores the event in the same database transaction as the business update.

Instead of publishing directly to Kafka during the business transaction, the application first saves the event in an outbox table.

A separate publisher, commonly called the Outbox relay, reads pending rows and publishes them to Kafka.

In this POC, processing an order event performs these operations in one PostgreSQL transaction:

1. Register the event in `processed_events`.
2. Update the order in `orders`.
3. Insert a notification event into `outbox_events`.

A separate scheduled publisher:
1. Finds unpublished outbox records.
2. Deserializes their payloads.
3. Publishes them to Kafka.
4. Marks them as published after receiving Kafka acknowledgement.

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

If Kafka is unavailable, the outbox row remains available for a later publication attempt.

The Outbox pattern therefore removes the need to atomically commit PostgreSQL and Kafka as part of one transaction.

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

The `eventType` and `topic` serve different purposes:

- `eventType` describes what the event means.
- `topic` identifies where it should be published.

The `aggregateId` is used as the Kafka key. This helps ensure that events belonging to the same aggregate are routed to the same partition, assuming the partitioning configuration remains stable.

### 6.4 Outbox publisher

The current publisher:

1. Runs periodically using Spring's `@Scheduled`.
2. Reads a limited batch of unpublished records.
3. Deserializes each payload into a `NotificationEvent`.
4. Sends the event to the topic stored in the outbox record.
5. Uses the aggregate ID as the Kafka key.
6. Waits for Kafka acknowledgement using `CompletableFuture.get()`.
7. Updates `publishedAt` only after successful acknowledgement.

The publisher uses a non-transactional `KafkaTemplate` for this operation.

Waiting for the acknowledgement means the publisher does not mark an event as published merely because it initiated an asynchronous send.

If sending fails, the row remains unpublished and can be attempted again during a later polling cycle.

### 6.5 Kafka acknowledgement versus consumer processing

`publishedAt` records that the publisher received Kafka's acknowledgement and successfully recorded that fact in PostgreSQL.

It does not mean that a consumer has received or processed the notification.

The notification listener can receive and log a record before the Outbox publisher updates the database row.

For example:

```text
Notification event received
Hibernate: update outbox_events ...
Outbox event published
```

This is expected.

Kafka makes the record available to consumers after the broker accepts the publication. The publisher then continues with the PostgreSQL update.

### 6.6 Publication and duplicate delivery

The Outbox pattern provides reliable eventual publication, but it does not guarantee exactly-once publication across PostgreSQL and Kafka.

Consider this failure:

1. Kafka acknowledges the event.
2. The application crashes before updating `publishedAt`.
3. The row still appears unpublished after restart.
4. The publisher sends it again.

Kafka can therefore contain multiple records representing the same business event, with the same event ID but different offsets.

This is why the Outbox pattern is normally considered an at-least-once publication mechanism.

The downstream consumer must be idempotent too.

In this POC, notification idempotency uses `processed_notifications` and the stable notification event ID to prevent duplicate business processing.

### 6.7 Multi-instance publishing

A single Outbox publisher is straightforward, but multiple application instances introduce a concurrency problem.

Without coordination, two instances could select the same unpublished rows and both publish them.

The POC uses PostgreSQL row locking with `FOR UPDATE SKIP LOCKED`:

```sql
SELECT *
FROM outbox_events
WHERE published_at IS NULL
ORDER BY created_at
LIMIT :limit
FOR UPDATE SKIP LOCKED;
```

`FOR UPDATE` locks the selected rows for the current transaction.

`SKIP LOCKED` tells PostgreSQL not to wait for rows already locked by another transaction. Instead, it skips those rows and selects other available rows.

This allows multiple publisher instances to process different batches concurrently.

The selection and publication/update operations must run inside the same database transaction. Otherwise, the locks would be released before publication finishes.

The POC was tested with two application instances running simultaneously. They processed different batches without publishing the same pending rows concurrently.

**Trade-off:** The current implementation holds database locks and a transaction while waiting for Kafka acknowledgements. This is acceptable for the learning POC, but long-running transactions can become a scalability concern in a larger system.

A more advanced design could use a short transaction to claim rows, publish outside the transaction, and then update their status. Such a design would need explicit claiming, ownership, expiry, and recovery handling.

**POC status:** Transactional Outbox, failure scenarios, notification idempotency, and multi-instance concurrency are implemented and verified.

## 7. Transactional vs. non-transactional producers in the POC

The POC uses both transactional and non-transactional producers.

This distinction became important after configuring:

```yaml
spring:
  kafka:
    producer:
      transaction-id-prefix: kafka-poc-${random.uuid}-
```

A transaction-capable producer factory is configured to support Kafka transactions. However, not every producer operation in the application needs a Kafka transaction.

For example:

- Transaction demonstration endpoints use a transactional producer.
- The regular order event producer uses a non-transactional producer.
- The Outbox publisher uses a non-transactional notification producer.

The configuration defines separate typed `ProducerFactory` and `KafkaTemplate` beans.

| Producer | Transactional? | Purpose |
|---|---|---|
| Order event transactional template | Yes | Demonstrate Kafka transactions |
| Regular order event template | No | Normal event publication |
| Notification event template | No | Outbox relay publication |

The transactional factory has a transaction ID prefix. The non-transactional factories do not.

Explicit `@Qualifier` annotations are used to inject the intended template. This is important because multiple beans of the same type exist in the Spring application context.

The transactional producer factory is marked `@Primary` to resolve the default producer-factory dependency used by Spring Boot's Kafka transaction manager.

This setup is more explicit than relying entirely on Spring Boot's default Kafka bean configuration. It also demonstrates that transactional and non-transactional publishing are separate use cases and do not need to share the same producer configuration.

**POC status:** Implemented and working with separate typed producer factories and templates.

## 9. Advanced concepts and remaining work

This section tracks concepts explored during the POC and identifies the remaining implementation and testing work.

### 9.1 Outbox failure scenarios

The Outbox pattern was tested under several failure conditions to verify its guarantees and limitations.

**A. Database rollback**

An exception was forced during order processing.

The following were verified:
- The order update was rolled back.
- The processed-event marker was rolled back.
- The outbox row was rolled back.

All three operations belong to the same PostgreSQL transaction. This demonstrates that the business update and the intention to publish its notification cannot be committed independently.

**B. Kafka unavailable**

Kafka was stopped while an unpublished Outbox event existed.

The following were verified:
- The publication attempt failed.
- The outbox row remained pending (`publishedAt` stayed null).
- After Kafka restarted, the publisher eventually published the pending event.

This demonstrates that the Outbox table acts as durable storage for events waiting to be published.

**C. Crash after Kafka acknowledgement**

A failure was simulated after Kafka acknowledged the message but before the database update.

The following were verified:
- The outbox row remained unpublished.
- The publisher attempted to send the event again.
- Duplicate Kafka records were created with the same event ID but different offsets.

This demonstrated why the Outbox relay provides at-least-once publication rather than exactly-once publication.

**D. Notification idempotency**

A separate `processed_notifications` table was implemented.

The notification consumer registers the notification event ID using an atomic PostgreSQL insert with `ON CONFLICT DO NOTHING`.

Duplicate notification events were tested and successfully ignored instead of being processed again.

**POC status:** Implemented and verified.

### 9.2 Outbox concurrency

The relay was extended to support multiple application instances.

PostgreSQL `FOR UPDATE SKIP LOCKED` prevents concurrent publishers from selecting the same locked pending rows.

The selection and publication/update operations run inside the same database transaction so that row locks remain active until the batch finishes.

Two application instances were tested simultaneously. They processed different batches without concurrent duplicate publication.

The current implementation holds database locks while waiting for Kafka acknowledgements. A claim/lease-based approach with shorter database transactions could be considered in a more scalable implementation.

**POC status:** Implemented and verified.

### 9.3 Consumer rebalancing

Consumer rebalancing occurs when Kafka redistributes partition assignments within a consumer group.

A partition is assigned to only one consumer within a group at a time, but a consumer can own multiple partitions. Different consumer groups independently consume the same topic.

Common rebalance triggers include:
- A consumer joins or leaves the group.
- A consumer crashes or stops sending heartbeats.
- A consumer exceeds `max.poll.interval.ms`.
- The number of partitions changes.
- A consumer's topic subscription changes.

**Partition assignment and consumer count**

The number of consumers that can actively process a topic in one group is limited by the number of partitions.

For example, with three partitions and two consumers, one consumer can own two partitions while the other owns one.

Adding more consumers than partitions does not increase parallelism. Some consumers remain idle.

**Effects of rebalancing**

During a rebalance, partition processing is temporarily interrupted while Kafka redistributes assignments.

Frequent rebalances can reduce throughput because consumers repeatedly stop normal processing and establish new assignments.

**In-flight processing and duplicate delivery**

A consumer may lose its partition assignment while processing a record. Its processing might still finish, but the new partition owner can also receive the record if its offset was not committed.

For example:

1. Consumer A receives a record.
2. Consumer A commits the corresponding database transaction.
3. Consumer A loses its partition assignment before committing the Kafka offset.
4. Consumer B receives the partition and reads the same record again.

This is another situation where database idempotency prevents duplicate business effects.

**Consumer configuration and stability**

Three important settings influence consumer group stability:

- `session.timeout.ms`: how long the broker waits without receiving heartbeats before considering a consumer dead.
- `heartbeat.interval.ms`: how frequently the consumer sends heartbeats.
- `max.poll.interval.ms`: maximum allowed time between calls to `poll()`.

A consumer can continue sending heartbeats while application processing is taking too long. However, if it does not call `poll()` within the configured maximum interval, it can lose its partition assignment.

`max.poll.records` also matters because processing a large batch can increase the time before the next poll.

**POC status:** Concepts explored and discussed. No dedicated rebalance experiment implemented.

### 9.4 Retry topics

Explore non-blocking retry topics and compare them with the current blocking retry and DLT approach.

Blocking retries hold up processing of subsequent records from the affected partition while the failed record is retried.

Retry topics provide another approach: failed records can be forwarded to separate topics with configured delays, allowing the original consumer to continue processing other records.

This introduces additional considerations:
- How are retry topics configured in Spring Kafka?
- How are retry delays implemented?
- What happens to ordering when a failed record is processed later than subsequent records?
- When is blocking retry preferable?
- When is a retry-topic strategy preferable?
- How are exhausted retry attempts routed to a DLT?

**POC status:** Not yet explored or implemented.

### 9.5 Testcontainers

Add automated integration tests using real Kafka and PostgreSQL containers.

The purpose is to verify the behavior of the complete application against actual infrastructure rather than relying only on unit tests or manual experiments.

Potential scenarios:

**Producer and consumer**
- Producer sends an event and consumer receives it.
- Serialization and deserialization work correctly.

**Kafka transactions**
- A committed transaction makes its records visible to `read_committed` consumers.
- An aborted transaction is invisible to `read_committed` consumers.

**Consumer error handling**
- Failed processing is retried.
- Exhausted retries result in DLT publication.
- Duplicate events do not repeat database operations.

**Transactional Outbox**
- Outbox records are published and marked as published.
- Failed publication leaves an outbox row pending.
- Duplicate publication does not repeat notification business processing.
- Multiple Outbox publisher instances process different batches without concurrent duplicate publication.

**POC status:** Next learning and implementation priority.

### 9.6 Monitoring and production readiness

Monitoring is necessary because a running application does not necessarily mean that Kafka processing is healthy.

A broker can be available while consumers are stuck, and an application can be running while its consumer lag continues to increase.

**Consumer monitoring**

Important indicators include:
- Consumer lag: the difference between the latest available offset and the consumer group's committed offset. Increasing lag can indicate that consumers cannot keep up with incoming events.
- Consumer group membership and partition assignments.
- Polling activity and frequent rebalances.
- Processing failures, processing duration, and offset commit failures.

**Producer monitoring**

Important indicators include:
- Send failures and delivery latency.
- Retry rate, including increasing retries even when messages are eventually delivered successfully.
- Broker request latency and producer buffer availability.

**Retry and DLT monitoring**

Important indicators include:
- Processing failures and retry attempts.
- Successfully recovered records and recovery failures.
- DLT message volume and DLT consumer lag.
- DLT publishing failures.

Spring Kafka's `RetryListener` callbacks (`failedDelivery`, `recovered`, and `recoveryFailed`) can be used to collect retry and recovery metrics. Custom Micrometer counters can expose these measurements.

**Broker health**

Important indicators include:
- Broker availability and request latency.
- Offline partitions and under-replicated partitions.
- ISR changes.
- Disk usage, CPU, and memory.

A single-broker setup does not provide replication or broker-failure protection. Under-replication monitoring is particularly relevant to multi-broker clusters.

**Monitoring tools**

The following tools and concepts were explored:

- Kafka CLI commands for inspecting consumer groups, offsets, lag, topic partitions, and broker connectivity.
- Kafka client metrics and Spring Kafka metrics.
- Micrometer as the metrics instrumentation layer.
- Prometheus for collecting and storing metrics.
- Grafana for dashboards and alerts.

**Important producer and consumer configuration**

The purpose and operational impact of the following settings were reviewed.

Consumer:
- `enable.auto.commit`: controls whether the Kafka client automatically commits offsets independently of listener processing.
- `auto.offset.reset`: determines where a consumer starts when it has no valid committed offset.
- `max.poll.records`: limits the number of records returned by one poll.
- `max.poll.interval.ms`: limits the time between poll calls.
- `session.timeout.ms`: controls how long the broker waits for heartbeats.
- `heartbeat.interval.ms`: controls heartbeat frequency.
- `isolation.level`: determines whether aborted transactional records can be consumed.

Producer:
- `acks`: controls the acknowledgement level required for successful publication.
- `enable.idempotence`: prevents duplicates caused by producer retries.
- `delivery.timeout.ms`: limits the total time allowed for delivery.
- `request.timeout.ms`: limits the time waiting for an individual broker request.
- `linger.ms`: allows the producer to wait briefly to accumulate records into batches.
- `batch.size`: controls the maximum batch size for a partition.
- `compression.type`: controls producer-side record compression.
- `max.in.flight.requests.per.connection`: controls the number of unacknowledged requests allowed on a connection.

**Operational handling of failed and replayed messages**

A DLT does not automatically recover messages when the original problem disappears.

A recovery process should:
1. Detect and investigate the failed record.
2. Identify and fix the underlying cause.
3. Replay the original event.
4. Preserve its original event ID.
5. Control the replay rate to avoid overwhelming downstream systems.
6. Verify successful recovery.

Preserving the original event ID is important because the database uses it to identify business events that have already been processed.

Generating a new event ID during replay would make an old business event appear to be new and could repeat its business operation.

Replay should also account for event ordering and should not blindly republish the entire DLT without considering the impact on downstream systems.

**POC status:** Monitoring and production-readiness concepts explored and discussed. Kafka CLI commands were reviewed, but Micrometer instrumentation, Prometheus collection, Grafana dashboards, and automated alerts have not been implemented.

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
| Outbox failure scenarios | Implemented and verified |
| Notification idempotency | Implemented and verified |
| Outbox multi-instance concurrency | Implemented and verified |
| Consumer rebalancing | Concepts explored |
| Retry topics | To explore |
| Testcontainers integration tests | Next priority |
| Monitoring and production readiness | Concepts explored; instrumentation not implemented |

## Key takeaways

- At-most-once delivery avoids duplicate processing but can lose work. At-least-once delivery avoids losing work in certain failure windows but can result in duplicate processing.
- Producer idempotence protects against duplicates caused by producer retries; it does not replace business-level idempotency.
- Kafka transactions provide atomicity within Kafka, not across Kafka and PostgreSQL.
- Database idempotency protects business effects when a record is delivered more than once.
- Event IDs identify individual business events, while aggregate IDs identify the business entities those events belong to.
- The Transactional Outbox atomically stores business changes and the event to be published.
- The Outbox relay can publish duplicates if it crashes after Kafka acknowledgement but before recording publication.
- Downstream consumers need their own idempotency mechanisms.
- PostgreSQL `FOR UPDATE SKIP LOCKED` can coordinate multiple Outbox publisher instances, although holding locks during publication has scalability trade-offs.
- Consumer rebalancing can interrupt processing and result in duplicate delivery when offsets have not been committed.
- Consumer and producer configuration must balance reliability, throughput, and processing time.
- DLT recovery requires investigation, controlled replay, and preservation of the original event identity.
- Reliable event-driven systems require both correct implementation and deliberate testing of failure scenarios.