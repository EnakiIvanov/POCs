# Kafka Fundamentals

This document records the Kafka concepts covered while building the Spring Boot Kafka POC. It focuses on how Kafka works and how the concepts are represented in this project.

## 1. What is Apache Kafka?

Apache Kafka is a distributed event-streaming platform. Applications publish events to Kafka, Kafka stores them, and other applications consume them independently.

Kafka is commonly used for:

- Communication between microservices
- Event-driven architectures
- Data integration between systems
- Real-time data processing and analytics
- Event sourcing and event-stream storage

Kafka was originally developed at LinkedIn and is now an Apache project.

## 2. Why use Kafka?

Consider an e-commerce application. When a customer places an order, several things may need to happen:

- Process the payment
- Reserve inventory
- Send an order confirmation email
- Notify the shipping service

One option is for the Order Service to call each service directly through REST APIs.

This can create tight coupling:

- The Order Service needs to know about its downstream services.
- A slow or unavailable service can affect the order request.
- Adding another service may require changes to the Order Service.
- Handling retries and temporary outages can become complicated.

Kafka provides another approach: the Order Service publishes an event, such as `OrderCreated`, to Kafka. Other services independently consume that event and perform their own work.

The Order Service does not need to know which services are interested in the event.

This supports **loose coupling** and asynchronous communication.

### Direct communication vs. Kafka

**Without Kafka:**

```text
                 ┌──────────────────┐
                 │   Order Service  │
                 └────────┬─────────┘
                          │
             ┌────────────┼────────────┐
             │            │            │
             ▼            ▼            ▼
         Payment      Inventory       Email
         Service       Service       Service
```

**With Kafka:**

```text
                 ┌──────────────────┐
                 │   Order Service  │
                 └────────┬─────────┘
                          │ publishes
                          ▼
                 ┌──────────────────┐
                 │      Kafka       │
                 │  orders topic    │
                 └────────┬─────────┘
                          │
             ┌────────────┼────────────┐
             │            │            │
             ▼            ▼            ▼
         Payment      Inventory       Email
         Service       Service       Service
        (consumer)    (consumer)    (consumer)
```

Each service consumes the events it is interested in. The services can process those events independently.

Kafka does not guarantee that every consumer will successfully process an event. Reliability mechanisms are still necessary.

## 3. Core Kafka concepts

### 3.1 Producer

A **producer** is an application that publishes records to Kafka.

For example, the Order Service might publish an event when a customer places an order:

```json
{
  "orderId": "order-1001",
  "customerId": "customer-1001",
  "amount": 250.00,
  "status": "CREATED"
}
```

The event could represent an `OrderCreated` business event.

In Spring Kafka, the `KafkaTemplate` class is commonly used to send messages.

**POC implementation:** `KafkaMessageProducer` uses `KafkaTemplate` to publish `OrderEvent` objects.

### 3.2 Consumer

A **consumer** is an application that reads records from Kafka.

For example, the Email Service consumes `OrderCreated` and sends a confirmation email to the customer.

In Spring Kafka, the `@KafkaListener` annotation is commonly used to receive messages.

**POC implementation:** `KafkaMessageListener` consumes order events. `NotificationEventListener` consumes notification events.

### 3.3 Topic

A **topic** is a named stream or category in which Kafka stores records.

Producers publish records to topics, and consumers subscribe to topics.

Example topics in an e-commerce application:

| Topic | Example events |
|---|---|
| `orders` | `OrderCreated`, `OrderCancelled` |
| `payments` | `PaymentCompleted`, `PaymentFailed` |
| `inventory` | `StockReserved`, `StockReleased` |

A topic is not the same as a traditional queue. Multiple independent consumer groups can read the same topic.

**Topics used in this POC:**

| Topic | Purpose |
|---|---|
| `poc.messages` | General message examples |
| `poc.order-events` | Order events |
| `poc.order-events.DLT` | Failed order events |
| `poc.notifications` | Notification events published by the Outbox relay |

### 3.4 Broker

A **broker** is a Kafka server. It stores topic data and handles requests from producers and consumers.

A Kafka deployment can contain one or more brokers. A group of brokers working together is called a **Kafka cluster**.

The POC uses a single broker running in Docker. This is sufficient for learning, but it does not provide the fault tolerance of a multi-broker production cluster.

### 3.5 KRaft

Kafka can use KRaft mode for metadata management instead of relying on ZooKeeper.

The POC uses Kafka 4.0.1 in KRaft mode, without ZooKeeper.

## 4. Kafka message retention and consumer offsets

One of the important differences between Kafka and a traditional message queue is how Kafka handles consumed messages.

In a typical work-queue model, a message may be removed after a consumer successfully processes and acknowledges it.

Kafka works differently:

**Kafka retains records independently of whether a consumer has read them.**

Records are retained according to the topic's retention configuration, for example a time-based or size-based retention policy. A record is not normally deleted just because a consumer processed it.

This means different consumers can read the same events at their own pace.

### Example

Imagine that the `orders` topic contains four messages:

| Offset | Message |
|---:|---|
| 0 | `OrderCreated` – order 101 |
| 1 | `OrderCreated` – order 102 |
| 2 | `OrderCreated` – order 103 |
| 3 | `OrderCreated` – order 104 |

The Email Service has processed all four messages, while the Inventory Service has processed only the first two.

Their progress is independent:

```text
Topic: orders

Offset:       0              1              2              3
          ┌──────────┬──────────────┬──────────────┬──────────────┐
Messages: │ Order101 │  Order102    │  Order103    │  Order104    │
          └──────────┴──────────────┴──────────────┴──────────────┘

Email Service:      processed 0, 1, 2, 3
Inventory Service:  processed 0, 1; next position is 2
```

The Email Service processing a record does not remove it or prevent the Inventory Service from reading it.

### What is an offset?

An **offset** is a record's position within a Kafka partition.

Offsets are used to identify records and track a consumer's progress. They are specific to a partition; offset 5 in one partition is unrelated to offset 5 in another partition.

Consumers typically commit offsets to record how far they have processed a partition.

When a consumer restarts, it can continue from its committed position, subject to the configured offset-reset behaviour and message-retention limits.

### `auto.offset.reset`

This setting determines what happens when a consumer group has no valid committed offset, or its committed offset is no longer available.

Common values:

- `earliest`: start from the earliest available record.
- `latest`: start from the end of the partition, consuming new records.
- `none`: throw an exception if no valid offset exists.

**POC configuration:** `auto-offset-reset: earliest`.

Important: this setting does not normally reset an existing valid committed offset. It applies when there is no usable committed position.

## 5. Partitions

A topic is divided into **partitions**. Each partition is an ordered, append-only log of records.

Partitions allow Kafka to distribute storage and processing across brokers and consumers.

For example, a topic with three partitions might look like this:

```text
Topic: orders

Partition 0: [record 0] [record 1] [record 2] ...
Partition 1: [record 0] [record 1] [record 2] ...
Partition 2: [record 0] [record 1] [record 2] ...
```

Each partition has its own independent sequence of offsets.

### Ordering guarantees

Kafka guarantees ordering **within a partition**.

It does not guarantee a single global order across all partitions.

For example, if two records are written to different partitions, a consumer may observe them in a different order than the order in which the application originally created them.

If related records must be processed in order, they should normally be routed to the same partition using a common key.

**POC configuration:** Application topics use three partitions and replication factor one.

## 6. Consumer groups

A **consumer group** is a set of consumers that cooperate to read a topic.

Within a group, each partition is assigned to at most one consumer at a time. This allows the group to divide the work.

For example:

```text
Topic: orders (3 partitions)

Consumer Group A
  Consumer A1 -> Partition 0
  Consumer A2 -> Partition 1
  Consumer A3 -> Partition 2

Consumer Group B
  Consumer B1 -> Partition 0, 1, 2
```

Both groups independently receive the records from the topic.

Important consequences:

- If a topic has three partitions, one group can actively use at most three consumers for that topic at a time.
- If the group has fewer consumers than partitions, some consumers may receive multiple partitions.
- If it has more consumers than partitions, some consumers remain idle for that topic.
- Different groups maintain independent offsets and can process the same topic independently.

### Consumer group IDs in Spring Kafka

Spring Kafka allows a consumer group to be configured globally:

```yaml
spring:
  kafka:
    consumer:
      group-id: kafka-poc-group
```

A listener can override the default group ID:

```java
@KafkaListener(
    topics = "poc.order-events",
    groupId = "order-processing-group"
)
public void listen(OrderEvent event) {
    // Process event
}
```

This is useful when different logical services need to consume the same topic independently.

**POC coverage:** Different listeners use different group IDs to demonstrate independent consumption.

### Consumer rebalancing

A **rebalance** happens when Kafka changes partition assignments within a consumer group.

It can happen when:

- A consumer joins the group.
- A consumer leaves the group.
- A consumer crashes or is considered unavailable.
- The subscribed topics or partition count change.

Rebalancing is important because it affects which consumer processes which partitions.

**POC status:** Discussed conceptually, but detailed rebalance experiments remain to be done.

## 7. Message keys and partitioning

A Kafka record can contain a key and a value.

The key is useful for deciding which partition receives a record.

With the default keyed partitioning behaviour, the same key is routed to the same partition as long as the partition count and partitioning configuration remain unchanged.

For example:

```text
Order 1001 -> Partition 1
Order 1002 -> Partition 2
Order 1001 -> Partition 1
Order 1003 -> Partition 0
```

This helps preserve the order of events belonging to the same order.

**POC implementation:** Order events use `orderId` as the Kafka key:

```java
kafkaTemplate.send(TOPIC, event.orderId(), event);
```

Important considerations:

- A key is not the same thing as the record value.
- Records without keys can be distributed using the producer's partitioning strategy, which can use sticky batching.
- Increasing a topic's partition count can change the partition selected for a given key.
- A key helps preserve ordering within a partition, but it does not provide global ordering across the topic.

## 8. Serialization and deserialization

Kafka stores record keys and values as bytes.

Producers serialize objects before sending them, and consumers deserialize those bytes.

The POC uses Spring Kafka's JSON serializer and deserializer to send Java event objects, including:

- `OrderEvent`
- `NotificationEvent`

The consumer configuration includes:

```yaml
spring:
  kafka:
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: com.example.kafka.messaging
        spring.json.use.type.headers: true
```

The producer uses Spring Kafka's JSON serializer.

The type headers help the consumer identify the Java type to deserialize.

### Schema evolution

When event structures change, producers and consumers must remain compatible.

For example, adding a new field to an event can affect older consumers if the change is not handled correctly.

Schema Registry-based formats such as Avro, Protobuf, or JSON Schema can enforce schema compatibility rules.

Common compatibility modes include:

- **Backward:** new consumers can read records written with the previous schema.
- **Forward:** old