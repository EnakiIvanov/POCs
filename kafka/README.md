# Kafka POC

A practical exploration of Apache Kafka and Spring Kafka, covering event-driven communication, producers, consumers, consumer groups, message delivery, and reliability patterns.

## Technologies

- **Java 21**
- **Spring Boot 3.5.3**
- **Spring Kafka**
- **Apache Kafka 4.0.1** – running in KRaft mode (without ZooKeeper)
- **PostgreSQL 18** – reserved for persistence-related exercises, such as idempotency and the Transactional Outbox Pattern.
- **Docker Compose** – used to run Kafka and PostgreSQL locally.
- **JUnit 5 & Testcontainers** – for integration testing.

## Prerequisites

- JDK 21
- Maven
- Docker Desktop (or another Docker environment)

## Running the infrastructure

The module uses Docker Compose to start Kafka and PostgreSQL.

From the Kafka module directory, execute:

```bash
docker compose up -d
```

This starts the following services:

| Service | Address | Database |
|---|---|---|
| Kafka | `localhost:9092` | — |
| PostgreSQL | `localhost:5432` | `kafka_poc` |

PostgreSQL credentials:

- Username: `kafka_poc`
- Password: `kafka_poc`

### Useful Docker commands

Check running containers:

```bash
docker compose ps
```

View Kafka logs:

```bash
docker compose logs -f kafka
```

Stop the containers while preserving data:

```bash
docker compose down
```

Stop the containers and remove persisted data:

```bash
docker compose down -v
```

## Running the application

Start the infrastructure using Docker Compose, then run the Spring Boot application from your IDE or with Maven:

```bash
mvn spring-boot:run
```

## Documentation

For explanations of Kafka fundamentals, terminology, architecture, and concepts covered by this POC, see the [Kafka Fundamentals Wiki](wiki/Kafka-Fundamentals.md).
For more advanced concepts, see the [Kafka Advanced Concepts Wiki](wiki/Kafka-Advanced-Concepts.md).

