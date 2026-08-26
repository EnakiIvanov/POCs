# Redis POC

A small project demonstrating Redis integration with Spring Boot.

## Concepts Demonstrated

- Redis data structures
- Key-value operations
- TTL and key expiration
- Serialization and deserialization
- Spring Data Redis
- Redis repositories
- RedisTemplate
- Redis Streams (publish/consume with Consumer Groups)
- Integration testing with TestContainers

## Structure

The project contains small, focused examples demonstrating different Redis features and their integration with Spring Boot.

## Testcontainers

The Redis module uses Testcontainers to automatically provide Redis both when running the application and when executing integration tests.

No separate Redis installation or manually configured container is required.

Docker must be running.