package com.example.kafka.messaging;

import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaMessageProducer {

  private static final String TOPIC = "poc.order-events";

  @Qualifier("kafkaTemplate")
  private final KafkaTemplate<String, OrderEvent> transactionalKafkaTemplate;

  @Qualifier("nonTransactionalKafkaTemplate")
  private final KafkaTemplate<String, OrderEvent> nonTransactionalKafkaTemplate;

  public CompletableFuture<SendResult<String, OrderEvent>> sendMessage(OrderEvent event) {
    try {
      return nonTransactionalKafkaTemplate.send(TOPIC, event.orderId(), event);
    } catch (Exception exception) {
      return CompletableFuture.failedFuture(exception);
    }
  }

  public void sendOrderEvents(OrderEvent orderCreated, OrderEvent paymentRequested) {
    transactionalKafkaTemplate.executeInTransaction(operations -> {
      operations.send(TOPIC, orderCreated.orderId(), orderCreated);
      operations.send(TOPIC, paymentRequested.orderId(), paymentRequested);

      return true;
    });
  }

  public void sendOrderEventsAndAbort(OrderEvent orderCreated, OrderEvent paymentRequested) {
    transactionalKafkaTemplate.executeInTransaction(operations -> {
      operations.send(TOPIC, orderCreated.orderId(), orderCreated);
      operations.send(TOPIC, paymentRequested.orderId(), paymentRequested);

      throw new IllegalStateException("Simulated transaction failure");
    });
  }
}
/*
1. Is it possible to use different type key/value serializers/deserializers? For example one for json and one for strings.
2. "The producer uses Spring Kafka's JSON serializer. The type headers help the consumer identify the Java type to deserialize." - are these headers send automatically?
 */