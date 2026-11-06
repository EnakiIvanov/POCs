package com.example.kafka.messaging;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaMessageListener {

  private final OrderEventService orderEventService;

  @KafkaListener(topics = "poc.order-events")
  public void listen(ConsumerRecord<String, OrderEvent> cRecord) {
    log.info("Processing message: {}, key: {}, partition: {}, offset: {}",
        cRecord.value(), cRecord.key(), cRecord.partition(), cRecord.offset()
    );

    orderEventService.process(cRecord.value());

    log.info("Successfully processed message: {}", cRecord.value());
  }

  @KafkaListener(topics = "poc.order-events.DLT")
  public void listenToDlt(ConsumerRecord<String, OrderEvent> cRecord) {
    String originalTopic = new String(
        cRecord.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC).value(),
        StandardCharsets.UTF_8
    );

    int originalPartition = ByteBuffer.wrap(
        cRecord.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION).value()
    ).getInt();

    long originalOffset = ByteBuffer.wrap(
        cRecord.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET).value()
    ).getLong();

    log.error("Failed order event: {}, original topic: {}, partition: {}, offset: {}",
        cRecord.value(), originalTopic, originalPartition, originalOffset
    );
  }

  //Retry topics don't preserve the original processing order. A later event can be processed before an earlier failed event.
  @RetryableTopic(
      attempts = "4", //up to four total processing attempts, including the original attempt.
      backoff = @Backoff(
          delay = 10_000, //wait 10 seconds before the first retry.
          multiplier = 2.0, //double the delay for each subsequent retry.
          maxDelay = 60_000 //don't let the delay exceed 60 seconds.
      ),
      include = {TransientDataAccessException.class},
      exclude = {IllegalArgumentException.class},
      listenerContainerFactory = "retryTopicKafkaListenerContainerFactory"
  )
  @KafkaListener(
      topics = "poc.order-events-retry-demo",
      containerFactory = "retryTopicKafkaListenerContainerFactory"
  )
  public void consumeWithRetryTopics(OrderEvent event) {
    orderEventService.process(event);
  }

  @DltHandler
  public void handleDlt(OrderEvent event) {
    log.error("Event exhausted retry attempts: {}", event);
  }
}
