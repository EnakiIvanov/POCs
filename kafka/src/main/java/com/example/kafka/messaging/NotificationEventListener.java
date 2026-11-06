package com.example.kafka.messaging;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationEventListener {

  @KafkaListener(topics = "poc.notifications")
  public void listen(ConsumerRecord<String, NotificationEvent> cRecord) {
    log.info("Notification event received: {}", cRecord.value());
  }
}