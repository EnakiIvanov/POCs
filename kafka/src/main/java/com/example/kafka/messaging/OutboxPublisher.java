package com.example.kafka.messaging;

import com.example.kafka.persistence.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPublisher {

  private final OutboxEventRepository outboxEventRepository;
  @Qualifier("nonTransactionalNotificationKafkaTemplate")
  private final KafkaTemplate<String, NotificationEvent> kafkaTemplate;
  private final ObjectMapper objectMapper;

  @Scheduled(fixedDelay = 5_000)
  public void publishPendingEvents() {
    var pendingEvents = outboxEventRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(
            PageRequest.of(0, 100)
    );

    for (OutboxEventEntity event : pendingEvents) {
      try {
        publishEvent(event);
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        log.warn("Outbox publishing interrupted");
        return;
      } catch (Exception exception) {
        log.error("Failed to publish outbox event: {}", event.getId(), exception);

        // Stop here so later events don't overtake a failed event.
        break;
      }
    }
  }

  private void publishEvent(OutboxEventEntity event) throws Exception {
    var notification = objectMapper.readValue(event.getPayload(), NotificationEvent.class);

    kafkaTemplate.send(event.getTopic(), event.getAggregateId(), notification)
        .get();

    int updated = outboxEventRepository.markAsPublished(
        event.getId(),
        LocalDateTime.now(Clock.systemUTC())
    );

    if (updated == 1) {
      log.info("Outbox event published: {}", event.getId());
    } else {
      log.warn("Outbox event was already marked as published: {}", event.getId());
    }
  }
}

