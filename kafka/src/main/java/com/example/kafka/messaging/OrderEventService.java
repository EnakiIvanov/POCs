package com.example.kafka.messaging;

import com.example.kafka.persistence.entity.OrderEntity;
import com.example.kafka.persistence.repository.OrderRepository;
import com.example.kafka.persistence.repository.OutboxEventRepository;
import com.example.kafka.persistence.repository.ProcessedEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderEventService {

  private static final String NOTIFICATION_TOPIC = "poc.notifications";
  private static final String NOTIFICATION_EVENT_TYPE = "ORDER_NOTIFICATION";

  private final ObjectMapper objectMapper;
  private final OrderRepository orderRepository;
  private final ProcessedEventRepository repository;
  private final OutboxEventRepository outboxEventRepository;

  @Transactional(transactionManager = "transactionManager")
  public void process(OrderEvent event) {
    if (!registerEvent(event)) {
      log.info("Duplicate event ignored: {}", event.eventId());
      return;
    }

    updateOrder(event);

    var notification = createNotification(event);
    saveNotificationToOutbox(notification, event.orderId());

    log.info("Successfully processed event: {}", event.eventId());
  }

  private boolean registerEvent(OrderEvent event) {
    int inserted = repository.insertIfNotExists(
        event.eventId(),
        event.orderId(),
        event.customerId(),
        event.amount(),
        event.status(),
        LocalDateTime.now(Clock.systemUTC())
    );

    return inserted > 0;
  }

  private void updateOrder(OrderEvent event) {
    OrderEntity order = findOrCreateOrder(event);
    order.setStatus(event.status());

    orderRepository.save(order);
  }

  private OrderEntity findOrCreateOrder(OrderEvent event) {
    return orderRepository.findById(event.orderId())
        .orElseGet(() -> {
          if (!"CREATED".equals(event.status())) {
            throw new IllegalArgumentException("Cannot process event for non-existing order: " + event.orderId());
          }

          return OrderEntity.builder()
              .orderId(event.orderId())
              .build();
        });
  }

  private NotificationEvent createNotification(OrderEvent event) {
    return new NotificationEvent(
        "notification-" + event.eventId(),
        event.orderId(),
        "Your order has been " + event.status().toLowerCase() + "."
    );
  }

  private void saveNotificationToOutbox(NotificationEvent notification, String orderId) {
    var outboxEvent = new OutboxEventEntity();
    outboxEvent.setId(notification.eventId());
    outboxEvent.setEventType(NOTIFICATION_EVENT_TYPE);
    outboxEvent.setAggregateId(orderId);
    outboxEvent.setTopic(NOTIFICATION_TOPIC);
    outboxEvent.setCreatedAt(LocalDateTime.now(Clock.systemUTC()));
    outboxEvent.setPayload(serializeNotification(notification));

    outboxEventRepository.save(outboxEvent);
  }

  private String serializeNotification(NotificationEvent notification) {
    try {
      return objectMapper.writeValueAsString(notification);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Failed to serialize notification event", exception);
    }
  }
}
