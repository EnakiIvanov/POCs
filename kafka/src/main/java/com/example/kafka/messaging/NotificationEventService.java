package com.example.kafka.messaging;

import com.example.kafka.persistence.repository.ProcessedNotificationRepository;
import java.time.Clock;
import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationEventService {

  private final ProcessedNotificationRepository repository;

  @Transactional(transactionManager = "transactionManager")
  public void process(NotificationEvent event) {
    int inserted = repository.insertIfNotExists(event.eventId(), LocalDateTime.now(Clock.systemUTC()));

    if (inserted == 0) {
      log.info("Ignoring duplicate notification: {}", event.eventId());
      return;
    }

    log.info("Processed notification: {}", event);
  }
}
