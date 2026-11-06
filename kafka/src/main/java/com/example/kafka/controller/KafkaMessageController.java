package com.example.kafka.controller;

import com.example.kafka.messaging.KafkaMessageProducer;
import com.example.kafka.messaging.OrderEvent;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/messages")
public class KafkaMessageController {

  private final KafkaMessageProducer messageProducer;

  @PostMapping
  public CompletableFuture<ResponseEntity<Void>> sendMessage(@RequestBody OrderEvent event) {
    return messageProducer.sendMessage(event)
        .thenApply(result -> ResponseEntity.accepted().<Void>build())
        .exceptionally(exception -> {
          log.error("Failed to send order event: {}", event.eventId(), exception);

          return ResponseEntity
              .status(HttpStatus.SERVICE_UNAVAILABLE)
              .build();
        });
  }

  @PostMapping("/transaction")
  public ResponseEntity<Void> sendTransactionalEvents(@RequestBody List<OrderEvent> events) {
    if (events.size() != 2) {
      return ResponseEntity.badRequest().build();
    }
    messageProducer.sendOrderEvents(events.get(0), events.get(1));

    return ResponseEntity.accepted().build();
  }

  @PostMapping("/transaction/abort")
  public ResponseEntity<Void> abortTransaction(@RequestBody List<OrderEvent> events) {
    messageProducer.sendOrderEventsAndAbort(events.get(0), events.get(1));

    return ResponseEntity.accepted().build();
  }
}
