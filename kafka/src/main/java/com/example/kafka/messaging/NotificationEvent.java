package com.example.kafka.messaging;

public record NotificationEvent(
    String eventId,
    String orderId,
    String message
) {

}
