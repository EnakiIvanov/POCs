package com.example.kafka.messaging;

import java.math.BigDecimal;

public record OrderEvent(
    String eventId,
    String orderId,
    String customerId,
    BigDecimal amount,
    String status
) {

}
