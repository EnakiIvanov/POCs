package com.example.kafka.persistence.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Getter
@Table(name = "processed_events")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ProcessedEventEntity{

  @Id
  private String eventId;

  private String orderId;
  private String customerId;
  private BigDecimal amount;
  private String status;
  private LocalDateTime processedAt;
}
