package com.example.kafka.messaging;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "outbox_events")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OutboxEventEntity {

  @Id
  private String id;

  private String eventType;

  private String aggregateId;

  @Column(columnDefinition = "TEXT")
  private String payload;

  @Column(nullable = false)
  private String topic;

  private LocalDateTime createdAt;

  private LocalDateTime publishedAt;
}
