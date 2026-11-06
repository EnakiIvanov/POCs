package com.example.kafka.persistence.repository;

import com.example.kafka.persistence.entity.ProcessedNotificationEntity;
import java.time.LocalDateTime;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedNotificationRepository extends JpaRepository<ProcessedNotificationEntity, String> {

  @Modifying
  @Query(value = """
        INSERT INTO processed_notifications (event_id, processed_at)
        VALUES (:eventId, :processedAt)
        ON CONFLICT (event_id) DO NOTHING
        """, nativeQuery = true)
  int insertIfNotExists(
      @Param("eventId") String eventId,
      @Param("processedAt") LocalDateTime processedAt
  );
}

