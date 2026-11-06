package com.example.kafka.persistence.repository;

import com.example.kafka.messaging.OutboxEventEntity;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, String> {

  @Query(value = """
    SELECT *
    FROM outbox_events
    WHERE published_at IS NULL
    ORDER BY created_at ASC
    LIMIT :#{#pageable.pageSize}
    OFFSET :#{#pageable.offset}
    FOR UPDATE SKIP LOCKED
    """, nativeQuery = true)
  List<OutboxEventEntity> findPendingEventsWithLock(Pageable pageable);

  @Modifying
  @Query("""
            UPDATE OutboxEventEntity e
            SET e.publishedAt = :publishedAt
            WHERE e.id = :id
              AND e.publishedAt IS NULL
            """)
  @Transactional
  int markAsPublished(
      @Param("id") String id,
      @Param("publishedAt") LocalDateTime publishedAt
  );
}
