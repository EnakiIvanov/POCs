package com.example.kafka.persistence.repository;

import com.example.kafka.persistence.entity.ProcessedEventEntity;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, String> {

    @Modifying
    @Query(value = """
            INSERT INTO processed_events (event_id, order_id, customer_id, amount, status, processed_at)
            VALUES (:eventId, :orderId, :customerId, :amount, :status, :processedAt)
            ON CONFLICT (event_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfNotExists(
        @Param("eventId") String eventId,
        @Param("orderId") String orderId,
        @Param("customerId") String customerId,
        @Param("amount") BigDecimal amount,
        @Param("status") String status,
        @Param("processedAt") LocalDateTime processedAt
    );

}
