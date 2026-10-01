package com.example.reportingnps.repository;

import com.example.reportingnps.entity.CallTranscription;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface CallTranscriptionRepository extends JpaRepository<CallTranscription, UUID> {

    Optional<CallTranscription> findByCallId(UUID callId);

    @Query("SELECT ct.sentiment, COUNT(ct) FROM CallTranscription ct WHERE ct.sentiment IS NOT NULL AND ct.callId IN (SELECT cm.callId FROM CallMetadata cm WHERE cm.callStartTime BETWEEN :from AND :to) GROUP BY ct.sentiment")
    List<Object[]> countBySentimentBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    @Query("SELECT COUNT(ct) FROM CallTranscription ct WHERE ct.sentiment = 'POSITIVE' AND ct.callId IN (SELECT cm.callId FROM CallMetadata cm WHERE cm.callStartTime BETWEEN :from AND :to)")
    long countPositiveBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    @Query("SELECT COUNT(ct) FROM CallTranscription ct WHERE ct.sentiment = 'NEUTRAL' AND ct.callId IN (SELECT cm.callId FROM CallMetadata cm WHERE cm.callStartTime BETWEEN :from AND :to)")
    long countNeutralBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    @Query("SELECT COUNT(ct) FROM CallTranscription ct WHERE ct.sentiment = 'NEGATIVE' AND ct.callId IN (SELECT cm.callId FROM CallMetadata cm WHERE cm.callStartTime BETWEEN :from AND :to)")
    long countNegativeBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);
}
