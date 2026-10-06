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

    @Query("SELECT LOWER(ct.sentiment), COUNT(ct) FROM CallTranscription ct WHERE ct.sentiment IS NOT NULL AND ct.callId IN (SELECT cm.callId FROM CallMetadata cm WHERE cm.callStartTime >= CURRENT_DATE) GROUP BY LOWER(ct.sentiment)")
    List<Object[]> countBySentimentBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    @Query("SELECT COUNT(ct) FROM CallTranscription ct WHERE LOWER(ct.sentiment) = 'positive' AND ct.callId IN (SELECT cm.callId FROM CallMetadata cm WHERE cm.callStartTime >= CURRENT_DATE)")
    long countPositiveBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    @Query("SELECT COUNT(ct) FROM CallTranscription ct WHERE LOWER(ct.sentiment) = 'neutral' AND ct.callId IN (SELECT cm.callId FROM CallMetadata cm WHERE cm.callStartTime >= CURRENT_DATE)")
    long countNeutralBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    @Query("SELECT COUNT(ct) FROM CallTranscription ct WHERE LOWER(ct.sentiment) = 'negative' AND ct.callId IN (SELECT cm.callId FROM CallMetadata cm WHERE cm.callStartTime >= CURRENT_DATE)")
    long countNegativeBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    @Query("SELECT ct.segment, COUNT(ct) FROM CallTranscription ct WHERE ct.segment IS NOT NULL AND ct.callId IN (SELECT cm.callId FROM CallMetadata cm WHERE cm.callStartTime >= CURRENT_DATE) GROUP BY ct.segment")
    List<Object[]> countBySegmentBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    @Query("SELECT ct.segment, COUNT(ct) FROM CallTranscription ct WHERE ct.segment IS NOT NULL AND ct.callId IN (SELECT cm.callId FROM CallMetadata cm WHERE cm.agentId = :agentId) GROUP BY ct.segment")
    List<Object[]> countByAgentIdAndSegment(@Param("agentId") String agentId);
}
