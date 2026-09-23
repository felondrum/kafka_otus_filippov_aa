package com.example.reportingnps.repository;

import com.example.reportingnps.entity.CallMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface CallMetadataRepository extends JpaRepository<CallMetadata, UUID> {

    Optional<CallMetadata> findByCallId(UUID callId);

    @Query("SELECT cm FROM CallMetadata cm WHERE cm.callStatus = :status ORDER BY cm.callStartTime DESC")
    java.util.List<CallMetadata> findByCallStatus(@Param("status") String status, org.springframework.data.domain.Pageable pageable);

    long countByCallStatus(String callStatus);

    long countByAgentId(String agentId);

    @Query("SELECT COUNT(cm) FROM CallMetadata cm WHERE cm.callStartTime BETWEEN :from AND :to")
    long countByCallStartTimeBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    @Query("SELECT AVG(cm.sentimentScore) FROM CallMetadata cm WHERE cm.sentimentScore IS NOT NULL AND cm.callStartTime BETWEEN :from AND :to")
    Double avgSentimentScoreBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    @Query("SELECT cm.agentId, COUNT(cm) FROM CallMetadata cm WHERE cm.callStartTime BETWEEN :from AND :to GROUP BY cm.agentId")
    java.util.List<Object[]> countByAgentIdBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    @Query("SELECT cm.segment, COUNT(cm) FROM CallMetadata cm WHERE cm.callStartTime BETWEEN :from AND :to GROUP BY cm.segment")
    java.util.List<Object[]> countBySegmentBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);
}
