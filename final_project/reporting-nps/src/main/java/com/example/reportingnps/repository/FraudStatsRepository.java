package com.example.reportingnps.repository;

import com.example.reportingnps.entity.FraudStats;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface FraudStatsRepository extends JpaRepository<FraudStats, Long> {

    Optional<FraudStats> findByPhoneAndPatternAndSeverity(String phone, String pattern, String severity);

    @Query("SELECT fs FROM FraudStats fs WHERE fs.agentId = :agentId")
    List<FraudStats> findByAgentId(@Param("agentId") String agentId);

    @Query("SELECT COUNT(fs) FROM FraudStats fs WHERE fs.agentId = :agentId")
    long countByAgentId(@Param("agentId") String agentId);
}
