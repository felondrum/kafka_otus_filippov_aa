# Fraud-Detector Change — Review Report

**Date:** 2026-09-10 (initial review) | 2026-09-10 (final review)
**Change:** `fraud-detector`
**Artifacts reviewed:** proposal.md, design.md, specs/fraud-detector/spec.md, tasks.md
**Cross-reference:** infrastructure, call-processor, reporting-nps, ARCHITECTURE/05-component-diagrams.md

---

## Summary

Fraud-detector change is **well-structured** with clear goals, reasonable technical decisions, and comprehensive task breakdown. All identified inconsistencies were addressed in the first round of fixes.

**Overall assessment: APPROVED** — all blocking and important issues resolved.

---

## Issues by Severity — Final Status

### 🔴 HIGH — Blocking / Logic Errors

#### H-1: Sliding Window vs Hopping Window — Terminology and Spec Mismatch

**Where:** design.md (Decision 2), spec.md (Window expiration scenario), tasks.md (4.4)

**Status: ✅ RESOLVED**

**Changes made:**
- ✅ Spec: `Frequent Calls Detection (Sliding Window)` → `Frequent Calls Detection (Hopping Window)`
- ✅ Spec: "1-minute sliding window" → "1-minute hopping window (10-second advance)"
- ✅ Spec: "Window expiration" scenario updated to hopping window semantics with overlapping windows note
- ✅ Design: Decision 2 renamed to "Hopping Window vs Tumbling Window" with Kafka Streams DSL note
- ✅ Design: Added note explaining sliding vs hopping terminology difference
- ✅ Design: Goals section updated: "Hopping window (1 мин размер, 10 сек advance)"
- ✅ Tasks: Section 4 renamed, task 4.4 uses "hopping window (1 minute size, 10 second advance)"
- ✅ Tasks: Task 4.7 updated to verify overlapping windows behavior
- ✅ Proposal: Updated What Changes with "Hopping window (1 мин, 10 сек advance)"

---

#### H-2: Schema Registry Integration Missing from Tasks

**Where:** tasks.md

**Status: ✅ RESOLVED**

**Changes made:**
- ✅ Added new section 3 "Schema Registry Integration" with 6 tasks (3.1–3.6):
  - 3.1: Add dependencies (confluent-kafka-schema-registry or spring-kafka SchemaRegistrySerializer)
  - 3.2: Configure Schema Registry URL from application.yml
  - 3.3: Register FraudAlert Avro schema on startup (idempotent)
  - 3.4: Configure Kafka Streams producer to use Schema Registry
  - 3.5: Verify fraud alerts serialized with correct Avro schema
  - 3.6: Integration test for Schema Registry
- ✅ Design: Goals updated with "Schema Registry integration для Avro сериализации"
- ✅ Proposal: Updated with "Schema Registry integration для Avro сериализации"
- ✅ Proposal: Dependencies updated with "Schema Registry"

---

#### H-3: Phone Number Format Consistency Not Addressed

**Where:** spec.md (NPS Escalation), design.md (Decision 1), tasks.md (4.2)

**Status: ✅ RESOLVED**

**Changes made:**
- ✅ Spec: New requirement "Phone Number Normalization" with 2 scenarios:
  - "Phone number normalization" — normalizes to E.164 before processing
  - "Normalized phone used for all detection" — grouping/counting/state store use normalized key
- ✅ Spec: NPS Escalation requirement has **Precondition** note referencing Phone Number Normalization
- ✅ Tasks: Task 4.2 "Normalize phone numbers to E.164 format before any processing"
- ✅ Tasks: Task 5.1 "Create Processor that tracks NPS scores per phone number (normalized E.164)"
- ✅ Design: Risks/Trade-offs updated: "phone number normalization to E.164 before processing; contract with call-processor guarantees E.164 format"

---

### 🟡 MEDIUM — Important Gaps

#### M-1: Anomalous Duration Detection Not in Pipeline Diagram

**Where:** design.md, ARCHITECTURE/05-component-diagrams.md

**Status: ✅ RESOLVED**

**Changes made:**
- ✅ ARCHITECTURE/05-component-diagrams.md: Added Anomalous Duration branch to fraud-detector pipeline diagram
  - Shows: Predicate (count > 5) → Anomalous Duration (> 300 sec) → Fraud Alert (ANOMALOUS_DURATION)
- ✅ ARCHITECTURE/05-component-diagrams.md: Components table updated with "Anomalous Duration Detector" and "Phone Normalizer"
- ✅ ARCHITECTURE/05-component-diagrams.md: Streams Configuration updated with schema.registry.url
- ✅ ARCHITECTURE/05-component-diagrams.md: Health Check section added (Actuator)

---

#### M-2: State Store TTL — Per-Phone vs Global Cleanup

**Where:** spec.md (State store TTL cleanup), design.md (Decision 3), tasks.md (7.4, 7.6, 7.7)

**Status: ✅ RESOLVED**

**Changes made:**
- ✅ Spec: "State store TTL cleanup" scenario clarified: "24 hours pass since a phone number's last activity → state store automatically removes the entry"
- ✅ Design: Goals updated: "RocksDB State Store с per-phone TTL 24ч (manual timestamp tracking)"
- ✅ Tasks: Section 7 (State Store) tasks updated:
  - 7.4: "Implement per-phone TTL with timestamp tracking: store (count, lastActivityTimestamp) as value"
  - 7.6: "Verify per-phone TTL cleanup removes stale entries after 24 hours of inactivity"
  - 7.7: "Verify periodic purge task cleans up expired entries (Kafka Streams cleanup.interval.ms)"

---

#### M-3: Exactly-Once as a Spec Requirement

**Where:** spec.md

**Status: ✅ RESOLVED**

**Changes made:**
- ✅ Spec: Removed standalone requirement "Exactly-once processing" (was config detail, not behavioral requirement)
- ✅ Design: Exactly-once remains in Goals and Risks/Trade-offs (appropriate location)
- ✅ Tasks: Section 8 "Exactly-Once Semantics" preserved (8.1–8.3)

---

#### M-4: No Task for FraudAlert Avro Schema Registration

**Where:** tasks.md (Section 2)

**Status: ✅ RESOLVED**

**Changes made:**
- ✅ Tasks: Section 2 restructured:
  - 2.1–2.3: Define FraudAlert Avro schema, pattern enum, severity enum (unchanged)
  - 2.4: "Verify Avro schema compiles with avro-gradle-plugin" (was "verify Schema Registry compatibility")
  - 2.5: "Register FraudAlert schema in Schema Registry with BACKWARD compatibility mode" (NEW)
  - 2.6: "Verify FraudAlert Avro schema is compatible with reporting-nps consumer expectations" (NEW)

---

#### M-5: Health Check Endpoint Missing from Design and Spec

**Where:** design.md (Non-Goals), spec.md, tasks.md (Section 11)

**Status: ✅ RESOLVED**

**Changes made:**
- ✅ Design: Non-Goals updated: "Complex REST API (only minimal health endpoint via Actuator)"
- ✅ Design: Goals updated: "Health check endpoint via Spring Boot Actuator"
- ✅ Tasks: New section 11 "Health Check and Monitoring" with 4 tasks (11.1–11.4):
  - 11.1: Add spring-boot-starter-actuator dependency
  - 11.2: Configure /actuator/health endpoint
  - 11.3: Add liveness and readiness probes
  - 11.4: Verify health endpoint returns 503 when Kafka is down
- ✅ ARCHITECTURE/05-component-diagrams.md: Health Check section added with Actuator details

---

### 🟢 LOW — Minor Improvements

#### L-1: Design "Open Questions: Нет" Is Overconfident

**Where:** design.md

**Status: ✅ RESOLVED**

**Changes made:**
- ✅ Design: Added "Open Questions" section with 4 items:
  - O-1: Schema Registry client library (confluent-kafka vs spring-kafka)
  - O-2: Anomalous duration detection placement (parallel branch vs separate KStream)
  - O-3: Per-key TTL implementation (manual timestamp tracking approach)
  - O-4: Health check depth (Actuator basic vs custom StreamsHealthIndicator)

---

#### L-2: Fraud Alert `count` Field Semantics Ambiguous

**Where:** spec.md (Fraud Alert Format)

**Status: ✅ RESOLVED**

**Changes made:**
- ✅ Spec: "Fraud alert contains required fields" scenario updated:
  - Added: "`count` semantics: for FREQUENT_CALLS = total calls in window; for NPS_ESCALATION = total negative NPS in 24h; for ANOMALOUS_DURATION = 1 (single-call pattern)"

---

#### L-3: Tasks — Missing Schema Compatibility Test

**Where:** tasks.md

**Status: ✅ RESOLVED**

**Changes made:**
- ✅ Tasks: Task 2.6 added: "Verify FraudAlert Avro schema is compatible with reporting-nps consumer expectations (cross-change check)"

---

#### L-4: Spec — Missing Scenario for Short Call Filter Interaction

**Where:** spec.md (Short Call Filter)

**Status: ✅ RESOLVED**

**Changes made:**
- ✅ Spec: New scenario "Filtered calls produce no alerts" added:
  - "WHEN a call event with duration < 5 seconds is received → no fraud alert of any pattern (FREQUENT_CALLS, NPS_ESCALATION, ANOMALOUS_DURATION) is produced"

---

#### L-5: Tasks — Task 3.3 Window Type Not Specified

**Where:** tasks.md (4.4)

**Status: ✅ RESOLVED**

**Changes made:**
- ✅ Tasks: Section 4 renamed to "Kafka Streams Topology — DSL (Frequent Calls)"
- ✅ Tasks: Task 4.4: "GroupBy phone number and apply hopping window (1 minute size, 10 second advance)"
- ✅ Tasks: Task 4.7: "Verify hopping window logic with Embedded Kafka test (6 calls in 30 sec → alert; verify overlapping windows)"
- ✅ Tasks: Task 9.1: "Write unit tests for hopping window logic (frequent calls detection)"

---

### 🔵 NEW — Additional Finding (Final Review)

#### N-1: Task Numbering Inconsistency in tasks.md

**Where:** tasks.md

**Status: ✅ RESOLVED**

**Problem:**
During final review, discovered that section numbering was inconsistent after the Schema Registry section was inserted:
- Section 4 had two subsections: "DSL (Frequent Calls)" and "Processor API (NPS Escalation)" — both numbered as section 4
- Section 8 "Exactly-Once" was correct, but Unit Tests section used task numbers 8.x instead of 9.x
- Integration Tests section used task numbers 9.x instead of 10.x
- Docker and Deployment section used task numbers 10.x instead of 12.x

**Why wrong:**
Task numbering must be sequential and match section numbers. Implementer would be confused by duplicate section numbers and mismatched task prefixes.

**Fix:**
- ✅ Section 4: "DSL (Frequent Calls)" — tasks 4.1–4.7
- ✅ Section 5: "Processor API (NPS Escalation)" — tasks 5.1–5.7
- ✅ Section 6: "Anomalous Duration Detection" — tasks 6.1–6.4
- ✅ Section 7: "State Store (RocksDB)" — tasks 7.1–7.7
- ✅ Section 8: "Exactly-Once Semantics" — tasks 8.1–8.3
- ✅ Section 9: "Unit Tests" — tasks 9.1–9.6
- ✅ Section 10: "Integration Tests" — tasks 10.1–10.7
- ✅ Section 11: "Health Check and Monitoring" — tasks 11.1–11.4
- ✅ Section 12: "Docker and Deployment" — tasks 12.1–12.5

---

## Cross-Change Consistency Matrix — Final

| Aspect | fraud-detector | infrastructure | call-processor | reporting-nps | Consistent? |
|--------|---------------|----------------|----------------|---------------|-------------|
| Kafka topic: calls.completed | Consumer | Producer (Avro) | — | — | ✅ |
| Kafka topic: calls.fraud-alerts | Producer (Avro via SR) | Created | — | Consumer | ✅ |
| Phone format | **Normalized to E.164** | — | Validates E.164 | — | ✅ |
| Avro Schema Registry | **Integrated (tasks 3.x)** | Deploys SR | Integrated | — | ✅ |
| Exactly-once | processing.guarantee=exactly_once_v2 | — | enable.idempotence=true, acks=all | — | ✅ |
| RocksDB state store | Local state (per-phone TTL) | — | — | — | ✅ |
| Port 8082 | — | — | 8081 | 8084 | ✅ (no conflict) |
| NPS < 2 threshold | Uses NPS < 2 | — | npsScore 0-10 | Uses NPS for reports | ✅ |
| Fraud alert severity | HIGH/MEDIUM/LOW | — | — | Consumes severity | ✅ |
| Health check | **Actuator (tasks 11.x)** | — | Actuator | Actuator | ✅ |

---

## Risk Summary — Final

| Risk | Severity | Status |
|------|----------|--------|
| H-1: Window type mismatch (sliding vs hopping vs tumbling) | 🔴 HIGH | ✅ RESOLVED |
| H-2: Schema Registry integration missing from tasks | 🔴 HIGH | ✅ RESOLVED |
| H-3: Phone number format consistency not guaranteed | 🔴 HIGH | ✅ RESOLVED |
| M-1: Anomalous duration not in pipeline diagram | 🟡 MEDIUM | ✅ RESOLVED |
| M-2: Per-phone TTL not natively supported by Kafka Streams | 🟡 MEDIUM | ✅ RESOLVED |
| M-3: Exactly-once is config, not spec requirement | 🟡 MEDIUM | ✅ RESOLVED |
| M-4: Schema verification before registration | 🟡 MEDIUM | ✅ RESOLVED |
| M-5: No health check endpoint | 🟡 MEDIUM | ✅ RESOLVED |
| L-1: Overconfident "Open Questions: Нет" | 🟢 LOW | ✅ RESOLVED |
| L-2: `count` field semantics ambiguous | 🟢 LOW | ✅ RESOLVED |
| L-3: No cross-change schema compatibility task | 🟢 LOW | ✅ RESOLVED |
| L-4: Missing filter interaction scenario | 🟢 LOW | ✅ RESOLVED |
| L-5: Task 3.3 uses wrong window terminology | 🟢 LOW | ✅ RESOLVED |
| N-1: Task numbering inconsistency | 🔵 NEW | ✅ RESOLVED |

---

## Artifacts Changed

| File | Changes |
|------|---------|
| `specs/fraud-detector/spec.md` | Hopping window rename, Phone Number Normalization requirement, count semantics, filtered calls scenario, removed Exactly-once requirement |
| `design.md` | Hopping window Decision 2, Open Questions section (O-1–O-4), Goals/Non-Goals updated, Risks updated |
| `tasks.md` | Schema Registry Integration section (3.1–3.6), phone normalization task, per-phone TTL tasks, health check section (11.1–11.4), cross-change schema task, fixed section numbering (1–12) |
| `proposal.md` | Updated What Changes, Capabilities, Dependencies |
| `ARCHITECTURE/05-component-diagrams.md` | Anomalous Duration branch, Phone Normalizer, Health Check section, Streams Configuration updated |

---

## Review Verdict

**Initial Status (2026-09-10):** APPROVE WITH CONDITIONS (13 issues found)
**Final Status (2026-09-10):** **APPROVED** ✅

All 13 identified issues + 1 additional finding resolved. The fraud-detector change is now **internally consistent**, **cross-change consistent**, and **ready for implementation**.

**Remaining Open Questions (non-blocking):**
- O-1: Schema Registry client library — implementation decision
- O-2: Anomalous duration detection placement — implementation decision
- O-3: Per-key TTL implementation — design decision, approach documented
- O-4: Health check depth — MVP: Actuator only, future enhancement
