## Context

The project has an event-driven architecture with Kafka as the central messaging backbone. The call-processor service exposes a REST API (`POST /api/calls`) that accepts call events and produces Kafka messages. The fraud-detector is a Kafka Streams application that consumes from `calls.completed` and applies three detection rules.

Currently, there is no service to generate synthetic call events for testing. Manual testing via curl or external tools lacks the burst patterns and fraud-scenario distribution needed to exercise the fraud-detector rules.

## Goals / Non-Goals

**Goals:**
- Generate synthetic call events at a configurable rate with burst patterns
- Support three fraud scenarios: ANOMALOUS_DURATION, FREQUENT_CALLS, NPS_ESCALATION
- Provide REST API with Swagger UI for manual control
- Integrate into existing docker-compose infrastructure
- Track generation progress (completed, failed, remaining)

**Non-Goals:**
- Direct Kafka integration (the service uses HTTP API only)
- Persistent storage of generated calls
- Call-processor retries or response handling (fire-and-forget)
- Metrics/export to Prometheus (future enhancement)
- Authentication/authorization for the load simulator API

## Decisions

### Decision 1: HTTP fire-and-forget vs direct Kafka

**Choice:** HTTP POST to call-processor's `/api/calls` endpoint (fire-and-forget).

**Rationale:**
- Simpler integration — no need to configure Kafka clients, JAAS auth, schema registries
- Matches how an external telephony system would interact (via HTTP webhook)
- Call-processor already has this endpoint and produces to Kafka
- Easier to test and debug

**Alternatives considered:**
- Direct Kafka production — more realistic but adds Kafka client dependency, SASL config, schema handling
- gRPC — not used in existing services, adds complexity

### Decision 2: Burst pattern with jitter

**Choice:** Configurable burst size + base interval with ±30% random jitter.

**Rationale:**
- Burst pattern creates realistic traffic spikes (real telephony has call bursts)
- Jitter prevents synchronized patterns that could mask timing bugs
- Configurable parameters allow tuning for different test scenarios

**Alternatives considered:**
- Uniform distribution — too simple, doesn't exercise burst-related code paths
- Fixed intervals — doesn't test timing edge cases

### Decision 3: Thread pool for concurrent HTTP calls

**Choice:** Java `ExecutorService` with fixed thread pool (configurable size, default 10).

**Rationale:**
- Fire-and-forget requires async execution — cannot use Spring's `@Async` with direct thread management
- Fixed thread pool prevents resource exhaustion under high load
- Simpler than reactive stack (WebFlux) for this use case

**Alternatives considered:**
- Reactive WebFlux — more efficient for high concurrency but adds complexity
- Spring `@Async` — less control over thread pool lifecycle

### Decision 4: Fraud scenario distribution

**Choice:** Fixed proportional distribution (85/5/5/5) with configurable fraudPercent.

**Rationale:**
- Simple and predictable — test results are reproducible
- fraudPercent controls total fraud ratio; remaining split evenly among the three patterns
- Easy to understand and configure

**Alternatives considered:**
- Per-pattern configurable percentages — more flexible but more complex API
- Random distribution per call — less predictable, harder to verify test results

### Decision 5: Instant stop vs graceful shutdown

**Choice:** Instant stop — all planned calls are cancelled immediately.

**Rationale:**
- Load testing often needs abrupt stops to test system behavior under stress
- Simpler to implement — no in-flight call tracking
- Users can always run a new generation with fewer calls if they need partial completion

**Alternatives considered:**
- Graceful shutdown — wait for in-flight calls to complete; more complex state tracking

### Decision 6: Port 8087

**Choice:** Use port 8087 for the load-simulator service.

**Rationale:**
- Port 8085 is already used by Schema Registry
- Port 8087 is not in use by any existing service
- Sequential numbering (8081-8088) keeps port assignments organized

## Risks / Trade-offs

| Risk | Mitigation |
|------|-----------|
| High load could overwhelm call-processor | Configurable burstSize and duration allow rate limiting; document recommended max rates |
| Fire-and-forget loses failed calls | Track and report failedCalls count; users can replay by restarting generation |
| Thread pool exhaustion under high load | Fixed thread pool with configurable size; document max recommended calls per second |
| Phone number collisions in FREQUENT_CALLS scenario | Use a dedicated pool of 3-5 phone numbers for fraud scenarios to ensure collisions |
| NPS_ESCALATION requires 3+ calls with same phone in 24h | Generate 3+ sequential calls with same phone and NPS < 2; within a burst this is guaranteed |
| No authentication on load simulator API | Document that this is a test-only service; can add auth later if needed |

## Open Questions

None — all key design decisions have been resolved.
