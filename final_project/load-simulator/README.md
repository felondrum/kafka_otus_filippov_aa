# Load Simulator Service

Synthetic load generator for testing the call-platform pipeline under realistic conditions.

## Overview

The load-simulator service generates synthetic call events against the call-processor HTTP API with configurable burst patterns and fraud-scenario distribution. It's designed for load testing and pipeline validation.

## Quick Start

### Docker Compose

```bash
# Start with load-test profile
docker compose --profile full --profile load-test up -d

# Or just the load simulator
docker compose --profile load-test up -d load-simulator
```

### Swagger UI

Open http://localhost:8087/swagger-ui.html in your browser.

## API Endpoints

### Start Load Generation

```bash
POST /api/load/start
```

**Request body:**
```json
{
  "totalCalls": 1000,
  "durationMinutes": 5,
  "burstSize": 20,
  "fraudPercent": 15
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| totalCalls | int | Yes | — | Total number of calls to generate |
| durationMinutes | int | Yes | — | Time span in minutes |
| burstSize | int | No | 20 | Calls per burst |
| fraudPercent | int | No | 15 | Percentage of fraud calls (0-100) |

**Response (202 Accepted):**
```json
{
  "started": true,
  "configuration": {
    "totalCalls": 1000,
    "durationMinutes": 5,
    "burstSize": 20,
    "fraudPercent": 15
  }
}
```

### Stop Load Generation

```bash
POST /api/load/stop
```

**Response (200 OK):**
```json
{
  "stopped": true,
  "completedCalls": 347,
  "remainingCalls": 653
}
```

### Get Status

```bash
GET /api/load/status
```

**Response (200 OK):**
```json
{
  "running": true,
  "totalCalls": 1000,
  "completedCalls": 347,
  "failedCalls": 3,
  "percentage": 34.7
}
```

## Fraud Scenario Distribution

The service distributes calls across four scenarios:

| Scenario | Percentage | Behavior |
|----------|-----------|----------|
| NORMAL | 100 - fraudPercent | Random phone, duration 30-300s, random NPS |
| ANOMALOUS_DURATION | fraudPercent / 3 | Duration 301-600s |
| FREQUENT_CALLS | fraudPercent / 3 | 6-10 calls from same phone in a burst |
| NPS_ESCALATION | fraudPercent / 3 | 3-5 calls from same phone with NPS 0-1 |

### Recommended fraudPercent for Testing

| Goal | fraudPercent | Notes |
|------|-------------|-------|
| Test ANOMALOUS_DURATION only | 5 | 5% anomalous, 95% normal |
| Test FREQUENT_CALLS only | 5 | 5% frequent, 95% normal |
| Test NPS_ESCALATION only | 5 | 5% NPS escalation, 95% normal |
| Test all fraud scenarios | 15 | Default: 5% each |
| Heavy fraud testing | 30 | 10% each scenario |

## Burst Pattern

Calls are generated in configurable bursts with jitter-based randomization:

- **burstSize**: Number of calls per burst (default: 20)
- **Interval**: `durationMinutes * 60000 / (totalCalls / burstSize)` with ±30% jitter
- **Last burst**: Handles remainder when totalCalls % burstSize != 0

### Example

```
1000 calls, 5 minutes, burstSize=20
→ 50 bursts of 20 calls each
→ Base interval: 6000ms (6 seconds)
→ Actual interval: 4200ms - 7800ms (±30% jitter)
```

## Docker

### Build

```bash
docker build -t load-simulator:latest ./load-simulator
```

### Run

```bash
docker run -p 8087:8087 \
  -e CALL_PROCESSOR_URL=http://call-processor:8081 \
  load-simulator:latest
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| SERVER_PORT | 8087 | Service port |
| CALL_PROCESSOR_URL | http://localhost:8081 | Call-processor URL |

## Architecture

```
User (Swagger UI)
    │
    ▼
Load Simulator (:8087)
    │  POST /api/load/start
    ▼
Call Processor (:8081)
    │  POST /api/calls (fire-and-forget)
    ▼
Kafka (calls.completed, calls.metadata)
    ▼
Fraud Detector (Kafka Streams)
```

## Testing

```bash
cd load-simulator
./gradlew test
```

34 unit tests covering:
- Call generation (normal, anomalous, frequent, NPS escalation)
- Fraud scenario classification
- Burst calculation and jitter
- Phone number generation
