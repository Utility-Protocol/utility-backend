# Solution Architecture: Structured Logging with OpenTelemetry Semantic Conventions

This document defines the structured logging and trace context integration architecture for the `utility-backend` services.

## Overview

Structured Logging ensures that all application logs are emitted as single-line JSON records. These records are enriched with:
1. Standard OpenTelemetry (OTel) Semantic Conventions (e.g. `service.name`, `service.version`, `service.environment`).
2. Distributed Tracing information (`trace_id` and `span_id`), which maps active tracing spans back to individual log lines.
3. Spatial Baggage attributes propagated across service boundaries (e.g., `region`, `substation.id`, `grid.segment`).

## Target JSON Format

Each log entry is serialized as a JSON object containing:

```json
{
  "timestamp": "2023-10-27T10:15:30.123456789Z",
  "level": "INFO",
  "severity_number": 9,
  "service.name": "utility-backend",
  "service.version": "0.1.0",
  "service.environment": "production",
  "target": "utility_backend::api::middleware",
  "body": "rate limit exceeded",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "attributes": {
    "source": "127.0.0.1",
    "region": "north-east",
    "substation_id": "SUB-42",
    "grid_segment": "grid-a",
    "code.filepath": "src/api/middleware.rs",
    "code.lineno": 250
  }
}
```

## Field Mappings & Semantic Conventions

### Severity Level Mapping

The standard `tracing` levels are mapped to standard OpenTelemetry severity text and number conventions:

| Tracing Level | OTel Severity Text (`level`) | OTel Severity Number (`severity_number`) |
|---------------|-----------------------------|------------------------------------------|
| `TRACE`       | `TRACE`                     | 1                                        |
| `DEBUG`       | `DEBUG`                     | 5                                        |
| `INFO`        | `INFO`                      | 9                                        |
| `WARN`        | `WARN`                      | 13                                       |
| `ERROR`       | `ERROR`                     | 17                                       |

### Service Attributes

- **`service.name`**: Standard OTel resource attribute identifying the service. Defaults to `utility-backend`. Can be overridden with the `OTEL_SERVICE_NAME` environment variable.
- **`service.version`**: The version of the service compiled dynamically from `CARGO_PKG_VERSION`.
- **`service.environment`**: The environment of the service (e.g., `production`, `development`, `test`). Defaults to `production` and can be overridden with the `APP_ENV` environment variable.

### Trace/Span Context Mapping

- **`trace_id`**: 32-character hex-encoded string of the active OpenTelemetry TraceId.
- **`span_id`**: 16-character hex-encoded string of the active OpenTelemetry SpanId.

### Spatial Baggage & Custom Attributes

- Any baggage fields propagated in the OpenTelemetry context (e.g. `region`, `substation_id`, `grid_segment`) are dynamically extracted from the thread-local context and inserted under `attributes`.
- Custom fields attached to events (such as `source` from the rate limiter) are captured using a custom `tracing::field::Visit` visitor and placed under `attributes`.
- Source code locations (`code.filepath` and `code.lineno`) are automatically injected into `attributes`.
