# Microservices Observability Stack

A practical demonstration of distributed tracing and observability in a microservices architecture using OpenTelemetry, Grafana Tempo, and modern monitoring tools.

## Architecture Overview

This project implements a three-tier microservices chain that processes user requests through multiple services, demonstrating how distributed tracing works in real-world scenarios:

```
API Gateway (Nginx) → User Service → Order Service → Audit Service
                                ↓
                         PostgreSQL Database
```

### Services

- **User Service** (`app1`) - Handles user authentication and profile management
- **Order Service** (`app2`) - Processes orders and calculates pricing  
- **Audit Service** (`app3`) - Logs transactions and maintains audit trails
- **API Gateway** - Nginx reverse proxy with OpenTelemetry instrumentation
- **PostgreSQL** - Shared database for user and audit data

### Observability Stack

- **OpenTelemetry Collector** - Centralized telemetry data collection and processing
- **Grafana Tempo** - Distributed tracing backend for trace storage and querying
- **Prometheus** - Metrics collection and time-series storage
- **Grafana** - Unified dashboard for traces, metrics, and service visualization

## Quick Start

### Prerequisites

- Docker and Docker Compose
- 8GB+ available RAM (recommended)
- Ports 3000, 5432, 8080, 8888, 9090 available

### Running the Stack

1. Clone and start all services:
```bash
docker-compose up -d
```

2. Wait for services to initialize (health checks will ensure proper startup):
```bash
docker-compose ps
```

3. Test the service chain:
```bash
curl -X POST http://localhost:8080/api/v1/process \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1001, "action": "test_trace"}'
```

### Accessing the Dashboards

- **Grafana UI**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **API Gateway**: http://localhost:8080
- **OpenTelemetry Collector Metrics**: http://localhost:8888/metrics

## Exploring Distributed Traces

### Generating Sample Traffic

Create a complete trace through all services:

```bash
# Simple request
curl -X POST http://localhost:8080/api/v1/process \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1001,
    "user_name": "John Doe",
    "user_email": "john@example.com",
    "action": "purchase"
  }'

# Generate multiple traces for analysis
for i in {1..10}; do
  curl -X POST http://localhost:8080/api/v1/process \
    -H "Content-Type: application/json" \
    -d "{\"user_id\": $((1000 + i)), \"action\": \"test_$i\"}"
  sleep 1
done
```

### Viewing Traces in Grafana

1. Navigate to Grafana at http://localhost:3000
2. Go to Explore → Select Tempo as data source
3. Use the trace ID from API responses to search specific traces
4. Or use service filters: `{service.name="user-service"}`

### Understanding Trace Data

Each trace shows:
- **Service-to-service communication** with automatic context propagation
- **Database operations** with query details and performance metrics
- **Business logic spans** with custom attributes (user info, order details)
- **Error handling** with exception details and span status
- **Processing times** across the entire request chain

## Service Details

### User Service (Port 5001)

Handles user operations and initiates the service chain:

- `POST /process` - Main endpoint that starts distributed processing
- `GET /users` - List users from database
- `GET /users/{id}` - Get specific user details
- `GET /health` - Service health check

Key features:
- PostgreSQL integration with automatic query tracing
- User validation and profile enrichment
- Prometheus metrics for request rates and latencies

### Order Service (Port 5002)

Processes orders with detailed business context:

- `POST /process` - Continues chain from User Service
- `GET /orders` - List orders
- `GET /orders/{id}` - Get specific order
- `GET /health` - Service health check

Demonstrates:
- Complex business logic tracing (order creation, pricing)
- Item-level span attributes
- Error propagation across service boundaries

### Audit Service (Port 5003)

Final service in chain, persists audit logs:

- `POST /process` - Completes the processing chain
- `GET /audit` - View audit trail
- `GET /health` - Service health check

Features:
- Database transaction tracing
- Audit log creation with full context
- Chain completion with summary data

## Development and Customization

### Adding Custom Spans

To add custom business logic tracing:

```python
from opentelemetry import trace

tracer = trace.get_tracer(__name__)

with tracer.start_as_current_span("custom_operation") as span:
    span.set_attributes({
        "business.operation": "data_processing",
        "custom.attribute": "value"
    })
    # Your business logic here
    span.add_event("Processing completed")
```

### Modifying Service Chain

To add a new service to the chain:

1. Create a new Flask application following the existing pattern
2. Update `docker-compose.yaml` with the new service definition
3. Modify the `NEXT_SERVICE_URL` environment variable in the previous service
4. Ensure OpenTelemetry instrumentation is configured

### Database Schema

The PostgreSQL database includes:
- `users` table - User profiles and authentication data
- `audit_logs` table - Transaction and processing audit trail

### Custom Attributes and Events

The services demonstrate rich context propagation:

- **User context**: ID, name, email propagated through entire chain
- **Business context**: Order details, pricing, processing times
- **Technical context**: Service versions, container info, database queries
- **Error context**: Exception details, recovery attempts, fallback data

## Monitoring and Alerts

### Key Metrics to Monitor

- **Request latency** across service boundaries
- **Error rates** by service and operation
- **Database connection** health and query performance
- **Trace sampling** rates and data volume

### Grafana Dashboard

The included dashboard provides:
- Service dependency mapping
- Request flow visualization
- Error rate trending
- Performance percentiles
- Database query analysis

## Troubleshooting

### Common Issues

**Services not starting**: Check Docker logs and ensure all ports are available
```bash
docker-compose logs <service-name>
```

**No traces appearing**: Verify OpenTelemetry Collector connectivity
```bash
curl http://localhost:8888/metrics | grep otelcol_receiver
```

**Database connection errors**: Ensure PostgreSQL is fully initialized
```bash
docker-compose logs postgres
```

### Debugging Traces

Use these Tempo queries in Grafana:
- All traces: `{}`
- Specific service: `{service.name="user-service"}`
- Error traces: `{status=error}`
- Slow requests: `{duration>1s}`

## Performance Considerations

- **Trace sampling**: Currently set to 100% for development; reduce for production
- **Collector batching**: Configured for balanced latency and throughput
- **Retention**: Tempo stores traces for 7 days; Prometheus metrics for 15 days
- **Resource usage**: Each service allocated modest resources suitable for development

This setup provides a solid foundation for understanding distributed tracing concepts and can be extended for production-like scenarios with additional services, custom instrumentation, and advanced observability features.
