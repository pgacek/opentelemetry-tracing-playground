from flask import Flask, request, jsonify
import psycopg2
import psycopg2.extras
import os
import time
import json
from datetime import datetime

# OpenTelemetry imports with database instrumentation
# Documentation: https://opentelemetry.io/docs/instrumentation/python/manual/
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import Status, StatusCode

app = Flask(__name__)

# Configuration
SERVICE_NAME = os.getenv('SERVICE_NAME', 'audit-service')
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'port': os.getenv('DB_PORT', 5432),
    'database': os.getenv('DB_NAME', 'microservices_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'password123')
}

def configure_telemetry():
    """
    Configure OpenTelemetry for the audit service.

    This service demonstrates database instrumentation alongside HTTP tracing.
    The database spans will automatically be created by the psycopg2 instrumentation,
    showing SQL queries, connection details, and execution times.

    Reference: https://opentelemetry.io/docs/instrumentation/python/automatic/psycopg2/
    """

    print(f"[{SERVICE_NAME}] Starting OpenTelemetry initialization...")

    resource = Resource.create({
        "service.name": SERVICE_NAME,
        "service.version": "1.0.0",
        "deployment.environment": "development",
        "container.name": os.getenv('HOSTNAME', 'unknown'),
        # Business context for audit/compliance domain
        "business.domain": "audit-compliance",
        "team.name": "platform",
        # Database context
        "db.system": "postgresql",
        "db.name": DB_CONFIG['database']
    })

    print(f"[{SERVICE_NAME}] Created resource with attributes: {dict(resource.attributes)}")

    trace.set_tracer_provider(TracerProvider(resource=resource))

    # Configure OTLP exporter to point to the OpenTelemetry Collector
    otlp_endpoint = os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'http://tempo:4317')
    print(f"[{SERVICE_NAME}] Configuring OTLP exporter with endpoint: {otlp_endpoint}")

    # Configure OTLP exporter
    otlp_exporter = OTLPSpanExporter(
        endpoint=otlp_endpoint,
        insecure=True,
        headers={}
    )

    print(f"[{SERVICE_NAME}] OTLP exporter created successfully")

    span_processor = BatchSpanProcessor(
        otlp_exporter,
        max_queue_size=512,
        schedule_delay_millis=1000,
        max_export_batch_size=128
    )

    print(f"[{SERVICE_NAME}] Span processor configured successfully")

    trace.get_tracer_provider().add_span_processor(span_processor)

    print(f"[{SERVICE_NAME}] Tracer provider configured with span processor")

    # Enable auto-instrumentation
    FlaskInstrumentor().instrument_app(app)
    print(f"[{SERVICE_NAME}] Flask instrumentation enabled")

    # Enable PostgreSQL instrumentation - this will automatically trace all database operations
    # Documentation: https://opentelemetry.io/docs/instrumentation/python/automatic/psycopg2/
    Psycopg2Instrumentor().instrument(
        enable_commenter=True,  # Add trace context to SQL comments for correlation
        commenter_options={
            "db_driver": True,
            "db_framework": True,
        }
    )
    print(f"[{SERVICE_NAME}] Database instrumentation enabled")

    print(f"[{SERVICE_NAME}] OpenTelemetry initialization completed successfully!")

# Initialize telemetry
configure_telemetry()
tracer = trace.get_tracer(__name__)

def get_db_connection():
    """Create database connection with retry logic"""
    max_retries = 5
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            return conn
        except psycopg2.OperationalError as e:
            if i < max_retries - 1:
                print(f"Database connection attempt {i+1} failed, retrying in 2 seconds...")
                time.sleep(2)
            else:
                raise e

@app.route('/health')
def health():
    """Health check WITHOUT custom tracing spans to reduce noise"""
    # Flask instrumentation captures basic HTTP metrics automatically
    # Database queries will still be traced, but we don't add custom business logic spans
    try:
        # Test database connectivity as part of health check
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        return jsonify({
            "status": "healthy",
            "service": SERVICE_NAME,
            "database": "connected",
            "total_users": user_count
        })
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "service": SERVICE_NAME,
            "error": str(e)
        }), 500

@app.route('/')
def home():
    """Service information with tracing"""
    with tracer.start_as_current_span("audit_service.service_info") as span:
        span.set_attributes({
            "service.operation": "get_service_info"
        })
        return jsonify({
            "service": SERVICE_NAME,
            "endpoints": [
                "/health",
                "/process",
                "/audit",
                "/audit/<trace_id>"
            ]
        })

@app.route('/process', methods=['POST'])
def process_request():
    """
    Final service in chain - saves audit trail to database with comprehensive tracing.

    This endpoint demonstrates:
    - How database operations are automatically traced by instrumentation
    - Creating business-focused spans for audit operations
    - Recording the completion of the entire microservices chain
    - Rich contextual attributes for compliance and analysis

    Reference: https://opentelemetry.io/docs/instrumentation/python/automatic/psycopg2/
    """
    with tracer.start_as_current_span("audit_service.process_request") as span:
        start_time = time.time()

        # Get data from previous service (order service)
        # Trace context is automatically propagated
        data = request.get_json() or {}
        trace_id = data.get('trace_id', 'unknown')
        user_id = data.get('user_id')
        order_id = data.get('order_id')
        order_total = data.get('order_total', 0)
        service_chain = data.get('service_chain', [])
        order_items = data.get('order_items', [])

        # Add comprehensive business context to the span
        span.set_attributes({
            "service.operation": "audit_transaction",
            "business.trace_id": trace_id,
            "user.id": str(user_id) if user_id else "unknown",
            "order.id": str(order_id) if order_id else "unknown",
            "order.total": order_total,
            "order.items_count": len(order_items),
            "service_chain.length": len(service_chain),
            "audit.type": "transaction_completion"
        })

        # Add current service to chain
        service_chain.append(SERVICE_NAME)

        try:
            # Create a business-focused span for audit processing
            with tracer.start_as_current_span("audit_service.compliance_processing") as audit_span:
                audit_span.set_attributes({
                    "audit.compliance_check": "transaction_integrity",
                    "audit.required_fields": "user_id,order_id,order_total"
                })

                # Simulate audit processing
                audit_span.add_event("Starting compliance checks")
                time.sleep(0.05)  # Simulate processing time
                audit_span.add_event("Compliance checks completed")

                # Validate required audit data
                if not all([user_id, order_id, order_total]):
                    audit_span.set_status(Status(StatusCode.ERROR, "Missing required audit data"))
                    span.set_status(Status(StatusCode.ERROR, "Audit validation failed"))
                    return jsonify({
                        "status": "error",
                        "trace_id": trace_id,
                        "message": "Missing required audit data"
                    }), 400

            # Create a span for database persistence operations
            # Note: The psycopg2 instrumentation will automatically create child spans for SQL operations
            with tracer.start_as_current_span("audit_service.persist_audit_record") as db_span:
                db_span.set_attributes({
                    "db.operation": "insert_audit_record",
                    "db.table": "request_traces",
                    "audit.record_type": "transaction_trace"
                })

                db_span.add_event("Opening database connection")
                conn = get_db_connection()
                cursor = conn.cursor()

                # Prepare audit record data
                audit_record = {
                    "trace_id": trace_id,
                    "user_id": user_id,
                    "order_id": order_id,
                    "service_chain": service_chain,
                    "order_data": {
                        "total": order_total,
                        "items": order_items,
                        "completion_time": datetime.utcnow().isoformat()
                    },
                    "processing_metadata": {
                        "chain_length": len(service_chain),
                        "final_service": SERVICE_NAME
                    }
                }

                db_span.add_event("Inserting audit record", {
                    "record.trace_id": trace_id,
                    "record.size": len(str(audit_record))
                })

                # Insert audit record - this will be automatically traced by psycopg2 instrumentation
                insert_query = """
                               INSERT INTO request_traces
                               (trace_id, user_id, order_id, service_name, request_data, processing_time_ms)
                               VALUES (%s, %s, %s, %s, %s, %s)
                                   RETURNING id; \
                               """

                processing_time = int((time.time() - start_time) * 1000)

                # The SQL execution will automatically create a child span with query details
                cursor.execute(insert_query, (
                    trace_id,
                    user_id,
                    order_id,
                    SERVICE_NAME,
                    json.dumps(audit_record),
                    processing_time
                ))

                audit_id = cursor.fetchone()[0]

                db_span.add_event("Audit record inserted", {
                    "audit.id": audit_id,
                    "db.rows_affected": 1
                })

                conn.commit()
                cursor.close()
                conn.close()

                db_span.set_attributes({
                    "audit.record_id": str(audit_id),
                    "db.operation.success": True,
                    "db.processing_time_ms": processing_time
                })
                db_span.set_status(Status(StatusCode.OK))

            # Record successful completion of the entire chain
            span.set_attributes({
                "audit.success": True,
                "audit.record_id": str(audit_id),
                "chain.completion_time_ms": processing_time,
                "chain.final_status": "completed",
                "transaction.total_value": order_total
            })
            span.set_status(Status(StatusCode.OK))

            # Add a span event to mark the completion of the entire microservices chain
            span.add_event("Microservices chain completed successfully", {
                "chain.services": service_chain,
                "chain.total_time_ms": processing_time,
                "audit.record_id": audit_id
            })

            return jsonify({
                "status": "success",
                "trace_id": trace_id,
                "message": f"Request chain completed and saved to audit log",
                "audit_id": audit_id,
                "service_chain": service_chain,
                "processing_time_ms": processing_time,
                "chain_summary": {
                    "total_services": len(service_chain),
                    "user_id": user_id,
                    "order_id": order_id,
                    "order_total": order_total,
                    "completion_time": datetime.utcnow().isoformat()
                }
            })

        except Exception as e:
            # Record database or processing errors
            processing_time = int((time.time() - start_time) * 1000)
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, f"Audit processing failed: {str(e)}"))
            span.set_attributes({
                "audit.success": False,
                "error.type": "audit_processing_error",
                "error.message": str(e),
                "chain.partial_completion": True
            })

            return jsonify({
                "status": "error",
                "trace_id": trace_id,
                "message": f"Database error: {str(e)}",
                "processing_time_ms": processing_time
            }), 500

@app.route('/audit', methods=['GET'])
def get_audit_logs():
    """Get all audit logs with comprehensive tracing"""
    with tracer.start_as_current_span("audit_service.get_audit_logs") as span:
        span.set_attributes({
            "service.operation": "get_audit_logs",
            "query.limit": 50,
            "query.order_by": "request_timestamp DESC"
        })

        try:
            # Database query will be automatically traced by psycopg2 instrumentation
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            cursor.execute("""
                           SELECT * FROM request_traces
                           ORDER BY request_timestamp DESC
                               LIMIT 50
                           """)

            logs = cursor.fetchall()
            cursor.close()
            conn.close()

            span.set_attributes({
                "audit.logs_retrieved": len(logs),
                "db.operation.success": True
            })
            span.set_status(Status(StatusCode.OK))

            return jsonify({
                "service": SERVICE_NAME,
                "total_logs": len(logs),
                "logs": [dict(log) for log in logs]
            })

        except Exception as e:
            span.record_exception(e)
            span.set_attributes({
                "audit.logs_retrieved": 0,
                "db.operation.success": False,
                "error.message": str(e)
            })
            span.set_status(Status(StatusCode.ERROR, str(e)))

            return jsonify({
                "status": "error",
                "message": f"Database error: {str(e)}"
            }), 500

@app.route('/audit/<trace_id>', methods=['GET'])
def get_trace_audit(trace_id):
    """Get audit logs for specific trace ID with detailed tracing"""
    with tracer.start_as_current_span("audit_service.get_trace_audit") as span:
        span.set_attributes({
            "service.operation": "get_trace_audit",
            "audit.trace_id": trace_id,
            "query.type": "trace_specific"
        })

        try:
            # Database query will be automatically traced
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            cursor.execute("""
                           SELECT * FROM request_traces
                           WHERE trace_id = %s
                           ORDER BY request_timestamp ASC
                           """, (trace_id,))

            logs = cursor.fetchall()
            cursor.close()
            conn.close()

            if not logs:
                span.set_attributes({
                    "audit.logs_found": False,
                    "audit.logs_count": 0
                })
                span.set_status(Status(StatusCode.ERROR, "Trace not found"))

                return jsonify({
                    "status": "not_found",
                    "message": f"No audit logs found for trace_id: {trace_id}"
                }), 404

            span.set_attributes({
                "audit.logs_found": True,
                "audit.logs_count": len(logs),
                "db.operation.success": True
            })
            span.set_status(Status(StatusCode.OK))

            return jsonify({
                "service": SERVICE_NAME,
                "trace_id": trace_id,
                "logs": [dict(log) for log in logs]
            })

        except Exception as e:
            span.record_exception(e)
            span.set_attributes({
                "audit.logs_found": False,
                "db.operation.success": False,
                "error.message": str(e)
            })
            span.set_status(Status(StatusCode.ERROR, str(e)))

            return jsonify({
                "status": "error",
                "message": f"Database error: {str(e)}"
            }), 500

if __name__ == '__main__':
    print(f"Starting {SERVICE_NAME} on port 5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
