from flask import Flask, request, jsonify
import requests
import psycopg2
import psycopg2.extras
import os
import time
import uuid
from datetime import datetime

# Prometheus metrics imports for comprehensive observability
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

# OpenTelemetry imports for distributed tracing
# Core API and SDK - fundamental components for creating and managing telemetry data
# Documentation: https://opentelemetry.io/docs/instrumentation/python/manual/
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.resources import Resource

# OTLP Exporter - sends telemetry data to our Tempo backend using OpenTelemetry Protocol
# Documentation: https://opentelemetry.io/docs/reference/specification/protocol/otlp/
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Auto-instrumentation for common libraries - automatically traces HTTP requests, database calls, etc.
# Documentation: https://opentelemetry.io/docs/instrumentation/python/automatic/
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor

# Semantic conventions for consistent attribute naming across services
# Documentation: https://opentelemetry.io/docs/reference/specification/trace/semantic_conventions/
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import Status, StatusCode

app = Flask(__name__)

# Configuration
SERVICE_NAME = os.getenv('SERVICE_NAME', 'user-service')
NEXT_SERVICE_URL = os.getenv('NEXT_SERVICE_URL', 'http://app2:5000')

# Prometheus metrics definitions
# These metrics provide the quantitative foundation that complements distributed tracing
REQUEST_COUNT = Counter(
    'user_service_requests_total',
    'Total requests processed by user service',
    ['method', 'endpoint', 'status']
)

REQUEST_DURATION = Histogram(
    'user_service_request_duration_seconds',
    'Request duration in seconds',
    ['method', 'endpoint']
)

ACTIVE_USERS = Gauge(
    'user_service_active_users',
    'Number of active users in the system'
)

DATABASE_CONNECTIONS = Gauge(
    'user_service_database_connections',
    'Number of active database connections'
)

# OpenTelemetry configuration
# This section sets up the complete telemetry pipeline following OpenTelemetry best practices

def configure_telemetry():
    """
    Initialize OpenTelemetry tracing for this service with enhanced debugging.

    This function demonstrates the standard pattern for setting up distributed tracing:
    1. Define service resource attributes (what/where is this service)
    2. Create and configure the tracer provider (how to generate traces)
    3. Set up exporters (where to send trace data)
    4. Enable auto-instrumentation (automatic tracing for common libraries)

    Reference: https://opentelemetry.io/docs/instrumentation/python/getting-started/
    """

    print(f"[{SERVICE_NAME}] Starting OpenTelemetry initialization...")

    # Step 1: Define resource attributes that identify this service instance
    # These attributes help observability tools categorize and filter trace data
    # Resource semantic conventions: https://opentelemetry.io/docs/reference/specification/resource/semantic_conventions/
    resource = Resource.create({
        "service.name": SERVICE_NAME,
        "service.version": "1.0.0",
        "deployment.environment": "development",
        # Container-specific attributes help with debugging in containerized environments
        "container.name": os.getenv('HOSTNAME', 'unknown'),
        # Custom business context attributes
        "business.domain": "user-management",
        "team.name": "platform"
    })

    print(f"[{SERVICE_NAME}] Created resource with attributes: {dict(resource.attributes)}")

    # Step 2: Create the tracer provider with our resource definition
    # The TracerProvider is the central component that manages trace creation and export
    trace.set_tracer_provider(TracerProvider(resource=resource))

    # Step 3: Configure the OTLP exporter to send traces to the OpenTelemetry Collector
    # The collector will then handle forwarding to Tempo and any other backends
    # Using environment variable allows easy reconfiguration without code changes
    otlp_endpoint = os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'http://tempo:4317')
    print(f"[{SERVICE_NAME}] Configuring OTLP exporter with endpoint: {otlp_endpoint}")

    otlp_exporter = OTLPSpanExporter(
        endpoint=otlp_endpoint,
        insecure=True,  # Using insecure connection since we're in a private Docker network
        headers={}  # Can add authentication headers here in production environments
    )

    print(f"[{SERVICE_NAME}] OTLP exporter created successfully")

    # Step 4: Set up batch processing for efficient trace export
    # BatchSpanProcessor collects spans and sends them in batches for better performance
    # Documentation: https://opentelemetry.io/docs/reference/specification/trace/sdk/#batching-processor
    span_processor = BatchSpanProcessor(
        otlp_exporter,
        max_queue_size=512,        # Maximum number of spans to queue before dropping
        schedule_delay_millis=1000,  # How long to wait before sending a batch
        max_export_batch_size=128    # Number of spans to include in each export batch
    )

    print(f"[{SERVICE_NAME}] Span processor configured with batch size 128")

    trace.get_tracer_provider().add_span_processor(span_processor)

    print(f"[{SERVICE_NAME}] Tracer provider configured with span processor")

    # Step 5: Enable auto-instrumentation for common libraries
    # This automatically creates spans for HTTP requests, database calls, etc.

    # Flask instrumentation captures incoming HTTP requests as spans
    # Documentation: https://opentelemetry.io/docs/instrumentation/python/automatic/flask/
    FlaskInstrumentor().instrument_app(app)
    print(f"[{SERVICE_NAME}] Flask instrumentation enabled")

    # Requests instrumentation captures outgoing HTTP calls to other services
    # Documentation: https://opentelemetry.io/docs/instrumentation/python/automatic/requests/
    RequestsInstrumentor().instrument()
    print(f"[{SERVICE_NAME}] Requests instrumentation enabled")

    # Psycopg2 instrumentation captures database queries and operations
    # Documentation: https://opentelemetry.io/docs/instrumentation/python/automatic/psycopg2/
    Psycopg2Instrumentor().instrument()
    print(f"[{SERVICE_NAME}] Database instrumentation enabled")

    print(f"[{SERVICE_NAME}] OpenTelemetry initialization completed successfully!")

# Initialize telemetry before setting up the application
configure_telemetry()

# Get a tracer instance for creating custom spans
# This tracer will be used to create spans that represent specific business operations
# Tracer naming conventions: https://opentelemetry.io/docs/reference/specification/trace/api/#get-a-tracer
tracer = trace.get_tracer(__name__)

# Database configuration - same as audit service but we'll only touch users table
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'port': os.getenv('DB_PORT', 5432),
    'database': os.getenv('DB_NAME', 'microservices_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'password123')
}

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
    return jsonify({
        "service": SERVICE_NAME,
        "endpoints": [
            "/health",
            "/process",
            "/users",
            "/users/<user_id>"
        ]
    })

@app.route('/process', methods=['POST'])
def process_request():
    """
    Main endpoint that triggers the full microservices chain with comprehensive tracing.

    This method demonstrates several advanced tracing concepts:
    - Creating custom spans for business operations
    - Adding contextual attributes to spans
    - Handling errors and recording them in traces
    - Propagating trace context to downstream services

    Reference: https://opentelemetry.io/docs/instrumentation/python/manual/#creating-spans
    """

    # Start a custom span for the entire user processing operation
    # This span will be the parent for all sub-operations in this service
    with tracer.start_as_current_span("user_service.process_request") as span:
        start_time = time.time()
        trace_id = str(uuid.uuid4())

        # Add business context attributes to the span
        # These attributes help with filtering and analysis in observability tools
        # Semantic conventions reference: https://opentelemetry.io/docs/reference/specification/trace/semantic_conventions/
        span.set_attributes({
            "service.operation": "process_request",
            "business.trace_id": trace_id,
            "user.service.version": "1.0.0"
        })

        # Extract or create user data
        request_data = request.get_json() or {}
        user_id = request_data.get('user_id')
        user_name = request_data.get('user_name')
        user_email = request_data.get('email')

        # Add request context to the span
        span.set_attributes({
            "user.id": str(user_id) if user_id else "unknown",
            "user.name": user_name or "unknown",
            "user.email": user_email or "unknown",
            "request.size": len(str(request_data))
        })

        try:
            # Create a child span specifically for database operations
            # This shows how to break down complex operations into meaningful sub-operations
            with tracer.start_as_current_span("user_service.database_lookup") as db_span:

                # Add database-specific attributes
                db_span.set_attributes({
                    "db.system": "postgresql",
                    "db.name": "microservices_db",
                    "db.operation": "user_lookup_and_update"
                })

                conn = get_db_connection()
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                # If user_id provided, look up the user
                if user_id:
                    db_span.add_event("Looking up user by ID", {"user.id": user_id})
                    cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
                    user_record = cursor.fetchone()

                    if user_record:
                        # Use database record as source of truth
                        user_name = user_record['name']
                        user_email = user_record['email']

                        db_span.add_event("User found, updating statistics")
                        # Update usage statistics - this is a common pattern in microservices
                        cursor.execute("""
                                       UPDATE users
                                       SET total_requests = total_requests + 1,
                                           last_request_at = CURRENT_TIMESTAMP
                                       WHERE id = %s
                                       """, (user_id,))

                        db_span.set_attributes({
                            "user.found": True,
                            "user.total_requests": user_record.get('total_requests', 0) + 1
                        })

                    else:
                        # User not found - record this as an error condition
                        db_span.set_status(Status(StatusCode.ERROR, "User not found"))
                        db_span.set_attributes({"user.found": False, "error.type": "user_not_found"})
                        cursor.close()
                        conn.close()
                        span.record_exception(ValueError(f"User with ID {user_id} not found"))
                        return jsonify({
                            "status": "error",
                            "trace_id": trace_id,
                            "message": f"User with ID {user_id} not found"
                        }), 404

                # If we have name and email but no ID, try to find or create user
                elif user_name and user_email:
                    db_span.add_event("Looking up user by email", {"user.email": user_email})
                    # First try to find existing user by email
                    cursor.execute("SELECT * FROM users WHERE email = %s", (user_email,))
                    user_record = cursor.fetchone()

                    if user_record:
                        # Found existing user, use their data
                        user_id = user_record['id']
                        user_name = user_record['name']

                        db_span.add_event("Existing user found, updating statistics")
                        # Update usage statistics
                        cursor.execute("""
                                       UPDATE users
                                       SET total_requests = total_requests + 1,
                                           last_request_at = CURRENT_TIMESTAMP
                                       WHERE id = %s
                                       """, (user_id,))

                        db_span.set_attributes({
                            "user.found": True,
                            "user.operation": "update_existing"
                        })

                    else:
                        # Create new user on-the-fly
                        db_span.add_event("Creating new user", {"user.email": user_email})
                        cursor.execute("""
                                       INSERT INTO users (name, email, total_requests, last_request_at)
                                       VALUES (%s, %s, 1, CURRENT_TIMESTAMP)
                                           RETURNING id
                                       """, (user_name, user_email))
                        user_id = cursor.fetchone()['id']

                        db_span.set_attributes({
                            "user.found": False,
                            "user.operation": "create_new",
                            "user.id": str(user_id)
                        })

                else:
                    # Invalid request - missing required data
                    db_span.set_status(Status(StatusCode.ERROR, "Missing required user data"))
                    span.record_exception(ValueError("Must provide either user_id or both user_name and email"))
                    cursor.close()
                    conn.close()
                    return jsonify({
                        "status": "error",
                        "trace_id": trace_id,
                        "message": "Must provide either user_id or both user_name and email"
                    }), 400

                conn.commit()
                cursor.close()
                conn.close()

                # Record successful database operation completion
                db_span.set_status(Status(StatusCode.OK))
                db_span.set_attributes({
                    "db.operation.success": True,
                    "user.final_id": str(user_id)
                })

            # Create a span for the business logic processing
            with tracer.start_as_current_span("user_service.business_processing") as business_span:
                business_span.set_attributes({
                    "processing.type": "user_validation",
                    "processing.duration_target": "100ms"
                })

                # Simulate user validation/processing time
                business_span.add_event("Starting user validation")
                time.sleep(0.1)
                business_span.add_event("User validation completed")

                # Prepare enriched data for next service
                payload = {
                    "trace_id": trace_id,
                    "user_id": user_id,
                    "user_name": user_name,
                    "user_email": user_email,
                    "timestamp": datetime.utcnow().isoformat(),
                    "service_chain": [SERVICE_NAME]
                }

                business_span.set_attributes({
                    "payload.size": len(str(payload)),
                    "next_service.url": NEXT_SERVICE_URL
                })

            # Create a span for the downstream service call
            # Note: The requests instrumentation will automatically create child spans for the HTTP call
            with tracer.start_as_current_span("user_service.call_downstream") as downstream_span:
                downstream_span.set_attributes({
                    "http.target_service": "order-service",
                    "http.method": "POST",
                    "http.url": f"{NEXT_SERVICE_URL}/process"
                })

                # Call next service in the chain
                # The requests instrumentation automatically propagates trace context
                response = requests.post(f"{NEXT_SERVICE_URL}/process",
                                         json=payload,
                                         timeout=10)

                # Add response details to the span
                downstream_span.set_attributes({
                    "http.status_code": response.status_code,
                    "http.response_size": len(response.text) if response.text else 0
                })

                processing_time = int((time.time() - start_time) * 1000)

                if response.status_code == 200:
                    result = response.json()

                    # Safely handle the service_chain - it might not exist in the response
                    if "chain_result" in result and "service_chain" in result["chain_result"]:
                        final_chain = result["chain_result"]["service_chain"]
                    elif "service_chain" in result:
                        final_chain = result["service_chain"]
                    else:
                        final_chain = [SERVICE_NAME]  # Fallback if chain info is missing

                    # Add ourselves to the beginning since we initiated the chain
                    final_chain.insert(0, SERVICE_NAME)

                    # Record successful processing in the main span
                    span.set_attributes({
                        "request.success": True,
                        "request.processing_time_ms": processing_time,
                        "service_chain.length": len(final_chain)
                    })

                    span.set_status(Status(StatusCode.OK))

                    return jsonify({
                        "status": "success",
                        "trace_id": trace_id,
                        "message": f"Request processed through {SERVICE_NAME}",
                        "service_chain": final_chain,
                        "user_info": {
                            "id": user_id,
                            "name": user_name,
                            "email": user_email
                        },
                        "data": result,
                        "processing_time_ms": processing_time
                    })
                else:
                    # Downstream service returned an error
                    downstream_span.set_status(Status(StatusCode.ERROR, f"Downstream service error: {response.status_code}"))
                    span.set_status(Status(StatusCode.ERROR, f"Chain processing failed"))
                    span.set_attributes({
                        "request.success": False,
                        "error.downstream_status": response.status_code
                    })

                    return jsonify({
                        "status": "error",
                        "trace_id": trace_id,
                        "message": f"Error calling next service: {response.status_code}",
                        "processing_time_ms": processing_time
                    }), 500

        except Exception as e:
            # Record any unexpected errors in the span
            processing_time = int((time.time() - start_time) * 1000)
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, f"Database error: {str(e)}"))
            span.set_attributes({
                "request.success": False,
                "error.type": "database_error",
                "error.message": str(e)
            })

            return jsonify({
                "status": "error",
                "trace_id": trace_id,
                "message": f"Database error: {str(e)}",
                "processing_time_ms": processing_time
            }), 500((time.time() - start_time) * 1000)
        return jsonify({
            "status": "error",
            "trace_id": trace_id,
            "message": f"Database error: {str(e)}",
            "processing_time_ms": processing_time
        }), 500

@app.route('/users', methods=['GET'])
def get_users():
    """Get all users with distributed tracing instrumentation"""
    with tracer.start_as_current_span("user_service.get_all_users") as span:
        span.set_attributes({
            "service.operation": "get_users",
            "db.operation": "select_all_users"
        })

        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Get all users with their metadata
            cursor.execute("""
                           SELECT id, name, email, created_at, total_requests, last_request_at
                           FROM users
                           ORDER BY created_at DESC
                           """)

            users = cursor.fetchall()
            cursor.close()
            conn.close()

            span.set_attributes({
                "users.count": len(users),
                "db.operation.success": True
            })
            span.set_status(Status(StatusCode.OK))

            return jsonify({
                "service": SERVICE_NAME,
                "total_users": len(users),
                "users": [dict(user) for user in users]
            })

        except Exception as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            return jsonify({
                "service": SERVICE_NAME,
                "error": f"Database error: {str(e)}"
            }), 500

@app.route('/users/<int:user_id>', methods=['GET'])
def get_user(user_id):
    """Get specific user by ID with tracing"""
    with tracer.start_as_current_span("user_service.get_user_by_id") as span:
        span.set_attributes({
            "service.operation": "get_user_by_id",
            "user.id": str(user_id),
            "db.operation": "select_user_by_id"
        })

        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            cursor.execute("""
                           SELECT id, name, email, created_at, total_requests, last_request_at
                           FROM users
                           WHERE id = %s
                           """, (user_id,))

            user = cursor.fetchone()
            cursor.close()
            conn.close()

            if user:
                span.set_attributes({
                    "user.found": True,
                    "user.name": user['name']
                })
                span.set_status(Status(StatusCode.OK))

                return jsonify({
                    "service": SERVICE_NAME,
                    "user": dict(user)
                })
            else:
                span.set_attributes({"user.found": False})
                span.set_status(Status(StatusCode.ERROR, "User not found"))

                return jsonify({
                    "service": SERVICE_NAME,
                    "error": f"User with ID {user_id} not found"
                }), 404

        except Exception as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            return jsonify({
                "service": SERVICE_NAME,
                "error": f"Database error: {str(e)}"
            }), 500

@app.route('/users', methods=['POST'])
def create_user():
    """Create a new user with comprehensive tracing"""
    with tracer.start_as_current_span("user_service.create_user") as span:
        try:
            data = request.get_json()
            span.set_attributes({
                "service.operation": "create_user",
                "user.name": data.get('name', 'unknown') if data else 'unknown',
                "user.email": data.get('email', 'unknown') if data else 'unknown'
            })

            if not data or 'name' not in data or 'email' not in data:
                span.set_status(Status(StatusCode.ERROR, "Missing required fields"))
                return jsonify({
                    "service": SERVICE_NAME,
                    "error": "Missing required fields: name and email"
                }), 400

            conn = get_db_connection()
            cursor = conn.cursor()

            # Insert new user with conflict handling
            insert_query = """
                           INSERT INTO users (name, email)
                           VALUES (%s, %s)
                               RETURNING id, name, email, created_at \
                           """

            cursor.execute(insert_query, (data['name'], data['email']))
            new_user = cursor.fetchone()

            conn.commit()
            cursor.close()
            conn.close()

            span.set_attributes({
                "user.created_id": str(new_user[0]),
                "db.operation.success": True
            })
            span.set_status(Status(StatusCode.OK))

            return jsonify({
                "service": SERVICE_NAME,
                "status": "created",
                "user": {
                    "id": new_user[0],
                    "name": new_user[1],
                    "email": new_user[2],
                    "created_at": new_user[3].isoformat()
                }
            }), 201

        except psycopg2.IntegrityError as e:
            span.set_attributes({"error.type": "integrity_violation"})
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, "Email already exists"))
            return jsonify({
                "service": SERVICE_NAME,
                "error": "Email already exists"
            }), 409
        except Exception as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            return jsonify({
                "service": SERVICE_NAME,
                "error": f"Database error: {str(e)}"
            }), 500

@app.route('/metrics')
def metrics():
    """Prometheus metrics endpoint - provides quantitative observability data"""
    # This endpoint is automatically discovered by Prometheus for metrics scraping
    # No custom tracing needed as this is operational, not business logic

    # Update active users gauge with current database count
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        ACTIVE_USERS.set(user_count)
        DATABASE_CONNECTIONS.set(1)  # Simplified - in production you'd track connection pool metrics
    except Exception:
        DATABASE_CONNECTIONS.set(0)

    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

if __name__ == '__main__':
    print(f"Starting {SERVICE_NAME} on port 5000")
    print(f"Metrics available at http://localhost:5000/metrics")
    print(f"Traces sent to: {os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'http://tempo:4317')}")
    app.run(host='0.0.0.0', port=5000, debug=True)
