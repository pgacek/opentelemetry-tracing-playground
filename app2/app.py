from flask import Flask, request, jsonify
import requests
import os
import time
import random
from datetime import datetime

# OpenTelemetry imports - identical setup across all services for consistency
# Documentation: https://opentelemetry.io/docs/instrumentation/python/manual/
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import Status, StatusCode

app = Flask(__name__)

# Configuration
SERVICE_NAME = os.getenv('SERVICE_NAME', 'order-service')
NEXT_SERVICE_URL = os.getenv('NEXT_SERVICE_URL', 'http://app3:5000')

def configure_telemetry():
    """
    Configure OpenTelemetry for the order service.

    This follows the same pattern as the user service but with order-specific context.
    The resource attributes help distinguish this service's traces in the observability system.
    """
    resource = Resource.create({
        "service.name": SERVICE_NAME,
        "service.version": "1.0.0",
        "deployment.environment": "development",
        "container.name": os.getenv('HOSTNAME', 'unknown'),
        # Business context specific to order processing
        "business.domain": "order-processing",
        "team.name": "commerce"
    })

    trace.set_tracer_provider(TracerProvider(resource=resource))

    # Same OTLP exporter configuration pointing to the collector
    otlp_endpoint = os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'http://tempo:4317')
    print(f"[{SERVICE_NAME}] Configuring OTLP exporter with endpoint: {otlp_endpoint}")

    otlp_exporter = OTLPSpanExporter(
        endpoint=otlp_endpoint,
        insecure=True,
        headers={}
    )

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

    RequestsInstrumentor().instrument()
    print(f"[{SERVICE_NAME}] Requests instrumentation enabled")

    print(f"[{SERVICE_NAME}] OpenTelemetry initialization completed successfully!")

# Initialize telemetry
configure_telemetry()
tracer = trace.get_tracer(__name__)

@app.route('/health')
def health():
    return jsonify({"status": "healthy", "service": SERVICE_NAME})

@app.route('/')
def home():
    return jsonify({
        "service": SERVICE_NAME,
        "endpoints": [
            "/health",
            "/process",
            "/orders",
            "/orders/<order_id>"
        ]
    })

@app.route('/process', methods=['POST'])
def process_request():
    """
    Process order request with comprehensive distributed tracing.

    This endpoint demonstrates:
    - How trace context automatically propagates from the upstream service
    - Creating spans for business operations like order creation and pricing
    - Adding rich contextual attributes for business analysis
    - Error handling with proper span status recording

    Reference: https://opentelemetry.io/docs/instrumentation/python/manual/#creating-spans
    """
    with tracer.start_as_current_span("order_service.process_request") as span:
        start_time = time.time()

        # Get data from previous service (user service)
        # The trace context is automatically propagated via HTTP headers
        data = request.get_json() or {}
        trace_id = data.get('trace_id', 'unknown')
        user_id = data.get('user_id')
        user_name = data.get('user_name')
        user_email = data.get('user_email')
        service_chain = data.get('service_chain', [])

        # Add business context to the main span
        span.set_attributes({
            "service.operation": "process_order",
            "business.trace_id": trace_id,
            "user.id": str(user_id) if user_id else "unknown",
            "user.name": user_name or "unknown",
            "user.email": user_email or "unknown",
            "service_chain.previous_length": len(service_chain)
        })

        try:
            # Create a dedicated span for order creation business logic
            with tracer.start_as_current_span("order_service.create_order") as order_span:
                order_span.add_event("Starting order creation process")

                # Simulate order processing with business context
                time.sleep(0.15)  # Simulate processing time
                order_id = random.randint(2000, 2999)
                order_total = round(random.uniform(10.99, 299.99), 2)

                order_span.set_attributes({
                    "order.id": str(order_id),
                    "order.total": order_total,
                    "order.currency": "USD",
                    "order.processing_time_target": "150ms"
                })

                order_span.add_event("Order created", {
                    "order.id": order_id,
                    "order.total": order_total
                })

            # Create a span for pricing calculations
            with tracer.start_as_current_span("order_service.calculate_pricing") as pricing_span:
                pricing_span.add_event("Starting pricing calculation")

                # Generate order items with pricing
                items = [
                    {"item": "Widget A", "price": round(order_total * 0.6, 2), "quantity": 1},
                    {"item": "Widget B", "price": round(order_total * 0.4, 2), "quantity": 2}
                ]

                pricing_span.set_attributes({
                    "pricing.total_items": len(items),
                    "pricing.calculation_method": "percentage_split",
                    "pricing.tax_rate": 0.0,  # No tax for simplicity
                    "pricing.discount": 0.0   # No discounts applied
                })

                for idx, item in enumerate(items):
                    pricing_span.set_attribute(f"item.{idx}.name", item["item"])
                    pricing_span.set_attribute(f"item.{idx}.price", item["price"])
                    pricing_span.set_attribute(f"item.{idx}.quantity", item["quantity"])

                pricing_span.add_event("Pricing calculation completed")

            # Add current service to chain
            service_chain.append(SERVICE_NAME)

            # Prepare enriched data for next service
            with tracer.start_as_current_span("order_service.prepare_payload") as payload_span:
                payload = {
                    "trace_id": trace_id,
                    "user_id": user_id,
                    "user_name": user_name,
                    "user_email": user_email,
                    "order_id": order_id,
                    "order_total": order_total,
                    "timestamp": datetime.utcnow().isoformat(),
                    "service_chain": service_chain,
                    "order_items": items
                }

                payload_span.set_attributes({
                    "payload.size": len(str(payload)),
                    "payload.items_count": len(items),
                    "next_service.name": "audit-service"
                })

            # Create a span for the downstream service call
            with tracer.start_as_current_span("order_service.call_downstream") as downstream_span:
                downstream_span.set_attributes({
                    "http.target_service": "audit-service",
                    "http.method": "POST",
                    "http.url": f"{NEXT_SERVICE_URL}/process"
                })

                # Call next service in the chain
                # Trace context automatically propagates via HTTP headers
                response = requests.post(f"{NEXT_SERVICE_URL}/process",
                                         json=payload,
                                         timeout=10)

                downstream_span.set_attributes({
                    "http.status_code": response.status_code,
                    "http.response_size": len(response.text) if response.text else 0
                })

                processing_time = int((time.time() - start_time) * 1000)

                if response.status_code == 200:
                    result = response.json()

                    # Record successful processing
                    span.set_attributes({
                        "order.processing_success": True,
                        "order.processing_time_ms": processing_time,
                        "order.final_total": order_total
                    })
                    span.set_status(Status(StatusCode.OK))
                    downstream_span.set_status(Status(StatusCode.OK))

                    return jsonify({
                        "status": "success",
                        "trace_id": trace_id,
                        "message": f"Order {order_id} processed through {SERVICE_NAME}",
                        "order_data": {
                            "order_id": order_id,
                            "total": order_total,
                            "user_id": user_id,
                            "items": items
                        },
                        "chain_result": result,
                        "processing_time_ms": processing_time
                    })
                else:
                    # Handle downstream service errors
                    error_msg = f"Error calling next service: {response.status_code}"
                    downstream_span.set_status(Status(StatusCode.ERROR, error_msg))
                    span.set_status(Status(StatusCode.ERROR, "Chain processing failed"))
                    span.set_attributes({
                        "order.processing_success": False,
                        "error.downstream_status": response.status_code
                    })

                    return jsonify({
                        "status": "error",
                        "trace_id": trace_id,
                        "message": error_msg,
                        "processing_time_ms": processing_time
                    }), 500

        except requests.RequestException as e:
            # Handle network/request errors
            processing_time = int((time.time() - start_time) * 1000)
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, f"Request failed: {str(e)}"))
            span.set_attributes({
                "order.processing_success": False,
                "error.type": "network_error",
                "error.message": str(e)
            })

            return jsonify({
                "status": "error",
                "trace_id": trace_id,
                "message": f"Failed to call next service: {str(e)}",
                "processing_time_ms": processing_time
            }), 500

        except Exception as e:
            # Handle any other unexpected errors
            processing_time = int((time.time() - start_time) * 1000)
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, f"Processing failed: {str(e)}"))
            span.set_attributes({
                "order.processing_success": False,
                "error.type": "processing_error",
                "error.message": str(e)
            })

            return jsonify({
                "status": "error",
                "trace_id": trace_id,
                "message": f"Order processing failed: {str(e)}",
                "processing_time_ms": processing_time
            }), 500

@app.route('/orders', methods=['GET'])
def get_orders():
    """Direct endpoint for order service functionality"""
    return jsonify({
        "service": SERVICE_NAME,
        "orders": [
            {"id": 2001, "user_id": 1001, "total": 89.99, "status": "completed"},
            {"id": 2002, "user_id": 1002, "total": 156.50, "status": "pending"}
        ]
    })

@app.route('/orders/<int:order_id>', methods=['GET'])
def get_order(order_id):
    """Get specific order by ID"""
    return jsonify({
        "service": SERVICE_NAME,
        "order": {
            "id": order_id,
            "user_id": 1001,
            "total": 123.45,
            "status": "completed",
            "items": [
                {"name": "Product X", "price": 75.00},
                {"name": "Product Y", "price": 48.45}
            ]
        }
    })

if __name__ == '__main__':
    print(f"Starting {SERVICE_NAME} on port 5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
