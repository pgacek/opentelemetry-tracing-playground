CREATE TABLE IF NOT EXISTS request_traces (
    id SERIAL PRIMARY KEY,
    trace_id VARCHAR(50) NOT NULL,
    user_id INTEGER,
    order_id INTEGER,
    service_name VARCHAR(50) NOT NULL,
    request_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    request_data JSONB,
    response_data JSONB,
    processing_time_ms INTEGER
    );

CREATE TABLE IF NOT EXISTS users (
     id SERIAL PRIMARY KEY,
     name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_requests INTEGER DEFAULT 0,
    last_request_at TIMESTAMP
    );

CREATE INDEX IF NOT EXISTS idx_trace_id ON request_traces(trace_id);
CREATE INDEX IF NOT EXISTS idx_service_name ON request_traces(service_name);
CREATE INDEX IF NOT EXISTS idx_request_timestamp ON request_traces(request_timestamp);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

INSERT INTO users (name, email) VALUES
                                    ('John Doe', 'john.doe@example.com'),
                                    ('Jane Smith', 'jane.smith@example.com'),
                                    ('Alice Johnson', 'alice.johnson@example.com'),
                                    ('Bob Wilson', 'bob.wilson@example.com')
    ON CONFLICT (email) DO NOTHING;

INSERT INTO request_traces (trace_id, service_name, request_data)
VALUES ('init-trace', 'database', '{"status": "initialized"}')
    ON CONFLICT DO NOTHING;
