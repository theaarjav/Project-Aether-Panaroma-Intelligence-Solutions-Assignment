-- Project Aether Database Initialization Script
-- PostgreSQL: Client Configuration Storage

-- Create database if it doesn't exist (handled by Docker)
-- This script runs after the database is created

-- Rate limiting configuration table (main configuration storage)
CREATE TABLE IF NOT EXISTS rate_limits (
    client_id VARCHAR(255) PRIMARY KEY,
    requests_per_minute INTEGER NOT NULL CHECK (requests_per_minute > 0),
    burst_limit INTEGER NOT NULL DEFAULT 10 CHECK (burst_limit > 0),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Client details table (extended client information)
CREATE TABLE IF NOT EXISTS clients (
    client_id VARCHAR(255) PRIMARY KEY,
    client_name VARCHAR(500),
    email VARCHAR(320),
    tier VARCHAR(50) DEFAULT 'standard', -- 'basic', 'standard', 'premium', 'enterprise'
    status VARCHAR(20) DEFAULT 'active', -- 'active', 'suspended', 'disabled'
    api_key_hash VARCHAR(256),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    last_seen TIMESTAMP
);

-- Audit log for configuration changes
CREATE TABLE IF NOT EXISTS config_audit_log (
    id SERIAL PRIMARY KEY,
    client_id VARCHAR(255) NOT NULL,
    action VARCHAR(50) NOT NULL, -- 'CREATE', 'UPDATE', 'DELETE'
    old_config JSONB,
    new_config JSONB,
    changed_by VARCHAR(255),
    changed_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_rate_limits_updated_at ON rate_limits(updated_at);
CREATE INDEX IF NOT EXISTS idx_clients_status ON clients(status);
CREATE INDEX IF NOT EXISTS idx_clients_tier ON clients(tier);
CREATE INDEX IF NOT EXISTS idx_clients_last_seen ON clients(last_seen);
CREATE INDEX IF NOT EXISTS idx_audit_log_client_id ON config_audit_log(client_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_changed_at ON config_audit_log(changed_at);

-- Function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$ language 'plpgsql';

-- Triggers to automatically update updated_at
CREATE TRIGGER update_rate_limits_updated_at 
    BEFORE UPDATE ON rate_limits 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_clients_updated_at 
    BEFORE UPDATE ON clients 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Insert sample client data
INSERT INTO clients (client_id, client_name, email, tier, status) VALUES
    ('client-123', 'Demo Application', 'demo@example.com', 'standard', 'active'),
    ('client-456', 'Analytics Service', 'analytics@company.com', 'premium', 'active'),
    ('client-789', 'Mobile App', 'mobile@startup.com', 'basic', 'active'),
    ('test-client', 'Load Testing', 'test@internal.com', 'enterprise', 'active'),
    ('premium-client', 'Enterprise Dashboard', 'enterprise@bigcorp.com', 'enterprise', 'active')
ON CONFLICT (client_id) DO NOTHING;

-- Insert rate limit configurations
INSERT INTO rate_limits (client_id, requests_per_minute, burst_limit) VALUES
    ('client-123', 100, 20),
    ('client-456', 200, 40),
    ('client-789', 50, 10),
    ('test-client', 1000, 100),
    ('premium-client', 500, 50)
ON CONFLICT (client_id) DO NOTHING;

-- Create a function to get comprehensive client information
CREATE OR REPLACE FUNCTION get_client_info(p_client_id VARCHAR(255))
RETURNS TABLE(
    client_id VARCHAR(255),
    client_name VARCHAR(500),
    email VARCHAR(320),
    tier VARCHAR(50),
    status VARCHAR(20),
    requests_per_minute INTEGER,
    burst_limit INTEGER,
    config_updated TIMESTAMP,
    last_seen TIMESTAMP
) AS $
BEGIN
    RETURN QUERY
    SELECT 
        c.client_id,
        c.client_name,
        c.email,
        c.tier,
        c.status,
        rl.requests_per_minute,
        rl.burst_limit,
        rl.updated_at as config_updated,
        c.last_seen
    FROM clients c
    LEFT JOIN rate_limits rl ON c.client_id = rl.client_id
    WHERE c.client_id = p_client_id;
END;
$ LANGUAGE plpgsql;

-- Create a view for client management dashboard
CREATE OR REPLACE VIEW client_dashboard AS
SELECT 
    c.client_id,
    c.client_name,
    c.email,
    c.tier,
    c.status,
    c.last_seen,
    rl.requests_per_minute,
    rl.burst_limit,
    rl.updated_at as rate_limit_updated,
    CASE 
        WHEN rl.client_id IS NULL THEN 'No Rate Limit'
        WHEN c.status != 'active' THEN 'Inactive'
        ELSE 'Active'
    END as configuration_status
FROM clients c
LEFT JOIN rate_limits rl ON c.client_id = rl.client_id
ORDER BY c.last_seen DESC NULLS LAST;

-- Log successful initialization
INSERT INTO config_audit_log (client_id, action, new_config, changed_by) 
VALUES ('system', 'INIT', '{"message": "PostgreSQL database initialized successfully", "tables": ["clients", "rate_limits", "config_audit_log"]}', 'init-script');

-- Display initialization summary
SELECT 
    'PostgreSQL initialization completed' as status,
    (SELECT COUNT(*) FROM clients) as client_records,
    (SELECT COUNT(*) FROM rate_limits) as rate_limit_configs,
    NOW() as initialized_at;