-- CDC Test Database Initialization Script

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    age INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_created_at ON users(created_at);

-- Function to auto-update updated_at column
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to auto-update updated_at on UPDATE
CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create publication for Debezium
-- This publication defines which tables Debezium should capture changes from
CREATE PUBLICATION dbz_publication FOR TABLE users;

-- Insert test data
INSERT INTO users (name, email, age) VALUES
    ('Alice', 'alice@example.com', 30),
    ('Bob', 'bob@example.com', 25),
    ('Charlie', 'charlie@example.com', 35),
    ('Diana', 'diana@example.com', 28),
    ('Eve', 'eve@example.com', 32);

-- Verification query
SELECT
    COUNT(*) as total_users,
    MIN(created_at) as oldest,
    MAX(created_at) as newest
FROM users;

-- Display configuration
\echo '========================================='
\echo 'PostgreSQL CDC Database Initialized!'
\echo '========================================='
\echo 'Database: testdb'
\echo 'User: testuser'
\echo 'Table: users (5 initial records)'
\echo 'Publication: dbz_publication'
\echo '========================================='
