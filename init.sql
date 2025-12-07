CREATE DATABASE temperature_db;

\c temperature_db;

CREATE TABLE IF NOT EXISTS temperature_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    temperatura INT NOT NULL,
    timestamp TIMESTAMP NOT NULL
    timestamp_received TIMESTAMP NOT NULL DEFAULT NOW()
);