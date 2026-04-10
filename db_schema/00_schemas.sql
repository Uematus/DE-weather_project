-- =============================================================================
-- 00_schemas.sql
-- Run once before starting Bruin. Creates all schemas.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS control;
