-- Minimal schema bootstrap for docker-compose db init.
-- Full DDL (tables, indexes, seed data) is in db_schema/ and must be run manually
-- in order: 00 → 01 → 02 → 03 → 04 → 05
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS control;
