-- =============================================================================
-- 02_control_tables.sql
-- Run once before starting Bruin. Pipeline observability: load log + gap view.
-- =============================================================================

CREATE TABLE IF NOT EXISTS control.load_log (
    id            SERIAL      PRIMARY KEY,
    run_date      DATE        NOT NULL,                   -- date of data being loaded
    city_name     TEXT        NOT NULL,                   -- city (matches location_name in stage)
    layer         TEXT        NOT NULL                    -- 'stage' | 'core'
                  CHECK (layer IN ('stage', 'core')),
    status        TEXT        NOT NULL                    -- 'success' | 'failed'
                  CHECK (status IN ('success', 'failed')),
    rows_loaded   INT,
    expected_rows INT,                                    -- 24 per city/day; 23 or 25 on DST days
    executed_at   TIMESTAMP   NOT NULL DEFAULT now(),
    error_msg     TEXT
);

CREATE INDEX IF NOT EXISTS idx_load_log_lookup
    ON control.load_log (run_date, city_name, layer, status);

-- =============================================================================
-- Gap detection view
-- Shows every (date, city, layer) combination where a successful load is missing.
-- Crosses dates from 2010-01-01 to yesterday against all cities in core.dim_cities.
-- Run: SELECT * FROM control.v_date_gaps ORDER BY missing_date, city_name, layer;
-- =============================================================================
CREATE OR REPLACE VIEW control.v_date_gaps AS
SELECT
    d::date  AS missing_date,
    c.city_name,
    layers.layer
FROM
    generate_series('2010-01-01'::date, CURRENT_DATE - 1, '1 day') AS d
    CROSS JOIN core.dim_cities                                       AS c
    CROSS JOIN (VALUES ('stage'), ('core'))                          AS layers(layer)
LEFT JOIN (
    SELECT run_date, city_name, layer
    FROM   control.load_log
    WHERE  status = 'success'
) ok ON ok.run_date = d AND ok.city_name = c.city_name AND ok.layer = layers.layer
WHERE ok.run_date IS NULL;
