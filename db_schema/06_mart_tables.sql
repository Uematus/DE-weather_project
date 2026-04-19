-- =============================================================================
-- 06_mart_tables.sql
-- Run once after 05_control_tables.sql.
-- mart.daily_weather is populated by the Bruin asset mart.daily_weather.
-- =============================================================================

CREATE TABLE IF NOT EXISTS mart.daily_weather (

    -- Grain: 1 day × 1 city
    date                DATE        NOT NULL,   -- relates to PBI Calendar table
    city_id             INTEGER     NOT NULL REFERENCES core.dim_cities(id),

    -- Weather description (human-readable WMO code)
    weather_description TEXT,

    -- Temperature (°C)
    temp_avg            FLOAT,
    temp_min            FLOAT,
    temp_max            FLOAT,
    apparent_temp_avg   FLOAT,

    -- Precipitation (mm)
    precipitation_sum   FLOAT,
    snowfall_sum        FLOAT,

    -- Sun
    sunshine_hours      FLOAT,

    -- Wind (km/h)
    wind_speed_avg      FLOAT,
    wind_gusts_max      FLOAT,

    -- Atmosphere
    humidity_avg        FLOAT,
    pressure_avg        FLOAT,          -- mean sea-level pressure, hPa

    -- Day flags
    is_frost_day        BOOLEAN,
    is_hot_day          BOOLEAN,
    is_rainy_day        BOOLEAN,
    is_snowy_day        BOOLEAN,

    PRIMARY KEY (date, city_id)
);

CREATE INDEX IF NOT EXISTS idx_mart_daily_weather_date
    ON mart.daily_weather (date);

CREATE INDEX IF NOT EXISTS idx_mart_daily_weather_city
    ON mart.daily_weather (city_id);


-- =============================================================================
-- mart.weather_forecast
-- Populated daily by the Bruin asset mart.weather_forecast (Prophet-based).
-- Grain: 1 row = 1 (forecast_run_date, target_date, city_id).
-- Insert-only — every daily run appends 7 forward-looking rows per city.
-- History is kept for forecast-accuracy analysis.
-- =============================================================================

CREATE TABLE IF NOT EXISTS mart.weather_forecast (

    forecast_run_date   DATE        NOT NULL,   -- the day the forecast was produced
    target_date         DATE        NOT NULL,   -- the day being predicted
    city_id             INTEGER     NOT NULL REFERENCES core.dim_cities(id),
    horizon_days        SMALLINT    NOT NULL,   -- target_date - forecast_run_date (1..7)

    -- Point forecasts (Prophet yhat) for all 8 metrics
    temp_avg            FLOAT,
    temp_min            FLOAT,
    temp_max            FLOAT,
    precipitation_sum   FLOAT,
    sunshine_hours      FLOAT,
    humidity_avg        FLOAT,
    pressure_avg        FLOAT,
    wind_speed_avg      FLOAT,

    -- Confidence intervals (yhat_lower / yhat_upper) — only for two key metrics
    temp_avg_lower           FLOAT,
    temp_avg_upper           FLOAT,
    precipitation_sum_lower  FLOAT,
    precipitation_sum_upper  FLOAT,

    loaded_at           TIMESTAMP   NOT NULL DEFAULT now(),

    PRIMARY KEY (forecast_run_date, target_date, city_id)
);

CREATE INDEX IF NOT EXISTS idx_mart_weather_forecast_target
    ON mart.weather_forecast (target_date);

CREATE INDEX IF NOT EXISTS idx_mart_weather_forecast_city
    ON mart.weather_forecast (city_id);

CREATE INDEX IF NOT EXISTS idx_mart_weather_forecast_run
    ON mart.weather_forecast (forecast_run_date);
