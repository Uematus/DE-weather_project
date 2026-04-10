-- =============================================================================
-- 06_mart_tables.sql
-- Run once after 05_control_tables.sql.
-- mart.daily_weather is populated by the Bruin asset mart.daily_weather.
-- =============================================================================

CREATE TABLE IF NOT EXISTS mart.daily_weather (

    -- Grain: 1 day × 1 city
    date                DATE        NOT NULL,   -- relates to PBI Calendar table
    city_name           TEXT        NOT NULL,

    -- City / Geography
    latitude            FLOAT       NOT NULL,
    longitude           FLOAT       NOT NULL,
    is_capital          BOOLEAN,
    climate_zone        TEXT,
    country_name        TEXT        NOT NULL,
    region              TEXT,

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

    -- Day flags
    is_frost_day        BOOLEAN,
    is_hot_day          BOOLEAN,
    is_rainy_day        BOOLEAN,
    is_snowy_day        BOOLEAN,

    PRIMARY KEY (date, city_name)
);

CREATE INDEX IF NOT EXISTS idx_mart_daily_weather_date
    ON mart.daily_weather (date);

CREATE INDEX IF NOT EXISTS idx_mart_daily_weather_city
    ON mart.daily_weather (city_name);
