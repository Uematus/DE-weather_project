-- =============================================================================
-- 04_core_facts.sql
-- Run once before starting Bruin. Creates core fact tables.
-- fact_weather_hourly: partitioned by year (RANGE on date_id).
-- fact_weather_daily:  not partitioned (max ~165K rows for 30 cities × 15 years).
-- =============================================================================


-- ---------------------------------------------------------------------------
-- fact_weather_hourly
-- Grain: 1 row = 1 hour × 1 city.
-- Partitioned by year using RANGE on date_id (YYYYMMDD integer).
-- Source: stage.weather_hourly → JOIN core dimensions.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.fact_weather_hourly (
    id                                   BIGSERIAL,
    date_id                              INT         NOT NULL,  -- FK → dim_calendar.id
    time_id                              INT         NOT NULL,  -- FK → dim_time.id
    city_id                              INT         NOT NULL,  -- FK → dim_cities.id
    weather_code_id                      SMALLINT,              -- FK → dim_weather_code.id
    wind_dir_10m_id                      SMALLINT,              -- FK → dim_wind_direction.id
    wind_dir_100m_id                     SMALLINT,              -- FK → dim_wind_direction.id

    -- Measurements
    temperature_2m                       FLOAT,
    relative_humidity_2m                 FLOAT,
    dew_point_2m                         FLOAT,
    apparent_temperature                 FLOAT,
    vapour_pressure_deficit              FLOAT,
    precipitation                        FLOAT,
    rain                                 FLOAT,
    snowfall                             FLOAT,
    snow_depth                           FLOAT,
    pressure_msl                         FLOAT,
    surface_pressure                     FLOAT,
    cloud_cover                          FLOAT,
    cloud_cover_low                      FLOAT,
    cloud_cover_mid                      FLOAT,
    cloud_cover_high                     FLOAT,
    wind_speed_10m                       FLOAT,
    wind_speed_100m                      FLOAT,
    wind_gusts_10m                       FLOAT,
    shortwave_radiation                  FLOAT,
    direct_radiation                     FLOAT,
    diffuse_radiation                    FLOAT,
    direct_normal_irradiance             FLOAT,
    sunshine_duration                    FLOAT,
    et0_fao_evapotranspiration           FLOAT,
    soil_temperature_0_to_7cm            FLOAT,
    soil_temperature_7_to_28cm           FLOAT,
    soil_temperature_28_to_100cm         FLOAT,
    soil_temperature_100_to_255cm        FLOAT,
    soil_moisture_0_to_7cm               FLOAT,
    soil_moisture_7_to_28cm              FLOAT,
    soil_moisture_28_to_100cm            FLOAT,
    soil_moisture_100_to_255cm           FLOAT,
    boundary_layer_height                FLOAT,
    wet_bulb_temperature_2m              FLOAT,
    total_column_integrated_water_vapour FLOAT,
    is_day                               BOOLEAN,

    loaded_at                            TIMESTAMP   NOT NULL DEFAULT now()
)
PARTITION BY RANGE (date_id);

-- Partitions: 1950–2050, matching dim_calendar range.
-- Generated via PL/pgSQL loop — no need to maintain manually.
DO $$
DECLARE
    yr INT;
BEGIN
    FOR yr IN 1950..2050 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS core.fact_weather_hourly_%s
             PARTITION OF core.fact_weather_hourly
             FOR VALUES FROM (%s0101) TO (%s0101)',
            yr,
            yr,
            yr + 1
        );
    END LOOP;
END;
$$;

-- Indexes on each partition are inherited automatically in PG14+.
-- The composite index below is created on the parent and propagates.
CREATE INDEX IF NOT EXISTS idx_fwh_city_date
    ON core.fact_weather_hourly (city_id, date_id);


-- ---------------------------------------------------------------------------
-- fact_weather_daily
-- Grain: 1 row = 1 calendar day × 1 city.
-- No partitioning — max ~165K rows for 30 cities × 15 years.
-- Source: stage.weather_hourly aggregated directly → JOIN core dimensions.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.fact_weather_daily (
    id                          SERIAL      PRIMARY KEY,
    date_id                     INT         NOT NULL,  -- FK → dim_calendar.id
    city_id                     INT         NOT NULL,  -- FK → dim_cities.id
    dominant_weather_code_id    SMALLINT,              -- FK → dim_weather_code.id (MODE)

    -- Temperature (°C)
    temp_avg                    FLOAT,
    temp_min                    FLOAT,
    temp_max                    FLOAT,
    apparent_temp_avg           FLOAT,
    apparent_temp_min           FLOAT,
    apparent_temp_max           FLOAT,
    wet_bulb_temp_avg           FLOAT,

    -- Precipitation (mm / cm)
    precipitation_sum           FLOAT,
    rain_sum                    FLOAT,
    snowfall_sum                FLOAT,

    -- Sun & radiation
    sunshine_duration_hours     FLOAT,                 -- SUM(sunshine_duration) / 3600
    shortwave_radiation_avg     FLOAT,

    -- Wind
    avg_wind_speed_10m          FLOAT,
    max_wind_speed_10m          FLOAT,
    max_wind_gusts              FLOAT,

    -- Atmosphere
    avg_cloud_cover             FLOAT,
    avg_relative_humidity       FLOAT,
    avg_pressure_msl            FLOAT,

    -- Derived flags
    is_frost_day                BOOLEAN,               -- temp_min < 0
    is_hot_day                  BOOLEAN,               -- temp_max >= 30
    is_rainy_day                BOOLEAN,               -- precipitation_sum >= 1
    is_snowy_day                BOOLEAN,               -- snowfall_sum > 0

    loaded_at                   TIMESTAMP   NOT NULL DEFAULT now(),

    UNIQUE (date_id, city_id)
);

CREATE INDEX IF NOT EXISTS idx_fwd_city_date
    ON core.fact_weather_daily (city_id, date_id);
