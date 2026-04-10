-- =============================================================================
-- 01_stage_tables.sql
-- Run once before starting Bruin. Creates stage layer tables.
-- =============================================================================

CREATE TABLE IF NOT EXISTS stage.weather_hourly (
    location_name                        TEXT        NOT NULL,
    country                              TEXT,
    latitude                             FLOAT,
    longitude                            FLOAT,
    elevation_m                          FLOAT,
    timezone                             TEXT,
    measured_at                          TIMESTAMP   NOT NULL,
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
    wind_direction_10m                   FLOAT,
    wind_direction_100m                  FLOAT,
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
    weather_code                         SMALLINT,
    boundary_layer_height                FLOAT,
    wet_bulb_temperature_2m              FLOAT,
    total_column_integrated_water_vapour FLOAT,
    is_day                               BOOLEAN,
    loaded_at                            TIMESTAMP
);

-- Optimise range scans used by core assets and gap detection
CREATE INDEX IF NOT EXISTS idx_stage_wh_location_date
    ON stage.weather_hourly (location_name, measured_at);
