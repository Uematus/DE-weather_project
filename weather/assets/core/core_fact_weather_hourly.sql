/* @bruin
name: core.fact_weather_hourly
type: pg.sql
depends:
  - stage.weather_hourly
  - core.dim_cities
  - core.dim_weather_code
@bruin */

-- Step 1: delete existing rows for this run's date range to ensure idempotency.
-- {{ start_date_nodash }} / {{ end_date_nodash }} → YYYYMMDD integers matching date_id.
DELETE FROM core.fact_weather_hourly
WHERE date_id BETWEEN {{ start_date_nodash }} AND {{ end_date_nodash }};

-- Step 2: insert from stage, joining all dimensions.
INSERT INTO core.fact_weather_hourly (
    date_id,
    time_id,
    city_id,
    weather_code_id,
    wind_dir_10m_id,
    wind_dir_100m_id,
    temperature_2m,
    relative_humidity_2m,
    dew_point_2m,
    apparent_temperature,
    vapour_pressure_deficit,
    precipitation,
    rain,
    snowfall,
    snow_depth,
    pressure_msl,
    surface_pressure,
    cloud_cover,
    cloud_cover_low,
    cloud_cover_mid,
    cloud_cover_high,
    wind_speed_10m,
    wind_speed_100m,
    wind_gusts_10m,
    shortwave_radiation,
    direct_radiation,
    diffuse_radiation,
    direct_normal_irradiance,
    sunshine_duration,
    et0_fao_evapotranspiration,
    soil_temperature_0_to_7cm,
    soil_temperature_7_to_28cm,
    soil_temperature_28_to_100cm,
    soil_temperature_100_to_255cm,
    soil_moisture_0_to_7cm,
    soil_moisture_7_to_28cm,
    soil_moisture_28_to_100cm,
    soil_moisture_100_to_255cm,
    boundary_layer_height,
    wet_bulb_temperature_2m,
    total_column_integrated_water_vapour,
    is_day,
    loaded_at
)
SELECT
    -- Date dimension: id is YYYYMMDD integer
    TO_CHAR(s.measured_at, 'YYYYMMDD')::INT                       AS date_id,

    -- Time dimension: id is HHMM integer
    (EXTRACT(HOUR   FROM s.measured_at)::INT * 100
     + EXTRACT(MINUTE FROM s.measured_at)::INT)::INT               AS time_id,

    dc.id                                                           AS city_id,
    s.weather_code                                                  AS weather_code_id,

    -- Wind direction 10m → compass id
    wd10.id                                                         AS wind_dir_10m_id,
    wd100.id                                                        AS wind_dir_100m_id,

    s.temperature_2m,
    s.relative_humidity_2m,
    s.dew_point_2m,
    s.apparent_temperature,
    s.vapour_pressure_deficit,
    s.precipitation,
    s.rain,
    s.snowfall,
    s.snow_depth,
    s.pressure_msl,
    s.surface_pressure,
    s.cloud_cover,
    s.cloud_cover_low,
    s.cloud_cover_mid,
    s.cloud_cover_high,
    s.wind_speed_10m,
    s.wind_speed_100m,
    s.wind_gusts_10m,
    s.shortwave_radiation,
    s.direct_radiation,
    s.diffuse_radiation,
    s.direct_normal_irradiance,
    s.sunshine_duration,
    s.et0_fao_evapotranspiration,
    s.soil_temperature_0_to_7cm,
    s.soil_temperature_7_to_28cm,
    s.soil_temperature_28_to_100cm,
    s.soil_temperature_100_to_255cm,
    s.soil_moisture_0_to_7cm,
    s.soil_moisture_7_to_28cm,
    s.soil_moisture_28_to_100cm,
    s.soil_moisture_100_to_255cm,
    s.boundary_layer_height,
    s.wet_bulb_temperature_2m,
    s.total_column_integrated_water_vapour,
    s.is_day,
    now()                                                           AS loaded_at

FROM stage.weather_hourly s

-- City dimension
JOIN core.dim_cities dc
    ON dc.city_name = s.location_name

-- Wind direction 10m (NULL-safe: no match → wind_dir_id = NULL)
LEFT JOIN core.dim_wind_direction wd10
    ON s.wind_direction_10m BETWEEN wd10.degree_from AND wd10.degree_to

-- Wind direction 100m
LEFT JOIN core.dim_wind_direction wd100
    ON s.wind_direction_100m BETWEEN wd100.degree_from AND wd100.degree_to

WHERE CAST(s.measured_at AS DATE)
      BETWEEN '{{ start_date }}'::date AND '{{ end_date }}'::date;

