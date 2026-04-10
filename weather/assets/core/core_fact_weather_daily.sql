/* @bruin
name: core.fact_weather_daily
type: pg.sql
depends:
  - stage.weather_hourly
  - core.dim_cities
  - core.dim_weather_code
@bruin */

-- Step 1: delete existing rows for idempotency.
-- {{ start_date_nodash }} / {{ end_date_nodash }} → YYYYMMDD integers matching date_id.
DELETE FROM core.fact_weather_daily
WHERE date_id BETWEEN {{ start_date_nodash }} AND {{ end_date_nodash }};

-- Step 2: aggregate from stage and insert
INSERT INTO core.fact_weather_daily (
    date_id,
    city_id,
    dominant_weather_code_id,
    temp_avg,
    temp_min,
    temp_max,
    apparent_temp_avg,
    apparent_temp_min,
    apparent_temp_max,
    wet_bulb_temp_avg,
    precipitation_sum,
    rain_sum,
    snowfall_sum,
    sunshine_duration_hours,
    shortwave_radiation_avg,
    avg_wind_speed_10m,
    max_wind_speed_10m,
    max_wind_gusts,
    avg_cloud_cover,
    avg_relative_humidity,
    avg_pressure_msl,
    is_frost_day,
    is_hot_day,
    is_rainy_day,
    is_snowy_day,
    loaded_at
)
SELECT
    TO_CHAR(CAST(s.measured_at AS DATE), 'YYYYMMDD')::INT   AS date_id,
    dc.id                                                    AS city_id,

    -- Dominant weather code: most frequent non-null code in the day
    (mode() WITHIN GROUP (ORDER BY s.weather_code))::SMALLINT AS dominant_weather_code_id,

    -- Temperature
    ROUND(AVG(s.temperature_2m)::NUMERIC, 2)::FLOAT         AS temp_avg,
    MIN(s.temperature_2m)                                    AS temp_min,
    MAX(s.temperature_2m)                                    AS temp_max,
    ROUND(AVG(s.apparent_temperature)::NUMERIC, 2)::FLOAT   AS apparent_temp_avg,
    MIN(s.apparent_temperature)                              AS apparent_temp_min,
    MAX(s.apparent_temperature)                              AS apparent_temp_max,
    ROUND(AVG(s.wet_bulb_temperature_2m)::NUMERIC, 2)::FLOAT AS wet_bulb_temp_avg,

    -- Precipitation
    ROUND(SUM(s.precipitation)::NUMERIC, 2)::FLOAT          AS precipitation_sum,
    ROUND(SUM(s.rain)::NUMERIC, 2)::FLOAT                   AS rain_sum,
    ROUND(SUM(s.snowfall)::NUMERIC, 2)::FLOAT               AS snowfall_sum,

    -- Sun
    ROUND((SUM(s.sunshine_duration) / 3600.0)::NUMERIC, 2)::FLOAT AS sunshine_duration_hours,
    ROUND(AVG(s.shortwave_radiation)::NUMERIC, 2)::FLOAT    AS shortwave_radiation_avg,

    -- Wind
    ROUND(AVG(s.wind_speed_10m)::NUMERIC, 2)::FLOAT         AS avg_wind_speed_10m,
    MAX(s.wind_speed_10m)                                    AS max_wind_speed_10m,
    MAX(s.wind_gusts_10m)                                    AS max_wind_gusts,

    -- Atmosphere
    ROUND(AVG(s.cloud_cover)::NUMERIC, 2)::FLOAT            AS avg_cloud_cover,
    ROUND(AVG(s.relative_humidity_2m)::NUMERIC, 2)::FLOAT   AS avg_relative_humidity,
    ROUND(AVG(s.pressure_msl)::NUMERIC, 2)::FLOAT           AS avg_pressure_msl,

    -- Derived flags
    MIN(s.temperature_2m) < 0                               AS is_frost_day,
    MAX(s.temperature_2m) >= 30                             AS is_hot_day,
    SUM(s.precipitation) >= 1                               AS is_rainy_day,
    SUM(s.snowfall) > 0                                     AS is_snowy_day,

    now()                                                    AS loaded_at

FROM stage.weather_hourly s
JOIN core.dim_cities dc
    ON dc.city_name = s.location_name

WHERE CAST(s.measured_at AS DATE)
      BETWEEN '{{ start_date }}'::date AND '{{ end_date }}'::date

GROUP BY
    CAST(s.measured_at AS DATE),
    dc.id,
    s.location_name;
