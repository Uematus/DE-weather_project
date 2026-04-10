/* @bruin
name: mart.daily_weather
type: pg.sql
depends:
  - core.fact_weather_daily
  - core.dim_cities
  - core.dim_countries
  - core.dim_weather_code
@bruin */

DELETE FROM mart.daily_weather
WHERE date BETWEEN '{{ start_date }}'::date AND '{{ end_date }}'::date;

INSERT INTO mart.daily_weather (
    date,
    city_name,
    latitude,
    longitude,
    is_capital,
    climate_zone,
    country_name,
    region,
    weather_description,
    temp_avg,
    temp_min,
    temp_max,
    apparent_temp_avg,
    precipitation_sum,
    snowfall_sum,
    sunshine_hours,
    wind_speed_avg,
    wind_gusts_max,
    humidity_avg,
    is_frost_day,
    is_hot_day,
    is_rainy_day,
    is_snowy_day
)
SELECT
    cal.date,
    ci.city_name,
    ci.latitude,
    ci.longitude,
    ci.is_capital,
    ci.climate_zone,
    co.country_name,
    co.region,
    wc.description          AS weather_description,
    f.temp_avg,
    f.temp_min,
    f.temp_max,
    f.apparent_temp_avg,
    f.precipitation_sum,
    f.snowfall_sum,
    f.sunshine_duration_hours AS sunshine_hours,
    f.avg_wind_speed_10m    AS wind_speed_avg,
    f.max_wind_gusts        AS wind_gusts_max,
    f.avg_relative_humidity AS humidity_avg,
    f.is_frost_day,
    f.is_hot_day,
    f.is_rainy_day,
    f.is_snowy_day

FROM core.fact_weather_daily f
JOIN core.dim_calendar      cal ON cal.id  = f.date_id  -- needed only to resolve date value
JOIN core.dim_cities        ci  ON ci.id   = f.city_id
JOIN core.dim_countries     co  ON co.id   = ci.country_id
LEFT JOIN core.dim_weather_code wc ON wc.id = f.dominant_weather_code_id

WHERE cal.date BETWEEN '{{ start_date }}'::date AND '{{ end_date }}'::date;
