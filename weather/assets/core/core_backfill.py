"""@bruin
name: core.backfill
connection: postgres_default
depends:
  - core.fact_weather_daily
  - mart.daily_weather
@bruin"""

# Detects and fills gaps in core.fact_weather_daily and mart.daily_weather.
#
# Runs automatically after the daily incremental pipeline. Compares row counts
# per month between stage → core and core → mart. Any month where the counts
# don't match (i.e. stage loaded new data that core/mart hasn't processed yet)
# is reloaded via DELETE + INSERT.

import os
import sys
from sqlalchemy import create_engine, text

############################################################################
# DB connection
############################################################################

PG_HOST = os.environ.get("BRUIN_PG_HOST",     "db")
PG_PORT = os.environ.get("BRUIN_PG_PORT",     "5432")
PG_DB   = os.environ.get("BRUIN_PG_DB",       "weather_db")
PG_USER = os.environ.get("BRUIN_PG_USER",     "de_user")
PG_PASS = os.environ.get("BRUIN_PG_PASSWORD", "de_password")

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True,
)

############################################################################
# Gap detection queries
############################################################################

# Months where core has fewer (city × day) rows than stage
CORE_GAP_SQL = text("""
    WITH stage_city_days AS (
        SELECT
            DATE_TRUNC('month', CAST(measured_at AS DATE))::DATE AS month_start,
            COUNT(DISTINCT (location_name, CAST(measured_at AS DATE)))  AS city_days
        FROM stage.weather_hourly
        GROUP BY 1
    ),
    core_city_days AS (
        SELECT
            DATE_TRUNC('month', cal.date)::DATE AS month_start,
            COUNT(*)                             AS city_days
        FROM core.fact_weather_daily f
        JOIN core.dim_calendar cal ON cal.id = f.date_id
        GROUP BY 1
    )
    SELECT
        s.month_start::DATE                                                        AS start_date,
        LEAST(
            (s.month_start + INTERVAL '1 month' - INTERVAL '1 day')::DATE,
            CURRENT_DATE - 1
        )                                                                          AS end_date
    FROM stage_city_days s
    LEFT JOIN core_city_days c ON c.month_start = s.month_start
    WHERE COALESCE(c.city_days, 0) < s.city_days
    ORDER BY s.month_start
""")

# Months where mart has fewer rows than core
MART_GAP_SQL = text("""
    WITH core_city_days AS (
        SELECT
            DATE_TRUNC('month', cal.date)::DATE AS month_start,
            COUNT(*)                             AS city_days
        FROM core.fact_weather_daily f
        JOIN core.dim_calendar cal ON cal.id = f.date_id
        GROUP BY 1
    ),
    mart_city_days AS (
        SELECT
            DATE_TRUNC('month', date)::DATE AS month_start,
            COUNT(*)                        AS city_days
        FROM mart.daily_weather
        GROUP BY 1
    )
    SELECT
        c.month_start::DATE                                                        AS start_date,
        LEAST(
            (c.month_start + INTERVAL '1 month' - INTERVAL '1 day')::DATE,
            CURRENT_DATE - 1
        )                                                                          AS end_date
    FROM core_city_days c
    LEFT JOIN mart_city_days m ON m.month_start = c.month_start
    WHERE COALESCE(m.city_days, 0) < c.city_days
    ORDER BY c.month_start
""")

############################################################################
# Core: DELETE + INSERT (mirrors core_fact_weather_daily.sql logic)
############################################################################

CORE_DELETE_SQL = text("""
    DELETE FROM core.fact_weather_daily
    WHERE date_id BETWEEN
        TO_CHAR(CAST(:start AS DATE), 'YYYYMMDD')::INT AND
        TO_CHAR(CAST(:end AS DATE),   'YYYYMMDD')::INT
""")

CORE_INSERT_SQL = text("""
    INSERT INTO core.fact_weather_daily (
        date_id, city_id, dominant_weather_code_id,
        temp_avg, temp_min, temp_max,
        apparent_temp_avg, apparent_temp_min, apparent_temp_max,
        wet_bulb_temp_avg,
        precipitation_sum, rain_sum, snowfall_sum,
        sunshine_duration_hours, shortwave_radiation_avg,
        avg_wind_speed_10m, max_wind_speed_10m, max_wind_gusts,
        avg_cloud_cover, avg_relative_humidity, avg_pressure_msl,
        is_frost_day, is_hot_day, is_rainy_day, is_snowy_day,
        loaded_at
    )
    SELECT
        TO_CHAR(CAST(s.measured_at AS DATE), 'YYYYMMDD')::INT    AS date_id,
        dc.id                                                     AS city_id,
        (mode() WITHIN GROUP (ORDER BY s.weather_code))::SMALLINT AS dominant_weather_code_id,
        ROUND(AVG(s.temperature_2m)::NUMERIC, 2)::FLOAT          AS temp_avg,
        MIN(s.temperature_2m)                                     AS temp_min,
        MAX(s.temperature_2m)                                     AS temp_max,
        ROUND(AVG(s.apparent_temperature)::NUMERIC, 2)::FLOAT    AS apparent_temp_avg,
        MIN(s.apparent_temperature)                               AS apparent_temp_min,
        MAX(s.apparent_temperature)                               AS apparent_temp_max,
        ROUND(AVG(s.wet_bulb_temperature_2m)::NUMERIC, 2)::FLOAT AS wet_bulb_temp_avg,
        ROUND(SUM(s.precipitation)::NUMERIC, 2)::FLOAT           AS precipitation_sum,
        ROUND(SUM(s.rain)::NUMERIC, 2)::FLOAT                    AS rain_sum,
        ROUND(SUM(s.snowfall)::NUMERIC, 2)::FLOAT                AS snowfall_sum,
        ROUND((SUM(s.sunshine_duration) / 3600.0)::NUMERIC, 2)::FLOAT
                                                                  AS sunshine_duration_hours,
        ROUND(AVG(s.shortwave_radiation)::NUMERIC, 2)::FLOAT     AS shortwave_radiation_avg,
        ROUND(AVG(s.wind_speed_10m)::NUMERIC, 2)::FLOAT          AS avg_wind_speed_10m,
        MAX(s.wind_speed_10m)                                     AS max_wind_speed_10m,
        MAX(s.wind_gusts_10m)                                     AS max_wind_gusts,
        ROUND(AVG(s.cloud_cover)::NUMERIC, 2)::FLOAT             AS avg_cloud_cover,
        ROUND(AVG(s.relative_humidity_2m)::NUMERIC, 2)::FLOAT    AS avg_relative_humidity,
        ROUND(AVG(s.pressure_msl)::NUMERIC, 2)::FLOAT            AS avg_pressure_msl,
        MIN(s.temperature_2m) < 0                                AS is_frost_day,
        MAX(s.temperature_2m) >= 30                              AS is_hot_day,
        SUM(s.precipitation) >= 1                                AS is_rainy_day,
        SUM(s.snowfall) > 0                                      AS is_snowy_day,
        now()                                                     AS loaded_at
    FROM stage.weather_hourly s
    JOIN core.dim_cities dc ON dc.city_name = s.location_name
    WHERE CAST(s.measured_at AS DATE)
          BETWEEN CAST(:start AS DATE) AND CAST(:end AS DATE)
    GROUP BY CAST(s.measured_at AS DATE), dc.id, s.location_name
""")

############################################################################
# Mart: DELETE + INSERT (mirrors mart_daily_weather.sql logic)
############################################################################

MART_DELETE_SQL = text("""
    DELETE FROM mart.daily_weather
    WHERE date BETWEEN CAST(:start AS DATE) AND CAST(:end AS DATE)
""")

MART_INSERT_SQL = text("""
    INSERT INTO mart.daily_weather (
        date, city_name, latitude, longitude, is_capital, climate_zone,
        country_name, region, weather_description,
        temp_avg, temp_min, temp_max, apparent_temp_avg,
        precipitation_sum, snowfall_sum, sunshine_hours,
        wind_speed_avg, wind_gusts_max, humidity_avg,
        is_frost_day, is_hot_day, is_rainy_day, is_snowy_day
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
        wc.description             AS weather_description,
        f.temp_avg,
        f.temp_min,
        f.temp_max,
        f.apparent_temp_avg,
        f.precipitation_sum,
        f.snowfall_sum,
        f.sunshine_duration_hours  AS sunshine_hours,
        f.avg_wind_speed_10m       AS wind_speed_avg,
        f.max_wind_gusts           AS wind_gusts_max,
        f.avg_relative_humidity    AS humidity_avg,
        f.is_frost_day,
        f.is_hot_day,
        f.is_rainy_day,
        f.is_snowy_day
    FROM core.fact_weather_daily f
    JOIN core.dim_calendar      cal ON cal.id  = f.date_id
    JOIN core.dim_cities        ci  ON ci.id   = f.city_id
    JOIN core.dim_countries     co  ON co.id   = ci.country_id
    LEFT JOIN core.dim_weather_code wc ON wc.id = f.dominant_weather_code_id
    WHERE cal.date BETWEEN CAST(:start AS DATE) AND CAST(:end AS DATE)
""")

############################################################################
# Runner
############################################################################

def fill_gaps(layer: str, gap_sql, delete_sql, insert_sql) -> int:
    with engine.connect() as conn:
        gaps = conn.execute(gap_sql).fetchall()

    if not gaps:
        print(f"[core.backfill] {layer}: up to date, nothing to fill.")
        return 0

    print(f"[core.backfill] {layer}: {len(gaps)} month(s) with gaps.")
    failures = 0
    for row in gaps:
        start, end = row.start_date.isoformat(), row.end_date.isoformat()
        print(f"  {start[:7]} ... ", end="", flush=True)
        try:
            with engine.begin() as conn:
                conn.execute(delete_sql, {"start": start, "end": end})
                conn.execute(insert_sql, {"start": start, "end": end})
            print("OK")
        except Exception as e:
            print(f"FAIL: {e}")
            failures += 1

    return failures


total_failures  = fill_gaps("core.fact_weather_daily", CORE_GAP_SQL, CORE_DELETE_SQL, CORE_INSERT_SQL)
total_failures += fill_gaps("mart.daily_weather",      MART_GAP_SQL, MART_DELETE_SQL, MART_INSERT_SQL)

print(f"[core.backfill] Finished. Failures: {total_failures}")
if total_failures:
    sys.exit(1)
