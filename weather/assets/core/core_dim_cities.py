"""@bruin
name: core.dim_cities
connection: postgres_default
depends:
  - core.dim_countries
@bruin"""

import os
import pathlib
import yaml
from sqlalchemy import create_engine, text


############################################################################
# Config
############################################################################

CONFIG_PATH = pathlib.Path(__file__).parent.parent / "config" / "cities.yml"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    _cfg = yaml.safe_load(f)

cities_meta = {c["name"]: c for c in _cfg["cities"]}

############################################################################
# DB connection
############################################################################

PG_HOST = os.environ.get("BRUIN_PG_HOST",     "db")
PG_PORT = os.environ.get("BRUIN_PG_PORT",     "5432")
PG_DB   = os.environ.get("BRUIN_PG_DB",       "weather_db")
PG_USER = os.environ.get("BRUIN_PG_USER",     "de_user")
PG_PASS = os.environ.get("BRUIN_PG_PASSWORD", "de_password")

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

############################################################################
# Read distinct cities from stage (source of elevation and coordinates)
############################################################################

with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT DISTINCT ON (location_name)
            location_name,
            latitude,
            longitude,
            elevation_m,
            timezone
        FROM stage.weather_hourly
        ORDER BY location_name, loaded_at DESC
    """)).fetchall()

if not rows:
    print("[core.dim_cities] No data in stage.weather_hourly yet. Skipping.")
    raise SystemExit(0)

############################################################################
# SCD1 MERGE
############################################################################

UPSERT_SQL = text("""
    INSERT INTO core.dim_cities
        (city_name, latitude, longitude, elevation_m, timezone,
         is_capital, climate_zone, country_id, loaded_at, updated_at)
    SELECT
        :city_name, :latitude, :longitude, :elevation_m, :timezone,
        :is_capital, :climate_zone,
        (SELECT id FROM core.dim_countries WHERE country_name = :country_name),
        now(), now()
    ON CONFLICT (city_name) DO UPDATE SET
        latitude     = EXCLUDED.latitude,
        longitude    = EXCLUDED.longitude,
        elevation_m  = EXCLUDED.elevation_m,
        timezone     = EXCLUDED.timezone,
        is_capital   = EXCLUDED.is_capital,
        climate_zone = EXCLUDED.climate_zone,
        country_id   = EXCLUDED.country_id,
        updated_at   = now()
""")

processed = 0
for row in rows:
    city_name = row.location_name
    meta      = cities_meta.get(city_name, {})

    params = {
        "city_name":    city_name,
        "latitude":     row.latitude,
        "longitude":    row.longitude,
        "elevation_m":  row.elevation_m,
        "timezone":     row.timezone,
        "is_capital":   meta.get("is_capital"),
        "climate_zone": meta.get("climate_zone"),
        "country_name": meta.get("country"),
    }

    with engine.begin() as conn:
        conn.execute(UPSERT_SQL, params)

    processed += 1

print(f"[core.dim_cities] Processed {processed} cities.")
