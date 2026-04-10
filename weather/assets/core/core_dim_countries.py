"""@bruin
name: core.dim_countries
connection: postgres_default
depends:
  - stage.weather_hourly
@bruin"""

import os
import pathlib
import yaml
from sqlalchemy import create_engine, text
from datetime import datetime


############################################################################
# Config
############################################################################

CONFIG_PATH = pathlib.Path(__file__).parent.parent / "config" / "cities.yml"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    _cfg = yaml.safe_load(f)

# Deduplicate countries by name
seen = {}
for city in _cfg["cities"]:
    name = city["country"]
    if name not in seen:
        seen[name] = {
            "country_name": name,
            "iso_code_2":   city.get("country_iso2"),
            "iso_code_3":   city.get("country_iso3"),
            "continent":    city.get("continent"),
            "region":       city.get("region"),
        }

countries = list(seen.values())

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
# SCD1 MERGE
############################################################################

UPSERT_SQL = text("""
    INSERT INTO core.dim_countries
        (country_name, iso_code_2, iso_code_3, continent, region, loaded_at, updated_at)
    VALUES
        (:country_name, :iso_code_2, :iso_code_3, :continent, :region, now(), now())
    ON CONFLICT (country_name) DO UPDATE SET
        iso_code_2 = EXCLUDED.iso_code_2,
        iso_code_3 = EXCLUDED.iso_code_3,
        continent  = EXCLUDED.continent,
        region     = EXCLUDED.region,
        updated_at = now()
""")

inserted = 0
updated  = 0

with engine.begin() as conn:
    for row in countries:
        result = conn.execute(UPSERT_SQL, row)
        # rowcount=1 for both INSERT and UPDATE in PG upsert
        # We check xmax to distinguish insert vs update
        inserted += 1  # simplified; all upserts counted

print(f"[core.dim_countries] Processed {len(countries)} countries.")
