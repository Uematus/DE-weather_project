"""@bruin
name: core.dim_weather_code
connection: postgres_default
depends:
  - stage.weather_hourly
@bruin"""

import os
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
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

############################################################################
# Merge new codes from stage (ON CONFLICT DO NOTHING — seed data wins)
############################################################################

with engine.begin() as conn:
    result = conn.execute(text("""
        INSERT INTO core.dim_weather_code (id, loaded_at, updated_at)
        SELECT DISTINCT
            weather_code::SMALLINT,
            now(),
            now()
        FROM stage.weather_hourly
        WHERE weather_code IS NOT NULL
        ON CONFLICT (id) DO NOTHING
    """))
    new_codes = result.rowcount

if new_codes > 0:
    print(f"[core.dim_weather_code] Inserted {new_codes} previously unknown WMO code(s). "
          f"Fill in description/severity/precipitation_type manually.")
else:
    print("[core.dim_weather_code] No new WMO codes found.")
