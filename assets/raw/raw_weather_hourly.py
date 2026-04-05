"""@bruin
name: raw.weather_hourly
type: python
connection: postgres_default

description: >
  Raw hourly weather data from Open-Meteo Historical API.
  All available variables. One row = one hour for one city.
@bruin"""

import os
import json
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta


############################################################################
# 1. Location Parameters
############################################################################

LOCATION_NAME = "London"
LATITUDE      = "51.5085"
LONGITUDE     = "-0.1257"
TIMEZONE      = "Europe/London"

# Bruin passes dates through environment variables
START_DATE = os.environ.get("BRUIN_START_DATE",
                             (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d"))
END_DATE   = os.environ.get("BRUIN_END_DATE",
                             (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d"))

print(f"[INFO] Location : {LOCATION_NAME} ({LATITUDE}, {LONGITUDE})")
print(f"[INFO] Period   : {START_DATE} -> {END_DATE}")


############################################################################
# 2. All hourly variables
############################################################################

HOURLY_VARS = [
    "temperature_2m", "relative_humidity_2m", "dew_point_2m",
    "apparent_temperature", "vapour_pressure_deficit",
    "precipitation", "rain", "snowfall", "snow_depth",
    "pressure_msl", "surface_pressure",
    "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
    "wind_speed_10m", "wind_speed_100m",
    "wind_direction_10m", "wind_direction_100m", "wind_gusts_10m",
    "shortwave_radiation", "direct_radiation", "diffuse_radiation",
    "direct_normal_irradiance", "sunshine_duration",
    "et0_fao_evapotranspiration",
    "soil_temperature_0_to_7cm", "soil_temperature_7_to_28cm",
    "soil_temperature_28_to_100cm", "soil_temperature_100_to_255cm",
    "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm",
    "soil_moisture_28_to_100cm", "soil_moisture_100_to_255cm",
    "weather_code", "boundary_layer_height",
    "wet_bulb_temperature_2m", "total_column_integrated_water_vapour",
    "is_day",
]


############################################################################
# 3. Request to API
############################################################################

API_URL = "https://archive-api.open-meteo.com/v1/archive"

params = {
    "latitude":           LATITUDE,
    "longitude":          LONGITUDE,
    "start_date":         START_DATE,
    "end_date":           END_DATE,
    "hourly":             ",".join(HOURLY_VARS),
    "timezone":           TIMEZONE,
    "wind_speed_unit":    "kmh",
    "precipitation_unit": "mm",
    "temperature_unit":   "celsius",
    "timeformat":         "iso8601",
}

print(f"[INFO] Requesting API...")
response = requests.get(API_URL, params=params, timeout=30)
response.raise_for_status()
data = response.json()

print(f"[INFO] Grid cell: lat={data.get('latitude')}, "
      f"lon={data.get('longitude')}, elevation={data.get('elevation')} m")


############################################################################
# 4. JSON -> DataFrame
############################################################################

hourly_data = data["hourly"]
units       = data.get("hourly_units", {})

df = pd.DataFrame(hourly_data)
df.rename(columns={"time": "measured_at"}, inplace=True)
df["measured_at"] = pd.to_datetime(df["measured_at"])

df.insert(0, "location_name", LOCATION_NAME)
df.insert(1, "latitude",      float(data["latitude"]))
df.insert(2, "longitude",     float(data["longitude"]))
df.insert(3, "elevation_m",   data.get("elevation"))
df.insert(4, "timezone",      data.get("timezone"))
df["units_json"] = json.dumps(units)
df["loaded_at"]  = datetime.now()

print(f"[INFO] Rows fetched: {len(df)}")
print(f"[INFO] Columns    : {list(df.columns)}")


############################################################################
# 5. Connection to PostgreSQL
############################################################################

PG_HOST = os.environ.get("BRUIN_PG_HOST",     "db")
PG_PORT = os.environ.get("BRUIN_PG_PORT",     "5432")
PG_DB   = os.environ.get("BRUIN_PG_DB",       "weather_db")
PG_USER = os.environ.get("BRUIN_PG_USER",     "de_user")
PG_PASS = os.environ.get("BRUIN_PG_PASSWORD", "de_password")

conn_str = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine   = create_engine(conn_str)


# ############################################################################
# 6. Create table if not exists + idempotent removal of duplicates
# ############################################################################

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS raw.weather_hourly (
    location_name                        TEXT,
    latitude                             FLOAT,
    longitude                            FLOAT,
    elevation_m                          FLOAT,
    timezone                             TEXT,
    measured_at                          TIMESTAMP,
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
    weather_code                         FLOAT,
    boundary_layer_height                FLOAT,
    wet_bulb_temperature_2m              FLOAT,
    total_column_integrated_water_vapour FLOAT,
    is_day                               FLOAT,
    units_json                           TEXT,
    loaded_at                            TIMESTAMP
);
"""

with engine.begin() as conn:
    conn.execute(text(CREATE_SQL))
    conn.execute(text("""
        DELETE FROM raw.weather_hourly
        WHERE location_name = :loc
          AND CAST(measured_at AS DATE) BETWEEN CAST(:sd AS DATE) AND CAST(:ed AS DATE)
    """), {"loc": LOCATION_NAME, "sd": START_DATE, "ed": END_DATE})

print(f"[INFO] Table ready. Old records cleared for {LOCATION_NAME}.")


# ############################################################################
# 7. Write to raw.weather_hourly
# ############################################################################

df.to_sql(
    name      = "weather_hourly",
    schema    = "raw",
    con       = engine,
    if_exists = "append",
    index     = False,
    method    = "multi",
    chunksize = 500,
)

print(f"[SUCCESS] Loaded {len(df)} rows -> raw.weather_hourly")