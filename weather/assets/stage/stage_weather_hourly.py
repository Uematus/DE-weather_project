"""@bruin
name: stage.weather_hourly
connection: postgres_default
@bruin"""

import os
import sys
import time
import pathlib
import calendar
import requests
import yaml
import pandas as pd
from datetime import date, datetime, timedelta
from sqlalchemy import create_engine, text


############################################################################
# 1. Load city config
############################################################################

CONFIG_PATH = pathlib.Path(__file__).parent.parent / "config" / "cities.yml"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    _cfg = yaml.safe_load(f)

LOCATIONS     = [c for c in _cfg["cities"] if c.get("active", False)]
LOCATIONS_MAP = {c["name"]: c for c in _cfg["cities"]}

if not LOCATIONS:
    print("[WARN] No active cities found in cities.yml. Exiting.")
    sys.exit(0)

print(f"[INFO] Active cities: {[c['name'] for c in LOCATIONS]}")


############################################################################
# 2. Config
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

API_URL       = "https://archive-api.open-meteo.com/v1/archive"
HISTORY_START = int(os.environ.get("HISTORY_START", "2010"))

# Seconds to sleep between monthly API calls (respects ~1 req/sec burst limit)
SLEEP_BETWEEN_CHUNKS = 2
# Retry delays on temporary 429 (burst): 2min → 5min → 10min
RETRY_DELAYS_BURST = [120, 300, 600]


############################################################################
# 3. DB connection
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
# 4. Exceptions
############################################################################

class DailyLimitExceeded(Exception):
    """Raised when Open-Meteo daily request quota is exhausted."""


############################################################################
# 5. API helpers
############################################################################

def _is_daily_limit(resp: requests.Response) -> bool:
    """Check if response indicates daily API quota exhaustion (any 4xx)."""
    if resp.status_code < 400 or resp.status_code >= 500:
        return False
    try:
        body = resp.json().get("reason", "")
    except Exception:
        body = resp.text
    body_lower = body.lower()
    return "daily" in body_lower and ("limit" in body_lower or "exceeded" in body_lower)


def fetch_period(loc: dict, start: str, end: str) -> pd.DataFrame:
    params = {
        "latitude":           loc["lat"],
        "longitude":          loc["lon"],
        "start_date":         start,
        "end_date":           end,
        "hourly":             ",".join(HOURLY_VARS),
        "timezone":           loc["tz"],
        "wind_speed_unit":    "kmh",
        "precipitation_unit": "mm",
        "temperature_unit":   "celsius",
        "timeformat":         "iso8601",
    }
    resp = requests.get(API_URL, params=params, timeout=60)

    if _is_daily_limit(resp):
        body = resp.text[:200]
        raise DailyLimitExceeded(body)

    if resp.status_code == 429:
        raise requests.HTTPError(response=resp)

    resp.raise_for_status()
    data = resp.json()

    df = pd.DataFrame(data["hourly"])
    df.rename(columns={"time": "measured_at"}, inplace=True)
    df["measured_at"] = pd.to_datetime(df["measured_at"])

    if "weather_code" in df.columns:
        df["weather_code"] = pd.to_numeric(df["weather_code"], errors="coerce") \
                               .astype("Int16")
    if "is_day" in df.columns:
        df["is_day"] = df["is_day"].astype("boolean")

    df.insert(0, "location_name", loc["name"])
    df.insert(1, "country",       loc.get("country"))
    df.insert(2, "latitude",      float(data["latitude"]))
    df.insert(3, "longitude",     float(data["longitude"]))
    df.insert(4, "elevation_m",   data.get("elevation"))
    df.insert(5, "timezone",      data.get("timezone"))
    df["loaded_at"] = datetime.now()
    return df


def fetch_with_retry(loc: dict, start: str, end: str) -> pd.DataFrame:
    """Retry on temporary 429 (burst). Re-raise DailyLimitExceeded immediately."""
    for attempt, wait in enumerate(RETRY_DELAYS_BURST):
        try:
            return fetch_period(loc, start, end)
        except DailyLimitExceeded:
            raise  # don't retry — daily limit means stop for today
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                print(f"  [429-burst] {loc['name']} {start}. "
                      f"Retry {attempt + 1}/{len(RETRY_DELAYS_BURST)} in {wait}s...")
                time.sleep(wait)
            else:
                raise
    return fetch_period(loc, start, end)


############################################################################
# 6. DB helpers
############################################################################

def upsert_chunk(df: pd.DataFrame, location_name: str,
                 start: str, end: str) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM stage.weather_hourly
            WHERE location_name = :loc
              AND CAST(measured_at AS DATE) BETWEEN CAST(:sd AS DATE) AND CAST(:ed AS DATE)
        """), {"loc": location_name, "sd": start, "ed": end})

    df.to_sql(
        name="weather_hourly", schema="stage", con=engine,
        if_exists="append", index=False, method="multi", chunksize=500,
    )


def log_days(start: str, end: str, city_name: str, status: str,
             rows_per_day: int = 24, error_msg: str = None) -> None:
    """Insert one load_log row per calendar day in [start, end]."""
    d     = date.fromisoformat(start)
    d_end = date.fromisoformat(end)
    rows  = []
    while d <= d_end:
        rows.append({
            "run_date": d.isoformat(),
            "city":     city_name,
            "status":   status,
            "rows":     rows_per_day if status == "success" else None,
            "expected": 24,
            "err":      error_msg,
        })
        d += timedelta(days=1)

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO control.load_log
                (run_date, city_name, layer, status, rows_loaded, expected_rows, error_msg)
            VALUES
                (:run_date, :city, 'stage', :status, :rows, :expected, :err)
        """), rows)


############################################################################
# 7. Backfill: chunk planning
############################################################################

def iter_months(year_from: int, day_to: date):
    """Yield (month_start, month_end) date pairs from year_from to day_to."""
    d = date(year_from, 1, 1)
    while d <= day_to:
        last_day = calendar.monthrange(d.year, d.month)[1]
        month_end = min(date(d.year, d.month, last_day), day_to)
        yield d, month_end
        # advance to first day of next month
        if d.month == 12:
            d = date(d.year + 1, 1, 1)
        else:
            d = date(d.year, d.month + 1, 1)


def get_pending_chunks(yesterday: date) -> list:
    """
    Return list of (loc_dict, start_str, end_str) monthly chunks not yet
    fully loaded. A chunk is 'done' when every day in it has a 'success'
    entry in control.load_log for that city.
    """
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT city_name, run_date
            FROM control.load_log
            WHERE layer = 'stage' AND status = 'success'
        """)).fetchall()

    loaded: set = {(r.city_name, r.run_date) for r in rows}

    pending = []
    for loc in LOCATIONS:
        for month_start, month_end in iter_months(HISTORY_START, yesterday):
            d = month_start
            complete = True
            while d <= month_end:
                if (loc["name"], d) not in loaded:
                    complete = False
                    break
                d += timedelta(days=1)
            if not complete:
                pending.append((loc, month_start.isoformat(), month_end.isoformat()))

    return pending


############################################################################
# 8. Main — always: incremental first, then backfill
############################################################################

yesterday    = date.today() - timedelta(days=1)
hard_failures = []          # real errors (DB, network, bad data)
daily_limit   = False       # set True when API quota is hit

# ── PHASE 1: INCREMENTAL (yesterday) ──────────────────────────────────────
start = os.environ.get("BRUIN_START_DATE", yesterday.isoformat())
end   = os.environ.get("BRUIN_END_DATE",   yesterday.isoformat())
print(f"[INCREMENTAL] {len(LOCATIONS)} cities for {start} → {end}")

incremental_ok = 0

for loc in LOCATIONS:
    if daily_limit:
        print(f"  [SKIP] {loc['name']} — daily limit already reached")
        continue

    try:
        df = fetch_with_retry(loc, start, end)
        upsert_chunk(df, loc["name"], start, end)
        log_days(start, end, loc["name"], "success")
        incremental_ok += 1
        print(f"  [OK] {loc['name']} — {len(df)} rows")

    except DailyLimitExceeded as e:
        daily_limit = True
        print(f"  [DAILY LIMIT] {loc['name']}: {e}")
        print(f"  [DAILY LIMIT] Skipping remaining cities.")

    except Exception as e:
        print(f"  [FAIL] {loc['name']}: {e}")
        hard_failures.append({"phase": "incremental", "city": loc["name"], "error": str(e)})
        log_days(start, end, loc["name"], "failed", error_msg=str(e))

print(f"[INCREMENTAL DONE] OK: {incremental_ok}/{len(LOCATIONS)}")

# ── PHASE 2: BACKFILL (history) ───────────────────────────────────────────
if daily_limit:
    print(f"\n[BACKFILL] Skipped — daily API limit already reached.")
else:
    pending = get_pending_chunks(yesterday)

    if not pending:
        print(f"\n[BACKFILL] All chunks already loaded. Nothing to do.")
    else:
        total = len(pending)
        print(f"\n[BACKFILL] {total} monthly chunks remaining "
              f"(~{total * SLEEP_BETWEEN_CHUNKS // 60} min at {SLEEP_BETWEEN_CHUNKS}s/chunk).")

        for idx, (loc, b_start, b_end) in enumerate(pending, 1):
            print(f"  [{idx}/{total}] {loc['name']} {b_start[:7]}", end=" ... ", flush=True)
            try:
                df = fetch_with_retry(loc, b_start, b_end)
                upsert_chunk(df, loc["name"], b_start, b_end)
                log_days(b_start, b_end, loc["name"], "success", rows_per_day=24)
                print(f"{len(df)} rows OK")

            except DailyLimitExceeded as e:
                print(f"\n[DAILY LIMIT] Open-Meteo quota exhausted: {e}")
                print(f"[BACKFILL] Progress saved. {total - idx} chunks remain.")
                break

            except Exception as e:
                print(f"FAIL: {e}")
                hard_failures.append({"phase": "backfill", "city": loc["name"],
                                      "period": b_start, "error": str(e)})
                log_days(b_start, b_end, loc["name"], "failed", error_msg=str(e))

            time.sleep(SLEEP_BETWEEN_CHUNKS)

        print(f"[BACKFILL DONE]")

# ── EXIT ──────────────────────────────────────────────────────────────────
if hard_failures:
    print(f"\n[WARN] {len(hard_failures)} hard failure(s):")
    for f in hard_failures:
        print(f"  - {f}")
    # Fail only if incremental loaded nothing — downstream needs at least some data
    if incremental_ok == 0:
        print("[FATAL] No incremental data loaded. Failing pipeline.")
        sys.exit(1)
    else:
        print("[INFO] Incremental partially succeeded — continuing pipeline.")

print("[DONE] stage.weather_hourly finished successfully.")
sys.exit(0)
