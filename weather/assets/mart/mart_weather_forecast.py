"""@bruin
name: mart.weather_forecast
connection: postgres_default
depends:
  - core.fact_weather_daily
  - core.dim_cities
  - mart.daily_weather
@bruin"""

# Produces a 7-day forward forecast per active city using Facebook Prophet.
#
# Runs daily. Each run appends exactly (active_cities * 7) new rows to
# mart.weather_forecast — insert-only, previous runs are preserved so that
# forecast accuracy can be analysed later.
#
# Source: core.fact_weather_daily (last 3 years per city, whatever is available).
# Metrics: 8 (temp_avg, temp_min, temp_max, precipitation_sum, sunshine_hours,
#             humidity_avg, pressure_avg, wind_speed_avg).
# Confidence intervals (yhat_lower/yhat_upper) are persisted only for
# temp_avg and precipitation_sum — the two metrics shown on the dashboard with
# uncertainty bands.

import os
import sys
import gc
import warnings
from datetime import date, timedelta

# Pin math libraries to 1 thread BEFORE importing prophet/cmdstanpy/numpy.
# Without this, on a small VPS cmdstan + OpenMP + BLAS try to spawn N-core
# threads per fit — several threads × 480 fits drives CPU to 100 % and
# spikes RAM. Keep one thread; the loop is already linear per (city, metric).
os.environ.setdefault("OMP_NUM_THREADS",       "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS",  "1")
os.environ.setdefault("MKL_NUM_THREADS",       "1")
os.environ.setdefault("STAN_NUM_THREADS",      "1")
os.environ.setdefault("CMDSTANPY_VERBOSE",     "false")

import pandas as pd
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

import logging
logging.getLogger("cmdstanpy").setLevel(logging.ERROR)
logging.getLogger("prophet").setLevel(logging.ERROR)

from prophet import Prophet

############################################################################
# Config
############################################################################

HORIZON_DAYS       = 7
HISTORY_YEARS_MAX  = 2          # 2 yrs is enough to catch yearly seasonality;
                                # longer history slows fit without accuracy gain
MIN_HISTORY_DAYS   = 60         # below this Prophet results are unreliable — skip city
INTERVAL_WIDTH     = 0.80       # 80 % confidence band (used only where persisted)

# Sample count for the uncertainty interval. Default Prophet = 1000 MCMC draws
# per fit — very expensive. We only persist CI for temp_avg and precipitation_sum,
# so other metrics fit with uncertainty_samples=0 (no sampling, ~3× faster,
# much lower memory footprint).
UNCERTAINTY_SAMPLES_WITH_CI    = 200
UNCERTAINTY_SAMPLES_WITHOUT_CI = 0

# Run gc.collect() every N cities. Prophet/cmdstanpy leak Stan objects between
# fits; without this RAM creeps up over the whole run.
GC_EVERY_N_CITIES  = 10

# Columns we forecast. Key = core.fact_weather_daily column, value = mart column.
METRICS = {
    "temp_avg":             "temp_avg",
    "temp_min":             "temp_min",
    "temp_max":             "temp_max",
    "precipitation_sum":    "precipitation_sum",
    "sunshine_hours":       "sunshine_hours",      # aliased from sunshine_duration_hours below
    "humidity_avg":         "humidity_avg",        # aliased from avg_relative_humidity
    "pressure_avg":         "pressure_avg",        # aliased from avg_pressure_msl
    "wind_speed_avg":       "wind_speed_avg",      # aliased from avg_wind_speed_10m
}

# Metrics that keep lower/upper bounds in the mart table
METRICS_WITH_INTERVAL = {"temp_avg", "precipitation_sum"}

############################################################################
# DB
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
# Load history: one DataFrame per city (date + 8 metrics, aliased to mart names)
############################################################################

LOAD_HISTORY_SQL = text(f"""
    SELECT
        ci.id                          AS city_id,
        ci.city_name                   AS city_name,
        cal.date                       AS ds,
        f.temp_avg                     AS temp_avg,
        f.temp_min                     AS temp_min,
        f.temp_max                     AS temp_max,
        f.precipitation_sum            AS precipitation_sum,
        f.sunshine_duration_hours      AS sunshine_hours,
        f.avg_relative_humidity        AS humidity_avg,
        f.avg_pressure_msl             AS pressure_avg,
        f.avg_wind_speed_10m           AS wind_speed_avg
    FROM core.fact_weather_daily f
    JOIN core.dim_calendar cal ON cal.id = f.date_id
    JOIN core.dim_cities   ci  ON ci.id  = f.city_id
    WHERE cal.date >= :cutoff
    ORDER BY ci.id, cal.date
""")

INSERT_SQL = text("""
    INSERT INTO mart.weather_forecast (
        forecast_run_date, target_date, city_id, horizon_days,
        temp_avg, temp_min, temp_max,
        precipitation_sum, sunshine_hours, humidity_avg,
        pressure_avg, wind_speed_avg,
        temp_avg_lower, temp_avg_upper,
        precipitation_sum_lower, precipitation_sum_upper
    ) VALUES (
        :forecast_run_date, :target_date, :city_id, :horizon_days,
        :temp_avg, :temp_min, :temp_max,
        :precipitation_sum, :sunshine_hours, :humidity_avg,
        :pressure_avg, :wind_speed_avg,
        :temp_avg_lower, :temp_avg_upper,
        :precipitation_sum_lower, :precipitation_sum_upper
    )
    ON CONFLICT (forecast_run_date, target_date, city_id) DO NOTHING
""")

############################################################################
# Prophet fit
############################################################################

def forecast_one(city_hist: pd.DataFrame, metric: str,
                 with_interval: bool) -> pd.DataFrame:
    """
    Fit Prophet on a single (city, metric) series and return a DataFrame
    with columns [ds, yhat, yhat_lower, yhat_upper] for the next HORIZON_DAYS.
    Returns None if the series is unfittable (all NaN / too short).
    """
    df = city_hist[["ds", metric]].rename(columns={metric: "y"}).dropna()
    if len(df) < MIN_HISTORY_DAYS:
        return None

    samples = UNCERTAINTY_SAMPLES_WITH_CI if with_interval else UNCERTAINTY_SAMPLES_WITHOUT_CI

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,   # weather has no weekday pattern
        daily_seasonality=False,
        interval_width=INTERVAL_WIDTH,
        uncertainty_samples=samples,
    )
    model.fit(df)

    future = model.make_future_dataframe(periods=HORIZON_DAYS, include_history=False)
    fc = model.predict(future)

    cols = ["ds", "yhat"]
    if with_interval:
        cols += ["yhat_lower", "yhat_upper"]
    result = fc[cols].copy()

    # Free Stan backend explicitly — prophet holds the cmdstanpy model which
    # keeps temp files / memory until the Python object is released.
    del model, future, fc
    return result

############################################################################
# Main
############################################################################

run_date  = date.today()
cutoff    = run_date - timedelta(days=HISTORY_YEARS_MAX * 365)

print(f"[mart.weather_forecast] run_date={run_date}  "
      f"history_cutoff={cutoff}  horizon={HORIZON_DAYS}d")

with engine.connect() as conn:
    hist = pd.read_sql(LOAD_HISTORY_SQL, conn, params={"cutoff": cutoff.isoformat()})

if hist.empty:
    print("[mart.weather_forecast] No history available. Nothing to forecast.")
    sys.exit(0)

hist["ds"] = pd.to_datetime(hist["ds"])

cities = hist[["city_id", "city_name"]].drop_duplicates().sort_values("city_id")
print(f"[mart.weather_forecast] {len(cities)} cities × {len(METRICS)} metrics "
      f"= {len(cities) * len(METRICS)} Prophet fits")

total_rows     = 0
skipped_cities = []

for idx, row in enumerate(cities.itertuples(index=False), 1):
    city_id   = int(row.city_id)
    city_name = row.city_name
    city_hist = hist[hist["city_id"] == city_id].copy()

    # Build a per-target-date dict we'll INSERT row-by-row
    per_date = {}   # target_date -> dict of metric values

    skipped_metrics = 0
    for metric in METRICS:
        with_interval = metric in METRICS_WITH_INTERVAL
        try:
            fc = forecast_one(city_hist, metric, with_interval)
        except Exception as e:
            print(f"  [WARN] {city_name} / {metric}: Prophet failed — {e}")
            fc = None

        if fc is None:
            skipped_metrics += 1
            continue

        for _, r in fc.iterrows():
            target = r["ds"].date()
            d = per_date.setdefault(target, {})
            # Clamp physically non-negative metrics
            yhat = r["yhat"]
            if metric in ("precipitation_sum", "sunshine_hours") and yhat is not None:
                yhat = max(0.0, float(yhat))
            d[metric] = yhat

            if with_interval:
                lo, hi = r["yhat_lower"], r["yhat_upper"]
                if metric == "precipitation_sum":
                    lo = max(0.0, float(lo)) if lo is not None else None
                    hi = max(0.0, float(hi)) if hi is not None else None
                d[f"{metric}_lower"] = lo
                d[f"{metric}_upper"] = hi

    if not per_date or skipped_metrics == len(METRICS):
        skipped_cities.append(city_name)
        continue

    # Insert 7 rows for this city
    payload = []
    for target_date, vals in sorted(per_date.items()):
        horizon = (target_date - run_date).days
        if horizon < 1 or horizon > HORIZON_DAYS:
            continue
        payload.append({
            "forecast_run_date":       run_date,
            "target_date":             target_date,
            "city_id":                 city_id,
            "horizon_days":            horizon,
            "temp_avg":                vals.get("temp_avg"),
            "temp_min":                vals.get("temp_min"),
            "temp_max":                vals.get("temp_max"),
            "precipitation_sum":       vals.get("precipitation_sum"),
            "sunshine_hours":          vals.get("sunshine_hours"),
            "humidity_avg":            vals.get("humidity_avg"),
            "pressure_avg":            vals.get("pressure_avg"),
            "wind_speed_avg":          vals.get("wind_speed_avg"),
            "temp_avg_lower":          vals.get("temp_avg_lower"),
            "temp_avg_upper":          vals.get("temp_avg_upper"),
            "precipitation_sum_lower": vals.get("precipitation_sum_lower"),
            "precipitation_sum_upper": vals.get("precipitation_sum_upper"),
        })

    if payload:
        with engine.begin() as conn:
            conn.execute(INSERT_SQL, payload)
        total_rows += len(payload)

    if idx % GC_EVERY_N_CITIES == 0:
        gc.collect()

    if idx % 10 == 0 or idx == len(cities):
        print(f"  [{idx}/{len(cities)}] {city_name}: +{len(payload)} rows "
              f"(cumulative {total_rows})")

print(f"[mart.weather_forecast] Inserted {total_rows} rows "
      f"for run_date={run_date}.")

if skipped_cities:
    print(f"[mart.weather_forecast] Skipped {len(skipped_cities)} cities "
          f"with insufficient history: {skipped_cities}")

sys.exit(0)
