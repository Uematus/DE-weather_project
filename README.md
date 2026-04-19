# European Weather Analytics Pipeline

A batch data pipeline that loads hourly weather for 95 European cities from 2010 onwards into PostgreSQL, shows it in Power BI, and produces a 7-day forecast every day.

Built as the capstone project for [DataTalks.Club Data Engineering Zoomcamp 2026](https://github.com/DataTalksClub/data-engineering-zoomcamp).

---

## Problem

Weather stations record millions of observations every day, but the data is spread across many formats and is hard to compare across cities or years. This project builds one place where you can:

- Load and keep hourly weather for 95 European cities from 2010 to today.
- Query any city, any day, any metric through a PostgreSQL star schema.
- See trends, extremes, and rankings on a Power BI dashboard.
- Get a 7-day forecast for every city, refreshed every night.

---

## What the project delivers

| Piece | Description |
|---|---|
| Ingestion | Daily load of yesterday's data for 95 European cities from the Open-Meteo Historical API. The same script also fills history back to 2010 whenever the API quota allows. |
| Data warehouse | PostgreSQL 16 with four schemas: `stage` (raw hourly), `core` (Kimball star), `mart` (Power BI-ready), `control` (load log and gap view). |
| Transformations | Bruin CLI assets: SCD1 dimensions, one partitioned hourly fact table, one daily aggregate, two mart tables. |
| Forecast | A Bruin Python asset runs Facebook Prophet every night and writes a 7-day forecast into `mart.weather_forecast`. Rows are never deleted, so we can later compare past forecasts to what actually happened. |
| Dashboard | Power BI with five KPI cards (with year-over-year deltas), a dynamic line chart, an extreme-weather ranking, a climate-group scatter map, and an map. |
| Reproducibility | Everything runs in Docker Compose. One `docker compose up -d` brings the whole stack up. |
| Observability | `control.load_log` records every load attempt. `control.v_date_gaps` lists any (date, city, layer) combination that is still missing. |

---

## Architecture

```
                                 Open-Meteo Historical API
                                            │
                                            ▼
                                  stage.weather_hourly
                          (raw hourly data, 39 variables per row)
                                            │
             ┌──────────────────────────────┼──────────────────────────┐
             ▼                              ▼                          ▼
    core.dim_countries            core.fact_weather_hourly   core.fact_weather_daily
    core.dim_cities               (partitioned by year)      (1 row / city / day)
    core.dim_weather_code                                                │
             │                                                           ▼
             └────────────────► mart.daily_weather ◄────────────────────┘
                                (denormalised table, one wide row per
                                 city × day, consumed by Power BI)
                                            │
                                            ▼
                                  mart.weather_forecast
                                (7-day forecast, insert-only,
                                 written daily by Prophet)
                                            │
                                            ▼
                                     Power BI Service
                                      (Import mode)
```

**Static dimensions** (seeded once, never updated by the pipeline):
`core.dim_calendar` (1950–2050), `core.dim_time`, `core.dim_wind_direction`.

**Why a separate mart layer?** The core star schema is normalised - good for storage, less convenient for BI. `mart.daily_weather` joins the dimensions once and adds text columns (`city_name`, `country_name`, `region`, `is_capital`, `climate_zone`, `weather_description`) so Power BI imports one wide table. Dashboard visuals stay simple and refreshes stay fast.

**Gap filling.** The stage asset always runs in two phases: yesterday first, then any older monthly chunks still missing from `control.load_log`. On top of that, the asset `core.backfill` runs near the end of each pipeline and copies any month that `stage` has but `core` or `mart` does not yet. That is how all four layers stay in sync while the backfill spans many days.

---

## Tech stack

| Piece | Tool |
|---|---|
| Ingestion and transformation | [Bruin CLI](https://github.com/bruin-data/bruin) (Python and SQL assets) |
| Forecasting | [Facebook Prophet](https://facebook.github.io/prophet/) - one model per city × metric |
| Storage | PostgreSQL 16 |
| Scheduler | System `cron` calling `run_pipeline.sh` |
| Containers | Docker and Docker Compose |
| DB admin UI | pgAdmin 4 |
| Dashboard | Power BI Service (Import mode) |
| Data source | [Open-Meteo Historical Weather API](https://archive-api.open-meteo.com/v1/archive) - free, no API key |

---

## Data model

All `core.dim_*` tables use **SCD1** (overwrite on change). All `core.fact_*` tables source their data from `stage` only - no core-to-core data dependencies.

| Table | Grain | Key |
|---|---|---|
| `core.dim_calendar` | 1 day (1950–2050) | `id = YYYYMMDD` |
| `core.dim_time` | 1 minute (00:00–23:59) | `id = HHMM` |
| `core.dim_cities` | 1 city | `SERIAL` |
| `core.dim_countries` | 1 country | `SERIAL` |
| `core.dim_weather_code` | 1 WMO code | code itself (SMALLINT) |
| `core.dim_wind_direction` | 16 compass points | SMALLINT 1–16 |
| `core.fact_weather_hourly` | 1 hour × 1 city | BIGSERIAL, partitioned by year |
| `core.fact_weather_daily` | 1 day × 1 city | SERIAL, `UNIQUE(date_id, city_id)` |
| `mart.daily_weather` | 1 day × 1 city | `PRIMARY KEY (date, city_id)` |
| `mart.weather_forecast` | 1 (forecast_run_date, target_date, city_id) | composite PK, **insert-only** |

---

## Project structure

```
DE-weather_project/
├── Dockerfile
├── docker-compose.yml
├── .env                           # not committed - see .env.example
├── .env.example
├── requirements.txt
├── run_pipeline.sh                # cron wrapper - edit paths for your host
├── db_schema/                     # DDL - run once when setting up the DB
│   ├── 00_schemas.sql
│   ├── 01_stage_tables.sql
│   ├── 02_core_dimensions.sql
│   ├── 03_core_facts.sql
│   ├── 04_seed_data.sql
│   ├── 05_control_tables.sql
│   └── 06_mart_tables.sql
├── init-db/                       # auto-runs on first PostgreSQL container start
└── weather/                       # Bruin pipeline root
    ├── .bruin.yml
    ├── pipeline.yml
    └── assets/
        ├── config/
        │   └── cities.yml         # list of cities - single source of truth
        ├── stage/
        │   └── stage_weather_hourly.py
        ├── core/
        │   ├── core_dim_countries.py
        │   ├── core_dim_cities.py
        │   ├── core_dim_weather_code.py
        │   ├── core_fact_weather_hourly.sql
        │   ├── core_fact_weather_daily.sql
        │   └── core_backfill.py          # fills gaps between stage → core → mart
        └── mart/
            ├── mart_daily_weather.sql
            └── mart_weather_forecast.py  # Prophet 7-day forecast
```

---

## How to reproduce

### Prerequisites

- A host with **Docker** and **Docker Compose** installed.
- A `psql` client that can reach PostgreSQL.
- Git.

---

### Step 1 - Clone the repository

```bash
git clone https://github.com/Uematus/DE-weather_project.git
cd DE-weather_project
```

---

### Step 2 - Configure environment

```bash
cp .env.example .env
```

Fill in `.env`:

```env
POSTGRES_DB=weather_db
POSTGRES_USER=de_user
POSTGRES_PASSWORD=your_secure_password
POSTGRES_PORT=5433

BRUIN_PG_HOST=db
BRUIN_PG_PORT=5432
BRUIN_PG_DB=weather_db
BRUIN_PG_USER=de_user
BRUIN_PG_PASSWORD=your_secure_password

PGADMIN_DEFAULT_EMAIL=admin@example.com
PGADMIN_DEFAULT_PASSWORD=your_pgadmin_password
```

`POSTGRES_PORT` is the port on the host (default 5433). Inside Docker, PostgreSQL always listens on 5432.

---

### Step 3 - Start the stack

```bash
docker compose up -d
```

This starts three services:

| Service | Role | How to reach it |
|---|---|---|
| `db` | PostgreSQL 16 | `localhost:${POSTGRES_PORT}` |
| `bruin` | Bruin CLI and Python runner | `docker exec -it de_bruin bash` |
| `pgadmin` | DB admin UI | `http://<host-ip>:5050` |

---

### Step 4 - Initialise the database

Run the DDL files in order. They are idempotent (every `CREATE` uses `IF NOT EXISTS`).

```bash
psql -h localhost -p 5433 -U de_user -d weather_db \
  -f db_schema/00_schemas.sql \
  -f db_schema/01_stage_tables.sql \
  -f db_schema/02_core_dimensions.sql \
  -f db_schema/03_core_facts.sql \
  -f db_schema/04_seed_data.sql \
  -f db_schema/05_control_tables.sql \
  -f db_schema/06_mart_tables.sql
```

Notes:
- Keep this order - later files reference tables from earlier ones.
- `04_seed_data.sql` fills the static dimensions (`dim_calendar` for 1950–2050, `dim_time`, `dim_wind_direction`, `dim_weather_code`). It runs only once.
- `03_core_facts.sql` creates one yearly partition of `fact_weather_hourly` for every year from 1950 to 2050. No manual partition work is needed until 2050.

---

### Step 5 - Initialise the Bruin git context

Bruin expects its pipeline folder to be a git repo.

```bash
docker exec -it de_bruin bash -c "cd /app && git init"
```

---

### Step 6 - Validate the pipeline

```bash
docker compose run --rm bruin bruin validate /app
```

---

### Step 7 - Run the pipeline

```bash
docker compose run --rm bruin bruin run /app
```

Bruin reads the `@bruin` header in each asset and runs them in the right order:

```
stage.weather_hourly
    → core.dim_countries
    → core.dim_cities
    → core.dim_weather_code
    → core.fact_weather_hourly
    → core.fact_weather_daily
    → mart.daily_weather
    → core.backfill              # fills gaps between stage → core → mart
    → mart.weather_forecast      # 7-day Prophet forecast per active city
```

By default this loads **yesterday's data** for every city marked `active: true` in `cities.yml`.

The stage asset also fills old months that are still missing from `control.load_log`. If the Open-Meteo daily quota runs out part-way through a run, the asset exits cleanly (exit code 0) and the next run resumes where it stopped.

---

### Step 8 - Historical backfill

There is no separate backfill command. The stage asset runs **incremental first, then backfill** on every invocation:

1. Load yesterday for every active city.
2. Find monthly chunks (2010 → yesterday) that do not yet have a successful entry in `control.load_log` for every day, and load them.
3. Stop cleanly when the Open-Meteo daily quota (around 10,000 requests/day on the free tier) is hit.

Check progress:

```bash
docker exec de_postgres psql -U de_user -d weather_db -c \
  "SELECT city_name, COUNT(*) AS missing_days
   FROM control.v_date_gaps
   WHERE layer = 'stage'
   GROUP BY city_name
   ORDER BY city_name;"
```

When this returns 0 rows, the stage layer is fully loaded.

Core and mart gaps are filled automatically by `core.backfill` on every pipeline run - no extra step is needed.

To see how far the daily mart has caught up:

```bash
docker exec de_postgres psql -U de_user -d weather_db -c \
  "SELECT DATE_TRUNC('month', date)::DATE AS month,
          COUNT(DISTINCT city_id)         AS cities
   FROM mart.daily_weather
   GROUP BY 1 ORDER BY 1;"
```

---

### Step 9 - Schedule with cron

`run_pipeline.sh` is a small wrapper around `docker exec de_bruin bruin run /app`. **Edit the two paths at the top of the script** (`COMPOSE_DIR`, `LOG_DIR`) so they match where you cloned the repo, then:

```bash
chmod +x run_pipeline.sh
mkdir -p ~/logs

crontab -e
```

Add one line - the pipeline runs at 08:00, which matches the schedule declared in `weather/pipeline.yml`:

```
0 8 * * * /home/USER/run_pipeline.sh >> /home/USER/logs/pipeline.log 2>&1
```

Replace `/home/USER/...` with your actual paths.

---

## Operations reference

| Task | Command |
|---|---|
| Run the full pipeline | `docker compose run --rm bruin bruin run /app` |
| Run one asset | `docker compose run --rm bruin bruin run /app --asset stage.weather_hourly` |
| Run only the forecast | `docker compose run --rm bruin bruin run /app --asset mart.weather_forecast` |
| Validate the pipeline | `docker compose run --rm bruin bruin validate /app` |
| Check data gaps | `docker exec de_postgres psql -U de_user -d weather_db -c "SELECT * FROM control.v_date_gaps LIMIT 20;"` |
| Check today's forecast | `docker exec de_postgres psql -U de_user -d weather_db -c "SELECT city_id, target_date, horizon_days, temp_avg, precipitation_sum FROM mart.weather_forecast WHERE forecast_run_date = CURRENT_DATE ORDER BY city_id, horizon_days LIMIT 30;"` |
| Shell into the Bruin container | `docker exec -it de_bruin bash` |
| Open a `psql` session | `docker exec -it de_postgres psql -U de_user -d weather_db` |
| Watch the cron log | `tail -f ~/logs/pipeline.log` |
| Restart the stack | `docker compose restart` |

---

## Observability

The `control` schema tracks every load attempt.

| Object | What it does |
|---|---|
| `control.load_log` | One row per (run_date, city, layer). Stores `status`, rows loaded, expected rows, error message. |
| `control.v_date_gaps` | A view that lists every (date, city, layer) combination with no successful load. When it returns 0 rows, the pipeline is fully caught up. |

---

## Forecast table

`mart.weather_forecast` is **insert-only**. Every daily run appends up to `active_cities × 7` rows - 7 horizons per active city. Previous runs are never deleted, so we can later compare past forecasts to what actually happened.

| Column | Meaning |
|---|---|
| `forecast_run_date` | The day the model produced the forecast. |
| `target_date` | The day being predicted. |
| `horizon_days` | `target_date - forecast_run_date` (1 .. 7). |
| `city_id` | FK → `core.dim_cities`. |
| 8 metric columns | Point forecast (`yhat`) for `temp_avg`, `temp_min`, `temp_max`, `precipitation_sum`, `sunshine_hours`, `humidity_avg`, `pressure_avg`, `wind_speed_avg`. |
| `*_lower` / `*_upper` | 80 % confidence interval - only for `temp_avg` and `precipitation_sum`. |

Each (city, metric) pair gets its own Prophet model. The model is fit on up to 2 years of daily history from `core.fact_weather_daily` and predicts 7 days forward. Yearly seasonality is on; weekly and daily are off (weather has no weekday pattern).

> **The forecast is not yet visualised in Power BI.** The table is populated every day, but the dashboard does not read from it yet. Forecast vs actuals, horizon sensitivity, and accuracy tracking will be added in a later iteration.

---

## Dashboard

Power BI Service, Import mode, direct PostgreSQL connection. Current version: **v1.1** (dark theme).

**[Open the live report →](https://app.powerbi.com/view?r=eyJrIjoiNDYzZGNlMjctOWU4Ny00YmEwLTkwMjUtZDYzMjlmMjRmODMzIiwidCI6Ijk4MDVhYWI3LWFjNjMtNGQxMC04YmY4LTJmOWMxNWQyZGNlMiJ9)** (published to Power BI Service, no sign-in required).

What is on the page:

- **Title** - "European Weather Observatory".
- **Five KPI cards** - Avg Temperature, Total Precipitation, Avg Pressure, Sunshine Hours, Rainy Days, each with a year-over-year delta.
- **Chart metric switcher** - Avg temperature | Total precipitation | Avg pressure. Drives the dynamic line chart and the map title.
- **Ranking metric switcher** - Hot days % | Frost days % | Rainy days %. Drives the ranking visual.
- **Dynamic line chart** - Current vs Previous period, with a Month / Quarter / Year period switcher.
- **Ranking block** - "Ranking by Region / Country / City", shows the selected extreme-day percentage plus Max temp, Min temp, and Max wind gusts.
- **Climate Map (scatter)** - X = Avg temperature, Y = Total precipitation, bubble size = Avg humidity, colour = climate group (six human-readable groups collapsed from ten Köppen codes). A "Capitals only" toggle limits the plot to around 38 capital cities for readability.
- **ArcGIS map** - synced with the chart metric switcher.
- **Filters panel** - Period range, Region / Country / City tree, Climate zone slicer.

Data model inside Power BI: one fact table `mart.daily_weather`, the dimensions `core.dim_cities` and `core.dim_countries`, plus a Power BI-side calendar.

---

## Data source

[Open-Meteo Historical Weather API](https://archive-api.open-meteo.com/v1/archive) - free, no API key required.

- **Coverage:** 95 European cities, 2010 onwards.
- **Variables:** 39 hourly weather variables (temperature, humidity, precipitation, wind, radiation, soil conditions, WMO weather code, and more).
- **Rate limits:** roughly 1 request per second burst, around 10,000 requests per day on the free tier.
- **Backfill strategy:** monthly chunks, progress stored in `control.load_log` so the pipeline resumes cleanly after the daily quota resets.

---

## Known limits

- **API daily quota.** When the Open-Meteo free-tier quota is reached, the stage asset catches the response, logs `[DAILY LIMIT]`, exits cleanly (exit code 0), and waits for the next day to continue. The rest of the pipeline still runs on whatever was loaded.
- **Forecast needs data.** The forecast reads `core.fact_weather_daily`. Cities with less than 60 days of history in that table are skipped for the current run and picked up automatically once history grows.
- **One Prophet model per (city, metric).** Roughly 480 fits per run take several minutes and are CPU-bound. If you run the pipeline on a small host, look at the top of `weather/assets/mart/mart_weather_forecast.py` - you can change `HORIZON_DAYS`, `HISTORY_YEARS_MAX`, `UNCERTAINTY_SAMPLES_*`, or `GC_EVERY_N_CITIES` to reduce the load. Thread pinning (`OMP_NUM_THREADS=1` and friends) is already set so Stan does not spawn extra threads.
- **Partitions through 2050.** `fact_weather_hourly` has yearly partitions from 1950 to 2050. If the project lives past 2050, add partitions for the new years.
- **`run_pipeline.sh` paths.** The paths inside the script are placeholders. Edit them for your host before adding the script to cron.

---

## Capstone rubric

This project targets the evaluation criteria of the DataTalks.Club DE Zoomcamp capstone.

| Criterion | Where to look |
|---|---|
| Problem description | The "Problem" section at the top. |
| Cloud / infrastructure | Docker and Docker Compose, deployable on any VPS. See `Dockerfile` and `docker-compose.yml`. |
| Data ingestion (batch) | `stage.weather_hourly` - cron-scheduled, with quota handling and a monthly-chunk backfill. |
| Data warehouse | PostgreSQL 16 with four schemas (`stage` / `core` / `mart` / `control`) and a Kimball star schema in `core`. |
| Transformations | Bruin SQL and Python assets: SCD1 dimensions, hourly partitioned fact, daily aggregate, mart tables, and the Prophet forecast. |
| Dashboard | Power BI v1.1 - see the "Dashboard" section. |
| Reproducibility | `docker compose up -d` plus the seven DDL files. Full steps in "How to reproduce". |
