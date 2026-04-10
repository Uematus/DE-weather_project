# European Weather Analytics Pipeline

End-to-end batch data pipeline collecting hourly weather data for 95 European cities from 2010 to present.

Built as a capstone project for [DataTalks.Club Data Engineering Zoomcamp 2026](https://github.com/DataTalksClub/data-engineering-zoomcamp).

---

## Problem Description

Weather data is collected by thousands of stations, but it is rarely structured for analysis across many cities and years at once. This project builds a reproducible pipeline that:

- Collects **hourly weather observations** for 95 European cities from 2010 to present
- Structures data in a **Kimball star schema** (PostgreSQL) for analytical queries
- Tracks **39 variables** per city-hour: temperature, precipitation, wind speed, solar radiation, soil moisture, and more

**Questions the data can answer:**
- Which cities have the most frost days per year?
- How do temperature extremes differ across Europe's climate zones?
- What is the trend in sunshine duration over 15 years?

---

## Architecture

```
Open-Meteo Historical API
        │
        ▼
stage.weather_hourly        ← Python asset (Bruin). Raw hourly data, all 39 variables.
        │                      Upsert by (city, date range). Logs every load to control.load_log.
        │
        ├──► core.dim_countries    (SCD1 merge from cities.yml)
        ├──► core.dim_cities       (SCD1 merge from stage + cities.yml metadata)
        ├──► core.dim_weather_code (SCD1 merge — new WMO codes from stage)
        ├──► core.fact_weather_hourly  (partitioned by year, 2010–2030)
        └──► core.fact_weather_daily   (aggregated from stage directly)

Static dimensions — seeded once, not managed by Bruin:
        core.dim_calendar        (1950–2050, id = YYYYMMDD integer)
        core.dim_time            (00:00–23:59 per minute, id = HHMM integer)
        core.dim_wind_direction  (16 compass points)

mart.*  ← in progress (Power BI-ready views)
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Ingestion + Transformation | [Bruin CLI](https://github.com/bruin-data/bruin) (Python + SQL assets) |
| Scheduling | System cron on VPS |
| Storage / Data Warehouse | PostgreSQL 16 (stage / core / mart / control schemas) |
| Containerization | Docker + Docker Compose |
| DB Admin UI | pgAdmin 4 |
| Dashboard | Power BI Service (Import mode) |
| Data Source | [Open-Meteo Historical Weather API](https://archive-api.open-meteo.com/v1/archive) |

---

## Data Model

All dimensions use **SCD1** (overwrite on change). All core tables source data from `stage` only — no core-to-core data dependencies.

| Table | Grain | Key type |
|---|---|---|
| `dim_calendar` | one row per day (1950–2050) | `id = YYYYMMDD` integer |
| `dim_time` | one row per minute (00:00–23:59) | `id = HHMM` integer |
| `dim_cities` | one row per city | SERIAL |
| `dim_countries` | one row per country | SERIAL |
| `dim_weather_code` | one row per WMO weather code | WMO code (SMALLINT) |
| `dim_wind_direction` | 16 compass directions | SMALLINT 1–16 |
| `fact_weather_hourly` | 1 hour × 1 city | BIGSERIAL, partitioned by year |
| `fact_weather_daily` | 1 day × 1 city | SERIAL, UNIQUE(date_id, city_id) |

---

## Project Structure

```
DE-weather_project/
├── Dockerfile
├── docker-compose.yml
├── .env                         # not committed — see .env.example
├── .env.example
├── requirements.txt
├── run_pipeline.sh
├── db_schema/                   # DDL scripts — run manually once (infrastructure setup)
│   ├── 00_schemas.sql           # CREATE SCHEMA: stage, core, mart, control
│   ├── 01_stage_tables.sql      # stage.weather_hourly
│   ├── 02_core_dimensions.sql   # all dim_* tables
│   ├── 03_core_facts.sql        # fact tables + year partitions (1950–2050)
│   ├── 04_seed_data.sql         # static dims: calendar, time, wind direction, WMO codes
│   └── 05_control_tables.sql    # control.load_log + control.v_date_gaps view
├── init-db/                     # auto-executed on first PostgreSQL container start
└── weather/                     # Bruin pipeline root
    ├── .bruin.yml
    ├── pipeline.yml
    └── assets/
        ├── config/
        │   └── cities.yml       # 95 European cities — single source of truth
        ├── stage/
        │   └── stage_weather_hourly.py
        └── core/
            ├── core_dim_countries.py
            ├── core_dim_cities.py
            ├── core_dim_weather_code.py
            ├── core_fact_weather_hourly.sql
            └── core_fact_weather_daily.sql
```

---

## How to Reproduce

### Prerequisites

- VPS or server with **Docker** and **Docker Compose** installed
- **psql** client available locally (or run DDL from inside the container)
- Git

---

### Step 1 — Clone the repository

```bash
git clone https://github.com/Uematus/DE-weather_project.git
cd DE-weather_project
```

---

### Step 2 — Configure environment

```bash
cp .env.example .env
```

Edit `.env` with your values:

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

`POSTGRES_PORT` is the external port on your server (default 5433). Inside Docker, PostgreSQL always listens on 5432.

---

### Step 3 — Build and start containers

```bash
docker compose up -d
```

This starts three services:

| Service | Description | Access |
|---|---|---|
| `db` | PostgreSQL 16 | `localhost:${POSTGRES_PORT}` |
| `bruin` | Bruin CLI + Python runner | — |
| `pgadmin` | Database admin UI | `http://<server-ip>:5050` |

---

### Step 4 — Initialize the database

Run DDL scripts in order. They create all schemas, tables, partitions, and seed static dimensions.

```bash
psql -h localhost -p 5433 -U de_user -d weather_db \
  -f db_schema/00_schemas.sql \
  -f db_schema/01_stage_tables.sql \
  -f db_schema/02_core_dimensions.sql \
  -f db_schema/03_core_facts.sql \
  -f db_schema/04_seed_data.sql \
  -f db_schema/05_control_tables.sql
```

> **Important:** Keep this order. `05_control_tables.sql` creates a view that references `core.dim_cities`, so it must run last.

> **Note on seed data:** `04_seed_data.sql` populates `dim_calendar` (1950–2050), `dim_time` (per minute), `dim_wind_direction`, and `dim_weather_code` (WMO codes with descriptions). This runs only once and is not updated by the pipeline.

> **Note on partitions:** `03_core_facts.sql` automatically generates year partitions for `fact_weather_hourly` from 1950 to 2050 using a PL/pgSQL loop — no manual statements needed.

---

### Step 5 — Initialize Bruin git context

Bruin CLI requires the pipeline directory to be a git repository.

```bash
docker exec -it de_bruin bash -c "cd /app && git init"
```

---

### Step 6 — Validate the pipeline

```bash
docker compose run --rm bruin bruin validate /app
```

---

### Step 7 — Run the pipeline

```bash
docker compose run --rm bruin bruin run /app
```

Bruin resolves dependencies and runs assets in order:

```
stage.weather_hourly
    → core.dim_countries
    → core.dim_cities
    → core.dim_weather_code
    → core.fact_weather_hourly
    → core.fact_weather_daily
    → mart.daily_weather
    → core.backfill          ← gap detection and fill for core + mart
```

By default this loads **yesterday's data** for all cities marked `active: true` in `cities.yml`.

`core.backfill` runs last and automatically detects months where `stage` has more data than `core` or `mart`, then fills those gaps. This is the key mechanism for keeping all layers in sync during the multi-day backfill process.

---

### Step 8 — Load historical data (backfill)

The pipeline supports two independent modes:

**Mode 1 — Daily incremental** (via Bruin, runs in cron):
```bash
docker exec de_bruin bruin run /app
```
Loads yesterday's data for all active cities. This is the normal daily operation.

**Mode 2 — Backfill** (directly via Python, bypasses Bruin):
```bash
docker exec de_bruin bash -c "BACKFILL=1 python /app/assets/stage/stage_weather_hourly.py"
```
Reads `control.load_log`, finds all monthly chunks not yet loaded from 2010 to yesterday, and fetches them one by one. When the Open-Meteo daily API limit is reached, the script exits cleanly (exit code 0). Run it again the next day — it resumes from where it stopped.

**Check stage backfill progress:**
```bash
docker exec de_postgres psql -U de_user -d weather_db -c \
  "SELECT city_name, COUNT(*) AS missing_days
   FROM control.v_date_gaps
   WHERE layer = 'stage'
   GROUP BY city_name
   ORDER BY city_name;"
```
When the query returns 0 rows, stage backfill is complete.

**Core and mart gaps are filled automatically** by `core.backfill` on every pipeline run — no separate command needed. To check how far core/mart have caught up:
```bash
docker exec de_postgres psql -U de_user -d weather_db -c \
  "SELECT
     DATE_TRUNC('month', date)::DATE AS month,
     COUNT(DISTINCT city_name)       AS cities_loaded
   FROM mart.daily_weather
   GROUP BY 1
   ORDER BY 1;"
```

---

### Step 9 — Set up cron

Make the pipeline script executable:
```bash
chmod +x run_pipeline.sh
mkdir -p ~/logs
```

Open crontab:
```bash
crontab -e
```

Add these two jobs:
```
# Daily incremental pipeline — runs at 08:00 every day
0 8 * * * docker exec de_bruin bruin run /app >> ~/logs/weather_pipeline.log 2>&1

# Backfill — runs at 02:00, remove this line when backfill is complete
0 2 * * * docker exec de_bruin bash -c "BACKFILL=1 python /app/assets/stage/stage_weather_hourly.py" >> ~/logs/weather_backfill.log 2>&1
```

Remove the backfill cron line when `control.v_date_gaps` returns 0 rows.

---

## Pipeline Operations Reference

| Task | Command |
|---|---|
| Run full pipeline | `docker compose run --rm bruin bruin run /app` |
| Run single asset | `docker compose run --rm bruin bruin run /app --asset stage.weather_hourly` |
| Run backfill manually | `docker exec de_bruin bash -c "BACKFILL=1 python /app/assets/stage/stage_weather_hourly.py"` |
| Check data gaps | `docker exec de_postgres psql -U de_user -d weather_db -c "SELECT * FROM control.v_date_gaps LIMIT 20;"` |
| Shell into Bruin container | `docker exec -it de_bruin bash` |
| Shell into PostgreSQL | `docker exec -it de_postgres psql -U de_user -d weather_db` |
| View pipeline logs | `tail -f ~/logs/weather_pipeline.log` |
| Restart all services | `docker compose restart` |

---

## Observability

The `control` schema tracks every load attempt:

| Object | Description |
|---|---|
| `control.load_log` | One row per (date, city, layer). Stores status, row count, expected rows, error message. |
| `control.v_date_gaps` | View showing all missing (date, city, layer) combinations. Zero rows = pipeline healthy. |

---

## City Configuration

`weather/assets/config/cities.yml` is the single source of truth for all city metadata:
- Which cities are loaded (`active: true/false`)
- Country, ISO codes, continent, region
- `is_capital`, `climate_zone`

To disable a city, set `active: false` — no code changes needed.

---

## Data Source

[Open-Meteo Historical Weather API](https://archive-api.open-meteo.com/v1/archive) — free, no API key required.

- **Coverage:** 95 European cities, 2010–present
- **Variables:** 39 hourly weather variables (temperature, humidity, precipitation, wind, radiation, soil conditions, WMO weather code, and more)
- **Rate limits:** ~1 request/second burst, ~10,000 requests/day on free tier
- **Backfill strategy:** monthly chunks (~720 rows each), progress saved in `control.load_log`

---

## Dashboard

Power BI Service (Import mode, direct PostgreSQL connection) — **in progress**.
