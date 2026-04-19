-- =============================================================================
-- 02_core_dimensions.sql
-- Run once before starting Bruin. Creates all core dimension tables.
-- Data is populated by 04_seed_data.sql (static dims) and Bruin assets (dynamic dims).
-- =============================================================================


-- ---------------------------------------------------------------------------
-- dim_calendar
-- Populated by seed (1950-01-01 → 2050-12-31). Never updated by pipeline.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.dim_calendar (
    id              INT         PRIMARY KEY,   -- surrogate key: YYYYMMDD format (e.g. 20260325)
    date            DATE        NOT NULL UNIQUE,
    year            SMALLINT    NOT NULL,
    quarter         SMALLINT    NOT NULL,      -- 1..4
    month           SMALLINT    NOT NULL,      -- 1..12
    month_name      TEXT        NOT NULL,      -- 'January'..'December'
    week_of_year    SMALLINT    NOT NULL,      -- ISO week 1..53
    day             SMALLINT    NOT NULL,      -- 1..31
    day_of_week     SMALLINT    NOT NULL,      -- ISO: 1=Monday .. 7=Sunday
    day_name        TEXT        NOT NULL,      -- 'Monday'..'Sunday'
    is_weekend      BOOLEAN     NOT NULL,
    is_leap_year    BOOLEAN     NOT NULL
);


-- ---------------------------------------------------------------------------
-- dim_time
-- Populated by seed (00:00:00 → 23:59:00, per minute). Never updated.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.dim_time (
    id          INT         PRIMARY KEY,   -- surrogate key: HHMM format (e.g. 1435)
    time_value  TIME        NOT NULL UNIQUE,
    hour_24h    SMALLINT    NOT NULL,      -- 0..23
    hour_12h    SMALLINT    NOT NULL,      -- 1..12
    minute      SMALLINT    NOT NULL,      -- 0..59
    am_pm       CHAR(2)     NOT NULL,      -- 'AM' | 'PM'
    time_24h    TEXT        NOT NULL,      -- '14:35'
    time_12h    TEXT        NOT NULL,      -- '2:35PM'
    part_of_day TEXT        NOT NULL       -- 'Night' | 'Morning' | 'Afternoon' | 'Evening'
                CHECK (part_of_day IN ('Night', 'Morning', 'Afternoon', 'Evening'))
    -- Night:     00:00-05:59
    -- Morning:   06:00-11:59
    -- Afternoon: 12:00-17:59
    -- Evening:   18:00-23:59
);


-- ---------------------------------------------------------------------------
-- dim_wind_direction
-- Populated by seed (16 compass points). Never updated.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.dim_wind_direction (
    id              SMALLINT    PRIMARY KEY,
    degree_from     SMALLINT    NOT NULL,
    degree_to       SMALLINT    NOT NULL,
    compass_short   CHAR(3)     NOT NULL,  -- 'N', 'NNE', 'NE'...
    compass_full    TEXT        NOT NULL   -- 'North', 'North-Northeast'...
);


-- ---------------------------------------------------------------------------
-- dim_countries
-- Populated by Bruin asset (MERGE from stage via cities.yml). SCD1.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.dim_countries (
    id              SERIAL      PRIMARY KEY,
    country_name    TEXT        NOT NULL UNIQUE,
    iso_code_2      CHAR(2),               -- GB, IE, DE...
    iso_code_3      CHAR(3),               -- GBR, IRL, DEU...
    continent       TEXT,                  -- 'Europe'
    region          TEXT,                  -- 'Western Europe', 'Northern Europe'...
    loaded_at       TIMESTAMP   NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP   NOT NULL DEFAULT now()
);


-- ---------------------------------------------------------------------------
-- dim_cities
-- Populated by Bruin asset (MERGE from stage via cities.yml). SCD1.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.dim_cities (
    id              SERIAL      PRIMARY KEY,
    city_name       TEXT        NOT NULL UNIQUE,
    latitude        FLOAT       NOT NULL,
    longitude       FLOAT       NOT NULL,
    elevation_m     FLOAT,
    timezone        TEXT,
    is_capital      BOOLEAN,
    climate_zone    TEXT,                  -- Köppen code: 'Cfb', 'BSk'...
    country_id      INT         REFERENCES core.dim_countries(id),
    loaded_at       TIMESTAMP   NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP   NOT NULL DEFAULT now()
);


-- ---------------------------------------------------------------------------
-- dim_weather_code
-- Pre-populated by seed with all known WMO codes + descriptions.
-- Bruin asset may INSERT unknown codes (description = NULL). SCD1.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.dim_weather_code (
    id              SMALLINT    PRIMARY KEY,  -- WMO code itself (0..99)
    description     TEXT,                     -- 'Clear sky', 'Thunderstorm'...
    severity        TEXT                      -- 'None' | 'Low' | 'Medium' | 'High'
                    CHECK (severity IN ('None', 'Low', 'Medium', 'High')),
    precipitation_type TEXT                   -- NULL | 'Rain' | 'Snow' | 'Mixed'
                    CHECK (precipitation_type IN ('Rain', 'Snow', 'Mixed')),
    loaded_at       TIMESTAMP   NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP   NOT NULL DEFAULT now()
);
