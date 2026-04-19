-- =============================================================================
-- 04_seed_data.sql
-- Run once after 02_core_dimensions.sql and 03_core_facts.sql.
-- Populates static dimensions: dim_calendar, dim_time, dim_wind_direction,
-- dim_weather_code.
-- Dynamic dimensions (dim_cities, dim_countries) are populated by Bruin assets.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- dim_calendar  (1950-01-01 → 2050-12-31)
-- id = YYYYMMDD integer
-- ---------------------------------------------------------------------------
INSERT INTO core.dim_calendar (
    id, date, year, quarter, month, month_name,
    week_of_year, day, day_of_week, day_name, is_weekend, is_leap_year
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT                                     AS id,
    d::DATE                                                          AS date,
    EXTRACT(YEAR    FROM d)::SMALLINT                                AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT                                AS quarter,
    EXTRACT(MONTH   FROM d)::SMALLINT                                AS month,
    TO_CHAR(d, 'Month')                                              AS month_name,
    EXTRACT(WEEK    FROM d)::SMALLINT                                AS week_of_year,
    EXTRACT(DAY     FROM d)::SMALLINT                                AS day,
    EXTRACT(ISODOW  FROM d)::SMALLINT                                AS day_of_week,
    TO_CHAR(d, 'Day')                                                AS day_name,
    EXTRACT(ISODOW  FROM d) IN (6, 7)                                AS is_weekend,
    (EXTRACT(YEAR FROM d)::INT % 4 = 0 AND (
        EXTRACT(YEAR FROM d)::INT % 100 <> 0 OR
        EXTRACT(YEAR FROM d)::INT % 400 = 0
    ))                                                               AS is_leap_year
FROM generate_series('1950-01-01'::date, '2050-12-31'::date, '1 day') AS d
ON CONFLICT (id) DO NOTHING;


-- ---------------------------------------------------------------------------
-- dim_time  (00:00 → 23:59, per minute)
-- id = HHMM integer (e.g. 14:35 → 1435)
-- ---------------------------------------------------------------------------
INSERT INTO core.dim_time (
    id, time_value, hour_24h, hour_12h, minute, am_pm,
    time_24h, time_12h, part_of_day
)
SELECT
    (EXTRACT(HOUR FROM t)::INT * 100 + EXTRACT(MINUTE FROM t)::INT)::INT AS id,
    t::TIME                                                                AS time_value,
    EXTRACT(HOUR   FROM t)::SMALLINT                                       AS hour_24h,
    CASE
        WHEN EXTRACT(HOUR FROM t) = 0  THEN 12
        WHEN EXTRACT(HOUR FROM t) <= 12 THEN EXTRACT(HOUR FROM t)::INT
        ELSE (EXTRACT(HOUR FROM t) - 12)::INT
    END::SMALLINT                                                          AS hour_12h,
    EXTRACT(MINUTE FROM t)::SMALLINT                                       AS minute,
    CASE WHEN EXTRACT(HOUR FROM t) < 12 THEN 'AM' ELSE 'PM' END           AS am_pm,
    TO_CHAR(t, 'HH24:MI')                                                  AS time_24h,
    CASE
        WHEN EXTRACT(HOUR FROM t) = 0  THEN '12:' || TO_CHAR(t, 'MI') || 'AM'
        WHEN EXTRACT(HOUR FROM t) < 12 THEN TO_CHAR(t, 'FMHH:MI') || 'AM'
        WHEN EXTRACT(HOUR FROM t) = 12 THEN '12:' || TO_CHAR(t, 'MI') || 'PM'
        ELSE (EXTRACT(HOUR FROM t) - 12)::INT::TEXT || ':' || TO_CHAR(t, 'MI') || 'PM'
    END                                                                    AS time_12h,
    CASE
        WHEN EXTRACT(HOUR FROM t) BETWEEN  6 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM t) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM t) BETWEEN 18 AND 23 THEN 'Evening'
        ELSE 'Night'
    END                                                                    AS part_of_day
FROM generate_series('2000-01-01 00:00'::timestamp,
                     '2000-01-01 23:59'::timestamp,
                     '1 minute') AS t
ON CONFLICT (id) DO NOTHING;


-- ---------------------------------------------------------------------------
-- dim_wind_direction  (16 compass points)
-- ---------------------------------------------------------------------------
INSERT INTO core.dim_wind_direction (id, degree_from, degree_to, compass_short, compass_full)
VALUES
    ( 1,   0,  11, 'N',   'North'),
    ( 2,  12,  33, 'NNE', 'North-Northeast'),
    ( 3,  34,  56, 'NE',  'Northeast'),
    ( 4,  57,  78, 'ENE', 'East-Northeast'),
    ( 5,  79, 101, 'E',   'East'),
    ( 6, 102, 123, 'ESE', 'East-Southeast'),
    ( 7, 124, 146, 'SE',  'Southeast'),
    ( 8, 147, 168, 'SSE', 'South-Southeast'),
    ( 9, 169, 191, 'S',   'South'),
    (10, 192, 213, 'SSW', 'South-Southwest'),
    (11, 214, 236, 'SW',  'Southwest'),
    (12, 237, 258, 'WSW', 'West-Southwest'),
    (13, 259, 281, 'W',   'West'),
    (14, 282, 303, 'WNW', 'West-Northwest'),
    (15, 304, 326, 'NW',  'Northwest'),
    (16, 327, 360, 'NNW', 'North-Northwest')
ON CONFLICT (id) DO NOTHING;


-- ---------------------------------------------------------------------------
-- dim_weather_code  (WMO Weather Interpretation Codes)
-- ---------------------------------------------------------------------------
INSERT INTO core.dim_weather_code (id, description, severity, precipitation_type)
VALUES
    -- Clear / cloudy
    ( 0, 'Clear sky',                              'None',   NULL),
    ( 1, 'Mainly clear',                           'None',   NULL),
    ( 2, 'Partly cloudy',                          'None',   NULL),
    ( 3, 'Overcast',                               'None',   NULL),
    -- Fog
    (45, 'Fog',                                    'Low',    NULL),
    (48, 'Depositing rime fog',                    'Low',    NULL),
    -- Drizzle
    (51, 'Drizzle: light',                         'Low',    'Rain'),
    (53, 'Drizzle: moderate',                      'Low',    'Rain'),
    (55, 'Drizzle: dense',                         'Low',    'Rain'),
    -- Freezing drizzle
    (56, 'Freezing drizzle: light',                'Medium', 'Mixed'),
    (57, 'Freezing drizzle: dense',                'Medium', 'Mixed'),
    -- Rain
    (61, 'Rain: slight',                           'Low',    'Rain'),
    (63, 'Rain: moderate',                         'Medium', 'Rain'),
    (65, 'Rain: heavy',                            'High',   'Rain'),
    -- Freezing rain
    (66, 'Freezing rain: light',                   'Medium', 'Mixed'),
    (67, 'Freezing rain: heavy',                   'High',   'Mixed'),
    -- Snow
    (71, 'Snowfall: slight',                       'Low',    'Snow'),
    (73, 'Snowfall: moderate',                     'Medium', 'Snow'),
    (75, 'Snowfall: heavy',                        'High',   'Snow'),
    (77, 'Snow grains',                            'Low',    'Snow'),
    -- Rain showers
    (80, 'Rain showers: slight',                   'Low',    'Rain'),
    (81, 'Rain showers: moderate',                 'Medium', 'Rain'),
    (82, 'Rain showers: violent',                  'High',   'Rain'),
    -- Snow showers
    (85, 'Snow showers: slight',                   'Low',    'Snow'),
    (86, 'Snow showers: heavy',                    'High',   'Snow'),
    -- Thunderstorm
    (95, 'Thunderstorm: slight or moderate',       'High',   'Rain'),
    (96, 'Thunderstorm with slight hail',          'High',   'Mixed'),
    (99, 'Thunderstorm with heavy hail',           'High',   'Mixed')
ON CONFLICT (id) DO NOTHING;
