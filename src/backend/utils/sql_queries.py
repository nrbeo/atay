"""
SQL Queries module for ATAY Backend API.

Centralizes all SQL queries used throughout the application.
Queries are organized by domain for maintainability.
"""

# =============================================================================
# META / SYSTEM QUERIES
# =============================================================================

META_LIST_TABLES = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;
"""

META_TABLE_ROW_COUNT = "SELECT COUNT(*) AS count FROM {table};"

# Tables used in the star schema
STAR_SCHEMA_TABLES = [
    "dim_date",
    "dim_location",
    "dim_shape",
    "dim_weather_station",
    "dim_frshtt",
    "fact_ufo_observation",
    "ufo_comments_raw",
]


# =============================================================================
# DIMENSION QUERIES
# =============================================================================

# Shape dimension
DIM_SHAPE_LIST = """
SELECT shape_key, shape, shape_category
FROM dim_shape
ORDER BY shape_key;
"""

DIM_SHAPE_BY_ID = """
SELECT shape_key, shape, shape_category
FROM dim_shape
WHERE shape_key = %s;
"""

# FRSHTT (weather conditions) dimension
DIM_FRSHTT_LIST = """
SELECT frshtt_key, fog, rain, snow, hail, thunder, tornado, blue_sky, label
FROM dim_frshtt
ORDER BY frshtt_key;
"""

DIM_FRSHTT_BY_ID = """
SELECT frshtt_key, fog, rain, snow, hail, thunder, tornado, blue_sky, label
FROM dim_frshtt
WHERE frshtt_key = %s;
"""

# Location dimension
DIM_LOCATION_BASE = """
SELECT location_key, city, state, country, latitude, longitude
FROM dim_location
WHERE 1=1
"""

DIM_LOCATION_BY_ID = """
SELECT location_key, city, state, country, latitude, longitude
FROM dim_location
WHERE location_key = %s;
"""

# Weather station dimension
DIM_WEATHER_STATION_LIST = """
SELECT station_key, station_id, station_name,
       station_latitude, station_longitude, station_elevation
FROM dim_weather_station
ORDER BY station_key
LIMIT %s OFFSET %s;
"""

DIM_WEATHER_STATION_BY_ID = """
SELECT station_key, station_id, station_name,
       station_latitude, station_longitude, station_elevation
FROM dim_weather_station
WHERE station_key = %s;
"""

# Date dimension
DIM_DATE_RANGE = """
SELECT MIN(full_date) AS start_date,
       MAX(full_date) AS end_date
FROM dim_date;
"""

DIM_DATE_BY_ID = """
SELECT date_key, full_date, year, month, day, day_of_week, quarter, is_weekend, season
FROM dim_date
WHERE date_key = %s;
"""


# =============================================================================
# UFO OBSERVATION QUERIES
# =============================================================================

UFO_OBSERVATION_BASE = """
SELECT
    f.fact_id,
    d.full_date AS date,
    l.city,
    l.state,
    l.country,
    s.shape,
    ws.station_name,
    f.duration_seconds,
    f.temp_mean,
    f.visibility_mean,
    f.has_comment,
    fr.label AS frshtt_label,
    l.latitude,
    l.longitude
FROM fact_ufo_observation f
JOIN dim_date d ON f.date_key = d.date_key
LEFT JOIN dim_location l ON f.location_key = l.location_key
LEFT JOIN dim_shape s ON f.shape_key = s.shape_key
LEFT JOIN dim_weather_station ws ON f.station_key = ws.station_key
LEFT JOIN dim_frshtt fr ON f.frshtt_key = fr.frshtt_key
WHERE 1=1
"""

UFO_OBSERVATION_BY_ID = """
SELECT
    f.fact_id,
    d.full_date AS date,
    l.city,
    l.state,
    l.country,
    s.shape,
    ws.station_name,
    f.duration_seconds,
    f.temp_mean,
    f.visibility_mean,
    f.has_comment,
    fr.label AS frshtt_label,
    l.latitude,
    l.longitude
FROM fact_ufo_observation f
JOIN dim_date d ON f.date_key = d.date_key
LEFT JOIN dim_location l ON f.location_key = l.location_key
LEFT JOIN dim_shape s ON f.shape_key = s.shape_key
LEFT JOIN dim_weather_station ws ON f.station_key = ws.station_key
LEFT JOIN dim_frshtt fr ON f.frshtt_key = fr.frshtt_key
WHERE f.fact_id = %s;
"""

# Comment retrieval
UFO_COMMENT_BY_FACT_ID = """
SELECT comment_text AS comment
FROM ufo_comments_raw
WHERE fact_id = %s;
"""


# =============================================================================
# MAP QUERIES
# =============================================================================

MAP_POINTS_BASE = """
SELECT
    l.latitude,
    l.longitude,
    l.city,
    l.country,
    COUNT(*) AS count
FROM fact_ufo_observation f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_location l ON f.location_key = l.location_key
WHERE l.latitude IS NOT NULL
  AND l.longitude IS NOT NULL
"""

MAP_POINTS_GROUP_BY = """
GROUP BY l.latitude, l.longitude, l.city, l.country
ORDER BY COUNT(*) DESC
LIMIT %s;
"""

MAP_HEATMAP_BASE = """
SELECT
    l.latitude,
    l.longitude,
    COUNT(*) AS intensity
FROM fact_ufo_observation f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_location l ON f.location_key = l.location_key
WHERE l.latitude IS NOT NULL
  AND l.longitude IS NOT NULL
"""

MAP_HEATMAP_GROUP_BY = """
GROUP BY l.latitude, l.longitude
ORDER BY intensity DESC
LIMIT %s;
"""


# =============================================================================
# STATISTICS QUERIES
# =============================================================================

STATS_OVERVIEW = """
SELECT
    (SELECT COUNT(*) FROM fact_ufo_observation) AS total_observations,
    (SELECT COUNT(*) FROM dim_location) AS total_locations,
    (SELECT COUNT(*) FROM dim_shape) AS total_shapes,
    (SELECT COUNT(*) FROM dim_weather_station) AS total_stations,
    (SELECT COUNT(*) FROM dim_frshtt) AS total_frshtt_patterns,
    (SELECT MIN(full_date) FROM dim_date) AS period_start,
    (SELECT MAX(full_date) FROM dim_date) AS period_end;
"""

STATS_TOP_LOCATIONS_BASE = """
SELECT
    l.city,
    l.country,
    COUNT(*) AS sightings
FROM fact_ufo_observation f
JOIN dim_location l ON f.location_key = l.location_key
WHERE 1=1
"""

STATS_TOP_LOCATIONS_GROUP_BY = """
GROUP BY l.city, l.country
ORDER BY sightings DESC
LIMIT %s;
"""

STATS_BY_SEASON = """
SELECT d.season, COUNT(*) AS sightings
FROM fact_ufo_observation f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.season
ORDER BY sightings DESC;
"""

STATS_BY_WEATHER = """
SELECT fr.label, COUNT(*) AS sightings
FROM fact_ufo_observation f
JOIN dim_frshtt fr ON f.frshtt_key = fr.frshtt_key
GROUP BY fr.label
ORDER BY sightings DESC;
"""

STATS_BY_SHAPE = """
SELECT s.shape, s.shape_category, COUNT(*) AS sightings
FROM fact_ufo_observation f
JOIN dim_shape s ON f.shape_key = s.shape_key
GROUP BY s.shape, s.shape_category
ORDER BY sightings DESC;
"""

STATS_BY_YEAR = """
SELECT d.year, COUNT(*) AS sightings
FROM fact_ufo_observation f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year
ORDER BY d.year;
"""

STATS_TIME_SERIES_MONTHLY_BASE = """
SELECT
    d.year,
    d.month,
    COUNT(*) AS observations
FROM fact_ufo_observation f
JOIN dim_date d ON f.date_key = d.date_key
LEFT JOIN dim_location l ON f.location_key = l.location_key
LEFT JOIN dim_shape s ON f.shape_key = s.shape_key
WHERE 1=1
"""

STATS_TIME_SERIES_MONTHLY_GROUP_BY = """
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
"""

STATS_DURATION_DISTRIBUTION = """
SELECT
    CASE
        WHEN f.duration_seconds < 60 THEN '< 1 min'
        WHEN f.duration_seconds < 300 THEN '1-5 min'
        WHEN f.duration_seconds < 900 THEN '5-15 min'
        WHEN f.duration_seconds < 3600 THEN '15-60 min'
        ELSE '> 1 hour'
    END AS duration_bucket,
    COUNT(*) AS count
FROM fact_ufo_observation f
WHERE f.duration_seconds IS NOT NULL
GROUP BY duration_bucket
ORDER BY MIN(f.duration_seconds);
"""

STATS_TOP_COUNTRIES = """
SELECT
    l.country,
    COUNT(*) AS sightings
FROM fact_ufo_observation f
JOIN dim_location l ON f.location_key = l.location_key
WHERE l.country IS NOT NULL
GROUP BY l.country
ORDER BY sightings DESC
LIMIT %s;
"""
