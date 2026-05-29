-- =============================================================================
-- analytics_queries.sql
-- Summary queries for the weather_pipeline.hourly_observations table
-- Replace `your-gcp-project-id` with your actual BigQuery project ID
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Query 1: Daily temperature summary per city (last 7 days)
-- Use case: Quick daily digest — how hot/cold was each city?
-- -----------------------------------------------------------------------------
SELECT
    city,
    date,
    ROUND(AVG(temperature_c), 2)          AS avg_temp_c,
    ROUND(MIN(temperature_c), 2)          AS min_temp_c,
    ROUND(MAX(temperature_c), 2)          AS max_temp_c,
    ROUND(AVG(apparent_temperature_c), 2) AS avg_feels_like_c,
    ROUND(AVG(relative_humidity_pct), 1)  AS avg_humidity_pct,
    ROUND(SUM(precipitation), 2)          AS total_precipitation_mm,
    COUNTIF(is_rainy_hour)                AS rainy_hours,
    ROUND(AVG(windspeed_kmh), 1)          AS avg_windspeed_kmh
FROM
    `your-gcp-project-id.weather_pipeline.hourly_observations`
WHERE
    date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY
    city, date
ORDER BY
    date DESC, city;


-- -----------------------------------------------------------------------------
-- Query 2: Hottest hours across all cities (Top 20)
-- Use case: Identify peak heat periods for operational planning
-- -----------------------------------------------------------------------------
SELECT
    city,
    timestamp,
    temperature_c,
    apparent_temperature_c,
    feels_like_delta,
    heat_index_category,
    relative_humidity_pct
FROM
    `your-gcp-project-id.weather_pipeline.hourly_observations`
ORDER BY
    apparent_temperature_c DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- Query 3: Heat index category distribution by city
-- Use case: Understand comfort conditions at a glance
-- -----------------------------------------------------------------------------
SELECT
    city,
    heat_index_category,
    COUNT(*)                                              AS hour_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY city), 1) AS pct_of_hours
FROM
    `your-gcp-project-id.weather_pipeline.hourly_observations`
GROUP BY
    city, heat_index_category
ORDER BY
    city,
    hour_count DESC;


-- -----------------------------------------------------------------------------
-- Query 4: Hourly average temperature by hour-of-day (time-of-day pattern)
-- Use case: Which hours are hottest? Useful for scheduling, marketing send times
-- -----------------------------------------------------------------------------
SELECT
    city,
    hour_of_day,
    ROUND(AVG(temperature_c), 2)         AS avg_temp_c,
    ROUND(AVG(relative_humidity_pct), 1) AS avg_humidity_pct,
    ROUND(AVG(windspeed_kmh), 1)         AS avg_windspeed_kmh
FROM
    `your-gcp-project-id.weather_pipeline.hourly_observations`
GROUP BY
    city, hour_of_day
ORDER BY
    city, hour_of_day;


-- -----------------------------------------------------------------------------
-- Query 5: Cities ranked by total rainfall in the period
-- Use case: Top-N aggregation — which city got the most rain?
-- -----------------------------------------------------------------------------
SELECT
    city,
    ROUND(SUM(precipitation), 2)   AS total_rain_mm,
    COUNTIF(is_rainy_hour)         AS rainy_hours,
    COUNT(*)                       AS total_hours,
    ROUND(
        COUNTIF(is_rainy_hour) * 100.0 / COUNT(*), 1
    )                              AS rainy_hour_pct
FROM
    `your-gcp-project-id.weather_pipeline.hourly_observations`
GROUP BY
    city
ORDER BY
    total_rain_mm DESC;


-- -----------------------------------------------------------------------------
-- Query 6: Most common weather conditions per city
-- Use case: What type of weather dominated during this period?
-- -----------------------------------------------------------------------------
SELECT
    city,
    weather_description,
    COUNT(*) AS hours,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY city), 1) AS pct
FROM
    `your-gcp-project-id.weather_pipeline.hourly_observations`
GROUP BY
    city, weather_description
ORDER BY
    city, hours DESC;


-- -----------------------------------------------------------------------------
-- Query 7: Deduplication check — find duplicate city+timestamp pairs
-- Use case: Data quality check after multiple pipeline runs
-- -----------------------------------------------------------------------------
SELECT
    city,
    timestamp,
    COUNT(*) AS row_count
FROM
    `your-gcp-project-id.weather_pipeline.hourly_observations`
GROUP BY
    city, timestamp
HAVING
    COUNT(*) > 1
ORDER BY
    row_count DESC;