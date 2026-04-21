-- BigQuery procedural SQL with DECLARE, IF/THEN, CREATE TEMP TABLE.
-- Multi-statement scripts like this are listed as "Not supported" in
-- OpenMetadata's parser (collate-sqllineage).

DECLARE cutoff_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);

CREATE TEMP TABLE filtered_events AS
SELECT
    user_id,
    event_name,
    event_timestamp,
    event_params
FROM `project.analytics.raw_events`
WHERE event_date >= cutoff_date;

CREATE OR REPLACE TABLE `project.analytics.user_engagement` AS
SELECT
    user_id,
    COUNT(DISTINCT event_name) AS distinct_events,
    COUNT(*) AS total_events,
    MAX(event_timestamp) AS last_active
FROM filtered_events
GROUP BY user_id;
