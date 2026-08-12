USE CATALOG marathos;

USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_time_events
  COMMENT "Serving view for gold layer, time-based events" AS
SELECT
  r.result_id,
  r.athlete_performance,
  r.athlete_average_speed,
  e.event_name,
  e.event_distance_length,
  e.event_distance_km,
  e.event_country,
  e.event_date,
  e.year_of_event,
  a.athlete_country,
  a.athlete_gender,
  a.athlete_age_category,
  a.athlete_year_of_birth
FROM
  fct_results r
    LEFT JOIN dim_event e
      ON r.event_id = e.event_id
    LEFT JOIN dim_athlete a
      ON r.athlete_id = a.athlete_id
WHERE
  r.event_type = 'time';