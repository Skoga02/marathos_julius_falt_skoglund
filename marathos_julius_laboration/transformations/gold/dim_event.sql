CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_event
  COMMENT "Event dimension" AS
SELECT
  event_id,
  FIRST(event_name) AS event_name,
  FIRST(event_type) AS event_type,
  FIRST(event_distance_length) AS event_distance_length,
  FIRST(event_distance_km) AS event_distance_km,
  FIRST(event_country) AS event_country,
  FIRST(event_date) AS event_date,
  FIRST(year_of_event) AS year_of_event,
  SUM(event_number_of_finishers) AS event_number_of_finishers
FROM
  marathos.silver.cleaned_marathos_2
GROUP BY
  event_id;