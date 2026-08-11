CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_event
  COMMENT "Event dimension" AS
SELECT DISTINCT
  event_id,
  event_name,
  event_type,
  event_distance_length,
  event_distance_km,
  event_country,
  event_date,
  year_of_event,
  event_number_of_finishers
FROM
  marathos.silver.cleaned_marathos_2;