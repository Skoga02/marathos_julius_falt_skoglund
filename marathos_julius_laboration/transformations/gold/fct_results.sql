CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.fct_results
  COMMENT "Fact table - gold layer" AS
SELECT
  result_id,
  event_id,
  athlete_id,
  athlete_performance,
  athlete_average_speed
FROM
  marathos.silver.cleaned_marathos_2
WHERE
  event_date IS NOT NULL;