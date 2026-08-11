CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_athlete
  COMMENT "Dim table - gold layer" AS
SELECT DISTINCT
  athlete_id,
  athlete_country,
  athlete_year_of_birth,
  athlete_gender,
  athlete_age_category
FROM
  marathos.silver.cleaned_marathos_2;