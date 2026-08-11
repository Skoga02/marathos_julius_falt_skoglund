CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_athlete
  COMMENT "Athlete dimension" AS
SELECT DISTINCT
  athlete_id,
  athlete_country,
  athlete_year_of_birth,
  athlete_gender,
  athlete_age_category,
  athlete_club
FROM
  marathos.silver.cleaned_marathos_2;