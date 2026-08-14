# Marathos Lab

A data engineering project built for **Marathos**, a global marathon-hosting company, as part of a Databricks and data engineering course collaboration. The project implements a medallion architecture (bronze → silver → gold) pipeline, dimensional model, dashboard, and Genie chatbot on top of a global ultramarathon results dataset.
___
## Purpose

Build a data platform and ETL pipeline to help Marathos business stakeholders make data-driven decisions, using Databricks, PySpark, and Plotly.

___
## Tech stack

- Databricks (Delta Live Tables / Lakeflow Pipelines)
- Python / PySpark
- SQL
- Plotly
- Databricks AI/BI Dashboards
- Databricks Genie
- Git & GitHub

___
## Data source

Raw ultramarathon results dataset, ingested into `marathos.default.raw` (Unity Catalog volume) and loaded into `marathos.bronze.raw_supply_chain`.

---

## Repository structure

```
marathos_<fname>_<lname>
├── dimensional_modeling
├── explorations
│   ├── bronze_eda
│   ├── silver_eda
│   └── gold_eda
├── transformations
│   ├── bronze
│   ├── silver
│   └── gold
└── utils
```

## Unity Catalog structure

```
marathos
├── bronze
├── default
│   └── raw
├── gold
├── information_schema
└── silver
```

---
## Pipeline overview

**Bronze** — raw ingestion of the marathon results dataset into `marathos.bronze.raw_supply_chain`, no transformations applied.

**Silver** — `marathos.silver.cleaned_marathos_2`: column names normalized to snake_case, athlete performance parsed into typed fields, event distance/duration parsed and standardized to kilometers, `athlete_average_speed` recalculated from scratch (source values were unreliable), event dates cleaned to extract start dates from date ranges, invalid records filtered (multi-day performances, day-unit events, unknown-country athletes, unrealistic speeds), surrogate keys (`event_id`, `athlete_id`, `result_id`) generated via SHA-256 hashing.

**Gold** — dimensional model built on top of silver:
- `dim_event` — one row per `event_id`
- `dim_athlete` — one row per `athlete_id`
- `fct_results` — one row per result, includes `event_type` sourced directly from silver (see known bugs below for why)
- `mart_distance_events` / `mart_time_events` — serving views split by marathon type, used directly by the dashboard

---
## Dimensional model

See `dimensional_modeling/` for the dbdiagram DBML source.

```dbml
Table dim_event {
  event_id varchar [primary key]
  event_name varchar
  event_type varchar
  event_distance_length varchar
  event_distance_km double
  event_country varchar
  event_date date
  year_of_event integer
  event_number_of_finishers integer
}

Table dim_athlete {
  athlete_id varchar [primary key]
  athlete_country varchar
  athlete_year_of_birth integer
  athlete_gender varchar
  athlete_age_category varchar
}

Table fct_results {
  result_id varchar [primary key]
  event_id varchar [ref: > dim_event.event_id]
  athlete_id varchar [ref: > dim_athlete.athlete_id]
  event_type varchar
  athlete_performance varchar
  athlete_average_speed double
}
```

---
## Dashboard

The Marathos Dashboard provides:
- **KPIs:** total results, unique events, unique athlete profiles, represented countries
- **Charts:** participation growth over time, country representation, gender speed comparison, top 10 most popular events, speed by age category, distance vs. time event split
- **Filter:** distance range slider (km) for distance-based events
- **Link:** direct access to Marathos Genie for ad hoc questions

*Link to published dashboard:* `https://dbc-e59ebbc1-d8ba.cloud.databricks.com/dashboardsv3/01f19662f9f81516b9171eb0e208b25c/published?o=7474656484280922&f_fc076892%7Efilter-year-event.i=2022%2C1977&f_da4ff67c%7E09102084.f=45`

---

## Marathos Genie

A Genie space was configured over the gold layer datasets to let business stakeholders ask ad hoc questions without writing SQL.

*Link to Genie space:* `https://dbc-e59ebbc1-d8ba.cloud.databricks.com/dashboardsv3/01f19662f9f81516b9171eb0e208b25c/published?o=7474656484280922&f_fc076892%7Efilter-year-of-event=_all_`

**Verification:** A handful of Genie-recommended questions were manually cross-checked against notebook queries to confirm answer correctness before handoff to stakeholders. See `explorations/gold_eda` for the verification queries.

---
---

## Known bugs

### dim_event bug

**What I discovered:**
`dim_event` had 79,793 rows compared to 79,560 unique `event_id` values. The cause was that `event_id` is hashed using only `event_name` and `event_date` in the silver layer, while multiple rows can represent different distance classes held on the same event and date.

**Example:**
"Ultra Trail Ibiza (ESP)" on 2017-12-02 had both an 85 km and a 46 km class. Both were assigned the same `event_id` but had different values for `event_distance_length`, `event_distance_km`, and `event_number_of_finishers`. This caused `SELECT DISTINCT` to add extra rows to `dim_event`, which in turn caused join fan-out downstream.

**Decision:**
Kept the original `event_id` hash and instead solved the fan-out by aggregating `dim_event` per `event_id`, using `FIRST()` for most attributes and `SUM()` for `event_number_of_finishers` (since it represents a genuine partial count per distance class, not an arbitrary variant). This introduces a known limitation: `dim_event` can only show one distance class per event, rather than all classes that were actually held.

---

### dim_athlete bug

**What I discovered:**
`dim_athlete` had 1.9M rows compared to 38,690 unique `athlete_id` values — roughly a 49x fan-out. This caused a downstream issue where `mart_distance_events` exploded to 331M written rows.

**Cause:**
`athlete_id` is hashed using only `athlete_country`, `athlete_year_of_birth`, `athlete_gender`, and `athlete_age_category` in the silver layer. However, `dim_athlete`'s original definition used `SELECT DISTINCT` across all columns, including `athlete_club`, which is not part of the hash. Since the same `athlete_id` can legitimately appear with many different `athlete_club` values, `SELECT DISTINCT` produced one row per unique (`athlete_id`, `athlete_club`) combination instead of one row per `athlete_id`.

**Decision:**
Dropped `athlete_club` from `dim_athlete` entirely, since it is not part of the athlete's identity as defined by the hash. This keeps the dimension at exactly one row per `athlete_id`.

---

### event_type misclassification bug

**What I discovered:**
Silver-to-mart consistency checks showed `mart_distance_events` had 108 more rows than expected, and `mart_time_events` had exactly 108 fewer — matching, opposite deviations pointing to systematic misclassification.

**Cause:**
16 events have both distance and time classes under the same `event_name` + `event_date`, causing a collision in `event_id`. `FIRST(event_type)` in `dim_event` arbitrarily picked one type per `event_id`, which meant some results were misclassified compared to their true `event_type` on their own silver row.

**Decision:**
Added `event_type` directly to `fct_results` (sourced per-row from silver, not from `dim_event`), and changed both marts to filter on `fct_results.event_type` instead of `dim_event.event_type`. This is correct because `event_type` is a property of each individual result, not of the event as a whole.

---

### Cross-month date range parsing (accepted, not fixed)

**What I discovered:**
Events spanning a month boundary (e.g. `"20.05.-10.06.2018"`) failed to parse under the original date-cleaning regex, which only stripped the day portion following the dash. This produced an invalid date string, causing `try_to_date` to return `null`, and since `event_date` is required, these rows were silently dropped by `dropna`.

**Quantified impact:**
71,498 bronze rows matched this date-range format; 54,096 of those would otherwise have passed every other cleaning filter (~0.7% of the bronze dataset). The affected rows are concentrated in August and skew toward long-format events (100km, 24h, 100mi).

**Decision:**
Accepted this data loss given its relatively small size and documented it here as a known, quantified limitation, rather than modifying the regex.

---

### Gender / age-category mismatch (fixed)

**What I discovered:**
4 rows had an age category prefix that disagreed with `athlete_gender` (e.g. age category `"F45"` but gender `"M"`).

**Decision:**
Added a filter to drop rows where the age category prefix and `athlete_gender` disagree, since this is almost certainly a source data-entry error affecting a negligible number of rows.

---

## Known limitations

- `athlete_id` represents a demographic profile (country, birth year, gender, age category), not a verified individual identity. Two different real athletes sharing all four attributes and posting the same result at the same event will collide into the same `athlete_id`.
- `dim_event` shows only one distance class per `event_id` when an event had multiple simultaneous classes. *Example:* if a race included both an 85 km and a 52 km class on the same day, only one is reflected in `dim_event`.

---

## LLM usage

An LLM (Claude) was used throughout this lab for debugging assistance, EDA design, and pipeline review — not to solve entire tasks. Specific uses:

- Improving and debugging regex patterns for performance/distance parsing:
  ```python
  TIME_PATTERN = r"^\d+:\d{2}:\d{2}\s*h$"
  KM_PATTERN = r"^\d+\.?\d*\s*km$"
  ```
- Debugging assistance during investigation of the cross-month date-range bug in silver layer validation (see `silver_eda_2`, cell 25):
  ```python
  cross_month_pattern = r"^\d{2}\.\d{2}\.-\d{2}\.\d{2}\.\d{4}$"
  ```
- Identifying and diagnosing the `dim_event` and `dim_athlete` fan-out bugs during gold layer validation, including designing the grain-check and mart-consistency EDA queries used to catch them.

---

Built by `Julius Fält Skoglund` as part of the Big Data and Cloud – Practical Development and NoSQL. 