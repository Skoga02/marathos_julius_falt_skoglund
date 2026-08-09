from pyspark import pipelines as dp
from utilities.utils import rename_columns_to_snake_case
from pyspark.sql.functions import (
    col,
    when,
    regexp_extract,
    regexp_replace,
    trim,
    sha2,
    concat_ws,
    expr,
    upper,
    round as spark_round,
    coalesce,
    lit,
)

# ___ Regex patterns ___
TIME_PATTERN = r"^\d+:\d{2}:\d{2}\s*h$"
KM_PATTERN = r"^\d+\.?\d*\s*km$"
MI_PATTERN = r"^\d+\.?\d*mi$"
HOUR_PATTERN = r"^\d+h$"


@dp.table(
    name="marathos.silver.cleaned_marathos_2",
    comment="Cleaned marathon data (Silver layer)",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def cleaned_marathos():

    # ___ Read Bronze table and standardize column names ___

    df_cleaned = (
        rename_columns_to_snake_case(
            spark.readStream.format("delta").table("marathos.bronze.raw_supply_chain")
        )
        # Normalize country codes (e.g. swe -> SWE)
        .withColumn("athlete_country", upper(col("athlete_country")))
        # Remove unnecessary quotes and whitespace from event names
        .withColumn("event_name", trim(regexp_replace(col("event_name"), '"', "")))
    )

    # ___ Clean athlete information ___
    df_cleaned = df_cleaned.withColumn(
        "athlete_year_of_birth",
        when(
            col("athlete_year_of_birth").cast("int").between(1900, 2010),
            col("athlete_year_of_birth").cast("int"),
        ).otherwise(None),
    )

    # ___ Parse athlete performance ___
    df_cleaned = (
        df_cleaned
        # Distance events:
        # "4:52:39 h" -> total seconds
        .withColumn(
            "performance_seconds",
            when(
                col("athlete_performance").rlike(TIME_PATTERN),
                regexp_extract(
                    col("athlete_performance"),
                    r"^(\d+)",
                    1,
                ).cast("double")
                * 3600
                + regexp_extract(
                    col("athlete_performance"),
                    r":(\d{2}):",
                    1,
                ).cast("double")
                * 60
                + regexp_extract(
                    col("athlete_performance"),
                    r":(\d{2})\s*h",
                    1,
                ).cast("double"),
            ).otherwise(None),
        )
        # Time-based events:
        # "245.6 km" -> 245.6
        .withColumn(
            "performance_km",
            when(
                col("athlete_performance").rlike(KM_PATTERN),
                regexp_extract(
                    col("athlete_performance"),
                    r"^(\d+\.?\d*)",
                    1,
                ).cast("double"),
            ).otherwise(None),
        )
    )

    # ___ Parse event distance ___
    df_cleaned = (
        df_cleaned
        # Example:
        # "24h" -> 24
        .withColumn(
            "event_hours",
            when(
                col("event_distance_length").rlike(HOUR_PATTERN),
                regexp_extract(
                    col("event_distance_length"),
                    r"^(\d+)",
                    1,
                ).cast("double"),
            ).otherwise(None),
        )
        # Convert all distance events to kilometers
        .withColumn(
            "event_distance_km",
            when(
                col("event_distance_length").rlike(KM_PATTERN),
                regexp_extract(
                    col("event_distance_length"),
                    r"^(\d+\.?\d*)",
                    1,
                ).cast("double"),
            )
            .when(
                col("event_distance_length").rlike(MI_PATTERN),
                regexp_extract(
                    col("event_distance_length"),
                    r"^(\d+\.?\d*)",
                    1,
                ).cast("double")
                * 1.60934,
            )
            .otherwise(None),
        )
        # Identify whether the event is distance- or time-based
        .withColumn(
            "event_type",
            when(
                col("event_distance_length").rlike(r"^\d+\.?\d*\s*(km|mi)$"),
                "distance",
            )
            .when(
                col("event_distance_length").rlike(HOUR_PATTERN),
                "time",
            )
            .otherwise(None),
        )
    )

    # ___ Recalculate athlete average speed ___
    # The original athlete_average_speed column contains
    # incorrect values discovered during EDA.
    # Speed is recalculated from the parsed performance values.
    df_cleaned = df_cleaned.withColumn(
        "athlete_average_speed",
        # Time-based events
        when(
            col("event_hours").isNotNull()
            & col("performance_km").isNotNull()
            & (col("event_hours") > 0),
            spark_round(
                col("performance_km") / col("event_hours"),
                3,
            ),
        )
        # Distance-based events
        .when(
            col("event_distance_km").isNotNull()
            & col("performance_seconds").isNotNull()
            & (col("performance_seconds") > 0),
            spark_round(
                col("event_distance_km") / (col("performance_seconds") / 3600),
                3,
            ),
        ).otherwise(None),
    )

    # ___ Clean and parse event dates ___
    # Some events span multiple days, for example:
    # "31.12.2022-03.01.2023"
    # Only the start date is kept because it represents when the event begins.
    df_cleaned = (
        df_cleaned.withColumn(
            "event_dates_clean", regexp_replace(col("event_dates"), r"\.?-[0-9]+", "")
        )
        .withColumn("event_date", expr("try_to_date(event_dates_clean, 'dd.MM.yyyy')"))
        .drop("event_dates_clean")
    )

    # ___ Extract event country ___
    df_cleaned = df_cleaned.withColumn(
        "event_country",
        trim(
            regexp_extract(
                col("event_name"),
                r"\(([^)]+)\)\s*$",
                1,
            )
        ),
    )

    # ___ Normalize age category prefix (W -> F, to match gender coding) ___
    df_cleaned = df_cleaned.withColumn(
        "athlete_age_category",
        regexp_replace(col("athlete_age_category"), "^W", "F")
    )

    # ___ Generate surrogate keys ___
    # SHA-256 is deterministic and works with streaming tables.
    # The same input always produces the same identifier,regardless of processing order.
    df_cleaned = (
        df_cleaned.withColumn(
            "event_id",
            sha2(
                concat_ws(
                    "||",
                    col("event_name"),
                    col("event_date").cast("string"),
                ),
                256,
            ),
        )
        .withColumn(
            "athlete_id",
            sha2(
                concat_ws(
                    "||",
                    col("athlete_country"),
                    col("athlete_year_of_birth").cast("string"),
                    col("athlete_gender"),
                    col("athlete_age_category"),
                ),
                256,
            ),
        )
        .withColumn(
            "result_id",
            sha2(
                concat_ws(
                    "||",
                    col("event_id"),
                    col("athlete_id"),
                    col("event_date").cast("string"),
                    col("athlete_performance"),
                ),
                256,
            ),
        )
    )

    # ___ Filter invalid records ___
    df_cleaned = (
        df_cleaned
        # Remove multi-day performances
        .filter(~col("athlete_performance").rlike(r"^\d+d\s+"))
        # Remove day-based events (lab specification)
        .filter(~col("event_distance_length").rlike(r"(?i)\d+d$"))
        # Remove unrealistic average speeds
        .filter(col("athlete_average_speed").between(0.5, 20))
        # Remove missing performances
        .filter(col("athlete_performance").isNotNull())
        # Remove records with invalid countries
        .filter(col("athlete_country") != "XXX")
        # ___ Remove rows where age category prefix with athlete_gender, found during gold EDA ___
        .filter(
            ~coalesce(
                (col("athlete_age_category").startswith("F") & (col("athlete_gender") == "M"))
                | (col("athlete_age_category").startswith("M") & (col("athlete_gender") == "F")),
                lit(False)
            )
        )

    )

    # ___ Remove rows required for downstream analysis ___
    df_cleaned = df_cleaned.dropna(
        subset=[
            "athlete_performance",
            "event_date",
            "event_type",
            "athlete_average_speed",
        ]
    )

    # ___ Remove duplicate results ___
    df_cleaned = df_cleaned.dropDuplicates(
        [
            "event_id",
            "athlete_id",
            "athlete_performance",
        ]
    )

    # ___ Remove intermediate helper columns ___
    df_cleaned = df_cleaned.drop(
        "performance_seconds",
        "performance_km",
        "event_hours",
    )

    return df_cleaned
