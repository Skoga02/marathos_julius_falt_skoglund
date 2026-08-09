from pyspark import pipelines as dp
from utilities.utils import rename_columns_to_snake_case
from pyspark.sql.functions import (
    col,
    when,
    regexp_extract,
    round as spark_round,
    expr,
    regexp_replace,
    trim,
    sha2,
    concat_ws,
)

@dp.table(
    name="marathos.silver.cleaned_marathos", 
    comment="Cleaned data silver layer",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
        }
)

def cleaned_marathos():
    df_cleaned = (
        rename_columns_to_snake_case(
            spark.readStream
                .format("delta")
                .table("marathos.bronze.raw_supply_chain")
        )

        .withColumn(
                "athlete_year_of_birth",
                when(
                    col("athlete_year_of_birth").cast("int").between(1900, 2010),
                    col("athlete_year_of_birth").cast("int")
                ).otherwise(None)
            )

            # ___ Parse performance to seconds (for km/mi events "4:52:39 h") ___
            .withColumn(
                "performance_seconds",
                when(
                    col("athlete_performance").rlike(r"^\d+:\d{2}:\d{2}\s*h$"),
                    regexp_extract(col("athlete_performance"), r"^(\d+)", 1).cast("double") * 3600 +
                    regexp_extract(col("athlete_performance"), r":(\d{2}):", 1).cast("double")* 60 +
                    regexp_extract(col("athlete_performance"), r":(\d{2})\s*h", 1).cast("double")
                ).otherwise(None)
            )

            # ___ Prase performace to km (for km/mi events "4.52 km") ___
            .withColumn(
                "performance_km",
                when(
                    col("athlete_performance").rlike(r"^\d+\.?\d*\s*km$"),
                    regexp_extract(col("athlete_performance"), r"^(\d+\.?\d*)", 1).cast("double")
                ).otherwise(None)
            )

            # ___ Prase event hours (for h events: "24h" -> 24.0) ___ 
            .withColumn(
                "event_hours",
                when(
                    col("event_distance_length").rlike(r"^\d+h$"),
                    regexp_extract(col("event_distance_length"), r"^(\d+)", 1).cast("double")
                ).otherwise(None)
            )

            # ___ Prase event distance to km (for km/mi events "4.52 km") ___
            .withColumn(
                "event_distance_km",
                when(
                    col("event_distance_length").rlike(r"^\d+\.?\d*\s*km$"),
                    regexp_extract(col("event_distance_length"), r"^(\d+\.?\d*)", 1).cast("double")
                )
                .when(
                    col("event_distance_length").rlike(r"^\d+\.?\d*mi$"),
                    regexp_extract(col("event_distance_length"), (r"^(\d+\.?\d*)"), 1).cast("double") * 1.60934
                ).otherwise(None)
            )

            # ___ event_type: distance or time base event ___
            .withColumn(
                "event_type",
                when(col("event_distance_length").rlike(r"^\d+\.?\d*\s*(km|mi)$"), "distance")
                .when(col("event_distance_length").rlike(r"^\d+h$"), "time")
                .otherwise(None)
            )

            # ___ Recalculate average speed from scratch ___
            # For h events: speed = km convered / hours 
            # For km/mi events: speed = distance / (seconds / 3600)
            # Original values not trusted due to misplaced data found in EDA 
            .withColumn(
                "athlete_average_speed",
                when(
                    col("event_hours").isNotNull() & col("performance_km").isNotNull() & (col("event_hours") > 0),
                    spark_round(col("performance_km") / col("event_hours"), 3)
                )
                .when(
                    col("event_distance_km").isNotNull() & col("performance_seconds").isNotNull() & (col("performance_seconds") > 0),
                    spark_round(col("event_distance_km") / (col("performance_seconds") / 3600), 3)
                )
                .otherwise(None)
            )

            #  ___ Clean event_dates -> keep only start date ___
            # Many events span multiple days e.g "32.12.2022-03.01.2023"
            # Decision: keep only start date as it represents when race begins
            .withColumn(
                "event_dates_clean",
                regexp_replace(col("event_dates"), r"\.?-[0-9]+", "")
            )

            .withColumn(
                "event_date",
                expr("try_to_date(event_dates_clean, 'dd.MM.yyyy')")
            )
            .drop("event_dates_clean")

            # ___ Extract event country from event name e.g "Stockholm Marathon (SWE)" -> "SWE" ___
            .withColumn(
                "event_name",
                regexp_replace(col("event_name"), '"', "")
            )
            .withColumn(
                "event_country",
                trim(regexp_extract(col("event_name"), r"\(([^)]+)\)\s*$", 1))
            )

            # ___ Generate surrogate IDs with sha2 ___
            # sha2 is used instead of dense_rank() becuase dense_rank() does not work -
            # on streaming tables. sha2 is deterministic and stateless - same input 
            # always produce the same hash regardless of processing order
            .withColumn(
                "event_id",
                sha2(concat_ws("||", col("event_name"), col("event_date").cast("string")), 256)
            )
            .withColumn(
                "athlete_id",
                sha2(concat_ws("||",
                    col("athlete_country"),
                    col("athlete_year_of_birth").cast("string"),
                    col("athlete_gender"),
                    col("athlete_age_category")
                    ), 256)
            )
            .withColumn(
                "result_id",
                sha2(concat_ws("||",
                    col("event_id"),
                    col("athlete_id"),
                    col("event_date"),
                    col("athlete_performance"),
                    ), 256)
            )
            # ___ filters ___
            # Drop multiple-day performance e.g "3d 02:03:00 h" - per lab specs
            .filter(~col("athlete_performance").rlike(r"^\d+d\s+"))
            # Drop d (days) distancce events per lab specs
            .filter(~col("event_distance_length").rlike(r"(?i)\bd\b"))
            # Drop speeds outside human possible range for ultramarathons
            .filter(col("athlete_average_speed").between(0.5, 25))
            # Drop rows with null perforamnce
            .filter(col("athlete_performance").isNotNull())

            .drop(
                "performance_seconds",
                "performance_km",
                "event_hours",
            )
    )

    # # Drop nulls for performance, date, event_type and average_speed otherwise the analysis will fail
    df_cleaned = df_cleaned.dropna(subset=[
    "athlete_performance",
    "event_date",
    "event_type",
    "athlete_average_speed",

])
    
    
    return df_cleaned