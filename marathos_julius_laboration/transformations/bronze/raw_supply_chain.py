from pyspark import pipelines as dp


"""
Streaming table definition for the bronze layer 
"""
BASE_DIR = "/Volumes/marathos/default/raw"

schema = (
    spark.read.format("csv")
    .options(header=True, inferSchema=True)
    .load(f"{BASE_DIR}/data/TWO_CENTURIES_OF_UM_RACES.csv")
    .schema
)


@dp.table(
    name="marathos.bronze.raw_supply_chain",
    comment="Raw data from the bronze layer",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)

def raw_supply_chain():
    return (
        spark.readStream.format("csv")
        .options(header=True, encoding="UTF-8")
        .schema(schema)
        .load(f"{BASE_DIR}/data")
    )