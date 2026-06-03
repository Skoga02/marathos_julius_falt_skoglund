from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
import re



# Ex. Event name -> event_name
def to_snake_case(name):
    name = name.strip().casefold()
    name = re.sub(r"[/]", "_", name)
    name = re.sub(r"[\s]+", "_", name)
    return name

def rename_columns_to_snake_case(df):
    new_columns = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_columns)
