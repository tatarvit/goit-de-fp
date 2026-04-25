from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim
from pyspark.sql.types import StringType

TABLES = ["athlete_bio", "athlete_event_results"]

spark = SparkSession.builder.appName("Bronze to Silver").getOrCreate()

def clean_text_columns(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                trim(regexp_replace(col(field.name), r"[^a-zA-Z0-9А-Яа-яІіЇїЄєҐґ\s\-\.,]", ""))
            )
    return df

for table_name in TABLES:
    print(f"Processing {table_name}...")

    input_path = f"bronze/{table_name}"
    output_path = f"silver/{table_name}"

    df = spark.read.parquet(input_path)

    clean_df = clean_text_columns(df).dropDuplicates()

    clean_df.write.mode("overwrite").parquet(output_path)

    print(f"Saved Silver table to {output_path}")
    clean_df.show(5, truncate=False)

spark.stop()