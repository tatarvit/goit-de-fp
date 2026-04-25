import os
import requests
from pyspark.sql import SparkSession

TABLES = {
    "athlete_bio": "https://ftp.goit.study/neoversity/athlete_bio.csv",
    "athlete_event_results": "https://ftp.goit.study/neoversity/athlete_event_results.csv",
}

spark = SparkSession.builder.appName("Landing to Bronze").getOrCreate()

os.makedirs("landing", exist_ok=True)

for table_name, url in TABLES.items():
    print(f"Downloading {table_name}...")

    csv_path = f"landing/{table_name}.csv"

    response = requests.get(url)
    response.raise_for_status()

    with open(csv_path, "wb") as file:
        file.write(response.content)

    print(f"Saved CSV to {csv_path}")

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)

    output_path = f"bronze/{table_name}"
    df.write.mode("overwrite").parquet(output_path)

    print(f"Saved Bronze table to {output_path}")
    df.show(5, truncate=False)

spark.stop()