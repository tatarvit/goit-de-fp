from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, round, col

spark = SparkSession.builder.appName("Silver to Gold").getOrCreate()

bio_df = spark.read.parquet("silver/athlete_bio").alias("bio")
results_df = spark.read.parquet("silver/athlete_event_results").alias("res")

joined_df = results_df.join(
    bio_df,
    col("res.athlete_id") == col("bio.athlete_id"),
    "inner"
)

gold_df = joined_df.groupBy(
    col("res.sport").alias("sport"),
    col("res.medal").alias("medal"),
    col("bio.sex").alias("sex"),
    col("bio.country_noc").alias("country_noc")
).agg(
    round(avg(col("bio.weight")), 2).alias("avg_weight"),
    round(avg(col("bio.height")), 2).alias("avg_height")
).withColumn(
    "timestamp",
    current_timestamp()
)

gold_df.write.mode("overwrite").parquet("gold/avg_stats")

print("Gold table: gold/avg_stats")
gold_df.show(30, truncate=False)

spark.stop()