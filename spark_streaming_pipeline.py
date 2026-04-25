import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    avg,
    current_timestamp,
    to_json,
    struct,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
INPUT_TOPIC = os.getenv("INPUT_TOPIC")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

MYSQL_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"


spark = SparkSession.builder \
    .appName("OlympicStreamingPipeline") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# Етап 1: зчитування фізичних даних атлетів з MySQL таблиці athlete_bio
athlete_bio_df = spark.read \
    .format("jdbc") \
    .option("url", MYSQL_URL) \
    .option("dbtable", "athlete_bio") \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASSWORD) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()


# Етап 2: фільтрація даних, де зріст або вага порожні чи не є числами
athlete_bio_clean_df = athlete_bio_df \
    .filter(col("height").isNotNull()) \
    .filter(col("weight").isNotNull()) \
    .filter(col("height").cast("double").isNotNull()) \
    .filter(col("weight").cast("double").isNotNull()) \
    .withColumn("height", col("height").cast("double")) \
    .withColumn("weight", col("weight").cast("double"))


# Схема JSON з Kafka
event_schema = StructType([
    StructField("athlete_id", IntegerType(), True),
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("country_noc", StringType(), True),
])


# Етап 3: зчитування результатів змагань з Kafka-топіку athlete_event_results
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT")\
    .option("kafka.sasl.mechanism", "PLAIN")\
    .option(
    "kafka.sasl.jaas.config",
    f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{USERNAME}" password="{PASSWORD}";'
    )\
    .load()


# Етап 3: перетворення JSON у dataframe-формат
events_df = kafka_stream_df \
    .selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), event_schema).alias("data")) \
    .select("data.*")


# Етап 4: об'єднання Kafka-даних з біологічними даними з MySQL за athlete_id
joined_df = events_df.alias("events").join(
    athlete_bio_clean_df.alias("bio"),
    on="athlete_id",
    how="inner"
    ).select(
        col("events.athlete_id"),
        col("events.sport"),
        col("events.medal"),
        col("events.country_noc").alias("country_noc"),
        col("bio.sex"),
        col("bio.height"),
        col("bio.weight")
        )


# Етап 5: обчислення середнього зросту і ваги
aggregated_df = joined_df \
    .groupBy(
        col("sport"),
        col("medal"),
        col("sex"),
        col("country_noc")
    ) \
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    ) \
    .withColumn("calc_timestamp", current_timestamp())


def write_to_kafka_and_mysql(batch_df, batch_id):
    print(f"Batch ID: {batch_id}")

    batch_df.show(truncate=False)

    # Етап 6.a): запис у вихідний Kafka-топік
    batch_df \
        .select(to_json(struct("*")).alias("value")) \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("topic", OUTPUT_TOPIC) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT")\
        .option("kafka.sasl.mechanism", "PLAIN")\
        .option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{USERNAME}" password="{PASSWORD}";'
            )\
        .save()

    # Етап 6.b): запис у MySQL базу даних
    batch_df.write \
        .format("jdbc") \
        .option("url", MYSQL_URL) \
        .option("dbtable", "athlete_avg_stats") \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()

# тест
# def write_to_kafka_and_mysql(batch_df, batch_id):
#     print(f"Batch ID: {batch_id}")
#     batch_df.show(50, truncate=False)


# Етап 6: запуск стриму через foreachBatch
query = aggregated_df.writeStream \
    .foreachBatch(write_to_kafka_and_mysql) \
    .outputMode("complete") \
    .option("checkpointLocation", "./checkpoint/athlete_avg_stats") \
    .start()

query.awaitTermination()