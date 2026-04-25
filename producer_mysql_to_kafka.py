import os
import json
import mysql.connector
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT")),
    "database": os.getenv("MYSQL_DB"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
}

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
INPUT_TOPIC = os.getenv("INPUT_TOPIC")


def main():
    # Етап 3: зчитування даних з MySQL таблиці athlete_event_results
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT *
        FROM athlete_event_results
        ORDER BY result_id DESC
        LIMIT 1000
        """)

    producer = KafkaProducer(
        bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS"),
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=os.getenv("USERNAME"),
        sasl_plain_password=os.getenv("PASSWORD"),
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    # Етап 3: запис даних у Kafka-топік athlete_event_results
    for row in cursor:
        producer.send(INPUT_TOPIC, value=row)
        print(f"Sent: {row}")

    producer.flush()
    cursor.close()
    conn.close()
    producer.close()


if __name__ == "__main__":
    main()