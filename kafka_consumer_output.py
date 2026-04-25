import os
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

consumer = KafkaConsumer(
    os.getenv("OUTPUT_TOPIC"),
    bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS"),
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username=os.getenv("USERNAME"),
    sasl_plain_password=os.getenv("PASSWORD"),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: v.decode("utf-8"),
)

for message in consumer:
    print(message.value)
