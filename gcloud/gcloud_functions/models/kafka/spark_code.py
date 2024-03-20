from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *  # Might need to import types

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-z1o60.europe-west1.gcp.confluent.cloud:9092") \
    .option("subscribe", "kafka-streaming-topic") \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option(
        "kafka.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required username='RMEQ7KRVMXNBPJBR' password='5blFzM3031yHbIqNiXhlMDTw4SPhSooIt4Kp1HVr6n198yZGpOY5blvvAtNm2vHd';"
    ) \
    .load()
