# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Event Hubs Kafka Connection

# COMMAND ----------

connSharedAccessKey = dbutils.secrets.get(scope = "my_secret_scope", key = "eventHubKafkaKey")

TOPIC = "kafka-lendingclub"
BOOTSTRAP_SERVERS = "my-event-hub.servicebus.windows.net:9093"
EH_SASL = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://my-event-hub.servicebus.windows.net/;SharedAccessKeyName=KafkaSendListen;SharedAccessKey=" + connSharedAccessKey + "\";"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Bronze Live Table - Read from Kafka

# COMMAND ----------

import dlt

@dlt.table(
  name="lendingclub_raw",
  comment="The lending club streaming dataset, ingested from Event Hubs kafka topic.",
  table_properties={
    "quality": "raw"
  }
)
def lendingclub_raw():
  stream_lendingclub_raw = (spark.readStream
    .format("kafka")
    .option("subscribe", TOPIC)
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "60000")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "latest")
    .load())
  return stream_lendingclub_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Silver Live Table

# COMMAND ----------

lendingclub_schema = StructType([ \
    StructField("event_type", StringType(), True), \
    StructField("loan_id", IntegerType(),True), \
    StructField("funded_amnt", FloatType(),True), \
    StructField("payment_amnt", FloatType(),True), \
    StructField("type", StringType(), True), \
    StructField("addr_state", StringType(), True)
  ])

# COMMAND ----------

@dlt.table(
  name="lendingclub_bronze",
  comment="Lending club events with value parsed as columns.",
  table_properties={
    "quality": "bronze"
  }
)
@dlt.expect("valid_kafka_message", "key IS NOT NULL")
def lendingclub_bronze():
  stream_lendingclub_bronze = (dlt.read_stream("lendingclub_raw")
  .select(
    col("timestamp"),
    col("key").cast("string"),
    from_json(col("value").cast("string"), lendingclub_schema).alias("parsed_value")
   )
  .select("timestamp", "key", "parsed_value.*"))
  
  return stream_lendingclub_bronze
