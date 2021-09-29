# Databricks notebook source
# MAGIC %md
# MAGIC # Real-time Data Applications Demo
# MAGIC 
# MAGIC Demo showcased in the [Azure Databricks Essentials webinar](https://databricks.com/p/webinar/azure-databricks-essentials-series) 
# MAGIC 
# MAGIC ## Event Hubs (Kafka) & Delta Live Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting up Event Hubs Kafka Topic
# MAGIC 
# MAGIC 1. Follow the instructions to create an Event Hubs Kafka topic - [Quickstart: Data streaming with Event Hubs using the Kafka protocol](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-quickstart-kafka-enabled-event-hubs)
# MAGIC 2. Create a [Databricks Secret](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/) for the Event Hubs Namespace SharedAccessKey

# COMMAND ----------

# Get Databricks secret value 
connSharedAccessKey = dbutils.secrets.get(scope = "my_secret_scope", key = "eventHubKafkaKey")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Producer
# MAGIC 
# MAGIC The producer will generate Lending Club event data as a real-time stream into Event Hubs (Kafka Protocol)

# COMMAND ----------

from pyspark.sql.functions import lit, col
from pyspark.sql.types import LongType, StringType
from pyspark.sql.functions import udf
import random
import json

# COMMAND ----------

lspq_path = "/databricks-datasets/samples/lending_club/parquet/"
loan_stats = spark.read.parquet(lspq_path)
df_states = loan_stats.groupBy("addr_state").count()
states_array = [str(row.addr_state) for row in df_states.filter("count > 1").collect()]
print(states_array)

# COMMAND ----------

def randomState():
#   validStates= [u'AR', u'SD', u'MT', u'LA', u'FL']
  validStates = states_array
  return validStates[random.randint(0,len(validStates)-1)]

def randomLoanPaidAmount():
  loan_amount = round(random.uniform(1000,25000), 2)
  paid_amount = round(loan_amount - random.uniform(10, loan_amount-1000), 2)
  return [loan_amount, paid_amount]

def randomLoanId():
  return random.randint(1000,5000)

# COMMAND ----------

def genLoanPaymentEventSchema():
    
    loan_payment = randomLoanPaidAmount()
    
    event = {
      "event_type": u"loan_payment",
      "loan_id": randomLoanId(),
      "funded_amnt": loan_payment[0],
      "payment_amnt": loan_payment[1],
      "type": u"batch",
      "addr_state": randomState()
    }

    event_string = json.dumps(event)
    return event_string
  
print(genLoanPaymentEventSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create stream of events

# COMMAND ----------

gen_event_schema_udf = udf(genLoanPaymentEventSchema, StringType())

source_schema = (
  spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .load()
    .withColumn("key", lit("event"))
    .withColumn("value", lit(gen_event_schema_udf()))
    .select("key", col("value").cast("string"))
)

display(source_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set Kafka Event Hubs config

# COMMAND ----------

TOPIC = "kafka-lendingclub"
BOOTSTRAP_SERVERS = "my-event-hub-kafka.servicebus.windows.net:9093"
EH_SASL = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://my-event-hub-kafka.servicebus.windows.net/;SharedAccessKeyName=KafkaSendListen;SharedAccessKey=" + connSharedAccessKey + "\";"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to stream to Event Hubs Kafka

# COMMAND ----------

# Write df to EventHubs using Spark's Kafka connector
write = (source_schema.writeStream
    .format("kafka")
    .outputMode("append")
    .option("topic", TOPIC)
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("checkpointLocation", "/tmp/lendingclub-kafka/events/_checkpoint")
    .trigger(processingTime='120 seconds')
    .start())

# COMMAND ----------

write.status

# COMMAND ----------

write.lastProgress

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consumer
# MAGIC 
# MAGIC The consumer will be created using Delta Live Tables to consume the Event Hubs Kafka stream and write to Delta tables
