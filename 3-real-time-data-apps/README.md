# Real-time Data Applications 

## Set-up demo enviroment 

1. Create cluster with DBR version 8.3 or above
2. Clone this repo or download the notebook files 

## Create Stream of Events in Kafka

Use notebook 3.1 to generate stream of events and send to Kafka topic on Event Hubs

## Create Delta Live Table Pipeline

Go to Jobs > Delta Live Tables > Create Pipeline

Use the following settings to create the pipeline

- name: "ADB Essentials Lending Club Streaming"
- notebooks: 
  - path: "/Repos/your@email.com/adb-essentials-demos/3.2 - Lending Club DLT Part 1 - Python"
  - path: "/Repos/your@email.com/adb-essentials-demos/3.3 - Lending Club DLT Part 2 - SQL"
- target: "delta_adb_essentials_dlt"
- storage: "abfss://data@dltdemostorage.dfs.core.windows.net/adbessentials/delta_adb_essentials_dlt"
- autoscaling: false
- continuous: true

## Deploy to production 

If there are no errors deploy to production using the production button in the top right corner

## Query Tables Created

Run the notebook 3.4 to check that the tables have all been created correctly

# Useful Links

- [Delta Live Tables quickstart](https://docs.microsoft.com/en-us/azure/databricks/data-engineering/delta-live-tables/delta-live-tables-quickstart)
- [Use Azure Event Hubs from Apache Kafka applications](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview)