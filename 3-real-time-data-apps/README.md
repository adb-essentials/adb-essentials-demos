# Real-time Data Applications 

## Set up environment

Follow instructions in [SET-UP-ENVIROMENT.md](../SET-UP-ENVIROMENT.md) to create ADLS storage account and Event Hubs Kafka topic

## Create Stream of Events in Kafka

Use notebook 3.1 to generate stream of events that will be sent to the to Event Hubs Kafka topic

## Create Delta Live Table Pipeline

Go to Jobs > Delta Live Tables > Create Pipeline

Use the following settings to create the pipeline

- name: "ADB Essentials Lending Club Streaming"
- notebooks: 
  - path: "/Repos/user.name@company.com/adb-essentials-demos/3-real-time-data-apps/3.2 - Lending Club DLT Part 1 - Python"
  - path: "/Repos/user.name@company.com/adb-essentials-demos/3-real-time-data-apps/3.3 - Lending Club DLT Part 2 - SQL"
- target: "delta_adb_essentials_<username>"
- storage: "abfss://data@dltdemo<storageaccountname>.dfs.core.windows.net/dlt/delta_adb_essentials_<username>"
- autoscaling: false
- continuous: true

## Deploy to production 

If there are no errors deploy to production using the production button in the top right corner

## Query Tables Created

Run the notebook 3.4 to check that the tables have all been created correctly

# Useful Links

- [Delta Live Tables quickstart](https://docs.microsoft.com/en-us/azure/databricks/data-engineering/delta-live-tables/delta-live-tables-quickstart)
- [Use Azure Event Hubs from Apache Kafka applications](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview)