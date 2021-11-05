# Data Engineering

## Set up environment

Follow instructions in [SET-UP-ENVIRONMENT.md](../SET-UP-ENVIRONMENT.md) to create ADLS storage account

## Create Stream of Events in Kafka

Use notebook 2.1 to create CSV files in ADLS storage account.

## Create Batch Delta Live Table Pipeline

Go to Jobs > Delta Live Tables > Create Pipeline

Use the settings in file `dlt-settings-loans-batch.json` to create a batch (triggered) pipeline.

Use the settings in file `dlt-settings-loans-streaming.json` to create a streaming (continuous) pipeline.


## Deploy to production 

If there are no errors deploy to production using the production button in the top right corner

## Query Tables Created

Run the notebook 2.3 to check that the tables have all been created correctly

# Useful Links

- [Delta Live Tables quickstart](https://docs.microsoft.com/en-us/azure/databricks/data-engineering/delta-live-tables/delta-live-tables-quickstart)