# Set up Azure enviroment

This guide contains the following steps to set up your enviroment for all the demos in this repo.

- Set up Azure CLI
- Set up Databricks CLI
- Set-up Databricks Cluster
- Set up ADLS gen2
- Set up Event Hubs

## Set up Azure CLI

Follow the instructions on [how to install the Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli).

### Define resource group for all Azure CLI cmds

```
az configure --defaults group=<your-resource-group>
```

## Set up Databricks CLI

Follow the instructions on [how to install the Azure Databricks CLI](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/cli/).

### Authenticate against the Databricks CLI

```
databricks configure --token
```

### Create secrets scope to be used for ADLS and Event Hubs access keys

```
databricks secrets create-scope --scope access_creds
```

## Set-up Databricks Cluster

### Create Databricks Cluster

Create a file called `create-cluster.json` and add the following:

```
{
  "cluster_name": "adb-essentials-9-0",
  "spark_version": "9.0.x-scala2.12",
  "node_type_id": "Standard_D3_v2",
  "spark_conf": {
    "spark.databricks.enableWsfs": false,
    "spark.hadoop.fs.azure.account.key.dltdemostorage.dfs.core.windows.net": "{{secrets/access_creds/adlsDltDemoStorageAccessKey}}"
  },
  "num_workers": 2
}

```

Run the clusters create command to create a cluster in the Databricks workspace.

```
databricks clusters create --json-file create-cluster.json
```


### Add cluster setting for Repos

Once cluster is created go advanced options > Spark config and check the following settings have been applied:

```
spark.databricks.enableWsfs false
spark.hadoop.fs.azure.account.key.dltdemostorage.dfs.core.windows.net {{secrets/access_creds/adlsDltDemoStorageAccessKey}}
```

## Set up ADLS gen2

### Define storage account name

```
export STORAGE_ACCOUNT=dltdemo<storageaccountname>
```


### Create ADLS gen2 bucket

```
az storage account create \
  --name $STORAGE_ACCOUNT \
  --location northeurope \
  --sku Standard_RAGRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  --allow-shared-key-access true
```

### Create container called data in storage account

```
az storage fs create -n data --account-name $STORAGE_ACCOUNT
```

### Add ADLS access key to Databricks secrets

```
export ADLS_PRIMARY_KEY=$(az storage account keys list --account-name $STORAGE_ACCOUNT --query '[0].value' --output tsv)
databricks secrets put --scope access_creds --key adlsDltDemoStorageAccessKey --string-value $ADLS_PRIMARY_KEY
```

## Set up Event Hubs

### Define namespace name and topic name

```
export EH_NAMESPACE=dlt-demo-eh
export EH_KAFKA_TOPIC=loans-events
```

### Create Event Hubs namespace with Kafka enabled

```
az eventhubs namespace create --name $EH_NAMESPACE \
  --location northeurope \
  --sku standard \
  --enable-kafka
```

### Create Event Hubs Kafka topic (hub)

```
az eventhubs eventhub create --name $EH_KAFKA_TOPIC \
  --namespace-name $EH_NAMESPACE
```

### Create Auth rules for send and listen

Send:

```
az eventhubs eventhub authorization-rule create \
  --namespace-name $EH_NAMESPACE \
  --eventhub-name $EH_KAFKA_TOPIC \
  --name adbSendDltDemoLoansEvents \
  --rights Send
```

Listen: 

```
az eventhubs eventhub authorization-rule create \
  --namespace-name $EH_NAMESPACE \
  --eventhub-name $EH_KAFKA_TOPIC \
  --name adbListenDltDemoLoansEvents \
  --rights Listen
```

### Check that the keys were created

Send:

```
az eventhubs eventhub authorization-rule keys list \
  --namespace-name $EH_NAMESPACE \
  --eventhub-name $EH_KAFKA_TOPIC \
  --name adbSendDltDemoLoansEvents
```

Listen:

```
az eventhubs eventhub authorization-rule keys list \
  --namespace-name $EH_NAMESPACE \
  --eventhub-name $EH_KAFKA_TOPIC \
  --name adbListenDltDemoLoansEvents
```


### Add Event Hubs access key to Databricks secrets

```
export SEND_PRIMARY_KEY=$(az eventhubs eventhub authorization-rule keys list --namespace-name $EH_NAMESPACE --eventhub-name $EH_KAFKA_TOPIC --name adbSendDltDemoLoansEvents --query 'primaryKey' --output tsv)
databricks secrets put --scope access_creds --key ehSendDltDemoLoansEventsAccessKey --string-value $SEND_PRIMARY_KEY
```

```
export LISTEN_PRIMARY_KEY=$(az eventhubs eventhub authorization-rule keys list --namespace-name $EH_NAMESPACE --eventhub-name $EH_KAFKA_TOPIC --name adbListenDltDemoLoansEvents --query 'primaryKey' --output tsv)
databricks secrets put --scope access_creds --key ehListenDltDemoLoansEventsAccessKey --string-value $LISTEN_PRIMARY_KEY
```



