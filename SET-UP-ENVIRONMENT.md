# Set up Azure enviroment

## Set up Azure CLI

### Define resource group for future az cmds

```
az configure --defaults group=<your-resource-group>
```

## Set up ADLS


### Create ADLS gen2 bucket

### Add ADLS access key to Databricks secrets

## Set up Event Hubs

### Create Event Hubs namespace with Kafka enabled

```
az eventhubs namespace create --name dlt-demo-eh \
  --location northeurope \
  --sku standard \
  --enable-kafka
```

### Create Event Hubs Kafka topic (hub)

```
az eventhubs eventhub create --name loan-events \
  --namespace-name dlt-demo-eh
```

### Create Auth rules for send and listen

Send:

```
az eventhubs eventhub authorization-rule create \
  --namespace-name dlt-demo-eh \
  --eventhub-name loan-events \
  --name adbSendDltDemoLoanEvents \
  --rights Send
```

Listen: 

```
az eventhubs eventhub authorization-rule create \
  --namespace-name dlt-demo-eh \
  --eventhub-name loan-events \
  --name adbListenDltDemoLoanEvents \
  --rights Listen
```

### Check that the keys were created

Send:

```
az eventhubs eventhub authorization-rule keys list --namespace-name dlt-demo-eh \
  --eventhub-name loan-events \
  --name adbSendDltDemoLoanEvents
```

Listen:

```
az eventhubs eventhub authorization-rule keys list --namespace-name dlt-demo-eh \
  --eventhub-name loan-events \
  --name adbListenDltDemoLoanEvents
```


### Add Event Hubs access key to Databricks secrets



