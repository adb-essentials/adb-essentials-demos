@description('Location for the resources')
param location string = resourceGroup().location

@description('Specifies whether to deploy Azure Databricks workspace with Secure Cluster Connectivity (No Public IP) enabled or not')
param disablePublicIp bool = false

@description('The name of the Azure Databricks workspace to create.')
param workspaceName string = 'adb-essentials-ws'

@description('The pricing tier of workspace.')
@allowed([
  'standard'
  'premium'
])
param pricingTier string = 'premium'

@description('Specifies the name of the Azure Storage account.')
param storageAccountName string = 'adbessentialsstorage'

@description('Specifies the name of the blob container.')
param containerName string = 'adb-demos'

var managedResourceGroupName = 'databricks-rg-${workspaceName}-${uniqueString(workspaceName, resourceGroup().id)}'

resource sa 'Microsoft.Storage/storageAccounts@2021-06-01' = {
  name: storageAccountName
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    accessTier: 'Cool'
  }
}

resource container 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-06-01' = {
  name: '${sa.name}/default/${containerName}'
}

resource ws 'Microsoft.Databricks/workspaces@2021-04-01-preview' = {
  name: workspaceName
  location: location
  tags: {
    usage: 'adb-essentials-demo'
  }
  sku: {
    name: pricingTier
  }
  properties: {
    managedResourceGroupId: managedResourceGroup.id
    parameters: {
      enableNoPublicIp: {
        value: disablePublicIp
      }
    }
  }
}

resource managedResourceGroup 'Microsoft.Resources/resourceGroups@2021-04-01' existing = {
  scope: subscription()
  name: managedResourceGroupName
}

output blobEndpoint string = 'https://${storageAccountName}.blob.${environment().suffixes.storage}'

output myContainerBlobEndpoint string = 'https://${storageAccountName}.blob.${environment().suffixes.storage}/${containerName}'

output wasbsURL string = 'wasbs://${containerName}@${storageAccountName}.blob.core.windows.net/'

//SAS to access (rw) just the adb-demo container.
output containerSASConnectionStr string = listServiceSAS(storageAccountName,'2021-04-01', {
  canonicalizedResource: '/blob/${storageAccountName}/${containerName}'
  signedResource: 'c'
  signedProtocol: 'https'
  signedPermission: 'rwl'
  signedServices: 'b'
  signedExpiry: '2026-07-01T00:00:00Z'
}).serviceSasToken

output blobAccountAccessKey string = sa.listKeys().keys[0].value
