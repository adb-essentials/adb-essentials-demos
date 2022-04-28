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

var identityName = 'adbessentialsid'

resource mi 'Microsoft.ManagedIdentity/userAssignedIdentities@2018-11-30' = {
  name: identityName
  location: location
}

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

var endpoint = 'https://${storageAccountName}.blob.${environment().suffixes.storage}'
var containerEndpoint = 'https://${storageAccountName}.blob.${environment().suffixes.storage}/${containerName}'
var containerURL = 'wasbs://${containerName}@${storageAccountName}.blob.core.windows.net/'
//SAS to access (rw) just the adb-demo container.
var sasString = listServiceSAS(storageAccountName,'2021-04-01', {
  canonicalizedResource: '/blob/${storageAccountName}/${containerName}'
  signedResource: 'c'
  signedProtocol: 'https'
  signedPermission: 'rwl'
  signedServices: 'b'
  signedExpiry: '2026-07-01T00:00:00Z'
}).serviceSasToken

var storageKey = sa.listKeys().keys[0].value

resource runPowerShellInline 'Microsoft.Resources/deploymentScripts@2020-10-01' = {
  name: 'runPowerShellInline'
  location: location
  kind: 'AzurePowerShell'
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${mi.id}': {}
    }
  }
  properties: {
    forceUpdateTag: '1'
    azPowerShellVersion: '6.4' // or azCliVersion: '2.28.0'
    environmentVariables: [
      {
        name: 'ADB_WORKSPACE_URL'
        value: ws.properties.workspaceUrl
      }
      {
        name: 'ADB_SECRET_SCOPE_NAME'
        value: 'essentials_secret_scope'
      }
      {
        name: 'SAS_ACCESS_KEY'
        value: sasString
      }
      {
        name: 'STORAGE_ACCESS_KEY'
        value: storageKey
      }
    ]
    scriptContent: '''
set PATH "%PATH%;C:\Python33\Scripts"
pip install databricks
$databricks_aad_token = az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d | jq .accessToken -r
$Env:DATABRICKS_AAD_TOKEN = $databricks_aad_token         
databricks configure --aad-token --host ${Env:ADB_WORKSPACE_URL}
databricks secrets create-scope --scope ${Env:ADB_SECRET_SCOPE_NAME}
databricks secrets put --scope access_creds --key sasKey --string-value ${Env:SAS_ACCESS_KEY}
databricks secrets put --scope access_creds --key storageKey --string-value ${Env:STORAGE_ACCESS_KEY}
    '''
    supportingScriptUris: []
    timeout: 'PT30M'
    cleanupPreference: 'OnSuccess'
    retentionInterval: 'P1D'
  }
  dependsOn: [
    ws
    sa
  ]
}

output blobEndpoint string = endpoint
output myContainerBlobEndpoint string = containerEndpoint
output wasbsURL string = containerURL
output containerSASConnectionStr string = sasString
output blobAccountAccessKey string = storageKey
