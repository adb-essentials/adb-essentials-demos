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

var ownerRoleDefId = '8e3af657-a8ff-443c-a75c-2fe8c4bcb635'

var secretScopeName = 'essentials_secret_scope'

resource mi 'Microsoft.ManagedIdentity/userAssignedIdentities@2018-11-30' = {
  name: identityName
  location: location
}

resource roleAssignment 'Microsoft.Authorization/roleAssignments@2020-08-01-preview' = {
  name:  guid(ownerRoleDefId,resourceGroup().id)
  scope: resourceGroup()
  properties: {
    principalType: 'ServicePrincipal'
    principalId: mi.properties.principalId
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', ownerRoleDefId)
  }
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

resource createSecretScope 'Microsoft.Resources/deploymentScripts@2020-10-01' = {
  name: secretScopeName
  location: location
  kind: 'AzureCLI'
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${mi.id}': {}
    }
  }
  properties: {
    forceUpdateTag: '1'
    azCliVersion: '2.28.0'
    timeout: 'PT1H'
    cleanupPreference: 'OnExpiration'
    retentionInterval: 'PT1H'
    environmentVariables: [
      {
        name: 'ADB_WORKSPACE_URL'
        value: ws.properties.workspaceUrl
      }
      {
        name: 'ADB_WORKSPACE_ID'
        value: ws.id
      }
      {
        name: 'ADB_SECRET_SCOPE_NAME'
        value: secretScopeName
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
    scriptContent: loadTextContent('./create_secret_scope.sh')
  }
  dependsOn: [
    ws
    sa
    roleAssignment
  ]
}

output blobEndpoint string = endpoint
output myContainerBlobEndpoint string = containerEndpoint
output wasbsURL string = containerURL
output containerSASConnectionStr string = sasString
output blobAccountAccessKey string = storageKey
output secretScopeName string = secretScopeName
