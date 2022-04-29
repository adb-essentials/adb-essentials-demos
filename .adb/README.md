Check in PowerShell if secret scope was created:

```
pip install databricks
$databricks_aad_token = az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d | jq .accessToken -r
$Env:DATABRICKS_AAD_TOKEN = $databricks_aad_token         
databricks configure --aad-token --host $ADB_WORKSPACE_URL
databricks secrets list-scopes --output JSON
```