Check in PowerShell if secret scope was created:

```
pip install databricks
$databricks_aad_token = az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d | jq .accessToken -r
$Env:DATABRICKS_AAD_TOKEN = $databricks_aad_token         
databricks configure --aad-token --host $ADB_WORKSPACE_URL
databricks secrets list-scopes --output JSON
```

Troubleshooting

If you redeploy after a failed attempt make sure to delete all resources already created in the RG + the RoleAssignment to the MI under IAM settings of the RG.


Helpful resources:
* https://github.com/lordlinus/databricks-all-in-one-bicep-template
* https://cloudarchitected.com/2020/01/using-azure-ad-with-the-azure-databricks-api/
* https://github.com/Azure/Hadoop-Migrations/tree/main/bicep/modules/create-databricks-with-load-balancer