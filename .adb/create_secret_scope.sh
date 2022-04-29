#/bin/bash -e

# Create secret scope backed by ADB
adb_secret_scope_payload=$(
    jq -n -c \
        --arg sc "$ADB_SECRET_SCOPE_NAME" \
        '{
        scope: $sc,
        initial_manage_principal: "users"
     }'
)

# Get PAT token for authentication with REST API
adbGlobalToken=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --output json | jq -r .accessToken)
azureApiToken=$(az account get-access-token --resource https://management.core.windows.net/ --output json | jq -r .accessToken)

authHeader="Authorization: Bearer $adbGlobalToken"
adbSPMgmtToken="X-Databricks-Azure-SP-Management-Token:$azureApiToken"
adbResourceId="X-Databricks-Azure-Workspace-Resource-Id:$ADB_WORKSPACE_ID"

echo "$adbGlobalToken" >> "$AZ_SCRIPTS_OUTPUT_PATH"
echo "$azureApiToken" >> "$AZ_SCRIPTS_OUTPUT_PATH"
echo "Create ADB secret scope backed by Databricks key vault" >> "$AZ_SCRIPTS_OUTPUT_PATH"
json=$(echo $adb_secret_scope_payload | curl -sS -X POST -H "$authHeader" -H "$adbSPMgmtToken" -H "$adbResourceId" --data-binary "@-" "https://${ADB_WORKSPACE_URL}/api/2.0/secrets/scopes/create")

function createKey() {
    local keyName=$1
    local secretValue=$2
    json_payload=$(
        jq -n -c \
            --arg sc "$ADB_SECRET_SCOPE_NAME" \
            --arg k "$keyName" \
            --arg v "$secretValue" \
            '{
            scope: $sc,
            key: $k,
            string_value: $v
        }'
    )
    response=$(echo $json_payload | curl -sS -X POST -H "$authHeader" -H "$adbSPMgmtToken" -H "$adbResourceId" --data-binary "@-" "https://${ADB_WORKSPACE_URL}/api/2.0/secrets/put")
    echo $response

}

createKey "sasKey" "$SAS_ACCESS_KEY"
createKey "storageKey" "$STORAGE_ACCESS_KEY"

echo "$json" >> "$AZ_SCRIPTS_OUTPUT_PATH"

# tail -f /dev/null