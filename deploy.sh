#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
# deploy.sh — Provision Azure infrastructure for Cartly scrapers
#
# Prerequisites:
#   - Azure CLI installed and logged in (az login)
#   - Privileged Role Administrator or Global Administrator for
#     granting Microsoft Graph permissions to the UMI
# ══════════════════════════════════════════════════════════════
set -euo pipefail

# ── Configuration — edit these ────────────────────────────────
RESOURCE_GROUP="cartly-rg"
LOCATION="canadacentral"                      # closest to Calgary
MYSQL_SERVER_NAME="cartly-sql-main"           # your existing server
MYSQL_DATABASE_NAME="<YOUR_DATABASE_NAME>"    # your database name

VNET_NAME="cartly-vnet"
VNET_CIDR="10.0.0.0/16"
SUBNET_CONTAINER_APPS="container-apps-subnet"
SUBNET_CONTAINER_APPS_CIDR="10.0.0.0/23"     # minimum /23 for Container Apps
SUBNET_MYSQL="mysql-subnet"
SUBNET_MYSQL_CIDR="10.0.2.0/24"

ACR_NAME="cartlyacr"                          # Azure Container Registry (lowercase, no hyphens)
CONTAINER_APPS_ENV="cartly-cae"
JOB_NAME="cartly-scraper-job"
USER_ASSIGNED_IDENTITY="cartly-scraper-identity"

# Entra ID user that will serve as MySQL Entra admin
MYSQL_ENTRA_ADMIN_EMAIL="<YOUR_ENTRA_ADMIN_EMAIL>"

# Biweekly cron: midnight UTC, 1st and 15th of every month
# Adjust to your preferred biweekly definition
CRON_EXPRESSION="0 0 1,15 * *"


echo "═══════════════════════════════════════════════════════"
echo "  Step 1: Create Resource Group"
echo "═══════════════════════════════════════════════════════"
az group create \
    --name "$RESOURCE_GROUP" \
    --location "$LOCATION"


echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Step 2: Create User-Assigned Managed Identity"
echo "═══════════════════════════════════════════════════════"
az identity create \
    --name "$USER_ASSIGNED_IDENTITY" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION"

IDENTITY_RESOURCE_ID=$(az identity show \
    --name "$USER_ASSIGNED_IDENTITY" \
    --resource-group "$RESOURCE_GROUP" \
    --query id -o tsv)

IDENTITY_CLIENT_ID=$(az identity show \
    --name "$USER_ASSIGNED_IDENTITY" \
    --resource-group "$RESOURCE_GROUP" \
    --query clientId -o tsv)

IDENTITY_PRINCIPAL_ID=$(az identity show \
    --name "$USER_ASSIGNED_IDENTITY" \
    --resource-group "$RESOURCE_GROUP" \
    --query principalId -o tsv)

echo "  Identity Client ID:    $IDENTITY_CLIENT_ID"
echo "  Identity Principal ID: $IDENTITY_PRINCIPAL_ID"
echo "  Identity Resource ID:  $IDENTITY_RESOURCE_ID"


echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Step 3: Grant Microsoft Graph permissions to the UMI"
echo "  (Required for MySQL Entra ID authentication)"
echo "═══════════════════════════════════════════════════════"
# The UMI needs User.Read.All, GroupMember.Read.All, Application.Read.All
# on the Microsoft Graph service principal to act as MySQL server identity.
GRAPH_SP_ID=$(az ad sp list --filter "displayName eq 'Microsoft Graph'" --query "[0].id" -o tsv)

# User.Read.All
az ad app permission grant --id "$IDENTITY_CLIENT_ID" --api 00000003-0000-0000-c000-000000000000 --scope User.Read.All 2>/dev/null || true
# Assign app roles
for ROLE_VALUE in "User.Read.All" "GroupMember.Read.All" "Application.Read.All"; do
    ROLE_ID=$(az ad sp show --id 00000003-0000-0000-c000-000000000000 \
        --query "appRoles[?value=='$ROLE_VALUE'].id | [0]" -o tsv)
    az rest --method POST \
        --uri "https://graph.microsoft.com/v1.0/servicePrincipals/$GRAPH_SP_ID/appRoleAssignedTo" \
        --body "{
            \"principalId\": \"$IDENTITY_PRINCIPAL_ID\",
            \"resourceId\": \"$GRAPH_SP_ID\",
            \"appRoleId\": \"$ROLE_ID\"
        }" 2>/dev/null || echo "  (Role $ROLE_VALUE may already be assigned)"
done
echo "  Graph permissions granted."


echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Step 4: Create Virtual Network and Subnets"
echo "═══════════════════════════════════════════════════════"
az network vnet create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$VNET_NAME" \
    --location "$LOCATION" \
    --address-prefix "$VNET_CIDR"

# Subnet for Container Apps (delegate to Microsoft.App/environments)
az network vnet subnet create \
    --resource-group "$RESOURCE_GROUP" \
    --vnet-name "$VNET_NAME" \
    --name "$SUBNET_CONTAINER_APPS" \
    --address-prefixes "$SUBNET_CONTAINER_APPS_CIDR"

# Subnet for MySQL private endpoint
az network vnet subnet create \
    --resource-group "$RESOURCE_GROUP" \
    --vnet-name "$VNET_NAME" \
    --name "$SUBNET_MYSQL" \
    --address-prefixes "$SUBNET_MYSQL_CIDR"

SUBNET_CA_ID=$(az network vnet subnet show \
    --resource-group "$RESOURCE_GROUP" \
    --vnet-name "$VNET_NAME" \
    --name "$SUBNET_CONTAINER_APPS" \
    --query id -o tsv)

echo "  VNet and subnets created."


echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Step 5: Create Private Endpoint for MySQL"
echo "═══════════════════════════════════════════════════════"
MYSQL_ID=$(az mysql flexible-server show \
    --name "$MYSQL_SERVER_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --query id -o tsv)

az network private-endpoint create \
    --name "${MYSQL_SERVER_NAME}-pe" \
    --resource-group "$RESOURCE_GROUP" \
    --vnet-name "$VNET_NAME" \
    --subnet "$SUBNET_MYSQL" \
    --private-connection-resource-id "$MYSQL_ID" \
    --group-id mysqlServer \
    --connection-name "${MYSQL_SERVER_NAME}-connection"

# Create Private DNS Zone for MySQL
az network private-dns zone create \
    --resource-group "$RESOURCE_GROUP" \
    --name "privatelink.mysql.database.azure.com"

az network private-dns link vnet create \
    --resource-group "$RESOURCE_GROUP" \
    --zone-name "privatelink.mysql.database.azure.com" \
    --name "${VNET_NAME}-mysql-link" \
    --virtual-network "$VNET_NAME" \
    --registration-enabled false

az network private-endpoint dns-zone-group create \
    --resource-group "$RESOURCE_GROUP" \
    --endpoint-name "${MYSQL_SERVER_NAME}-pe" \
    --name "mysql-dns-group" \
    --private-dns-zone "privatelink.mysql.database.azure.com" \
    --zone-name "mysql"

echo "  Private endpoint and DNS configured."


echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Step 6: Configure MySQL Entra ID Authentication"
echo "═══════════════════════════════════════════════════════"
ADMIN_OID=$(az ad user show --id "$MYSQL_ENTRA_ADMIN_EMAIL" --query id -o tsv)

az mysql flexible-server ad-admin create \
    --resource-group "$RESOURCE_GROUP" \
    --server-name "$MYSQL_SERVER_NAME" \
    --display-name "$MYSQL_ENTRA_ADMIN_EMAIL" \
    --object-id "$ADMIN_OID" \
    --identity "$USER_ASSIGNED_IDENTITY"

echo "  Entra admin set. Authentication mode: MySQL and Microsoft Entra."
echo ""
echo "  MANUAL STEP REQUIRED:"
echo "  Connect to MySQL as the Entra admin and run:"
echo "  ────────────────────────────────────────────────"
echo "  SET aad_auth_validate_oids_in_tenant = OFF;"
echo "  CREATE AADUSER 'cartly-scraper-identity' IDENTIFIED BY '$IDENTITY_CLIENT_ID';"
echo "  GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER"
echo "      ON \`$MYSQL_DATABASE_NAME\`.* TO 'cartly-scraper-identity'@'%';"
echo "  FLUSH PRIVILEGES;"
echo "  ────────────────────────────────────────────────"
echo ""


echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Step 7: Create Azure Container Registry"
echo "═══════════════════════════════════════════════════════"
az acr create \
    --name "$ACR_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku Basic

# Grant the managed identity AcrPull role
ACR_ID=$(az acr show --name "$ACR_NAME" --query id -o tsv)
az role assignment create \
    --assignee "$IDENTITY_PRINCIPAL_ID" \
    --role "AcrPull" \
    --scope "$ACR_ID"

echo "  ACR created and identity granted pull access."


echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Step 8: Build and Push Container Image"
echo "═══════════════════════════════════════════════════════"
az acr build \
    --registry "$ACR_NAME" \
    --image cartly-scrapers:latest \
    --file Dockerfile \
    .

echo "  Image built and pushed to $ACR_NAME.azurecr.io/cartly-scrapers:latest"


echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Step 9: Create Container Apps Environment with VNet"
echo "═══════════════════════════════════════════════════════"
az containerapp env create \
    --name "$CONTAINER_APPS_ENV" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --infrastructure-subnet-resource-id "$SUBNET_CA_ID" \
    --internal-only false

echo "  Container Apps Environment created with VNet integration."


echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Step 10: Create Scheduled Container Apps Job"
echo "═══════════════════════════════════════════════════════"
az containerapp job create \
    --name "$JOB_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --environment "$CONTAINER_APPS_ENV" \
    --trigger-type "Schedule" \
    --cron-expression "$CRON_EXPRESSION" \
    --replica-timeout 14400 \
    --replica-retry-limit 1 \
    --image "$ACR_NAME.azurecr.io/cartly-scrapers:latest" \
    --registry-server "$ACR_NAME.azurecr.io" \
    --registry-identity "$IDENTITY_RESOURCE_ID" \
    --mi-user-assigned "$IDENTITY_RESOURCE_ID" \
    --cpu "2.0" \
    --memory "4Gi" \
    --env-vars \
        "AZURE_MYSQL_HOST=${MYSQL_SERVER_NAME}.mysql.database.azure.com" \
        "AZURE_MYSQL_NAME=${MYSQL_DATABASE_NAME}" \
        "AZURE_MYSQL_USER=cartly-scraper-identity" \
        "AZURE_MYSQL_PORT=3306" \
        "AZURE_MYSQL_CLIENTID=${IDENTITY_CLIENT_ID}"

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  DEPLOYMENT COMPLETE"
echo "═══════════════════════════════════════════════════════"
echo ""
echo "  Job '$JOB_NAME' is scheduled: $CRON_EXPRESSION"
echo "  To trigger a manual run:"
echo "    az containerapp job start -n $JOB_NAME -g $RESOURCE_GROUP"
echo ""
echo "  To check execution history:"
echo "    az containerapp job execution list -n $JOB_NAME -g $RESOURCE_GROUP"
echo ""
echo "  REMEMBER: Complete the manual MySQL step in Step 6 above!"
echo ""
