# Databricks notebook source
# MAGIC %md
# MAGIC # Setup 
# MAGIC 
# MAGIC We recommend to store prodiction data in Azure Data Lake Storage (ADLS) Gen2. This notebook shows how an Azure administrator can set up access to storage. See an overview of how to access other supported storage data sources [here](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/).
# MAGIC 
# MAGIC **Note that the notebook it will not run as is!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## ADLS access key
# MAGIC The easiest way to access files in ADLS is via an [ADLS access key](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-get-started). Storing the key in a secret scope provides additional security. However, using an access key is a less secure option and should be used only for non-production scenarios such as developing or testing notebooks.
# MAGIC 
# MAGIC In the code below, replace:<br>
# MAGIC *\<storage-account-name\> with the ADLS Gen2 storage account name.<br>
# MAGIC \<scope-name\> with the Azure Databricks secret scope name.<br>
# MAGIC \<storage-account-access-key-name\> with the name of the key containing the Azure storage account access key.*

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.<storage-account-name>.dfs.core.windows.net",
    dbutils.secrets.get(scope="<scope-name>",key="<storage-account-access-key-name>"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## OAuth 2.0 / service principal
# MAGIC The recommended way to access files in ADLS is via OAuth 2.0, with an Azure Active Directory (Azure AD) application [service principal](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access) for authentication. An admin first grants the service principal access to the storage. We then give a cluster access the corresponding file store by adding the lines below to the cluster's [Spark configuration](https://docs.microsoft.com/en-us/azure/databricks/clusters/configure#--spark-configuration). Service principals are also used to set up cloud storage access for [SQL endpoints](https://docs.microsoft.com/en-us/azure/databricks/sql/user/security/cloud-storage-access).
# MAGIC 
# MAGIC     fs.azure.account.auth.type.<storage-account-name>.dfs.core.windows.net OAuth
# MAGIC     fs.azure.account.oauth.provider.type.<storage-account-name>.dfs.core.windows.net org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
# MAGIC     fs.azure.account.oauth2.client.id.<storage-account-name>.dfs.core.windows.net <application-id>
# MAGIC     fs.azure.account.oauth2.client.secret.<storage-account-name>.dfs.core.windows.net {{secrets/<scope-name>/<secret-name>}}
# MAGIC     fs.azure.account.oauth2.client.endpoint.<storage-account-name>.dfs.core.windows.net https://login.microsoftonline.com/<directory-id>/oauth2/token
# MAGIC 
# MAGIC Replace: <br>
# MAGIC *\<storage-account-name\> with the name of the ADLS Gen2 storage account. <br>
# MAGIC \<application-id\> with the Application (client) ID for the Azure Active Directory application. <br>
# MAGIC \<scope-name\> with the Databricks secret scope name. <br>
# MAGIC \<service-credential-key-name\> with the name of the key containing the client secret. <br>
# MAGIC \<directory-id\> with the Directory (tenant) ID for the Azure Active Directory application.*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access files
# MAGIC After the setup it is possible to read files from directly from cloud storage, using it's full path.
# MAGIC 
# MAGIC     abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<directory-name>
# MAGIC 
# MAGIC Replace: <br>
# MAGIC *\<container-name\> with the name of a container in the ADLS Gen2 storage account. <br>
# MAGIC \<storage-account-name\> with the ADLS Gen2 storage account name. <br>
# MAGIC \<directory-name\> with an optional path in the storage account.*

# COMMAND ----------

# Load csv file in ADLS to Spark dataframe
file = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<directory-name>/<file-name>.csv"
df = spark.read.format('csv').options(header='true', inferSchema='true').load(file)
