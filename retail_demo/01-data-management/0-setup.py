# Databricks notebook source
# MAGIC %md
# MAGIC # Setup 
# MAGIC 
# MAGIC **This notebook shows how an Azure administrator can set up access to data lake storage. Note that the notebook it will not run as is!**
# MAGIC 
# MAGIC We reccomend to store prodiction data (raw files and Delta tables) in Azure Data Lake Storage (ADLS) Gen2. To access files in ADLS we use OAuth 2.0, with an Azure Active Directory (Azure AD) application [service principal](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access) for authentication. An admin first grants the service principal access to the storage. We then give a cluster access the corresponding file store by adding the lines below to the cluster's [Spark configuration](https://docs.microsoft.com/en-us/azure/databricks/clusters/configure#--spark-configuration). Service principals are also used to set up cloud storage access for [SQL endpoints](https://docs.microsoft.com/en-us/azure/databricks/sql/user/security/cloud-storage-access).
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
# MAGIC 
# MAGIC See more options to access data in ADLS in the [Azure Data Lake Storage Gen2](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/) and [Data Governance ](https://docs.microsoft.com/en-us/azure/databricks/security/data-governance) overview.

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
