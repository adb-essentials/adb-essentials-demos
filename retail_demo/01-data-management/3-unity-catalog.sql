-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Unity Catalog Quickstart
-- MAGIC 
-- MAGIC [Unity Catalog](https://docs.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/) is a fine-grained governance solution for data and AI on the Lakehouse. It helps simplify security and governance of your data by offering a single place to administer data access policies that apply across all workspaces and personas. The security model is based on standard ANSI SQL, and allows administrators to grant permissions at the level of catalogs, databases (also called schemas), tables, and views in their existing data lake using familiar syntax. Unity Catalog furthermore captures user-level audit logs that record access to your data. See more on data governance in [this overview](https://docs.microsoft.com/en-us/azure/databricks/security/data-governance).
-- MAGIC 
-- MAGIC This notebook provides an example workflow for getting started with Unity Catalog by showing how to do the following:
-- MAGIC 
-- MAGIC - Choose a catalog and creating a new schema.
-- MAGIC - Create a managed table and adding it to the schema.
-- MAGIC - Query the table using the the three-level namespace.
-- MAGIC - Manage data access permissions on the tables.
-- MAGIC 
-- MAGIC **This notebook will only work if Unity Catalog is set up for your workspace!**
-- MAGIC 
-- MAGIC **Requirements**:
-- MAGIC 
-- MAGIC - Unity Catalog is available for your subscription. Check the availablilty or sign up for the preview [here](https://databricks.com/product/unity-catalog).
-- MAGIC - Unity Catalog is set up on the workspace. See [get started documentation](https://docs.databricks.com/data-governance/unity-catalog/get-started.html).
-- MAGIC - Notebook is attached to a cluster that uses DBR 10.3 or higher and uses the User Isolation [cluster security mode](https://docs.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/key-concepts#cluster-security-mode).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Three-level namespace
-- MAGIC 
-- MAGIC Unity Catalog provides a three-level namespace for organizing data: catalogs, schemas (also called databases), and tables and views. To refer to a table, use the following syntax
-- MAGIC 
-- MAGIC    `<catalog>.<schema>.<table>`
-- MAGIC 
-- MAGIC If you already have data in a workspace's Hive metastore, Unity Catalog is **additive**: The workspace’s metastore becomes one catalog within the 3-layer namespace (called `hive_metastore`) and tables in the Hive metastore can be accessed using the notation: `hive_metastore.<schema>.<table>`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a new catalog
-- MAGIC 
-- MAGIC Each Unity Catalog metastore contains a default catalog named `main` with an empty schema called `default`.
-- MAGIC 
-- MAGIC To create a new catalog, use the `CREATE CATALOG` command. You must be a metastore admin to create a new catalog.

-- COMMAND ----------

--- create a new catalog
CREATE CATALOG IF NOT EXISTS adb_essentials_catalog;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create and manage schemas
-- MAGIC Schemas, also referred to as databases, are the second layer of the Unity Catalog namespace. They logically organize tables and views.

-- COMMAND ----------

--- Create a new schema (database) in the quick_start catalog
CREATE SCHEMA IF NOT EXISTS adb_essentials_catalog.adb_essentials_schema
COMMENT "Azure Databricks Essentials demo schema";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a managed table
-- MAGIC 
-- MAGIC *Managed tables* are the default way to create table with Unity Catalog. An admin defines the default cloud storage location for all tables when configuring the metastore. This way users do not have to manage cloud storage. It is possible to define a different location using the `LOCATION` option.

-- COMMAND ----------

-- Create a managed Delta table and insert two records
CREATE TABLE IF NOT EXISTS adb_essentials_catalog.adb_essentials_schema.adb_essentials_table
  (columnA Int, columnB String) PARTITIONED BY (columnA);

INSERT INTO TABLE adb_essentials_table
VALUES
  (1, "one"),
  (2, "two");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Access a table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC With the three level namespaces you can access tables in several different ways:
-- MAGIC - Access the table with a fully qualified name.
-- MAGIC - Select a default catalog and access the table using the schema and table name.
-- MAGIC - Select a default schema and use the table name.
-- MAGIC 
-- MAGIC The following three commands are functionally equivalent.

-- COMMAND ----------

-- Query the table using the three-level namespace
SELECT * FROM adb_essentials_catalog.adb_essentials_schema.adb_essentials_table;

-- COMMAND ----------

-- Set the default catalog and default schema and query the table using the table name
USE CATALOG adb_essentials_catalog;
USE adb_essentials_schema;

SELECT * FROM adb_essentials_table;

-- COMMAND ----------

-- View the table details
DESCRIBE TABLE EXTENDED adb_essentials_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Drop (delete) table**
-- MAGIC 
-- MAGIC If a *managed table* is dropped with the `DROP TABLE` command, the underlying data files are removed as well. 
-- MAGIC 
-- MAGIC If an *external table* is dropped with the `DROP TABLE` command, the metadata about the table is removed from the catalog but the underlying data files are not deleted. 

-- COMMAND ----------

-- Drop the managed table (uncomment the following line)
-- DROP TABLE adb_essentials_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Manage permissions on data
-- MAGIC 
-- MAGIC You use `GRANT` and `REVOKE` statements to manage access to your data. Unity Catalog is secure by default, and access to data is not automatically granted. Initially, all users have no access to data. Metastore admins and data owners can grant and revoke access to account-level users and groups. Grants are not recursive; you must explicitly grant permissions on the catalog, schema, and table or view.
-- MAGIC 
-- MAGIC #### Ownership
-- MAGIC Each object in Unity Catalog has an owner. The owner can be any account-level user or group, called a *principals*. A principal becomes the owner of a securable object when they create it or when ownership is transferred by using an `ALTER` statement.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Manage privileges
-- MAGIC The following privileges can be granted on Unity Catalog objects:
-- MAGIC - `SELECT`: Allows the grantee to read data from a table or view.
-- MAGIC - `MODIFY`: Allows the grantee to insert, update and delete data to or from a table.
-- MAGIC - `CREATE`: Allows the grantee to create child securable objects within a catalog or schema.
-- MAGIC - `USAGE`: This privilege does not grant access to the securable objectitself, but allows the grantee to traverse the securable object in order to access its child objects. For example, to select data from a table, a user needs the `SELECT` privilege on that table and the `USAGE` privilege on its parent schema and parent catalog. You can use this privilege to restrict access to sections of your data namespace to specific groups.
-- MAGIC 
-- MAGIC Three additional privileges are relevant only to external tables and external storage locations that contain data files. This notebook does not explore these privileges.
-- MAGIC 
-- MAGIC - `CREATE TABLE`: Allows the grantee to create an external table at a given external location.
-- MAGIC - `READ FILES`: Allows the grantee to read data files from a given external location.
-- MAGIC - `WRITE FILES`: Allows the grantee to write data files to a given external location.
-- MAGIC 
-- MAGIC Privileges are NOT inherited on child securable objects. Granting a privilege on a securable DOES NOT grant the privilege on its child securables.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Grant a privilege**: Grants a privilege on securable(s) to a principal. Only Metastore Admin and owners of the securable can perform privilege granting.

-- COMMAND ----------

--- Grant create & usage permissions for the catalog to all users on the account
--- This also works for other account-level groups and individual users
GRANT CREATE, USAGE
ON CATALOG adb_essentials_catalog
TO `account users`;

-- COMMAND ----------

-- Grant USAGE on a schema
GRANT USAGE
ON SCHEMA adb_essentials_schema
TO `account users`;

-- COMMAND ----------

-- Grant SELECT privilege on a table
GRANT SELECT
ON TABLE adb_essentials_table
TO `account users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Show grants**: Lists all privileges that are granted on a securable object.

-- COMMAND ----------

-- Show grants on quickstart_schema
SHOW GRANTS
ON SCHEMA adb_essentials_schema;

-- COMMAND ----------

-- Show grants on quickstart_table
SHOW GRANTS
ON TABLE adb_essentials_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Revoke a privilege**: Revokes a previously granted privilege on a securable object from a principal.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fine-grained permissions
-- MAGIC 
-- MAGIC You can use dynamic views to configure fine-grained access control, including columns or row level security and data masking.

-- COMMAND ----------

-- REVOKE SELECT
-- ON TABLE adb_essentials_table
-- FROM `account users`;

-- COMMAND ----------

-- Create a dynamic view of a table and set-up column-level permissions.
CREATE OR REPLACE VIEW adb_essentials_view AS
SELECT
  columnA,
  CASE WHEN
    is_account_group_member('managers') THEN columnB
    ELSE 'REDACTED'
  END AS columnB
FROM adb_essentials_table;

-- COMMAND ----------

-- Now only users in the 'managers' group can see the content of columnB.
SELECT * FROM adb_essentials_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean up

-- COMMAND ----------

-- Drop a schema (uncomment the following line)
-- DROP SCHEMA adb_essentials_schema CASCADE
