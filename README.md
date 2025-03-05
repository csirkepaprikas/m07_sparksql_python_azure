# Spark SQL Homework

### Balázs Mikes

#### github link:
https://github.com/csirkepaprikas/m06_sparkbasics_python_azure.git

This Spark SQL task is designed to introduce me to the world of Databricks and demonstrate how to implement Spark logic using this toolset. The assignment encourages the use of Delta tables, compute clusters, and connection to cloud infrastructure.
As Databricks is widely used in production environments, it is essential to be familiar with this toolset. Additionally, analyzing the Spark plan and deploying notebooks are valuable skills that will be covered in this homework.
Through this assignment, I gained an understanding of how to test Spark applications using these tools.


The actual task was to apply an ETL job – coded in python - on the source datas - saved on Azure Blob storage -: a set of hotels data in csv files and a set of weather datas in parquet format. The execution taoes place on Azure AKS. Both data sets contains longitude and latitude columns but in case of the hotels’ they might be missing or being saved in inappropiate format. The task was to clean this hotels’ data, fill with the proper coordinates by applying an API, then attach geohash to both of the data sources, then join and save them -also on Blob storage- in the same structured, partioned format as the source weather data.


## First I created the Azure Blob storage and the container, where I uploaded all the source data:
![source_storage](https://github.com/user-attachments/assets/99e9b6c4-bea0-4814-80dc-271ecf28e6dc)


![source_cont](https://github.com/user-attachments/assets/8c443e32-d848-4b36-ae4b-9e4bd771a516)

## Then started the terraform infra creation, filled main.tf, then with the "terraform init" command:
![tf_main_fill](https://github.com/user-attachments/assets/654853e8-9086-41a2-a7a3-b79852f4d08c)

![tf_init](https://github.com/user-attachments/assets/a8bdb614-178e-42e8-8864-e776f3d3bcea)

## After that I planned the infra with the "terraform plan" command:
```python
Acquiring state lock. This may take a few moments...
data.azurerm_client_config.current: Reading...
data.azurerm_client_config.current: Read complete A3Njg5Yjt0Zg=]

Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # azurerm_databricks_workspace.bdcc will be created
  + resource "azurerm_databricks_workspace" "bdcc" {
      + customer_managed_key_enabled      = false
      + disk_encryption_set_id            = (known after apply)
      + id                                = (known after apply)
      + infrastructure_encryption_enabled = false
      + location                          = "westeurope"
      + managed_disk_identity             = (known after apply)
      + managed_resource_group_id         = (known after apply)
      + managed_resource_group_name       = (known after apply)
      + name                              = (known after apply)
      + public_network_access_enabled     = true
      + resource_group_name               = (known after apply)
      + sku                               = "standard"
      + storage_account_identity          = (known after apply)
      + tags                              = {
          + "env"    = "development"
          + "region" = "global"
        }
      + workspace_id                      = (known after apply)
      + workspace_url                     = (known after apply)

      + custom_parameters (known after apply)
    }

  # azurerm_resource_group.bdcc will be created
  + resource "azurerm_resource_group" "bdcc" {
      + id       = (known after apply)
      + location = "westeurope"
      + name     = (known after apply)
      + tags     = {
          + "env"    = "development"
          + "region" = "global"
        }
    }

  # azurerm_storage_account.bdcc will be created
  + resource "azurerm_storage_account" "bdcc" {
      + access_tier                        = (known after apply)
      + account_kind                       = "StorageV2"
      + account_replication_type           = "LRS"
      + account_tier                       = "Standard"
      + allow_nested_items_to_be_public    = true
      + cross_tenant_replication_enabled   = false
      + default_to_oauth_authentication    = false
      + dns_endpoint_type                  = "Standard"
      + https_traffic_only_enabled         = true
      + id                                 = (known after apply)
      + infrastructure_encryption_enabled  = false
      + is_hns_enabled                     = true
      + large_file_share_enabled           = (known after apply)
      + local_user_enabled                 = true
      + location                           = "westeurope"
      + min_tls_version                    = "TLS1_2"
      + name                               = (known after apply)
      + nfsv3_enabled                      = false
      + primary_access_key                 = (sensitive value)
      + primary_blob_connection_string     = (sensitive value)
      + primary_blob_endpoint              = (known after apply)
      + primary_blob_host                  = (known after apply)
      + primary_blob_internet_endpoint     = (known after apply)
      + primary_blob_internet_host         = (known after apply)
      + primary_blob_microsoft_endpoint    = (known after apply)
      + primary_blob_microsoft_host        = (known after apply)
      + primary_connection_string          = (sensitive value)
      + primary_dfs_endpoint               = (known after apply)
      + primary_dfs_host                   = (known after apply)
      + primary_dfs_internet_endpoint      = (known after apply)
      + primary_dfs_internet_host          = (known after apply)
      + primary_dfs_microsoft_endpoint     = (known after apply)
      + primary_dfs_microsoft_host         = (known after apply)
      + primary_file_endpoint              = (known after apply)
      + primary_file_host                  = (known after apply)
      + primary_file_internet_endpoint     = (known after apply)
      + primary_file_internet_host         = (known after apply)
      + primary_file_microsoft_endpoint    = (known after apply)
      + primary_file_microsoft_host        = (known after apply)
      + primary_location                   = (known after apply)
      + primary_queue_endpoint             = (known after apply)
      + primary_queue_host                 = (known after apply)
      + primary_queue_microsoft_endpoint   = (known after apply)
      + primary_queue_microsoft_host       = (known after apply)
      + primary_table_endpoint             = (known after apply)
      + primary_table_host                 = (known after apply)
      + primary_table_microsoft_endpoint   = (known after apply)
      + primary_table_microsoft_host       = (known after apply)
      + primary_web_endpoint               = (known after apply)
      + primary_web_host                   = (known after apply)
      + primary_web_internet_endpoint      = (known after apply)
      + primary_web_internet_host          = (known after apply)
      + primary_web_microsoft_endpoint     = (known after apply)
      + primary_web_microsoft_host         = (known after apply)
      + public_network_access_enabled      = true
      + queue_encryption_key_type          = "Service"
      + resource_group_name                = (known after apply)
      + secondary_access_key               = (sensitive value)
      + secondary_blob_connection_string   = (sensitive value)
      + secondary_blob_endpoint            = (known after apply)
      + secondary_blob_host                = (known after apply)
      + secondary_blob_internet_endpoint   = (known after apply)
      + secondary_blob_internet_host       = (known after apply)
      + secondary_blob_microsoft_endpoint  = (known after apply)
      + secondary_blob_microsoft_host      = (known after apply)
      + secondary_connection_string        = (sensitive value)
      + secondary_dfs_endpoint             = (known after apply)
      + secondary_dfs_host                 = (known after apply)
      + secondary_dfs_internet_endpoint    = (known after apply)
      + secondary_dfs_internet_host        = (known after apply)
      + secondary_dfs_microsoft_endpoint   = (known after apply)
      + secondary_dfs_microsoft_host       = (known after apply)
      + secondary_file_endpoint            = (known after apply)
      + secondary_file_host                = (known after apply)
      + secondary_file_internet_endpoint   = (known after apply)
      + secondary_file_internet_host       = (known after apply)
      + secondary_file_microsoft_endpoint  = (known after apply)
      + secondary_file_microsoft_host      = (known after apply)
      + secondary_location                 = (known after apply)
      + secondary_queue_endpoint           = (known after apply)
      + secondary_queue_host               = (known after apply)
      + secondary_queue_microsoft_endpoint = (known after apply)
      + secondary_queue_microsoft_host     = (known after apply)
      + secondary_table_endpoint           = (known after apply)
      + secondary_table_host               = (known after apply)
      + secondary_table_microsoft_endpoint = (known after apply)
      + secondary_table_microsoft_host     = (known after apply)
      + secondary_web_endpoint             = (known after apply)
      + secondary_web_host                 = (known after apply)
      + secondary_web_internet_endpoint    = (known after apply)
      + secondary_web_internet_host        = (known after apply)
      + secondary_web_microsoft_endpoint   = (known after apply)
      + secondary_web_microsoft_host       = (known after apply)
      + sftp_enabled                       = false
      + shared_access_key_enabled          = true
      + table_encryption_key_type          = "Service"
      + tags                               = {
          + "env"    = "development"
          + "region" = "global"
        }

      + blob_properties (known after apply)

      + network_rules {
          + bypass                     = (known after apply)
          + default_action             = "Allow"
          + ip_rules                   = [
              + "174.128.60.160",
              + "174.128.60.162",
              + "185.44.13.36",
              + "195.56.119.209",
              + "195.56.119.212",
              + "203.170.48.2",
              + "204.153.55.4",
              + "213.184.231.20",
              + "85.223.209.18",
              + "86.57.255.94",
            ]
          + virtual_network_subnet_ids = (known after apply)
        }

      + queue_properties (known after apply)

      + routing (known after apply)

      + share_properties (known after apply)
    }

  # azurerm_storage_data_lake_gen2_filesystem.gen2_data will be created
  + resource "azurerm_storage_data_lake_gen2_filesystem" "gen2_data" {
      + default_encryption_scope = (known after apply)
      + group                    = (known after apply)
      + id                       = (known after apply)
      + name                     = "data"
      + owner                    = (known after apply)
      + storage_account_id       = (known after apply)

      + ace (known after apply)
    }

  # random_string.suffix will be created
  + resource "random_string" "suffix" {
      + id          = (known after apply)
      + length      = 4
      + lower       = true
      + min_lower   = 0
      + min_numeric = 0
      + min_special = 0
      + min_upper   = 0
      + number      = true
      + numeric     = true
      + result      = (known after apply)
      + special     = false
      + upper       = false
    }

Plan: 5 to add, 0 to change, 0 to destroy.

───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

Note: You didn't use the -out option to save this plan, so Terraform can't guarantee to take exactly these actions if you run "terraform apply" now.
Releasing state lock. This may take a few moments...
```
## Then I applied them with the "terraform apply" comand:

```python
random_string.suffix: Creating...
random_string.suffix: Creation complete after 0s [id=6o]
azurerm_resource_group.bdcc: Creating...
azurerm_resource_group.bdcc: Still creating... [10s elapsed]
azurerm_resource_group.bdcc: Still creating... [20s elapsed]
azurerm_resource_group.bdcc: Creation complete after 23s [id=/subscriptions/69d1cd19-02e9-411d-8f6d-68149807689b/resourceGroups/rg-development-westeurope-6o]
azurerm_databricks_workspace.bdcc: Creating...
azurerm_storage_account.bdcc: Creating...
azurerm_databricks_workspace.bdcc: Still creating... [10s elapsed]
azurerm_storage_account.bdcc: Still creating... [10s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [20s elapsed]
azurerm_storage_account.bdcc: Still creating... [20s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [30s elapsed]
azurerm_storage_account.bdcc: Still creating... [30s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [40s elapsed]
azurerm_storage_account.bdcc: Still creating... [40s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [50s elapsed]
azurerm_storage_account.bdcc: Still creating... [50s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [1m0s elapsed]
azurerm_storage_account.bdcc: Still creating... [1m0s elapsed]
azurerm_storage_account.bdcc: Creation complete after 1m6s [id=/subscriptions/69d1cd19-02e9-411d-8f6d-68149807689b/resourceGroups/rg-development-westeurope-6o/providers/Microsoft.Storage/storageAccounts/developmentwesteurope6o]
azurerm_storage_data_lake_gen2_filesystem.gen2_data: Creating...
azurerm_databricks_workspace.bdcc: Still creating... [1m10s elapsed]
azurerm_storage_data_lake_gen2_filesystem.gen2_data: Still creating... [10s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [1m20s elapsed]
azurerm_storage_data_lake_gen2_filesystem.gen2_data: Creation complete after 17s [id=https://developmentwesteurope6o.dfs.core.windows.net/data]
azurerm_databricks_workspace.bdcc: Still creating... [1m30s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [1m40s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [1m50s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [2m0s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [2m10s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [2m20s elapsed]
azurerm_databricks_workspace.bdcc: Still creating... [2m30s elapsed]
azurerm_databricks_workspace.bdcc: Creation complete after 2m36s [id=/subscriptions/69d1cd19-02e9-411d-8f6d-68149807689b/resourceGroups/rg-development-westeurope-6o/providers/Microsoft.Databricks/workspaces/dbw-development-westeurope-6o]
Releasing state lock. This may take a few moments...

Apply complete! Resources: 5 added, 0 changed, 3 destroyed.
```
## Here are all the resource groups:

![resourcces_after_tf](https://github.com/user-attachments/assets/fad1ab54-f413-4432-8e7f-03b161153c1c)

## Here is the Databricks service resource:

![db_in_resource](https://github.com/user-attachments/assets/bae81798-ef15-43e8-86e0-1001ef5fb0e8)

## Here is the actual Workspace:

![kép](https://github.com/user-attachments/assets/128a53ba-5937-4b7b-ba91-51fbd1aca12e)

## I created an Access token, to being able to create a SecretScope in Databricks, where I can store my secrets:

![access_token](https://github.com/user-attachments/assets/d250b3a8-6bbf-4b8a-b901-6a9f4696c200)

## Then configured the token via CLI and created the actual secretscope:

```python
c:\data_eng\házi\2\m07_sparksql_python_azure-master\terraform>databricks configure --token
Databricks Host (should begin with https://): https://adb-512.azuredatabricks.net/
Token:

c:\data_eng\házi\2\m07_sparksql_python_azure-master\terraform>
c:\data_eng\házi\2\m07_sparksql_python_azure-master\terraform>databricks configure --token
Databricks Host (should begin with https://): https://adb-512.8.azuredatabricks.net/
Token:

c:\data_eng\házi\2\m07_sparksql_python_azure-master\terraform>databricks configure --token
Databricks Host (should begin with https://): https://adb-518.8.azuredatabricks.net/
Token:

c:\data_eng\házi\2\m07_sparksql_python_azure-master\terraform>databricks secrets create-scope --scope hw2secret --initial-manage-principal users

c:\data_eng\házi\2\m07_sparksql_python_azure-master\terraform>databricks secrets list-scopes
Scope      Backend     KeyVault URL
---------  ----------  --------------
hw2secret  DATABRICKS  N/A
```

## Putting the secrets to the actual scope:

![db_secrets put](https://github.com/user-attachments/assets/0b15fddd-a5b0-4f47-b243-e66beb9e60a7)

## I wrote a basic python and sql cell notebook:
```python
#input_storage_account_key = dbutils.secrets.get(scope="hw2secret", key="AZURE_STORAGE_ACCOUNT_KEY_SOURCE")
#output_storage_account_key = dbutils.secrets.get(scope="hw2secret", key="AZURE_STORAGE_ACCOUNT_KEY_DESTINATION")
input_file_path = "data/input.csv"
output_file_path = "data/output.csv"

spark.conf.set(
    f"fs.azure.account.key.{input_storage_account}.blob.core.windows.net",
    dbutils.secrets.get(scope="hw2secret", key="AZURE_STORAGE_ACCOUNT_KEY_SOURCE"))

# Output Storage Account konfiguráció
spark.conf.set(
    f"fs.azure.account.key.{output_storage_account}.blob.core.windows.net",
    dbutils.secrets.get(scope="hw2secret", key="AZURE_STORAGE_ACCOUNT_KEY_DESTINATION")
)
#DB creation
spark.sql("CREATE DATABASE IF NOT EXISTS mydatabase")

#input expedia
expedia_df = spark.read.format("avro").load(f"wasbs://hw2@{input_storage_account}.blob.core.windows.net/expedia/")
expedia_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"wasbs://{output_container}@{output_storage_account}.blob.core.windows.net/delta/expedia")
#delta table registration in DB metastore
spark.sql("CREATE DATABASE IF NOT EXISTS mydatabase")
spark.sql("DROP TABLE IF EXISTS mydatabase.expedia")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS mydatabase.expedia
    USING DELTA
    LOCATION 'wasbs://{output_container}@{output_storage_account}.blob.core.windows.net/delta/expedia/'
""")

#input hotel-weather
hotel_weather_df = spark.read.format("parquet").load(f"wasbs://{input_container}@{input_storage_account}.blob.core.windows.net/hotel-weather/hotel-weather/year=2017/month=09/day=02/")
hotel_weather_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"wasbs://{output_container}@{output_storage_account}.blob.core.windows.net/delta/hotel-weather/")
#delta table registration in DB metastore
spark.sql("DROP TABLE IF EXISTS mydatabase.hotel_weather")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS mydatabase.hotel_weather
    USING DELTA
    LOCATION 'wasbs://{output_container}@{output_storage_account}.blob.core.windows.net/delta/hotel-weather'
""")
```

```python
%sql
SELECT * FROM mydatabase.expedia LIMIT 10;
SELECT * FROM mydatabase.hotel_weather LIMIT 10;
```
```python
%sql
SELECT * FROM mydatabase.hotel_weather LIMIT 10;
```

## Created a simple Compute cluster resource too:

![simple_cluster](https://github.com/user-attachments/assets/c5024d60-75e3-44ff-8564-bee56763f166)

## Here are the results:

![gyak_1](https://github.com/user-attachments/assets/e6ef6040-c6e3-40fc-9384-049128e11546)

![gyak2](https://github.com/user-attachments/assets/1225b32b-ba6b-40b6-9326-6106532d1766)








