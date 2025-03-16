![1ST_JOB_DELTA_TABLES](https://github.com/user-attachments/assets/fad40838-d2bc-444d-8bc0-329e5289e294)![1ST_JOB_DELTA_TABLES](https://github.com/user-attachments/assets/ddab335e-03ac-45f3-aedc-9e663c316a71)![1ST_JOB_RUNNING](https://github.com/user-attachments/assets/4547a2a4-fc50-46a7-a2c2-54243dda086f)# Spark SQL Homework

### Balázs Mikes

#### github link:
https://github.com/csirkepaprikas/m07_sparksql_python_azure.git

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
azurerm_resource_group.bdcc: Creation complete after 23s [id=/subscriptions//resourceGroups/rg-development-westeurope-6o]
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
azurerm_storage_account.bdcc: Creation complete after 1m6s [id=/subscriptions//resourceGroups/rg-development-westeurope-6o/providers/Microsoft.Storage/storageAccounts/developmentwesteurope6o]
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
azurerm_databricks_workspace.bdcc: Creation complete after 2m36s [id=/subscriptions//resourceGroups/rg-development-westeurope-6o/providers/Microsoft.Databricks/workspaces/dbw-development-westeurope-6o]
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

## Then I finalized the file reading, writing, delta table create version, also added the joined dataframe creation:

```python
# Azure Storage settings
input_container = "hw2"
output_container = "data"

# Setting up Storage account keys
spark.conf.set(
    f"fs.azure.account.key.{input_storage_account}.blob.core.windows.net",
    dbutils.secrets.get(scope="hw2secret", key="AZURE_STORAGE_ACCOUNT_KEY_SOURCE"))

spark.conf.set(
    f"fs.azure.account.key.{output_storage_account}.blob.core.windows.net",
    dbutils.secrets.get(scope="hw2secret", key="AZURE_STORAGE_ACCOUNT_KEY_DESTINATION"))

# Create database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS mydatabase")

# Read Expedia data from the source container and save it in Delta format to the data output container 
expedia_df = spark.read.format("avro").load(f"wasbs://{input_container}@{input_storage_account}.blob.core.windows.net/expedia/")

expedia_df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"wasbs://{output_container}@{output_storage_account}.blob.core.windows.net/delta/expedia/")

# Register the Expedia Delta table in the Metastore
spark.sql("DROP TABLE IF EXISTS mydatabase.expedia")
spark.sql(f"""
    CREATE TABLE mydatabase.expedia
    USING DELTA
    LOCATION 'wasbs://{output_container}@{output_storage_account}.blob.core.windows.net/delta/expedia/'
""")

# Read Hotel-Weather data from the source container and save it in Delta format to the data output container, also partitioning is applied
hotel_weather_df = spark.read.format("parquet").load(f"wasbs://{input_container}@{input_storage_account}.blob.core.windows.net/hotel-weather/hotel-weather/")

hotel_weather_df.write.format("delta").mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .option("overwriteSchema", "true") \
    .save(f"wasbs://{output_container}@{output_storage_account}.blob.core.windows.net/delta/hotel-weather/")

# Register the Hotel-Weather Delta table in the Metastore
spark.sql("DROP TABLE IF EXISTS mydatabase.hotel_weather")
spark.sql(f"""
    CREATE TABLE mydatabase.hotel_weather
    USING DELTA
    LOCATION 'wasbs://{output_container}@{output_storage_account}.blob.core.windows.net/delta/hotel-weather/'
""")

# Refresh cache to see the most up-to-date data
spark.sql("REFRESH TABLE mydatabase.expedia")
spark.sql("REFRESH TABLE mydatabase.hotel_weather")

#Due to the same column name in the two dataframes, we need to rename the column
hotel_weather_df = hotel_weather_df.withColumnRenamed("id", "accomodation_id")
# Join the Expedia and Hotel Weather data
joined_df = expedia_df.join(hotel_weather_df, expedia_df.hotel_id == hotel_weather_df.accomodation_id, "left")

# Save the intermediate DataFrame partitioned
joined_df.write.format("parquet") \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .save(f"wasbs://{output_container}@{output_storage_account}.blob.core.windows.net/joined_data/")
```
## Then I tested the first query :
# Top 10 hotels with max absolute temperature difference by month


![1st_commented](https://github.com/user-attachments/assets/765c68ea-7e43-4e22-bb13-5c1c22ddd772)

## It seemed OK, so I added the "EXPLAIN" clause to the first row:

```python
EXPLAIN
SELECT 
    address,
    year, 
    month, 
    ROUND(ABS(MAX(avg_tmpr_c) - MIN(avg_tmpr_c)), 2) AS temp_diff
FROM mydatabase.hotel_weather
GROUP BY address, year, month
ORDER BY temp_diff DESC
LIMIT 10;
```

## And got this execution plan, which I analyzed:
```python
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
## AdaptiveSparkPlan: The plan is adaptive, meaning it adjusts based on data distribution during execution.
+- == Initial Plan ==
   ColumnarToRow
   +- PhotonResultStage
   ##PhotonResultStage & PhotonTopK: These indicate the use of Photon for efficient query execution. Sorting is done by temp_diff in descending order to return the top 10 results.
      +- PhotonTopK(sortOrder=[temp_diff#361 DESC NULLS LAST], partitionOrderCount=0)
         +- PhotonShuffleExchangeSource
            +- PhotonShuffleMapStage
               +- PhotonShuffleExchangeSink SinglePartition
			   ## Shuffle & Partitioning: Data is shuffled and partitioned by address, year, and month to perform the GROUP BY operation.
                  +- PhotonTopK(sortOrder=[temp_diff#361 DESC NULLS LAST], partitionOrderCount=0)
                     +- PhotonGroupingAgg(keys=[address#391, year#402, month#403], functions=[finalmerge_max(merge max#409) AS max(avg_tmpr_c#392)#405, finalmerge_min(merge min#411) AS min(avg_tmpr_c#392)#406])
					 ## Aggregation: The query performs partial aggregation (partial_max and partial_min) followed by final aggregation (finalmerge_max and finalmerge_min) to compute the temperature difference for each group.
                        +- PhotonShuffleExchangeSource
                           +- PhotonShuffleMapStage
                              +- PhotonShuffleExchangeSink hashpartitioning(address#391, year#402, month#403, 200)
							  ## The data is distributed across 200 partitions to balance parallelism.
                                 +- PhotonGroupingAgg(keys=[address#391, year#402, month#403], functions=[partial_max(avg_tmpr_c#392) AS max#409, partial_min(avg_tmpr_c#392) AS min#411])
                                    +- PhotonProject [address#391, avg_tmpr_c#392, year#402, month#403]
                                       +- PhotonScan parquet spark_catalog.mydatabase.hotel_weather[address#391,avg_tmpr_c#392,year#402,month#403,day#404] DataFilters: [], DictionaryFilters: [], Format: parquet, Location: PreparedDeltaFileIndex(1 paths)[wasbs://data@developmentwesteurope6o.blob.core.windows.net/delta/..., OptionalDataFilters: [], PartitionFilters: [], ReadSchema: struct<address:string,avg_tmpr_c:double>, RequiredDataFilters: []
									   ## Data Scan: The PhotonScan reads the data from Delta Parquet files, which is efficient for large datasets.


== Photon Explanation ==
The query is fully supported by Photon.

## The most time consuming parts are Shuffle and Aggregation are resource-intensive, but Spark uses multi-phase aggregation to optimize it. Sorting by temp_diff with PhotonTopK is computationally expensive.

```

## Then the second query:
#  Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months

```sql
-- First CTE: Generate one row per day for each hotel booking.
WITH exploded_dates AS (
    SELECT
        ex.hotel_id,          -- The hotel ID from the Expedia bookings table.
        hw.address,           -- The hotel address from the hotel weather table.
        -- Generate a sequence of dates from the check-in date (inclusive) to the day before check-out (inclusive)
        -- and then create a separate row for each date.
        explode(
            sequence(
                CAST(ex.srch_ci AS DATE), 
                CAST(ex.srch_co AS DATE) - INTERVAL 1 DAY, 
                interval 1 day
            )
        ) AS visit_date
    FROM mydatabase.expedia ex
    LEFT JOIN mydatabase.hotel_weather hw
        ON ex.hotel_id = hw.id  -- Join the hotel weather data on matching hotel IDs.
    WHERE 
        CAST(ex.srch_ci AS DATE) < CAST(ex.srch_co AS DATE)  -- Only consider bookings with a valid date range.
        AND hw.address IS NOT NULL                           -- Only include records where the hotel address is available.
),
-- Second CTE: Aggregate visit counts by hotel address for each month.
monthly_visits AS (
    SELECT
        address,
        YEAR(visit_date) AS year,   -- Extract the year from the visit date.
        MONTH(visit_date) AS month, -- Extract the month from the visit date.
        COUNT(*) AS visits_count    -- Count the number of visit days per address in the given month.
    FROM exploded_dates
    GROUP BY address, YEAR(visit_date), MONTH(visit_date)
),
-- Third CTE: Rank hotels within each month based on the number of visits.
ranked_hotels AS (
    SELECT *,
        -- Use DENSE_RANK to assign a rank to each hotel within each year and month partition,
        -- ordering by visits_count in descending order so that hotels with the most visits rank highest.
        DENSE_RANK() OVER (PARTITION BY year, month ORDER BY visits_count DESC) AS rank
    FROM monthly_visits
)
-- Final selection: Return hotels that are among the top 10 for each month.
SELECT * 
FROM ranked_hotels 
WHERE rank <= 10;
```
## You can see the fraction of the result:

![2nd_result](https://github.com/user-attachments/assets/87b5914d-1e87-4948-b5a8-5892e1a80090)

## I also applied to EXPLAIN clause to get the execution plan:
```sql
%sql
EXPLAIN
WITH exploded_dates AS (
    SELECT
        ex.hotel_id,
        hw.address,
        explode(sequence(
            CAST(ex.srch_ci AS DATE), 
            CAST(ex.srch_co AS DATE) - INTERVAL 1 DAY, 
            interval 1 day)) AS visit_date
    FROM mydatabase.expedia ex
    LEFT JOIN mydatabase.hotel_weather hw
    ON ex.hotel_id = hw.id
    WHERE CAST(ex.srch_ci AS DATE) < CAST(ex.srch_co AS DATE)
    AND hw.address IS NOT NULL
),
monthly_visits AS (
    SELECT
        address,
        YEAR(visit_date) AS year,
        MONTH(visit_date) AS month,
        COUNT(*) AS visits_count
    FROM exploded_dates
    GROUP BY address, YEAR(visit_date), MONTH(visit_date)
),
ranked_hotels AS (
    SELECT *,
        DENSE_RANK() OVER (PARTITION BY year, month ORDER BY visits_count DESC) AS rank
    FROM monthly_visits
)
SELECT * FROM ranked_hotels WHERE rank <= 10;
```
## You can see the commented execution plan below:
```python
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- == Initial Plan ==
   ## Filter stage: Keeps rows where the computed rank is <= 10.
   Filter (rank#670 <= 10)
   +- RunningWindowFunction [address#740, year#667, month#668, visits_count#669L, 
         dense_rank(visits_count#669L) OVER (PARTITION BY year#667, month#668 ORDER BY visits_count#669L DESC NULLS LAST)
         AS rank#670], [year#667, month#668], [visits_count#669L DESC NULLS LAST], false
      +- Sort [year#667 ASC NULLS FIRST, month#668 ASC NULLS FIRST, visits_count#669L DESC NULLS LAST], false, 0
         ## Exchange stage: Data is shuffled across 200 partitions by year and month.
         +- Exchange hashpartitioning(year#667, month#668, 200), ENSURE_REQUIREMENTS, [plan_id=917]
            ## Project stage: Selects address, year, month, and visits_count for further processing.
            +- Project [address#740, year#667, month#668, visits_count#669L]
               ## Filter stage: Applies a local dense rank filter (<= 10) on each group.
               +- Filter (_local_dense_rank#808 <= 10)
                  ## RunningWindowFunction stage: Computes local dense rank within each (year, month) partition.
                  +- RunningWindowFunction [address#740, year#667, month#668, visits_count#669L, 
                        dense_rank(visits_count#669L) OVER (PARTITION BY year#667, month#668 ORDER BY visits_count#669L DESC NULLS LAST)
                        AS _local_dense_rank#808], [year#667, month#668], [visits_count#669L DESC NULLS LAST], true
                     +- Sort [year#667 ASC NULLS FIRST, month#668 ASC NULLS FIRST, visits_count#669L DESC NULLS LAST], false, 0
                        ## HashAggregate stage: Performs grouping by address, extracting year and month from visit_date,
                        ## and computes the count of visits per group.
                        +- HashAggregate(keys=[address#740, _groupingexpression#769, _groupingexpression#770],
                              functions=[finalmerge_count(merge count#772L) AS count(1)#754L])
                           ## Exchange: Data shuffling for aggregation based on grouping keys.
                           +- Exchange hashpartitioning(address#740, _groupingexpression#769, _groupingexpression#770, 200), ENSURE_REQUIREMENTS, [plan_id=911]
                              ## HashAggregate: Partial aggregation stage computing the count per group.
                              +- HashAggregate(keys=[address#740, _groupingexpression#769, _groupingexpression#770],
                                    functions=[partial_count(1) AS count#772L])
                                 ## Project: Extracts address and computes grouping keys (year and month from visit_date).
                                 +- Project [address#740, year(visit_date#759) AS _groupingexpression#769, month(visit_date#759) AS _groupingexpression#770]
                                    ## Generate: Explodes the date sequence from check-in to check-out-1 day.
                                    +- Generate explode(sequence(cast(srch_ci#732 as date),
                                          date_add(cast(srch_co#733 as date), -1),
                                          Some(INTERVAL '1' DAY), Some(Etc/UTC))),
                                          [address#740], false, [visit_date#759]
                                       ## Converts columnar format to row format.
                                       +- ColumnarToRow
                                          ## PhotonResultStage: Initial stage using Photon engine.
                                          +- PhotonResultStage
                                             ## PhotonProject: Projects required columns (srch_ci, srch_co, address) from expedia.
                                             +- PhotonProject [srch_ci#732, srch_co#733, address#740]
                                                ## PhotonBroadcastHashJoin: Joins expedia and hotel_weather on hotel_id.
                                                +- PhotonBroadcastHashJoin [hotel_id#739L], [cast(id#746 as bigint)], Inner, BuildRight, false, true
                                                   :- PhotonScan parquet spark_catalog.mydatabase.expedia[...]  ## Reads expedia data.
                                                   +- PhotonShuffleExchangeSource
                                                      +- PhotonShuffleMapStage
                                                         +- PhotonShuffleExchangeSink SinglePartition
                                                            ## PhotonProject: Projects columns (address, id) from hotel_weather.
                                                            +- PhotonProject [address#740, id#746]
                                                               ## PhotonScan: Reads hotel_weather data from Parquet.
                                                               +- PhotonScan parquet spark_catalog.mydatabase.hotel_weather[...]  ## Reads hotel_weather data.


Generate explode(sequence(...)) operator is themost resource consuming part, which breaks each hotel booking into individual days between the check-in and check-out dates, is the heaviest operation. This step can dramatically increase the number of rows that need to be processed, putting significant pressure on subsequent operations like aggregations and window functions
```
## And the third query:
# For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay
```sql
-- First CTE: Create one row per day for each booking that lasts between 7 and 30 days.
WITH exploded_dates AS (
    SELECT
        ex.id AS booking_id,     -- Unique booking identifier.
        ex.hotel_id,             -- Hotel identifier.
        ex.srch_ci,              -- Check-in date.
        ex.srch_co,              -- Check-out date.
        -- Generate a sequence of dates from the check-in date to the day before the check-out date.
        -- The 'explode' function creates a separate row for each date in the sequence.
        explode(sequence(
            CAST(ex.srch_ci AS DATE), 
            CAST(ex.srch_co AS DATE) - INTERVAL 1 DAY,
            INTERVAL 1 DAY
        )) AS visit_date
    FROM mydatabase.expedia ex
    WHERE 
        -- Only include bookings with a duration between 7 and 30 days.
        DATEDIFF(ex.srch_co, ex.srch_ci) BETWEEN 7 AND 30
        AND ex.srch_co > ex.srch_ci  -- Ensure the check-out date is after the check-in date.
),
-- Second CTE: Join the exploded booking dates with hotel weather data.
joined_weather AS (
    SELECT 
        ed.booking_id,   -- Booking identifier from the exploded_dates CTE.
        ed.hotel_id,     -- Hotel identifier.
        ed.visit_date,   -- The individual visit date generated earlier.
        hw.avg_tmpr_c,   -- The average temperature from the hotel_weather table.
        hw.address       -- Hotel address.
    FROM exploded_dates ed
    LEFT JOIN mydatabase.hotel_weather hw
      -- Join on matching hotel IDs and where the weather date corresponds to the visit_date.
      ON ed.hotel_id = hw.id 
         AND CAST(hw.wthr_date AS DATE) = ed.visit_date
    WHERE hw.avg_tmpr_c IS NOT NULL  -- Only include records with temperature data.
),
-- Third CTE: Apply window functions to capture the first and last temperature of each booking.
windowed_temps AS (
    SELECT
        booking_id,
        hotel_id,
        address,
        visit_date,
        avg_tmpr_c,
        -- Get the first average temperature for the booking (earliest visit_date).
        FIRST_VALUE(avg_tmpr_c) OVER (PARTITION BY booking_id ORDER BY visit_date) AS first_temp,
        -- Get the last average temperature for the booking (latest visit_date)
        -- using an unbounded frame to cover the full partition.
        LAST_VALUE(avg_tmpr_c) OVER (
            PARTITION BY booking_id 
            ORDER BY visit_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_temp
    FROM joined_weather
),
-- Fourth CTE: Calculate temperature trend and average temperature for each booking.
temp_calculations AS (
    SELECT
        booking_id,
        hotel_id,
        address,
        MIN(visit_date) AS first_day,      -- The first day of the booking.
        MAX(visit_date) AS last_day,         -- The last day of the booking.
        -- Calculate the temperature trend as the difference between last and first temperatures.
        ROUND(last_temp - first_temp, 2) AS temp_trend,
        -- Calculate the average temperature during the booking.
        ROUND(AVG(avg_tmpr_c), 2) AS avg_temperature
    FROM windowed_temps
    GROUP BY booking_id, hotel_id, address, first_temp, last_temp
)
-- Final query: Retrieve bookings with valid temperature trends and a minimum duration of 7 days,
-- and order the results by the absolute temperature trend in descending order.
SELECT *
FROM temp_calculations
WHERE temp_trend IS NOT NULL
  AND avg_temperature IS NOT NULL
  AND DATEDIFF(last_day, first_day) >= 7
ORDER BY ABS(temp_trend) DESC;
```
## You can se the franction of the query's result:

![3rd_result](https://github.com/user-attachments/assets/f3cf0d69-3d34-4d6d-bbfa-27da4e4485eb)

## Here is the whole query with the EXPLAIN clause:
```sql
%sql
EXPLAIN
WITH exploded_dates AS (
    SELECT
        ex.id AS booking_id,
        ex.hotel_id,
        ex.srch_ci,
        ex.srch_co,
        explode(sequence(
            CAST(ex.srch_ci AS DATE), 
            CAST(ex.srch_co AS DATE) - INTERVAL 1 DAY,
            INTERVAL 1 DAY
        )) AS visit_date
    FROM mydatabase.expedia ex
    WHERE DATEDIFF(ex.srch_co, ex.srch_ci) BETWEEN 7 AND 30
      AND ex.srch_co > ex.srch_ci
),
joined_weather AS (
    SELECT 
        ed.booking_id,
        ed.hotel_id,
        ed.visit_date,
        hw.avg_tmpr_c,
        hw.address
    FROM exploded_dates ed
    LEFT JOIN mydatabase.hotel_weather hw
      ON ed.hotel_id = hw.id 
         AND CAST(hw.wthr_date AS DATE) = ed.visit_date
    WHERE hw.avg_tmpr_c IS NOT NULL
),
windowed_temps AS (
    SELECT
        booking_id,
        hotel_id,
        address,
        visit_date,
        avg_tmpr_c,
        FIRST_VALUE(avg_tmpr_c) OVER (PARTITION BY booking_id ORDER BY visit_date) AS first_temp,
        LAST_VALUE(avg_tmpr_c) OVER (PARTITION BY booking_id ORDER BY visit_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_temp
    FROM joined_weather
),
temp_calculations AS (
    SELECT
        booking_id,
        hotel_id,
        address,
        MIN(visit_date) AS first_day,
        MAX(visit_date) AS last_day,
        ROUND(last_temp - first_temp, 2) AS temp_trend,
        ROUND(AVG(avg_tmpr_c), 2) AS avg_temperature
    FROM windowed_temps
    GROUP BY booking_id, hotel_id, address, first_temp, last_temp
)
SELECT *
FROM temp_calculations
WHERE temp_trend IS NOT NULL
  AND avg_temperature IS NOT NULL
  AND DATEDIFF(last_day, first_day) >= 7
ORDER BY ABS(temp_trend) DESC;
```

## And finally the analyzed execution plan:
```sql
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false  -- Adaptive Query Execution (AQE) is enabled for dynamic optimizations

+- == Initial Plan ==
   Sort [abs(temp_trend#871) DESC NULLS LAST], true, 0  -- Sorting the final output by absolute temperature trend in descending order
   +- Exchange rangepartitioning(abs(temp_trend#871) DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=1246]  
      -- Shuffling data across partitions based on temperature trend for parallel processing

      +- Filter (((isnotnull(last_day#870) AND isnotnull(first_day#869)) AND isnotnull(avg_temperature#872)) 
                 AND (datediff(last_day#870, first_day#869) >= 7))
         -- Filtering out records where first_day, last_day, or avg_temperature is NULL
         -- Ensuring that only bookings with a duration of at least 7 days are included

         +- HashAggregate(
              keys=[booking_id#865L, hotel_id#927L, address#942, 
                    knownfloatingpointnormalized(normalizenanandzero(first_temp#867)) AS first_temp#867, 
                    knownfloatingpointnormalized(normalizenanandzero(last_temp#868)) AS last_temp#868], 
              functions=[min(visit_date#957), max(visit_date#957), avg(avg_tmpr_c#943)])
            -- Aggregating data per booking_id, hotel_id, and address
            -- Calculating the first and last visit date, temperature trend, and average temperature

            +- Filter isnotnull(round((last_temp#868 - first_temp#867), 2))
               -- Filtering out any records where the temperature trend calculation is NULL

               +- Window [
                    booking_id#865L, hotel_id#927L, address#942, visit_date#957, avg_tmpr_c#943, 
                    first_value(avg_tmpr_c#943, false) OVER 
                      (PARTITION BY booking_id ORDER BY visit_date ASC NULLS FIRST 
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_temp#867, 
                    last_value(avg_tmpr_c#943, false) OVER 
                      (PARTITION BY booking_id ORDER BY visit_date ASC NULLS FIRST 
                      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_temp#868
                  ], 
                  [booking_id#865L], [visit_date#957 ASC NULLS FIRST]
                 -- Using window functions to determine the first and last temperature per booking_id
                 -- FIRST_VALUE() gets the temperature at the first visit date
                 -- LAST_VALUE() gets the temperature at the last visit date

                  +- Sort [booking_id#865L ASC NULLS FIRST, visit_date#957 ASC NULLS FIRST], false, 0
                     -- Sorting data by booking ID and visit date to optimize window function execution

                     +- Exchange hashpartitioning(booking_id#865L, 200), ENSURE_REQUIREMENTS, [plan_id=1240]
                        -- Shuffling data to partition by booking_id before applying window functions

                        +- Project [booking_id#865L, hotel_id#927L, address#942, visit_date#957, avg_tmpr_c#943]
                           -- Selecting relevant columns for further processing

                           +- BroadcastHashJoin [hotel_id#927L, visit_date#957], 
                                                [cast(id#948 as bigint), cast(wthr_date#952 as date)], 
                                                Inner, BuildRight, false, true
                              -- Performing a **broadcast hash join** between Expedia bookings and weather data
                              -- The weather data is **broadcasted** (small table optimization)

                              :- Project [id#908L AS booking_id#865L, hotel_id#927L, visit_date#957]
                              :  +- Generate explode(sequence(
                                         cast(srch_ci#920 as date), 
                                         date_add(cast(srch_co#921 as date), -1), 
                                         Some(INTERVAL '1' DAY), 
                                         Some(Etc/UTC))), 
                                         [id#908L, hotel_id#927L], false, [visit_date#957]
                              :     -- Expanding (exploding) each booking into multiple records, one for each visit date
                              :     -- sequence() generates a list of all dates between check-in and check-out
                              :     -- explode() transforms the list into individual rows

                              :     +- ColumnarToRow
                              :        +- PhotonResultStage
                              :           +- PhotonScan parquet spark_catalog.mydatabase.expedia
                                           [id#908L, srch_ci#920, srch_co#921, hotel_id#927L] 
                                           DataFilters: [isnotnull(hotel_id#927L), 
                                                         isnotnull(srch_co#921), 
                                                         isnotnull(srch_ci#920), 
                                                         (srch_co#921 > srch_ci...)], 
                                           Format: parquet, Location: Delta Lake
                                           -- Scanning the Expedia dataset from Delta Lake in **columnar format**
                                           -- Filtering out NULL values and invalid date ranges

                              +- Exchange SinglePartition, EXECUTOR_BROADCAST, [plan_id=1237]
                                 -- Broadcasting the weather dataset to all partitions

                                 +- ColumnarToRow
                                    +- PhotonResultStage
                                       +- PhotonProject [address#942, avg_tmpr_c#943, id#948, wthr_date#952]
                                          +- PhotonScan parquet spark_catalog.mydatabase.hotel_weather
                                             [address#942, avg_tmpr_c#943, id#948, wthr_date#952] 
                                             DataFilters: [isnotnull(avg_tmpr_c#943), 
                                                           isnotnull(id#948), 
                                                           isnotnull(wthr_date#952)], 
                                             Format: parquet, Location: Delta Lake
                                             -- Scanning the hotel weather dataset from Delta Lake
                                             -- Filtering out NULL values in key columns

-- The most "expensive" operation is the sort +- Sort [booking_id#865L ASC NULLS FIRST, visit_date#957 ASC NULLS FIRST], false, 0 before the window function.
--I can improve the operation with application of ZORDER BY (booking_id, visit_date) is the particular Delta table.

```

## As I wrote above I applied some optimizations:
## 1. Reorganization of the tables iwth ZORDER BY for more efficient querys(less shuffling, faster querys):
```sql
OPTIMIZE mydatabase.expedia
ZORDER BY (booking_id, srch_ci);

OPTIMIZE mydatabase.hotel_weather
ZORDER BY (id, wthr_date);
```
## 2. I avoided the expensive CAST conversions and the dynamic sequence() functions with a predefined date_squence table:
 ```sql
CREATE TABLE mydatabase.date_sequence AS 
SELECT explode(sequence(
    to_date('2016-10-01'), 
    to_date('2018-10-06'), 
    INTERVAL 1 DAY)) AS visit_date;
```
## 3. Data preprocessing: the window functions are very expensive, so I precomputed them to an aggregated table:
```sql
CREATE TABLE mydatabase.windowed_temps AS 
SELECT
    booking_id,
    hotel_id,
    address,
    visit_date,
    avg_tmpr_c,
    FIRST_VALUE(avg_tmpr_c) OVER (
        PARTITION BY booking_id ORDER BY visit_date
    ) AS first_temp,
    LAST_VALUE(avg_tmpr_c) OVER (
        PARTITION BY booking_id ORDER BY visit_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_temp
FROM mydatabase.joined_weather;
```
## Then optimized it:
```sql
OPTIMIZE mydatabase.windowed_temps
ZORDER BY (booking_id, visit_date);
```
## This will be the whole query:

```sql
-- First, optimize the base tables
OPTIMIZE mydatabase.expedia
ZORDER BY (booking_id, srch_ci);

OPTIMIZE mydatabase.hotel_weather
ZORDER BY (id, wthr_date);

-- Create the date sequence table to avoid recalculating it each time
CREATE TABLE mydatabase.date_sequence AS 
SELECT explode(sequence(
    to_date('2016-10-01'), 
    to_date('2018-10-06'), 
    INTERVAL 1 DAY)) AS visit_date;

-- Create the exploded_dates table with an efficient join
CREATE TABLE mydatabase.exploded_dates AS
SELECT 
    ex.id AS booking_id,
    ex.hotel_id,
    ds.visit_date
FROM mydatabase.expedia ex
JOIN mydatabase.date_sequence ds
  ON ds.visit_date BETWEEN CAST(ex.srch_ci AS DATE) AND CAST(ex.srch_co AS DATE) - INTERVAL 1 DAY
WHERE DATEDIFF(ex.srch_co, ex.srch_ci) BETWEEN 7 AND 30;

-- Prepare the join with the ZORDER optimized join
CREATE TABLE mydatabase.joined_weather AS
SELECT 
    ed.booking_id,
    ed.hotel_id,
    ed.visit_date,
    hw.avg_tmpr_c,
    hw.address
FROM mydatabase.exploded_dates ed
LEFT JOIN mydatabase.hotel_weather hw
  ON ed.hotel_id = hw.id 
     AND hw.wthr_date = ed.visit_date
WHERE hw.avg_tmpr_c IS NOT NULL;

-- Calculate window functions in advance and store them in a separate table
CREATE OR REPLACE TABLE mydatabase.windowed_temps AS 
SELECT
    booking_id,
    hotel_id,
    address,
    visit_date,
    avg_tmpr_c,
    FIRST_VALUE(avg_tmpr_c) OVER (
        PARTITION BY booking_id ORDER BY visit_date
    ) AS first_temp,
    LAST_VALUE(avg_tmpr_c) OVER (
        PARTITION BY booking_id ORDER BY visit_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_temp
FROM mydatabase.joined_weather;

-- Now optimize the newly created tables as well
OPTIMIZE mydatabase.exploded_dates
ZORDER BY (booking_id, visit_date);

OPTIMIZE mydatabase.joined_weather
ZORDER BY (booking_id, visit_date);

OPTIMIZE mydatabase.windowed_temps
ZORDER BY (booking_id, visit_date);

--  Finally, run the main query
WITH temp_calculations AS (
    SELECT
        booking_id,
        hotel_id,
        address,
        MIN(visit_date) AS first_day,
        MAX(visit_date) AS last_day,
        ROUND(last_temp - first_temp, 2) AS temp_trend,
        ROUND(AVG(avg_tmpr_c), 2) AS avg_temperature
    FROM mydatabase.windowed_temps
    GROUP BY booking_id, hotel_id, address, first_temp, last_temp
)
SELECT *
FROM temp_calculations
WHERE temp_trend IS NOT NULL
  AND avg_temperature IS NOT NULL
  AND DATEDIFF(last_day, first_day) >= 7
ORDER BY ABS(temp_trend) DESC;
```

## Then I saved the first query preserving the original date based partitioning:
```python
df_first = spark.sql("""
	SELECT 
    address,    -- Select the hotel address.
    year,       -- Select the year from the data.
    month,      -- Select the month from the data.
    -- Calculate the temperature difference within each group:
    --   1. Find the maximum average temperature (avg_tmpr_c) in the group.
    --   2. Find the minimum average temperature (avg_tmpr_c) in the group.
    --   3. Compute the absolute difference between these values.
    --   4. Round the result to 2 decimal places and alias it as 'temp_diff'.
    ROUND(ABS(MAX(avg_tmpr_c) - MIN(avg_tmpr_c)), 2) AS temp_diff
	FROM mydatabase.hotel_weather   -- Data is sourced from the hotel_weather table.
	GROUP BY address, year, month    -- Group the records by hotel address, year, and month.
	ORDER BY temp_diff DESC          -- Order the groups by temperature difference in descending order.
	LIMIT 10;                       -- Limit the result to the top 10 groups with the largest temperature differences.
""")

df_first.write.format("parquet") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .save("dbfs:/mnt/data/final_datamarts/first")
```
## Also checked it:
```python
df_check = spark.read.parquet("dbfs:/mnt/data/final_datamarts/first")
df_check.show(10)
```
+--------------------+---------+----+-----+
|             address|temp_diff|year|month|
+--------------------+---------+----+-----+
|         Rodeway Inn|     20.2|2016|   10|
|Americas Best Val...|     20.3|2016|   10|
|Quality Inn and S...|     21.1|2016|   10|
|             Motel 6|     21.7|2016|   10|
|            Studio 6|     23.0|2016|   10|
|         Comfort Inn|     23.5|2016|   10|
|Americas Best Val...|     19.6|2017|    9|
|         Comfort Inn|     19.9|2017|    9|
|Quality Inn & Suites|     20.2|2017|    9|
|Quality Inn and S...|     21.7|2017|    9|
+--------------------+---------+----+-----+

## Here you can see the saved data:

![first_saved](https://github.com/user-attachments/assets/6e626d29-905b-4b13-a585-f82b846f3e8c)

## Then I saved the second query preserving the original date based partitioning:
```python
df_second = spark.sql("""
	WITH exploded_dates AS (
		SELECT
			ex.hotel_id,
			hw.address,
			explode(sequence(
				CAST(ex.srch_ci AS DATE), 
				CAST(ex.srch_co AS DATE) - INTERVAL 1 DAY, 
				interval 1 day)) AS visit_date
		FROM mydatabase.expedia ex
		LEFT JOIN mydatabase.hotel_weather hw
		ON ex.hotel_id = hw.id
		WHERE CAST(ex.srch_ci AS DATE) < CAST(ex.srch_co AS DATE)
		AND hw.address IS NOT NULL
	),
	monthly_visits AS (
		SELECT
			address,
			YEAR(visit_date) AS year,
			MONTH(visit_date) AS month,
			COUNT(*) AS visits_count
		FROM exploded_dates
		GROUP BY address, YEAR(visit_date), MONTH(visit_date)
	),
	ranked_hotels AS (
		SELECT *,
			DENSE_RANK() OVER (PARTITION BY year, month ORDER BY visits_count DESC) AS rank
		FROM monthly_visits
	)
	SELECT * FROM ranked_hotels WHERE rank <= 10;
""")
df_second.write.format("parquet") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .save("dbfs:/mnt/data/final_datamarts/second")
```

## Also checked it:
```python
df_check = spark.read.parquet("dbfs:/mnt/data/final_datamarts/second")
df_check.show(10)
```
+--------------------+------------+----+----+-----+
|             address|visits_count|rank|year|month|
+--------------------+------------+----+----+-----+
|Sofitel Paris Le ...|         420|   1|2017|   11|
|The Pelham Starho...|         300|   2|2017|   11|
|   Hotel Millersburg|         300|   2|2017|   11|
|Best Western Hote...|         270|   3|2017|   11|
|     TH Street Duomo|         270|   3|2017|   11|
|IH Hotels Milano ...|         270|   3|2017|   11|
|      Shoshone Lodge|         270|   3|2017|   11|
|            Nu Hotel|         270|   3|2017|   11|
|Conservatorium Hotel|         246|   4|2017|   11|
|Mercure Vaugirard...|         240|   5|2017|   11|
+--------------------+------------+----+----+-----+

![second_saved](https://github.com/user-attachments/assets/6cf6dc1c-a23e-4fc4-a5ce-219e7e6205e6)


## Then I saved the third query preserving the original date based partitioning:
```python
df_third = spark.sql("""
	WITH exploded_dates AS (
		SELECT
			ex.id AS booking_id,
			ex.hotel_id,
			ex.srch_ci,
			ex.srch_co,
			explode(sequence(
				CAST(ex.srch_ci AS DATE), 
				CAST(ex.srch_co AS DATE) - INTERVAL 1 DAY,
				INTERVAL 1 DAY
			)) AS visit_date
		FROM mydatabase.expedia ex
		WHERE DATEDIFF(ex.srch_co, ex.srch_ci) BETWEEN 7 AND 30
		AND ex.srch_co > ex.srch_ci
	),
	joined_weather AS (
		SELECT 
			ed.booking_id,
			ed.hotel_id,
			ed.visit_date,
			hw.avg_tmpr_c,
			hw.address
		FROM exploded_dates ed
		LEFT JOIN mydatabase.hotel_weather hw
		ON ed.hotel_id = hw.id 
			AND CAST(hw.wthr_date AS DATE) = ed.visit_date
		WHERE hw.avg_tmpr_c IS NOT NULL
	),
	windowed_temps AS (
		SELECT
			booking_id,
			hotel_id,
			address,
			visit_date,
			avg_tmpr_c,
			FIRST_VALUE(avg_tmpr_c) OVER (PARTITION BY booking_id ORDER BY visit_date) AS first_temp,
			LAST_VALUE(avg_tmpr_c) OVER (PARTITION BY booking_id ORDER BY visit_date 
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_temp
		FROM joined_weather
	),
	temp_calculations AS (
		SELECT
			booking_id,
			hotel_id,
			address,
			MIN(visit_date) AS first_day,
			MAX(visit_date) AS last_day,
			ROUND(last_temp - first_temp, 2) AS temp_trend,
			ROUND(AVG(avg_tmpr_c), 2) AS avg_temperature
		FROM windowed_temps
		GROUP BY booking_id, hotel_id, address, first_temp, last_temp
	)
	SELECT *
	FROM temp_calculations
	WHERE temp_trend IS NOT NULL
	AND avg_temperature IS NOT NULL
	AND DATEDIFF(last_day, first_day) >= 7
	ORDER BY ABS(temp_trend) DESC;
""")

df_third.write.format("parquet") \
    .mode("overwrite") \
    .partitionBy("first_day", "last_day") \
    .save("dbfs:/mnt/data/final_datamarts/third")
```

## then checked it:
```python
df_check = spark.read.parquet("dbfs:/mnt/data/final_datamarts/third")
df_check.show(10)
df_check.count()
```
+----------+-------------+--------------------+----------+---------------+----------+----------+
|booking_id|     hotel_id|             address|temp_trend|avg_temperature| first_day|  last_day|
+----------+-------------+--------------------+----------+---------------+----------+----------+
|    660388|1262720385024|Holiday Inn Expre...|      -1.6|           34.1|2017-08-25|2017-09-03|
|     22002|1262720385024|Holiday Inn Expre...|      -1.6|           34.1|2017-08-25|2017-09-03|
|   2484059|1812476198912|  Staunton Hotel B B|      -3.2|           15.5|2017-08-25|2017-09-03|
|   2456610|1855425871872|           Avo Hotel|      -3.2|           15.5|2017-08-25|2017-09-03|
|   2418879|2310692405248|DoubleTree by Hil...|      -3.2|           15.5|2017-08-25|2017-09-03|
|   2389551|2602750181378|Old Ship Inn Hackney|      -3.2|           15.5|2017-08-25|2017-09-03|
|   2338050|2834678415363|Club Quarters Hot...|      -3.2|           15.5|2017-08-25|2017-09-03|
|   2318405|2276332666883|Holiday Inn Londo...|      -3.2|           15.5|2017-08-25|2017-09-03|
|   2302704|1812476198912|  Staunton Hotel B B|      -3.2|           15.5|2017-08-25|2017-09-03|
|   2271827|2413771620353|Comfort Inn Suite...|      -3.2|           15.5|2017-08-25|2017-09-03|
+----------+-------------+--------------------+----------+---------------+----------+----------+
only showing top 10 rows

1410

![third_saved](https://github.com/user-attachments/assets/4e5a944c-f740-42d6-bde0-7705bc5f62f3)

## Then headed to the CI/CD task. The concept is implement it locally with terraform and a Makefile.
## I made  a main.tf file:
```python
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 4.0.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.0.0"
    }
  }

  backend "azurerm" {
    resource_group_name  = "ro"
    storage_account_name = "deo"
    container_name       = "tfstate"
    key                  = "terraform.tfstate"
  }
}

provider "azurerm" {
  features {}

  subscription_id = var.AZURE_SUBSCRIPTION_ID
  tenant_id       = var.AZURE_TENANT_ID
  client_id       = var.AZURE_CLIENT_ID
  client_secret   = var.AZURE_CLIENT_SECRET
}

provider "databricks" {
  host  = var.DATABRICKS_HOST
  token = var.DATABRICKS_TOKEN
}


data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

resource "databricks_cluster" "cicd1" {
  cluster_name           = "Azure_Spark_SQL"
  spark_version          = data.databricks_spark_version.latest_lts.id 
  node_type_id           = "Standard_DS3_v2"
  autotermination_minutes = 30

  spark_conf = {
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.master"                     = "local[*]"
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
}

resource "azurerm_storage_account" "Azure_Spark_SQL_storage" {
  name                     = var.STORAGE_ACCOUNT_NAME
  resource_group_name       = var.RESOURCE_GROUP_NAME
  location                 = "West Europe"
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "data" {
  name                  = "data"
  storage_account_name  = azurerm_storage_account.Azure_Spark_SQL_storage.name
  container_access_type = "container"
}


resource "databricks_notebook" "preconfig" {
  path     = "/Users/balm/preconfig"
  language = "PYTHON"
  source   = "${path.module}/preconfig.py"
}

resource "databricks_notebook" "saving" {
  path     = "/Users/baim/saving"
  language = "PYTHON"
  source   = "${path.module}/saving.py"
}

resource "databricks_job" "run_preconfig" {
  name = "run preconfig"

  task {
    task_key = "preconfig"
    
    notebook_task {
      notebook_path = databricks_notebook.preconfig.path
    }

    existing_cluster_id = databricks_cluster.cicd1.id
  }
}

resource "databricks_job" "run_saving" {
  name = "run saving"

  task {
    task_key = "saving"
    
    notebook_task {
      notebook_path = databricks_notebook.saving.path
    }

    existing_cluster_id = databricks_cluster.cicd1.id
  }
}

output "job_ids" {
  value = {
    preconfig = databricks_job.run_preconfig.id
    saving = databricks_job.run_saving.id
  }
}

```
## Here is the variables.tf file:
```python
variable "AZURE_SUBSCRIPTION_ID" {}
variable "AZURE_TENANT_ID" {}
variable "AZURE_CLIENT_ID" {}
variable "AZURE_CLIENT_SECRET" {}
variable "DATABRICKS_HOST" {}
variable "DATABRICKS_TOKEN" {}

variable "STORAGE_ACCOUNT_NAME" {}
variable "RESOURCE_GROUP_NAME" {}
```
## And terraform.tfvars, where the secrets are defined.
## Here you can see the Makefile:
```python
# Terraform initialization
init:
	@echo "Initializing Terraform..."
	terraform init

# Terraform plan creation
plan:
	@echo "Generating Terraform plan..."
	terraform plan -var-file=terraform.tfvars -lock=false

# Terraform apply
apply:
	@echo "Applying Terraform changes..."
	terraform apply -var-file=terraform.tfvars -lock=false -auto-approve

output job_ids:
	@echo "the job IDs are..."
	terraform output job_ids

# Terraform deletion
destroy:
	@echo "Destroying Terraform infrastructure..."
	terraform destroy -auto-approve
```
## The result of the init:
```python
C:\ProgramData\chocolatey\bin\make.exe -f C:/data_eng/házi/2/Makefile -C C:\data_eng\házi\2 init
make: Entering directory 'C:/data_eng/hßzi/2'
"Initializing Terraform..."
terraform init
Initializing the backend...
Initializing provider plugins...
- Reusing previous version of hashicorp/azurerm from the dependency lock file
- Reusing previous version of databricks/databricks from the dependency lock fil
e
- Using previously-installed hashicorp/azurerm v4.23.0
- Using previously-installed databricks/databricks v1.70.0

Terraform has been successfully initialized!

You may now begin working with Terraform. Try running "terraform plan" to see
any changes that are required for your infrastructure. All Terraform commands
should now work.

If you ever set or change modules or backend configuration for Terraform,
rerun this command to reinitialize your working directory. If you forget, other
commands will detect it and remind you to do so if necessary.
make: Leaving directory 'C:/data_eng/h�zi/2'

Process finished with exit code 0
```
## The result of the plan:
```python
C:\ProgramData\chocolatey\bin\make.exe -f C:/data_eng/házi/2/Makefile -C C:\data_eng\házi\2 plan
make: Entering directory 'C:/data_eng/hßzi/2'
"Generating Terraform plan..."
terraform plan -var-file=terraform.tfvars -lock=false
data.databricks_spark_version.latest_lts: Reading...
databricks_notebook.simple_1: Refreshing state... [id=/Users/balag
/simple_1]
databricks_job.run_simple_1: Refreshing state... [id=73]
data.databricks_spark_version.latest_lts: Read complete after 1s [id=15.4.x-scal
a2.12]
databricks_cluster.cicd1: Refreshing state... [id=0316-125836-v3rso6m0]
azurerm_storage_account.Azure_Spark_SQL_storage: Refreshing state... [id=/subscr
iptions/69d7689b/resourceGroups/rg-development-weste
urope-6o/providers/Microsoft.Storage/storageAccounts/developmentwesteurope6o]
azurerm_storage_container.data: Refreshing state... [id=https://developmentweste
urope6o.blob.core.windows.net/data]

Note: Objects have changed outside of Terraform

Terraform detected the following changes made outside of Terraform since the
last "terraform apply" which may have affected this plan:

  # databricks_cluster.cicd1 has been deleted
  - resource "databricks_cluster" "cicd1" {
      - id                           = "0316-125836-v3rso6m0" -> null
        # (17 unchanged attributes hidden)

        # (1 unchanged block hidden)
    }


Unless you have made equivalent changes to your configuration, or ignored the
relevant attributes using ignore_changes, the following plan may include
actions to undo or respond to these changes.

───────────────────────────────────────────────────────────────────────────────

Terraform used the selected providers to generate the following execution plan.
Resource actions are indicated with the following symbols:
  + create
  - destroy

Terraform will perform the following actions:

  # databricks_cluster.cicd1 will be created
  + resource "databricks_cluster" "cicd1" {
      + autotermination_minutes      = 30
      + cluster_id                   = (known after apply)
      + cluster_name                 = "Azure_Spark_SQL"
      + custom_tags                  = {
          + "ResourceClass" = "SingleNode"
        }
      + default_tags                 = (known after apply)
      + driver_instance_pool_id      = (known after apply)
      + driver_node_type_id          = (known after apply)
      + enable_elastic_disk          = (known after apply)
      + enable_local_disk_encryption = (known after apply)
      + id                           = (known after apply)
      + node_type_id                 = "Standard_DS3_v2"
      + num_workers                  = 0
      + spark_conf                   = {
          + "spark.databricks.cluster.profile" = "singleNode"
          + "spark.master"                     = "local[*]"
        }
      + spark_version                = "15.4.x-scala2.12"
      + state                        = (known after apply)
      + url                          = (known after apply)
    }

  # databricks_job.run_preconfig will be created
  + resource "databricks_job" "run_preconfig" {
      + always_running      = false
      + control_run_state   = false
      + format              = (known after apply)
      + id                  = (known after apply)
      + max_concurrent_runs = 1
      + name                = "run preconfig"
      + url                 = (known after apply)

      + run_as (known after apply)

      + task {
          + existing_cluster_id = (known after apply)
          + retry_on_timeout    = (known after apply)
          + task_key            = "preconfig"

          + notebook_task {
              + notebook_path = "/Users/b.com/preconfig"
            }
        }
    }

  # databricks_job.run_saving will be created
  + resource "databricks_job" "run_saving" {
      + always_running      = false
      + control_run_state   = false
      + format              = (known after apply)
      + id                  = (known after apply)
      + max_concurrent_runs = 1
      + name                = "run saving"
      + url                 = (known after apply)

      + run_as (known after apply)

      + task {
          + existing_cluster_id = (known after apply)
          + retry_on_timeout    = (known after apply)
          + task_key            = "saving"

          + notebook_task {
              + notebook_path = "/Users/balage330@gmail.com/saving"
            }
        }
    }

  # databricks_job.run_simple_1 will be destroyed
  # (because databricks_job.run_simple_1 is not in configuration)
  - resource "databricks_job" "run_simple_1" {
      - always_running            = false -> null
      - control_run_state         = false -> null
      - format                    = "MULTI_TASK" -> null
      - id                        = "7313" -> null
      - max_concurrent_runs       = 1 -> null
      - max_retries               = 0 -> null
      - min_retry_interval_millis = 0 -> null
      - name                      = "Run Simple SQL Notebook" -> null
      - retry_on_timeout          = false -> null
      - timeout_seconds           = 0 -> null
      - url                       = "https://adb-51228788865048.8.azuredatabrick
s.net/#job/706668043510313" -> null

      - email_notifications {
          - no_alert_for_skipped_runs              = false -> null
          - on_duration_warning_threshold_exceeded = [] -> null
          - on_failure                             = [] -> null
          - on_start                               = [] -> null
          - on_streaming_backlog_exceeded          = [] -> null
          - on_success                             = [] -> null
        }

      - run_as {
          - user_name              = "balom" -> null
            # (1 unchanged attribute hidden)
        }

      - task {
          - disable_auto_optimization = false -> null
          - existing_cluster_id       = "031so6m0" -> null
          - max_retries               = 0 -> null
          - min_retry_interval_millis = 0 -> null
          - retry_on_timeout          = false -> null
          - run_if                    = "ALL_SUCCESS" -> null
          - task_key                  = "simple_1_task" -> null
          - timeout_seconds           = 0 -> null
            # (3 unchanged attributes hidden)

          - email_notifications {
              - no_alert_for_skipped_runs              = false -> null
              - on_duration_warning_threshold_exceeded = [] -> null
              - on_failure                             = [] -> null
              - on_start                               = [] -> null
              - on_streaming_backlog_exceeded          = [] -> null
              - on_success                             = [] -> null
            }

          - notebook_task {
              - base_parameters = {} -> null
              - notebook_path   = "/Users/balage330@gmail.com/simple_1" -> null
              - source          = "WORKSPACE" -> null
                # (1 unchanged attribute hidden)
            }
        }

      - webhook_notifications {
        }
    }

  # databricks_notebook.preconfig will be created
  + resource "databricks_notebook" "preconfig" {
      + format         = (known after apply)
      + id             = (known after apply)
      + language       = "PYTHON"
      + md5            = "different"
      + object_id      = (known after apply)
      + object_type    = (known after apply)
      + path           = "/Users/balage330@gmail.com/preconfig"
      + source         = "./preconfig.py"
      + url            = (known after apply)
      + workspace_path = (known after apply)
    }

  # databricks_notebook.saving will be created
  + resource "databricks_notebook" "saving" {
      + format         = (known after apply)
      + id             = (known after apply)
      + language       = "PYTHON"
      + md5            = "different"
      + object_id      = (known after apply)
      + object_type    = (known after apply)
      + path           = "/Users/balage330@gmail.com/saving"
      + source         = "./saving.py"
      + url            = (known after apply)
      + workspace_path = (known after apply)
    }

  # databricks_notebook.simple_1 will be destroyed
  # (because databricks_notebook.simple_1 is not in configuration)
  - resource "databricks_notebook" "simple_1" {
      - format         = "SOURCE" -> null
      - id             = "/Users/bam/simple_1" -> null
      - language       = "SQL" -> null
      - md5            = "12f15c25fbe89dcd196e6d87ce99b34d" -> null
      - object_id      = 557163049195297 -> null
      - object_type    = "NOTEBOOK" -> null
      - path           = "/Users/balam/simple_1" -> null
      - source         = "./simple.sql" -> null
      - url            = "https://icks.net/#work
space/Users/balage330@gmail.com/simple_1" -> null
      - workspace_path = "/Workspace/Users/bacom/simple_1" -> null
    }

Plan: 5 to add, 0 to change, 2 to destroy.

Changes to Outputs:
  + job_ids = {
      + preconfig = (known after apply)
      + saving    = (known after apply)
    }
╷
│ Warning: Argument is deprecated
│
│   with azurerm_storage_container.data,
│   on main.tf line 66, in resource "azurerm_storage_container" "data":
│   66:   storage_account_name  = azurerm_storage_account.Azure_Spark_SQL_storag
e.name
│
│ the `storage_account_name` property has been deprecated in favour of
│ `storage_account_id` and will be removed in version 5.0 of the Provider.
╵

───────────────────────────────────────────────────────────────────────────────

Note: You didn't use the -out option to save this plan, so Terraform can't
guarantee to take exactly these actions if you run "terraform apply" now.
make: Leaving directory 'C:/data_eng/h�zi/2'

Process finished with exit code 0
```
## The result of the successful apply:
```python
C:\ProgramData\chocolatey\bin\make.exe -f C:/data_eng/házi/2/Makefile -C C:\data_eng\házi\2 apply
make: Entering directory 'C:/data_eng/hßzi/2'
"Applying Terraform changes..."
terraform apply -var-file=terraform.tfvars -lock=false -auto-approve
data.databricks_spark_version.latest_lts: Reading...
databricks_notebook.simple_1: Refreshing state... [id=/Users/baom
/simple_1]
databricks_job.run_simple_1: Refreshing state... [id=703]
data.databricks_spark_version.latest_lts: Still reading... [10s elapsed]
data.databricks_spark_version.latest_lts: Read complete after 15s [id=15.4.x-sca
la2.12]
databricks_cluster.cicd1: Refreshing state... [id=0316-125836-v3rso6m0]
azurerm_storage_account.Azure_Spark_SQL_storage: Refreshing state... [id=/subscr
iptions/69d1cd19689b/resourceGroups/rg-development-weste
urope-6o/providers/Microsoft.Storage/storageAccounts/developmentwesteurope6o]
azurerm_storage_container.data: Refreshing state... [id=https://developmentweste
urope6o.blob.core.windows.net/data]

Note: Objects have changed outside of Terraform

Terraform detected the following changes made outside of Terraform since the
last "terraform apply" which may have affected this plan:

  # databricks_cluster.cicd1 has been deleted
  - resource "databricks_cluster" "cicd1" {
      - id                           = "0316m0" -> null
        # (17 unchanged attributes hidden)

        # (1 unchanged block hidden)
    }


Unless you have made equivalent changes to your configuration, or ignored the
relevant attributes using ignore_changes, the following plan may include
actions to undo or respond to these changes.

───────────────────────────────────────────────────────────────────────────────

Terraform used the selected providers to generate the following execution plan.
Resource actions are indicated with the following symbols:
  + create
  - destroy

Terraform will perform the following actions:

  # databricks_cluster.cicd1 will be created
  + resource "databricks_cluster" "cicd1" {
      + autotermination_minutes      = 30
      + cluster_id                   = (known after apply)
      + cluster_name                 = "Azure_Spark_SQL"
      + custom_tags                  = {
          + "ResourceClass" = "SingleNode"
        }
      + default_tags                 = (known after apply)
      + driver_instance_pool_id      = (known after apply)
      + driver_node_type_id          = (known after apply)
      + enable_elastic_disk          = (known after apply)
      + enable_local_disk_encryption = (known after apply)
      + id                           = (known after apply)
      + node_type_id                 = "Standard_DS3_v2"
      + num_workers                  = 0
      + spark_conf                   = {
          + "spark.databricks.cluster.profile" = "singleNode"
          + "spark.master"                     = "local[*]"
        }
      + spark_version                = "15.4.x-scala2.12"
      + state                        = (known after apply)
      + url                          = (known after apply)
    }

  # databricks_job.run_preconfig will be created
  + resource "databricks_job" "run_preconfig" {
      + always_running      = false
      + control_run_state   = false
      + format              = (known after apply)
      + id                  = (known after apply)
      + max_concurrent_runs = 1
      + name                = "run preconfig"
      + url                 = (known after apply)

      + run_as (known after apply)

      + task {
          + existing_cluster_id = (known after apply)
          + retry_on_timeout    = (known after apply)
          + task_key            = "preconfig"

          + notebook_task {
              + notebook_path = "/Users/balaom/preconfig"
            }
        }
    }

  # databricks_job.run_saving will be created
  + resource "databricks_job" "run_saving" {
      + always_running      = false
      + control_run_state   = false
      + format              = (known after apply)
      + id                  = (known after apply)
      + max_concurrent_runs = 1
      + name                = "run saving"
      + url                 = (known after apply)

      + run_as (known after apply)

      + task {
          + existing_cluster_id = (known after apply)
          + retry_on_timeout    = (known after apply)
          + task_key            = "saving"

          + notebook_task {
              + notebook_path = "/Users/balage330@gmail.com/saving"
            }
        }
    }

  # databricks_job.run_simple_1 will be destroyed
  # (because databricks_job.run_simple_1 is not in configuration)
  - resource "databricks_job" "run_simple_1" {
      - always_running            = false -> null
      - control_run_state         = false -> null
      - format                    = "MULTI_TASK" -> null
      - id                        = "706668043510313" -> null
      - max_concurrent_runs       = 1 -> null
      - max_retries               = 0 -> null
      - min_retry_interval_millis = 0 -> null
      - name                      = "Run Simple SQL Notebook" -> null
      - retry_on_timeout          = false -> null
      - timeout_seconds           = 0 -> null
      - url                       = "https://redatabrick
s.net/#job/706668043510313" -> null

      - email_notifications {
          - no_alert_for_skipped_runs              = false -> null
          - on_duration_warning_threshold_exceeded = [] -> null
          - on_failure                             = [] -> null
          - on_start                               = [] -> null
          - on_streaming_backlog_exceeded          = [] -> null
          - on_success                             = [] -> null
        }

      - run_as {
          - user_name              = "balom" -> null
            # (1 unchanged attribute hidden)
        }

      - task {
          - disable_auto_optimization = false -> null
          - existing_cluster_id       = "0316-m0" -> null
          - max_retries               = 0 -> null
          - min_retry_interval_millis = 0 -> null
          - retry_on_timeout          = false -> null
          - run_if                    = "ALL_SUCCESS" -> null
          - task_key                  = "simple_1_task" -> null
          - timeout_seconds           = 0 -> null
            # (3 unchanged attributes hidden)

          - email_notifications {
              - no_alert_for_skipped_runs              = false -> null
              - on_duration_warning_threshold_exceeded = [] -> null
              - on_failure                             = [] -> null
              - on_start                               = [] -> null
              - on_streaming_backlog_exceeded          = [] -> null
              - on_success                             = [] -> null
            }

          - notebook_task {
              - base_parameters = {} -> null
              - notebook_path   = "/Users/balm/simple_1" -> null
              - source          = "WORKSPACE" -> null
                # (1 unchanged attribute hidden)
            }
        }

      - webhook_notifications {
        }
    }

  # databricks_notebook.preconfig will be created
  + resource "databricks_notebook" "preconfig" {
      + format         = (known after apply)
      + id             = (known after apply)
      + language       = "PYTHON"
      + md5            = "different"
      + object_id      = (known after apply)
      + object_type    = (known after apply)
      + path           = "/Users/baom/preconfig"
      + source         = "./preconfig.py"
      + url            = (known after apply)
      + workspace_path = (known after apply)
    }

  # databricks_notebook.saving will be created
  + resource "databricks_notebook" "saving" {
      + format         = (known after apply)
      + id             = (known after apply)
      + language       = "PYTHON"
      + md5            = "different"
      + object_id      = (known after apply)
      + object_type    = (known after apply)
      + path           = "/Users/bam/saving"
      + source         = "./saving.py"
      + url            = (known after apply)
      + workspace_path = (known after apply)
    }

  # databricks_notebook.simple_1 will be destroyed
  # (because databricks_notebook.simple_1 is not in configuration)
  - resource "databricks_notebook" "simple_1" {
      - format         = "SOURCE" -> null
      - id             = "/Users/balage330@gmail.com/simple_1" -> null
      - language       = "SQL" -> null
      - md5            = "12f187ce99b34d" -> null
      - object_id      = 557163049195297 -> null
      - object_type    = "NOTEBOOK" -> null
      - path           = "/Users/bom/simple_1" -> null
      - source         = "./simple.sql" -> null
      - url            = "https://acks.net/#work
space/Users/balage330@gmail.com/simple_1" -> null
      - workspace_path = "/Workspace/Users/balage330@gmail.com/simple_1" -> null
    }

Plan: 5 to add, 0 to change, 2 to destroy.

Changes to Outputs:
  + job_ids = {
      + preconfig = (known after apply)
      + saving    = (known after apply)
    }
databricks_notebook.saving: Creating...
databricks_notebook.preconfig: Creating...
databricks_job.run_simple_1: Destroying... [id=70313]
databricks_job.run_simple_1: Destruction complete after 1s
databricks_notebook.simple_1: Destroying... [id=/Users/bom/simpl
e_1]
databricks_cluster.cicd1: Creating...
databricks_notebook.preconfig: Creation complete after 1s [id=/Users/bal
mail.com/preconfig]
databricks_notebook.saving: Creation complete after 1s [id=/Users/
l.com/saving]
databricks_notebook.simple_1: Destruction complete after 0s
databricks_cluster.cicd1: Still creating... [10s elapsed]
databricks_cluster.cicd1: Still creating... [20s elapsed]
databricks_cluster.cicd1: Still creating... [30s elapsed]
databricks_cluster.cicd1: Still creating... [40s elapsed]
databricks_cluster.cicd1: Still creating... [50s elapsed]
databricks_cluster.cicd1: Still creating... [1m0s elapsed]
databricks_cluster.cicd1: Still creating... [1m10s elapsed]
databricks_cluster.cicd1: Still creating... [1m20s elapsed]
databricks_cluster.cicd1: Still creating... [1m30s elapsed]
databricks_cluster.cicd1: Still creating... [1m40s elapsed]
databricks_cluster.cicd1: Still creating... [1m50s elapsed]
databricks_cluster.cicd1: Still creating... [2m0s elapsed]
databricks_cluster.cicd1: Still creating... [2m10s elapsed]
databricks_cluster.cicd1: Still creating... [2m20s elapsed]
databricks_cluster.cicd1: Still creating... [2m30s elapsed]
databricks_cluster.cicd1: Still creating... [2m40s elapsed]
databricks_cluster.cicd1: Still creating... [2m50s elapsed]
databricks_cluster.cicd1: Still creating... [3m0s elapsed]
databricks_cluster.cicd1: Still creating... [3m10s elapsed]
databricks_cluster.cicd1: Still creating... [3m20s elapsed]
databricks_cluster.cicd1: Still creating... [3m30s elapsed]
databricks_cluster.cicd1: Still creating... [3m40s elapsed]
databricks_cluster.cicd1: Still creating... [3m50s elapsed]
databricks_cluster.cicd1: Creation complete after 3m59s [id=0316-143233-vldng4wr
]
databricks_job.run_saving: Creating...
databricks_job.run_preconfig: Creating...
databricks_job.run_saving: Creation complete after 1s [id=1063074396808775]
databricks_job.run_preconfig: Creation complete after 1s [id=552003478637995]
╷
│ Warning: Argument is deprecated
│
│   with azurerm_storage_container.data,
│   on main.tf line 66, in resource "azurerm_storage_container" "data":
│   66:   storage_account_name  = azurerm_storage_account.Azure_Spark_SQL_storag
e.name
│
│ the `storage_account_name` property has been deprecated in favour of
│ `storage_account_id` and will be removed in version 5.0 of the Provider.
╵

Apply complete! Resources: 5 added, 0 changed, 2 destroyed.

Outputs:

job_ids = {
  "preconfig" = "552003478637995"
  "saving" = "1063074396808775"
}
make: Leaving directory 'C:/data_eng/h�zi/2'

Process finished with exit code 0
```
## You can see the notebooks preconfig and saving created by the terraform:

![wbs](https://github.com/user-attachments/assets/346088c2-139a-453b-826e-6f614663c2ea)

## And the cluster created by the terrafom, too:

![cluster](https://github.com/user-attachments/assets/570e6a6f-8489-4b5c-bf92-cb507d92a3eb)

## And the jobs, which was also created:

![1st_job_WF_screenshot](https://github.com/user-attachments/assets/ca685dd2-1faa-49a6-8ab9-52a1b7bc36a3)

## All set for the running of the jobs, I run them via cmd, with databricks jobs run-now --job-id <job-id>:

![manually_jobs](https://github.com/user-attachments/assets/be0cb982-6f4e-414f-860e-a8c66ce6b2eb)

## As you can see in the main.tf, my approach was to solve the task with 2 notebooks, the first's task are to executing the creation of delta tables and saving the intermediate datas
## The second's are to execute all the queries, saving the results in dataframes and finally saving them in partitioned parquet format.

## Below you can see the notebook preconfig notebook:
```python
# Azure Storage settings
input_storage_account = ""
output_storage_account = ""
input_container = "hw2"
output_container = "data"

# Setting up Storage account keys
spark.conf.set(
    f"fs.azure.account.key.{input_storage_account}.blob.core.windows.net",
    dbutils.secrets.get(scope="hw2secret", key="AZURE_STORAGE_ACCOUNT_KEY_SOURCE"))

spark.conf.set(
    f"fs.azure.account.key.{output_storage_account}.blob.core.windows.net",
    dbutils.secrets.get(scope="hw2secret", key="STORAGE_FINAL"))

# Create database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS mydatabase")

# Read Expedia data from the source container and save it in Delta format to the data output container 
expedia_df = spark.read.format("avro").load(f"wasbs://{input_container}@{input_storage_account}.blob.core.windows.net/expedia/")

expedia_df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"wasbs://{output_container}@{output_storage_account}.blob.core.windows.net/delta/expedia/")

# Register the Expedia Delta table in the Metastore
spark.sql("DROP TABLE IF EXISTS mydatabase.expedia")
spark.sql(f"""
    CREATE TABLE mydatabase.expedia
    USING DELTA
    LOCATION 'wasbs://{output_container}@{output_storage_account}.blob.core.windows.net/delta/expedia/'
""")

# Read Hotel-Weather data from the source container and save it in Delta format to the data output container, also partitioning is applied
hotel_weather_df = spark.read.format("parquet").load(f"wasbs://{input_container}@{input_storage_account}.blob.core.windows.net/hotel-weather/hotel-weather/")

hotel_weather_df.write.format("delta").mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .option("overwriteSchema", "true") \
    .save(f"wasbs://{output_container}@{output_storage_account}.blob.core.windows.net/delta/hotel-weather/")

# Register the Hotel-Weather Delta table in the Metastore
spark.sql("DROP TABLE IF EXISTS mydatabase.hotel_weather")
spark.sql(f"""
    CREATE TABLE mydatabase.hotel_weather
    USING DELTA
    LOCATION 'wasbs://{output_container}@{output_storage_account}.blob.core.windows.net/delta/hotel-weather/'
""")

# Refresh cache to see the most up-to-date data
spark.sql("REFRESH TABLE mydatabase.expedia")
spark.sql("REFRESH TABLE mydatabase.hotel_weather")

#Due to the same column name in the two dataframes, we need to rename the column
hotel_weather_df = hotel_weather_df.withColumnRenamed("id", "accomodation_id")
# Join the Expedia and Hotel Weather data
joined_df = expedia_df.join(hotel_weather_df, expedia_df.hotel_id == hotel_weather_df.accomodation_id, "left")

# Save the intermediate DataFrame partitioned
joined_df.write.format("parquet") \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .save(f"wasbs://{output_container}@{output_storage_account}.blob.core.windows.net/joined_data/")

```
## Below you can see the processing of the first notebook, named preparation:

![1ST_JOB_RUNNING](https://github.com/user-attachments/assets/81582498-9e8a-4b3b-9b73-476a51f718b5)

## Here is a screenshot from the Spark UI:

![JOB_SPARK_UI](https://github.com/user-attachments/assets/bec9c4dd-7a3d-4e41-a9a0-61370680d408)

## And about the completed job:

![1ST_JIB_COMPLETE](https://github.com/user-attachments/assets/0a6bca65-cc40-41f5-857e-2bf010bda6c4)

## Screenshot about the created delta tables:

![1ST_JOB_DELTA_TABLES](https://github.com/user-attachments/assets/e93ac5e3-77c5-4f75-9345-e572f98ee04f)

## Below you can see the second notebook, called saving:

```python
# Azure Storage settings
input_storage_account = ""
output_storage_account = ""
input_container = "hw2"
output_container = "data"

spark.conf.set(
    f"fs.azure.account.key.{output_storage_account}.blob.core.windows.net",
    dbutils.secrets.get(scope="hw2secret", key="STORAGE_FINAL"))
	
df_first = spark.sql("""
	SELECT 
    address,    -- Select the hotel address.
    year,       -- Select the year from the data.
    month,      -- Select the month from the data.
    -- Calculate the temperature difference within each group:
    --   1. Find the maximum average temperature (avg_tmpr_c) in the group.
    --   2. Find the minimum average temperature (avg_tmpr_c) in the group.
    --   3. Compute the absolute difference between these values.
    --   4. Round the result to 2 decimal places and alias it as 'temp_diff'.
    ROUND(ABS(MAX(avg_tmpr_c) - MIN(avg_tmpr_c)), 2) AS temp_diff
	FROM mydatabase.hotel_weather   -- Data is sourced from the hotel_weather table.
	GROUP BY address, year, month    -- Group the records by hotel address, year, and month.
	ORDER BY temp_diff DESC          -- Order the groups by temperature difference in descending order.
	LIMIT 10;                       -- Limit the result to the top 10 groups with the largest temperature differences.
""")

df_first.write.format("parquet") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .save(f"wasbs://{output_container}@{output_storage_account}.blob.core.windows.net/datamart/1/")
	
df_second = spark.sql("""
	WITH exploded_dates AS (
		SELECT
			ex.hotel_id,
			hw.address,
			explode(sequence(
				CAST(ex.srch_ci AS DATE), 
				CAST(ex.srch_co AS DATE) - INTERVAL 1 DAY, 
				interval 1 day)) AS visit_date
		FROM mydatabase.expedia ex
		LEFT JOIN mydatabase.hotel_weather hw
		ON ex.hotel_id = hw.id
		WHERE CAST(ex.srch_ci AS DATE) < CAST(ex.srch_co AS DATE)
		AND hw.address IS NOT NULL
	),
	monthly_visits AS (
		SELECT
			address,
			YEAR(visit_date) AS year,
			MONTH(visit_date) AS month,
			COUNT(*) AS visits_count
		FROM exploded_dates
		GROUP BY address, YEAR(visit_date), MONTH(visit_date)
	),
	ranked_hotels AS (
		SELECT *,
			DENSE_RANK() OVER (PARTITION BY year, month ORDER BY visits_count DESC) AS rank
		FROM monthly_visits
	)
	SELECT * FROM ranked_hotels WHERE rank <= 10;
""")
df_second.write.format("parquet") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .save(f"wasbs://{output_container}@{output_storage_account}.blob.core.windows.net/datamart/2/")
	

df_third = spark.sql("""
	WITH exploded_dates AS (
		SELECT
			ex.id AS booking_id,
			ex.hotel_id,
			ex.srch_ci,
			ex.srch_co,
			explode(sequence(
				CAST(ex.srch_ci AS DATE), 
				CAST(ex.srch_co AS DATE) - INTERVAL 1 DAY,
				INTERVAL 1 DAY
			)) AS visit_date
		FROM mydatabase.expedia ex
		WHERE DATEDIFF(ex.srch_co, ex.srch_ci) BETWEEN 7 AND 30
		AND ex.srch_co > ex.srch_ci
	),
	joined_weather AS (
		SELECT 
			ed.booking_id,
			ed.hotel_id,
			ed.visit_date,
			hw.avg_tmpr_c,
			hw.address
		FROM exploded_dates ed
		LEFT JOIN mydatabase.hotel_weather hw
		ON ed.hotel_id = hw.id 
			AND CAST(hw.wthr_date AS DATE) = ed.visit_date
		WHERE hw.avg_tmpr_c IS NOT NULL
	),
	windowed_temps AS (
		SELECT
			booking_id,
			hotel_id,
			address,
			visit_date,
			avg_tmpr_c,
			FIRST_VALUE(avg_tmpr_c) OVER (PARTITION BY booking_id ORDER BY visit_date) AS first_temp,
			LAST_VALUE(avg_tmpr_c) OVER (PARTITION BY booking_id ORDER BY visit_date 
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_temp
		FROM joined_weather
	),
	temp_calculations AS (
		SELECT
			booking_id,
			hotel_id,
			address,
			MIN(visit_date) AS first_day,
			MAX(visit_date) AS last_day,
			ROUND(last_temp - first_temp, 2) AS temp_trend,
			ROUND(AVG(avg_tmpr_c), 2) AS avg_temperature
		FROM windowed_temps
		GROUP BY booking_id, hotel_id, address, first_temp, last_temp
	)
	SELECT *
	FROM temp_calculations
	WHERE temp_trend IS NOT NULL
	AND avg_temperature IS NOT NULL
	AND DATEDIFF(last_day, first_day) >= 7
	ORDER BY ABS(temp_trend) DESC;
""")

df_third.write.format("parquet") \
    .mode("overwrite") \
    .partitionBy("first_day", "last_day") \
    .save(f"wasbs://{output_container}@{output_storage_account}.blob.core.windows.net/datamart/3/")
```
## Here you can see the successful second job:

![2nd_job_succ](https://github.com/user-attachments/assets/8bb1542e-203b-46c1-ab57-f748b83a48ad)

## The created partitioned parquet files in the destination data container:

![job_result_container](https://github.com/user-attachments/assets/3b30cf0b-8a83-4cf4-a51b-90a47df9db29)

## You can see the screenshots about the jobs and runs:

![all_the_jobs](https://github.com/user-attachments/assets/d7bb6a43-0f31-4010-ad54-0eec700a2cef)

![all_jobs](https://github.com/user-attachments/assets/c3297dac-b558-426c-a388-40a5514cae58)














