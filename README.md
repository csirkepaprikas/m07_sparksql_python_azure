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
OPTIMIZE mydatabase.expedia
ZORDER BY (booking_id, srch_ci);

OPTIMIZE mydatabase.hotel_weather
ZORDER BY (id, wthr_date);

CREATE TABLE mydatabase.date_sequence AS 
SELECT explode(sequence(
    to_date('2016-10-01'), 
    to_date('2018-10-06'), 
    INTERVAL 1 DAY)) AS visit_date;

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

WITH exploded_dates AS (
    -- we are using the predefined exploded table
    SELECT 
        ex.id AS booking_id,
        ex.hotel_id,
        ds.visit_date
    FROM mydatabase.expedia ex
    JOIN mydatabase.date_sequence ds
      ON ds.visit_date BETWEEN CAST(ex.srch_ci AS DATE) AND CAST(ex.srch_co AS DATE) - INTERVAL 1 DAY
    WHERE DATEDIFF(ex.srch_co, ex.srch_ci) BETWEEN 7 AND 30
),
joined_weather AS (
    -- efficient ZORDER optimized JOIN
    SELECT 
        ed.booking_id,
        ed.hotel_id,
        ed.visit_date,
        hw.avg_tmpr_c,
        hw.address
    FROM exploded_dates ed
    LEFT JOIN mydatabase.hotel_weather hw
      ON ed.hotel_id = hw.id 
         AND hw.wthr_date = ed.visit_date
    WHERE hw.avg_tmpr_c IS NOT NULL
),
windowed_temps AS (
    -- predefined window table is used here
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
    FROM joined_weather
    ORDER BY booking_id, visit_date 
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



