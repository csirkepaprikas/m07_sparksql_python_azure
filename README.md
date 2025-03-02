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
## Then I applied them




