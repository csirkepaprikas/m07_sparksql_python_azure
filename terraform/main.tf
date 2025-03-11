terraform {
  backend "azurerm" {
    resource_group_name  = "rg-development-westeurope-6o"
    storage_account_name = "developmentwesteurope6o"
    container_name       = "tfstate"
    key                  = "terraform.tfstate"
  }
}

provider "databricks" {
  host  = var.DATABRICKS_HOST
  token = var.DATABRICKS_TOKEN
}

resource "databricks_notebook" "Azure_Spark_SQL" {
  path     = "/Users/balage330@gmail.com/Azure_Spark_SQL"
  language = "SQL"
  source   = ""
}

resource "databricks_cluster" "example" {
  cluster_name = "Azure_Spark_SQL_cluster"
  spark_version = "7.3.x-scala2.12"
  node_type_id  = "Standard_DS3_v2"
  autotermination_minutes = 30
  num_workers = 1
}

resource "azurerm_storage_account" "Azure_Spark_SQL_storage" {
  name                     = var.STORAGE_ACCOUNT_NAME
  resource_group_name       = var.RESOURCE_GROUP_NAME
  location                 = "West Europe"
  account_tier              = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "data" {
  name                  = "data"
  storage_account_name  = azurerm_storage_account.Azure_Spark_SQL_storage.name
  container_access_type = "container"
}

resource "databricks_job" "run_hotel_weather_query" {
  name = "run_hotel_weather_query"

  new_cluster {
    spark_version = "15.4.x-scala2.12"
    node_type_id  = "Standard_DS3_v2"
    num_workers   = 2
  }

  notebook_task {
    notebook_path = databricks_notebook.Azure_Spark_SQL.path

  }

  schedule {
    quartz_cron_expression = "0 0 1 * ? *"
    timezone_id = "UTC"
  }
}

