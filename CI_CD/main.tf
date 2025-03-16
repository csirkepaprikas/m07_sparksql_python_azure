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
    resource_group_name  = ""
    storage_account_name = ""
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
  path     = "/Users/bm/preconfig"
  language = "PYTHON"
  source   = "${path.module}/preconfig.py"
}

resource "databricks_notebook" "saving" {
  path     = "/Users/m/saving"
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
