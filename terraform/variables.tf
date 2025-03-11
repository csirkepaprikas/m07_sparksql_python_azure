variable "databricks_token" {
  type = string
}

variable "databricks_host" {
  description = "Databricks workspace URL"
  type        = string
}

variable "storage_account_name" {
  description = "Azure Storage Account name"
  type        = string
}

variable "resource_group_name" {
  description = "Azure Storage resource_group_name"
  type        = string
}
