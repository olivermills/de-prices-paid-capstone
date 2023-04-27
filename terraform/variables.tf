locals {
  data_lake_bucket = "pp_data_lake"
}

variable "project" {
    description = "spatial-thinker-384120"
}

variable "region" {
    description = "Region for GCP resources"
    default = "eu"
    type = string
}

variable "storage_class" {
    description = "Storage class type for my bucket"
    default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset, will write data from GCS into BQ"
  type = string
  default = "pp_bq_dataset"
}