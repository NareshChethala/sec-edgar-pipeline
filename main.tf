terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.17.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.location
}


resource "google_bigquery_dataset" "sec_financials_dataset" {
  dataset_id    = var.bq_dataset_name
  friendly_name = "SEC Financials Dataset"
  description   = "This dataset contains SEC financial data"
  location      = var.location
}