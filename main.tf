terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "7.17.0"
    }
  }
}

provider "google" {
    project = "sec-financials-edgar"
    region  = "us-central1-f"
}