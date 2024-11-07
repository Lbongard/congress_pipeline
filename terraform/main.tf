# Create new storage bucket in the US multi-region
# with standard storage

variable "gcp_project" {
  description = "GCP Project ID"
}

variable "google_credentials" {
  description = "Path to google credentials file"
}

provider "google" {
  project     = var.gcp_project
  credentials = var.google_credentials
  region      = "US"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  # Update the below to a unique bucket name
  # default = "congress_data"
}

# Define top-level bucket
resource "google_storage_bucket" "static" {
  force_destroy = true
  name                       = var.gcs_bucket_name
  location                   = "US"
  storage_class              = "STANDARD"
  public_access_prevention   = "enforced"
  uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = "Congress"
  location   = "US"
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "dataset_stg" {
  dataset_id = "Congress_Stg"
  location   = "US"
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "dataset_target" {
  dataset_id = "Congress_Target"
  location   = "US"
  delete_contents_on_destroy = true
}


