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
  name                       = var.gcs_bucket_name
  location                   = "US"
  storage_class              = "STANDARD"
  public_access_prevention   = "enforced"
  uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = "Congress"
  location   = "US"
}

# # Define subdirectories (placeholders) by creating empty objects
# resource "google_storage_bucket_object" "bills_subdir" {
#   bucket = google_storage_bucket.static.name
#   name   = "bills/"
#   source = "/dev/null"  # Use /dev/null as the source to create an empty object
# }

# resource "google_storage_bucket_object" "actions_subdir" {
#   bucket = google_storage_bucket.static.name
#   name   = "actions/"
#   source = "/dev/null"  # Use /dev/null as the source to create an empty object
# }

# resource "google_storage_bucket_object" "member_subdir" {
#   bucket = google_storage_bucket.static.name
#   name   = "member/"
#   source = "/dev/null"  # Use /dev/null as the source to create an empty object
# }

# resource "google_storage_bucket_object" "votes_subdir" {
#   bucket = google_storage_bucket.static.name
#   name   = "votes/"
#   source = "/dev/null"  # Use /dev/null as the source to create an empty object
# }

# # Upload a text file as an object
# # to the storage bucket

