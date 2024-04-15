# Create new storage bucket in the US multi-region
# with standard storage

provider "google" {
  project = "${GCP_PROJECT}"
  region = "US"
}

# Define top-level bucket
resource "google_storage_bucket" "static" {
 name          = "congress_data"
 location      = "US"
 storage_class = "STANDARD"
 public_access_prevention = "enforced"

 uniform_bucket_level_access = true
}

# Define subdirectories (placeholders) by creating empty objects
resource "google_storage_bucket_object" "bills_subdir" {
  bucket = google_storage_bucket.static.name
  name   = "bills/"
  source = "/dev/null"  # Use /dev/null as the source to create an empty object
}

resource "google_storage_bucket_object" "actions_subdir" {
  bucket = google_storage_bucket.static.name
  name   = "actions/"
  source = "/dev/null"  # Use /dev/null as the source to create an empty object
}

resource "google_storage_bucket_object" "member_subdir" {
  bucket = google_storage_bucket.static.name
  name   = "member/"
  source = "/dev/null"  # Use /dev/null as the source to create an empty object
}

resource "google_storage_bucket_object" "votes_subdir" {
  bucket = google_storage_bucket.static.name
  name   = "votes/"
  source = "/dev/null"  # Use /dev/null as the source to create an empty object
}

# Upload a text file as an object
# to the storage bucket

