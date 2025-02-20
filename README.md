# Project Objective
In this project, I created a pipeline and web tool that allows users to analyze congressional legislation and voting data by member. The data is sourced from GovInfo.gov and Congress.gov. The link to the streamlit web app can be found [here](https://congress-pipeline-4347055658.us-central1.run.app/).

The pipeline defines a dimensional model by creating a fact (roll call vote) table and multiple dimension tables which contain information on members, vote metadata and congressional committees. The data used is for the previous (118th) and current (119th) US Congresses.

This project was otiginally created for the [Data Talks Club Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main).

# Data
Information on the API and bulk data download used in this project can be found [here](https://www.congress.gov/help/using-data-offsite) and [here](https://www.govinfo.gov/bulkdata)

# Project Architecture
![Architecture Diagram](assets/architecture_diagram.gif)

## Extraction
Data is incrementally extracted by filtering data more recent than the latest 'update' date in the target warehouse.

**Bills**
* Bills are downloaded from GovInfo bulk data website

**Votes**
* Roll call votes are extracted by parsing bill jsons
* Roll call vote XMLs are downloaded using roll call vote urls

**Members**
* Member data is downloaded from the Congress API

## Storage and Transformation
* Raw extracted files are uploaded to GCS buckets 
* Files are loaded into staging tables and parsed into final fact/dim tables using DBT

# Prerequisites
In order to replicate this project, ensure the following list of programs is installed on your computer:
* [Docker and docker compose](https://docs.docker.com/compose/install/)
* [Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
* Git
* [Google Cloud Platform](https://cloud.google.com/gcp?hl=en). Google offers a 3 month free trial for those signing up with a new email address.

Additionally, you will need to sign up for a [congress.gov api key](https://api.congress.gov/sign-up/)

# Recreating the Project

1. **First, create a new GCP Project and service account.** Service accounts can be created under the 'IAM & Admin' section of the GCP console. Ensure that this service account has admin access to 'BigQuery', 'Compute' and 'Storage'. Download a json key associated with the account and note the directory in which you save it.

![Screenshot 2024-04-16 at 10 04 23 PM](https://github.com/Lbongard/congress_pipeline/assets/62773555/23f4c900-17c1-40e0-be43-3de6f7992de3)

2. **Clone this repository**
3. **Navigate to the root of the cloned repo and activate the virtual environment**

```
# Set up virtual env
source venv/bin/activate
pip install -r requirements.txt
```
4. **Set required environment variables**. These are needed for Terraform, Airflow and DBT. Replace placeholders with variables where appropriate below.

```
# Set Environment Variables

export TF_VAR_google_credentials=<path_to_google_credentials_key>
export TF_VAR_gcp_project=<your_gcp_project_id>
export TF_VAR_gcs_bucket_name="congress_data_${TF_VAR_gcp_project}"

export congress_api_key=<your_congress_api_key>

export google_credentials_dir=$(dirname ${TF_VAR_google_credentials})
export google_credentials_file=$(basename ${TF_VAR_google_credentials})


export DBT_GOOGLE_CREDENTIALS=$TF_VAR_google_credentials
export DBT_GCP_PROJECT=$TF_VAR_GCP_PROJECT
export DBT_PROFILES_DIR=./

export DBT_GOOGLE_CREDENTIALS=$TF_VAR_google_credentials
export DBT_GCP_PROJECT=$TF_VAR_gcp_project
```
5. **Switch to the Terraform directory and create infra**

```
# Switch to Terraform Directory
cd ./terraform
terraform init
terraform plan
terraform apply
```
6. **Switch back to the root directory and run airflow.** The following make command will build the docker container and start running the pipeline. After a few minutes, you can check the status of the pipeline by going to localhost:8080 in your browser. Ensure that the pipeline has started.
```
make airflow-up
```

8. **Once done, make sure that you destroy the google cloud storage bucket and BigQuery Dataset.** Navigate to the terraform directory and run the following:
```
terraform destroy
```

# Dashboard
In addition to the pipeline, I've created a [streamlit app](https://congress-pipeline-4347055658.us-central1.run.app/) to allow users to interact with the data. The instance of the data pipeline which feeds the app is hosted on a Google Cloud VM and runs every weeknight. The Streamlit app runs on a Google Cloud Run instance.

