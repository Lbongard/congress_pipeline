# Project Objective
This project creates a data pipline using US congressional data from GovInfo.gov and Congress.gov. 

While Congress originates and votes on various types of bills each year, this project seeks to create greater clarity around the types of bills that Congress addresses on a yearly basis. The pipeline defines a dimensional model by creating a fact (roll call vote) table and multiple dimension tables which contain information on members, vote metadata and congressional committees. The data used is for the current (118th) US Congress.

This project was created for the [Data Talks Club Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main). See the 'Future Improvements' section for planned improvements to the pipeline.

# Project Architecture
![Screenshot 2024-04-16 at 9 49 40 PM](https://github.com/Lbongard/congress_pipeline/images/Architecture_Diagram.gif)

# Data
Information on the API and bulk data download used in this project can be found [here](https://www.congress.gov/help/using-data-offsite) and [here](https://www.govinfo.gov/bulkdata)

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
6. **Switch back to the root directory and run airflow.** The following make command will build the docker container and start running the pipeline. After a few minutes, you can check the status of the pipeline by going to localhost:8080 in your browser. Ensure that the pipeline has started. Grab some coffee while it runs.
```
make airflow-up
```
**Unpaused and running DAG:**
![image](https://github.com/Lbongard/congress_pipeline/assets/62773555/b3a5961b-3c35-46b0-bd1d-e8b16aab08a7)

**Completed DAG:**

![image](https://github.com/Lbongard/congress_pipeline/assets/62773555/328ba252-a442-46f3-8c4c-f6e65615bed1)


7. **Navigate to the dbt directory, and run the following**. This will build the final dimension and fact tables.

```
dbt deps
dbt seed
dbt run
```
Note: This data model contains a list of congressional districts by zip code published by Open Source Activism. You can find the dataset and list of caveats here: https://github.com/OpenSourceActivismTech/us-zipcodes-congress

8. **Once done, make sure that you destroy the google cloud storage bucket and BigQuery Dataset.** Navigate to the terraform directory and run the following:
```
terraform destroy
```

# Dashboard
Using this data, it is possible to create a dashboard in Looker by selecting the BigQuery dataset as a source. See the example I created [here](https://lookerstudio.google.com/reporting/134e8ca6-c712-42f5-8cbb-7ee197ced7ec)

![Screenshot 2024-04-16 at 10 26 43 PM](https://github.com/Lbongard/congress_pipeline/assets/62773555/564ce1c3-882c-4a21-aefe-630b12169e94)


# Future Improvements
Future improvements may involve the following:
* Adding more data / refined charts to Looker dashboard (e.g., more insights into members / voting history, 'closest' votes, etc.)
* Improving performance of pipeline by collating files into fewer, larger files and uploading to GCS
* Improving the dimensional model by adding more data / adjusting current relationships
* Adding CI/CD jobs

