# Project Objective
This project creates a data pipline using US congressional data from GovInfo.gov and Congress.gov. 

While Congress originates and votes on various types of bills each year, this project seeks to create greater clarity around the types of bills that Congress addresses on a yearly basis. The pipeline defines a dimensional model by creating a fact (roll call vote) table and multiple dimension tables which contain information on members, vote metadata and congressional committees. The data used is for the current (118th) US Congress.

This project was created for the (Data Talks Club Data Engineering Zoomcamp)[https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main]. See the 'Future Improvements' section for planned improvements to the pipeline.

# Project Architecture
![Screenshot 2024-04-16 at 9 49 40 PM](https://github.com/Lbongard/congress_pipeline/assets/62773555/47d3ce6b-bb5b-49be-9808-293cc566fcf9)

# Data
Information on the API and bulk data download used in this project can be found (here)[https://www.congress.gov/help/using-data-offsite] and (here)[https://www.govinfo.gov/bulkdata]

# Prerequisites
In order to replicate this project, ensure the following list of programs is installed on your computer:
* (Docker and docker compose)[https://docs.docker.com/compose/install/]
* (Terraform)[https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli]
* Git
* (Google Cloud Platform)[https://cloud.google.com/gcp?hl=en]. Google offers a 3 month free trial for those signing up with a new email address.

Additionally, you will need to sign up for a (congress.gov api key)[https://api.congress.gov/sign-up/]

# Recreating the Project
First, create a new GCP Project and service account. Service accounts can be created under the 'IAM & Admin' section of the GCP console. Ensure that this service account has admin access to 'BigQuery', 'Compute' and 'Storage'
![Screenshot 2024-04-16 at 10 04 23 PM](https://github.com/Lbongard/congress_pipeline/assets/62773555/23f4c900-17c1-40e0-be43-3de6f7992de3)


