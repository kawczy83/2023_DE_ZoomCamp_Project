# NYC Restaurant Inspection DE Project

## Table of contents

- [Problem statement](#problem-statement)
- [Technologies Used](#technologies-used)
- [Data Pipeline](#data-pipeline)
- [Instructions](#instructions)
- [Dashboard](#dashboard)

## Problem statement

The data set contains information on restaurant inspections conducted by the New York City Department of Health and Mental Hygiene (DOHMH).
The data was gathered from the NYC Open Data website located here: <https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j>

This dashboard can help answer questions such as:

1. Which restaurants contain the most healthcode violations?
2. Which restaurants have good inspection scores (the lower the better)?
3. Can we use this dashboard to identify restaurants that are at risk of foodborne illness outbreaks?

## Technologies Used

- Cloud: GCP (Google Cloud Platform)
- Data Lake: Google Cloud Storage
- Data Warehouse: BigQuery
- Infrastructure as code (IaC): Terraform
- Workflow orchestration: Prefect
- Transforming Data: DBT
- Dashboard: Looker

## Data Pipeline

![data pipeline diagram](img/NYC%20Health%20Inspection%20Data%20Pipeline.png)

## Instructions

1. First  we `git clone https://github.com/kawczy83/2023_DE_ZoomCamp_Project` into an empty directory and then `cd` into the repo folder.

2. Set up your python virtual environment with the following commmands:
- `python3 -m venv <directory>`
- `source venv/bin/activate`
- `pip install -r ./setup/requirements.txt`

3. Save your Google Cloud service account credentials json file to the hidden .credentials folder.

4. Set an environment variable for your service account file that you just saved with this command: `export GOOGLE_APPLICATION_CREDENTIALS="<absolute path to the json file in the ./creds folder>"`

5. Update the GOOGLE_APPLICATION_CREDENTIALS environment variable in the ./.env file, using the same absolute path to the .json service account file.

6. Run `gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS` to authenticate with Google Cloud, with your service account .json file.

7. Run Terraform to deploy your infrastructure to your Google Cloud Project. Run the following commands:

    - `terraform -chdir="./terraform" init` - to initialize terraform

    - `terraform -chdir="./terraform" plan -var="project=<project id here>"`, replacing with your Google Project ID. This will build a deployment plan that you can review.

    - `terraform -chdir="./terraform" apply -var="project=<project id here>"`, replacing with your Google Project ID. This will apply the deployment plan and deploy the infrastructure

8. Run the following commands to set up a local Prefect profile

   - `prefect profile create <profile_name>`
   - `prefect profile use <profile_name>`
   - `prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api`

9. Start your Prefect session with the following command: `prefect orion start`

10. From your terminal session, run this command to setup some blocks for your GCP credentials in prefect:
`python3 ./setup/make_gcp_blocks.py`

11. Run the following command in your terminal: `python3 prefect/flows/03_deployments/parameterized_flow.py`

## Dashboard

![dashboard](img/dashboard.png)

The dashboard can be found at: <https://lookerstudio.google.com/s/kwHOY4XMrHQ>
