
import json
import pandas as pd
from pandas.io.json import json_normalize
import os
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse
from prefect_dbt.cli.commands import DbtCoreOperation
from dotenv import load_dotenv
from sodapy import Socrata

load_dotenv()

with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], 'r') as creds:
    json_creds = json.load(creds)
    gcp_project_name = json_creds['project_id']

@task(name="Get API data", retries=3, log_prints=True)
def get_api_data() -> pd.DataFrame:
    year = 2023
    month = 3
    dataset_file = f"{year}-{month}-nyc_rest_inspection_data"
    client = Socrata("data.cityofnewyork.us",
                  '########', # MyAppToken
                  username="#######",
                  password="######")
    # Returned as JSON from API / converted to Python list of
    # dictionaries by sodapy.
    results = client.get("43nn-pn8j",limit=10000000)
    # Convert to pandas DataFrame
    results_df = pd.DataFrame.from_records(results) 
    return results_df

@task(name="Format DataFrame", log_prints=True)
def format_df(df: pd.DataFrame) -> pd.DataFrame:
    """Format DataFrame"""

    df = df.astype({
                'camis': 'string',
                'boro': 'string',
                'building': 'string',
                'street': 'string',
                'zipcode': 'string',
                'phone': 'string',
                'inspection_date': 'datetime64',
                'critical_flag': 'string',
                'record_date': 'datetime64',
                'latitude': int,
                'longitude': int,
                'community_board': 'string',
                'council_district': 'string',
                'census_tract': 'string',
                'bin': 'string',
                'bbl': 'string',
                'nta': 'string',
                'dba': 'string',
                'cuisine_description': 'string',
                'action': 'string',
                'inspection_type': 'string',
                'score': int,
                'violation_code': 'string',
                'violation_description': 'string',
                'grade': 'string',
                'grade_date': 'datetime64',
                })
    print(f"rows: {len(df)}")

    return df   

@task(name="Save data to Parquet file")
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/de_project/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(name="Upload local parquet file to GCS")
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return

@task(name="Stage GCS to BigQuery")
def stage_bq():
    """Stage data to BigQuery"""
    bq_ext_table = f"""
            CREATE OR REPLACE EXTERNAL TABLE `{gcp_project_name}.de_zoomcamp_project.external_nyc_inspect_data`
            OPTIONS (
                format = 'PARQUET',
                uris = ['gs://dtc_data_lake_{gcp_project_name}/data/de_project/*.parquet']
            )
        """
    with BigQueryWarehouse.load("zoom-bq") as warehouse:
        operation = bq_ext_table
        warehouse.execute(operation)

    bq_part_table = f"""
            CREATE OR REPLACE TABLE `{gcp_project_name}.de_zoomcamp_project.nyc_inspect_data_partitioned_clustered`
            PARTITION BY DATE_TRUNC(record_date,MONTH)
            CLUSTER BY boro AS
            SELECT * FROM `{gcp_project_name}.de_zoomcamp_project.external_nyc_inspect_data`;
        """

    with BigQueryWarehouse.load("zoom-bq") as warehouse:
        operation = bq_part_table
        warehouse.execute(operation)   

@task(name="dbt modelling")
def dbt_model():
    """Run dbt models"""

    dbt_path = Path(f"dbt/nyc_inspect")

    dbt_run = DbtCoreOperation(
                    commands=["dbt deps", 
                              "dbt seed -t prod", 
                              "dbt build -t prod"],
                    project_dir=dbt_path,
                    profiles_dir=dbt_path,
    )

    dbt_run.run()

    return
@flow(name="Process-Data-Subflow")
def process_data(year: int, month: int):
    """Main function to process a year and month of inspection data"""

    data = get_api_data(year, month)
    df = format_df(data)
    pq_path = write_local(df, year)
    write_gcs(pq_path)

@flow(name="Process-Data-Parent")
def parent_process_data(year: int, month: int):
    """Parent process"""
    stage_bq()
    dbt_model()
if __name__ == "__main__":
    month = 3
    year = 2023
    parent_process_data(month, year)
