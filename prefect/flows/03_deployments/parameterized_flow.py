import json
import pandas as pd
import os
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional
from pathlib import Path
from prefect import flow, task, get_run_logger
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse
from prefect_dbt.cli.commands import DbtCoreOperation
from dotenv import load_dotenv
from sodapy import Socrata
from requests.exceptions import RequestException

load_dotenv()

COLUMN_DATA_TYPES = {
    'camis': 'string',
    'boro': 'string',
    'building': 'string',
    'street': 'string',
    'zipcode': 'string',
    'phone': 'string',
    'inspection_date': 'datetime64',
    'critical_flag': 'string',
    'record_date': 'datetime64',
    'latitude': float,
    'longitude': float,
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
    'score': float,
    'violation_code': 'string',
    'violation_description': 'string',
    'grade': 'string',
    'grade_date': 'datetime64',
}

with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], 'r') as creds:
    json_creds = json.load(creds)
    gcp_project_name = json_creds['project_id']

@task(name="Get API data", retries=3, log_prints=True)
def get_api_data(year: int, month: int, day: int) -> pd.DataFrame:
    try:
        client = Socrata("data.cityofnewyork.us",
                         os.environ['API_KEY'],  # MyAppToken
                         username=os.environ['username'],
                         password=os.environ['password'])
        
        # Set a variable for the limit
        limit = 1000000

        # Fetch the data
        results = client.get("43nn-pn8j", limit=limit)
        results_df = pd.DataFrame.from_records(results)
        return results_df
    except RequestException as e:
        print(f"Error fetching data from API: {e}")
        raise

@task(name="Format DataFrame", log_prints=True)
def format_df(df: pd.DataFrame) -> pd.DataFrame:
    """Format DataFrame"""

    for col, dtype in COLUMN_DATA_TYPES.items():
        try:
            df[col] = df[col].astype(dtype)
        except KeyError:
            get_run_logger.warning(f"Column '{col}' not found in the DataFrame.")
        except ValueError as e:
            get_run_logger.warning(f"Unable to convert column '{col}' to type '{dtype}': {e}")

    return df

@task(name="Save data to Parquet file")
def write_local(df: pd.DataFrame, dataset_file: str, compression: Optional[str] = "gzip") -> Path:
    """
    Write DataFrame out locally as a parquet file.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.
        dataset_file (str): The name of the parquet file without the file extension.
        compression (Optional[str], default="gzip"): The compression method to be used when saving the parquet file.

    Returns:
        Path: The file path of the saved parquet file.
    """
    path = Path(os.path.join("data", "de_project", f"{dataset_file}.parquet"))
    df.to_parquet(path, compression=compression)
    return path

@task(name="Upload local parquet file to GCS")
def write_gcs(bucket_name: str, path: Path) -> None:
    """Upload local parquet file to GCS"""

    try:
        # Load the GCS bucket
        gcs_bucket = GcsBucket.load(bucket_name)

        # Log the bucket name and file path
        logging.info(f"Uploading file {path} to bucket {bucket_name}")

        # Upload the local file to the specified bucket
        gcs_bucket.upload_from_path(from_path=path, to_path=path)

        # Log a message after the file is uploaded
        logging.info(f"File {path} uploaded to bucket {bucket_name}")

    except Exception as e:
        logging.error(f"Error uploading file {path} to bucket {bucket_name}: {e}")
        raise e

@task(name="Stage GCS to BigQuery")
def stage_bq(project_id: str, dataset_id: str) -> None:
    """Stage data to BigQuery"""

    def execute_sql(warehouse, sql_statement):
        try:
            warehouse.execute(sql_statement)
        except Exception as e:
            logging.error(f"Error executing SQL statement: {e}")
            raise e

    try:
        # Define the SQL statement for creating the external table
        bq_ext_table = f"""
            CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{dataset_id}.external_nyc_inspect_data`
            OPTIONS (
                format = 'PARQUET',
                uris = ['gs://dtc_data_lake_{project_id}/data/de_project/*.parquet']
            )
        """

        # Define the SQL statement for creating the partitioned and clustered table
        bq_part_table = f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.nyc_inspect_data_partitioned_clustered`
            PARTITION BY DATE(inspection_date)
            CLUSTER BY boro, cuisine_description, violation_code AS
            SELECT * FROM `{project_id}.{dataset_id}.external_nyc_inspect_data`;
        """

        # Execute the SQL statements concurrently
        with BigQueryWarehouse.load("zoom-bq") as warehouse:
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(execute_sql, warehouse, bq_ext_table),
                    executor.submit(execute_sql, warehouse, bq_part_table)
                ]

                # Wait for the futures to complete and gather the results
                for future in futures:
                    future.result()

        # Log messages after the tables are created
        logging.info(f"External table created in {project_id}.{dataset_id}")
        logging.info(f"Partitioned and clustered table created in {project_id}.{dataset_id}")

    except Exception as e:
        logging.error(f"Error staging data to BigQuery: {e}")
        raise e

@task(name="dbt modelling")
def dbt_model(dbt_project_path: str) -> None:
    """Run dbt models"""

    def execute_dbt_command(command):
        try:
            dbt_operation = DbtCoreOperation(
                commands=[command],
                project_dir=dbt_path,
                profiles_dir=dbt_path,
            )
            dbt_operation.run()
        except Exception as e:
            logging.error(f"Error running dbt command {command}: {e}")
            raise e

    try:
        # Convert the input path string to a Path object
        dbt_path = Path(dbt_project_path)

        # Log the dbt project path
        logging.info(f"Running dbt models in {dbt_path}")

        # Execute the 'dbt deps' command
        execute_dbt_command("dbt deps")

        # Execute the 'dbt seed' and 'dbt build' commands concurrently
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(execute_dbt_command, "dbt seed -t prod"),
                executor.submit(execute_dbt_command, "dbt build -t prod"),
            ]

            # Wait for the futures to complete and gather the results
            for future in futures:
                future.result()

        # Log a message after the dbt commands are executed
        logging.info(f"dbt models executed successfully in {dbt_path}")

    except Exception as e:
        logging.error(f"Error running dbt models: {e}")
        raise e
    
@task(name="Get year, month and day")    
def get_year_month_day() -> tuple[int, int, int]:
    """Return the year, month, and day for the current run"""
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day
    return year, month, day

# Define the main flow
@flow(name="Process-Data")
def process_data():
    year_month__day_task = get_year_month_day()
    data = get_api_data(year=year_month__day_task[0], month=year_month__day_task[1], day=year_month__day_task[2])
    df = format_df(data)
    dataset_file = f"{year_month__day_task[0]}-{year_month__day_task[1]}-{year_month__day_task[2]}-nyc_rest_inspection_data"
    pq_path = write_local(df, dataset_file)
    write_gcs("zoom-gcs", pq_path)
    stage_bq(gcp_project_name, "de_zoomcamp_project")
    dbt_model("dbt/nyc_inspect")

# Run the flow
if __name__ == "__main__":
    process_data()
