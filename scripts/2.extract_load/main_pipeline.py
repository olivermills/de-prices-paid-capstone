import requests
from urllib.request import urlretrieve
import io
import pandas as pd
from datetime import datetime, date
from pathlib import Path
import os
import json
from dotenv import load_dotenv

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse
from prefect_dbt.cli.commands import DbtCoreOperation
from prefect_gcp import GcpCredentials

load_dotenv()

gcp_account_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

creds = open("/Users/olivermills/Desktop/de-zoomcamp-secrets/spatial-thinker-384120-55e94ebf25fd.json")
json_creds = json.load(creds)
gcp_project_name = json_creds['project_id']

# Define headers for the CSV files
headers = ['transaction_unique_identifier', 'price', 'date_of_transfer', 'postcode',
           'property_type', 'old_or_new', 'duration', 'primary_addressable_object_name', 'secondary_addressable_object_name', 'street',
           'locality', 'town_or_city', 'district', 'county', 'ppd_category_type',
           'record_status']


@task(name="Get API data", retries=3, log_prints=True)
def fetch(dataset_url: str) -> pd.DataFrame:
    """read price paid data from web into pandas dataframe"""

    df = pd.read_csv(dataset_url)
    df.columns = headers
    return df


@task(name="dataframe datatype formatting", log_prints=True)
def format_df(df: pd.DataFrame) -> pd.DataFrame:
    """Format DataFrame"""

    # convert the "date_of_transfer" column to datetime format
    df['date_of_transfer'] = pd.to_datetime(df['date_of_transfer'])

    #extract year for bq partitioning
    df['year_of_transfer'] = df['date_of_transfer'].dt.year

    df = df.astype({
                'transaction_unique_identifier': 'str', 
                'price': 'Int64', 
                'date_of_transfer': 'datetime64[ms]', 
                'year_of_transfer': 'Int64',
                'postcode': 'str',
                'property_type': 'str', 
                'old_or_new': 'str', 
                'duration': 'str', 
                'primary_addressable_object_name': 'str',
                'secondary_addressable_object_name': 'str',
                'street': 'str',
                'locality': 'str', 
                'town_or_city': 'str', 
                'district': 'str', 
                'county': 'str', 
                'ppd_category_type': 'str',
                'record_status': 'str'
                })

    print(f"rows: {len(df)}")

    print(df.dtypes)

    return df

@task(name="write data locally", retries=3, log_prints=True)
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write dataframe out locally as parquet file"""

    path = Path(f"data/parquet/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(name="upload to GCS", retries=3, log_prints=True)
def write_gcs(path: Path) -> None:
    """upload local parquet file to GCS"""

    gcs_block = GcsBucket.load("pp-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@task(name="Stage GCS to BQ", retries=3, log_prints=True)
def stage_bq():
    """Stage data in BigQuery"""

    bq_ext_tbl = f"""
            CREATE OR REPLACE EXTERNAL TABLE `{gcp_project_name}.pp_bq_dataset.external_pp_data`
            OPTIONS (
                format = 'PARQUET',
                uris = ['gs://pp_data_lake_{gcp_project_name}/data/parquet/pp-*.parquet']
            )
        """

    with BigQueryWarehouse.load("pp-bq") as warehouse:
        operation = bq_ext_tbl
        warehouse.execute(operation)

    bq_part_tbl = f"""
            CREATE OR REPLACE TABLE `{gcp_project_name}.pp_bq_dataset.pp_data_partitioned_clustered`
            PARTITION BY DATE_TRUNC(date_of_transfer, YEAR)
            CLUSTER BY county AS
            SELECT * FROM `{gcp_project_name}.pp_bq_dataset.external_pp_data`;
        """

    with BigQueryWarehouse.load("pp-bq") as warehouse:
        operation = bq_part_tbl
        warehouse.execute(operation)


@task(name="dbt modelling")
def dbt_build():
    """Run dbt models"""

    dbt_path = Path(f"dbt/prices_paid_data")

    dbt_run = DbtCoreOperation(
                    commands=["dbt build"],
                    project_dir=dbt_path,
                    profiles_dir=dbt_path,
    )

    dbt_run.run()

    return


@flow(name="main etl function", retries=3, log_prints=True)
def etl_web_to_gcs(year: int) -> None:
    """The main etl function"""

    dataset_file = f"pp-{year}"
    dataset_url = f"http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-{year}.csv"
    df = fetch(dataset_url)
    df.head()
    df_clean = format_df(df)
    df_clean.head()
    path = write_local(df_clean, dataset_file)
    write_gcs(path)


@flow(name="parent etl function", retries=3, log_prints=True)
def etl_parent_flow(years: list[int]):
    for year in years:
        etl_web_to_gcs(year)
    
    stage_bq()
    dbt_build()


if __name__ == "__main__":
    years = [2016, 2017, 2018, 2019, 2020, 2021, 2022]
    etl_parent_flow(years)