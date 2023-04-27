from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import BigQueryWarehouse
import os
import json
from dotenv import load_dotenv

load_dotenv()

gcp_account_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

creds = open("/Users/olivermills/Desktop/de-zoomcamp-secrets/spatial-thinker-384120-55e94ebf25fd.json")
json_creds = json.load(creds)
gcp_project_name = json_creds['project_id']


# alternative to creating GCP blocks in the UI
# copy your own service_account_info dictionary from the json file you downloaded from google
# IMPORTANT - do not store credentials in a publicly available repository!

credentials_block = GcpCredentials(
    service_account_file="/Users/olivermills/Desktop/de-zoomcamp-secrets/spatial-thinker-384120-55e94ebf25fd.json"  # point to your credentials .json file
)
credentials_block.save("pp-gcp-creds", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("pp-gcp-creds"),
    bucket="pp_data_lake_spatial-thinker-384120",  # insert your GCS bucket name
)

bucket_block.save("pp-gcs", overwrite=True)

bq_block = BigQueryWarehouse(
    gcp_credentials=GcpCredentials.load("pp-gcp-creds"),
    fetch_size = 1
)

bq_block.save("pp-bq", overwrite=True)