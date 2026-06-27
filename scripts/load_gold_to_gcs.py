import pandas as pd
from datetime import datetime
from airflow.providers.google.cloud.hooks.gcs import GCSHook

def load_gold_to_gcs(**context):
    gold_path = context["ti"].xcom_pull(
        key = "gold_file",
        task_ids = "gold_aggregate",
    )

    execution_date = context["ds_nodash"]

    if not gold_path:
        raise ValueError("Gold file not found in xcom")

    hook_gcs = GCSHook(gcp_conn_id="google_cloud_storage_connection")

    hook_gcs.upload(
        bucket_name = "aggregated_flight_information",
        object_name = f"flights_data/gold_data_{execution_date}.csv",
        filename = gold_path
    )

