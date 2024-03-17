import csv
import json

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone

import requests
from google.cloud import bigquery, storage
from google.oauth2 import service_account


BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"
PROJECT_ID = "velvety-castle-414308"
DAGS_FOLDER = "/opt/airflow/dags"
DATA = "addresses"
DATA_2 = "order_items"
DATA_2_API = "order-items"
DATA_3 = "products"
DATA_4 = "promos"
DATA_5 = "events"

def _extract_data():
    # address
    url = f"http://34.87.139.82:8000/{DATA}/"
    response = requests.get(url)
    data = response.json()

    with open(f"{DAGS_FOLDER}/{DATA}.csv", "w") as f:
        writer = csv.writer(f)
        header = [
            "address_id",
            "address",
            "zipcode",
            "state",
            "country",
        ]
        writer.writerow(header)
        for each in data:
            data = [
                each["address_id"],
                each["address"],
                each["zipcode"],
                each["state"],
                each["country"],
            ]
            writer.writerow(data)
    # order_items
    # url = f"http://34.87.139.82:8000/{DATA_2_API}/"
    # response = requests.get(url)
    # data = response.json()

    # with open(f"{DAGS_FOLDER}/{DATA_2}.csv", "w") as f:
    #     writer = csv.writer(f)
    #     header = [
    #         "order_id",
    #         "product",
    #         "quantity",
    #     ]
    #     writer.writerow(header)
    #     for each in data:
    #         data = [
    #             each["order_id"],
    #             each["product"],
    #             each["quantity"],
    #         ]
    #         writer.writerow(data)
    # # products
    # url = f"http://34.87.139.82:8000/{DATA_3}/"
    # response = requests.get(url)
    # data = response.json()

    # with open(f"{DAGS_FOLDER}/{DATA_3}.csv", "w") as f:
    #     writer = csv.writer(f)
    #     header = [
    #         "product_id",
    #         "name",
    #         "price",
    #         "inventory",
    #     ]
    #     writer.writerow(header)
    #     for each in data:
    #         data = [
    #             each["product_id"],
    #             each["name"],
    #             each["price"],
    #             each["inventory"],
    #         ]
    #         writer.writerow(data)
    # # promos
    # url = f"http://34.87.139.82:8000/{DATA_4}/"
    # response = requests.get(url)
    # data = response.json()

    # with open(f"{DAGS_FOLDER}/{DATA_4}.csv", "w") as f:
    #     writer = csv.writer(f)
    #     header = [
    #         "promo_id",
    #         "discount",
    #         "status",
    #     ]
    #     writer.writerow(header)
    #     for each in data:
    #         data = [
    #             each["promo_id"],
    #             each["discount"],
    #             each["status"],
    #         ]
    #         writer.writerow(data)

def _extract_data_with_partition(ds):
    url = f"http://34.87.139.82:8000/{DATA_5}/?created_at={ds}"
    response = requests.get(url)
    data = response.json()

    if data:
        with open(f"{DAGS_FOLDER}/{DATA_5}-{ds}.csv", "w") as f:
            writer = csv.writer(f)
            header = [
                "event_id",
                "session_id",
                "page_url",
                "created_at",
                "event_type",
                "user",
                "order",
                "product",
            ]
            writer.writerow(header)
            for each in data:
                data = [
                    each["event_id"],
                    each["session_id"],
                    each["page_url"],
                    each["created_at"],
                    each["event_type"],
                    each["user"],
                    each["order"],
                    each["product"]
                ]
                writer.writerow(data)
        return "_load_data_to_gcs_with_partition"
    else:
        return "do_nothing"


def _load_data_to_gcs(ds):
    keyfile_gcs = "/opt/airflow/dags/uploading-files-to-gcs-key.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    # Load data from Local to GCS
    bucket_name = "deb3-bootcamp-27"
    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    file_path = f"{DAGS_FOLDER}/{DATA}.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

def _load_data_to_gcs_with_partition(ds):
    # YOUR CODE HERE
    keyfile_gcs = "/opt/airflow/dags/uploading-files-to-gcs-key.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    # Load data from Local to GCS
    bucket_name = "deb3-bootcamp-27"
    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)
    
    file_path = f"{DAGS_FOLDER}/{DATA_5}-{ds}.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA_5}/{ds}/{DATA_5}-{ds}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

def _load_data_from_gcs_to_bigquery():
    keyfile_bigquery = "/opt/airflow/dags/service-account-deb3-load-files-to-bigquery.json"
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )

    bigquery_client = bigquery.Client(
        project=PROJECT_ID,
        credentials=credentials_bigquery,
        location=LOCATION,
    )

    table_id = f"{PROJECT_ID}.deb_bootcamp.{DATA}"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )

    bucket_name = "deb3-bootcamp-27"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=LOCATION,
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2021, 2, 9),
}
with DAG(
    dag_id="greenery_addresses_data_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["DEB", "Skooldio", "greenery"],
):

    # Extract data from Postgres, API, or SFTP
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
    )
    extract_data_with_partition = BranchPythonOperator(
        task_id="extract_data_with_partition",
        python_callable=_extract_data_with_partition,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs
    )
    load_data_to_gcs_with_partition = PythonOperator(
        task_id="load_data_to_gcs_with_partition",
        python_callable=_load_data_to_gcs_with_partition,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery,
    )
    
    do_nothing = EmptyOperator(task_id="do_nothing")
    # Default is every path has to succeed, but that's impossible in branchs.
    end = EmptyOperator(task_id="end", trigger_rule="one_success")

    # Task dependencies
    # extract_data >> load_data_to_gcs >> load_data_from_gcs_to_bigquery
    extract_data_with_partition >> load_data_to_gcs_with_partition >> do_nothing >> end
    extract_data_with_partition >> do_nothing >> end
    