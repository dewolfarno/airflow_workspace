import json
import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime


with DAG(
    dag_id="rocket_launches",
    start_date=datetime(2023, 9, 20),
    schedule="@daily",
    catchup=True
) as dag:

    get_api_status = HttpSensor(
        task_id="get_api_status",
        http_conn_id="thespacedevs_dev",
        method="get",
        endpoint="/",
        mode="reschedule"
    )

    api_parameters = {
        "net__gte": "{{ ds  }} 00:00:00",
        "net__lt": "{{  next_ds  }} 00:00:00"
    }

    get_api_result = SimpleHttpOperator(
        task_id="get_api_result",
        http_conn_id="thespacedevs_dev",
        method="get",
        endpoint="",
        data=api_parameters,
        log_response=True
    )

    def _check_launches_today(task_instance, **context):
        response = task_instance.xcom_pull(task_ids="get_api_result")
        response_dict = json.loads(response)
        if response_dict["count"] == 0:
            raise AirflowSkipException(f"No data found on date {context['ds']}")

    are_there_launches_today = PythonOperator(
        task_id="are_there_launches_today",
        python_callable=_check_launches_today
    )

    def _extract_relevant_data(x: dict):
        return {
            "id": x.get("id"),
            "name": x.get("name"),
            "status": x.get("status").get("abbrev"),
            "country_code": x.get("pad").get("country_code"),
            "service_provider_name": x.get("launch_service_provider").get("name"),
            "service_provider_type": x.get("launch_service_provider").get("type")
        }

    def _preprocess_data(task_instance):
        response = task_instance.xcom_pull(task_ids="get_api_result")
        response_dict = json.loads(response)
        response_results = response_dict["results"]
        df_results = pd.DataFrame([_extract_relevant_data(i) for i in response_results])
        df_results.to_parquet(path="/tmp/preprocessed_data.parquet")

    preprocess_data = PythonOperator(
        task_id="preprocess_data",
        python_callable=_preprocess_data
    )

    # create_bigquery_dataset = BigQueryCreateEmptyDatasetOperator(
    #     task_id="create_bigquery_dataset",
    #     # gcp_conn_id = "google_cloud_conn",
    #     dataset_id="adw_dataset",
    #     project_id="aflow-training-rabo-2023-10-02"
    # )

    # define DAG order
    (
        get_api_status
        >> get_api_result
        >> are_there_launches_today
        >> preprocess_data
    )
