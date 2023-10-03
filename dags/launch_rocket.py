from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

def print_context(**context):
    print(f"context: {context}")

with DAG(
    dag_id="launch_rocket",
    start_date=datetime(2023, 9, 1),
    schedule="@daily",
    catchup=True
) as dag:

    context = PythonOperator(
        task_id="context",
        provide_context=True,
        python_callable=print_context
    )

    procure_rocket_material = EmptyOperator(
        task_id="procure_rocket_material"
    )

    procure_fuel = EmptyOperator(
        task_id="procure_fuel"
    )

    build_stage_1 = EmptyOperator(
        task_id="build_stage_1"
    )

    build_stage_2 = EmptyOperator(
        task_id="build_stage_2"
    )

    build_stage_3 = EmptyOperator(
        task_id="build_stage_3"
    )

    launch = EmptyOperator(
        task_id="launch"
    )

    build_stages = [build_stage_1, build_stage_2, build_stage_3]

    context >> [procure_fuel, procure_rocket_material]
    procure_fuel >> build_stage_3
    procure_rocket_material >> build_stages >> launch
