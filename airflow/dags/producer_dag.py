from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2025,1,1),
    schedule="@daily",
    catchup=False
) as dag:

    run_producer = BashOperator(
        task_id="run_producer",
        bash_command="python /opt/airflow/producer/producer.py"
    )