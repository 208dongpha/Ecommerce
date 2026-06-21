from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2025, 8, 6),
    schedule="@daily",
    catchup=False,
    tags=["ecommerce"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="""
cd /opt/airflow/producer &&
KAFKA_BOOTSTRAP_SERVERS=kafka:9092 NUM_EVENTS=1000 python producer.py
""",
    )

    wait_for_streaming = BashOperator(
        task_id="wait_for_streaming",
        bash_command="sleep 60",
    )

    gold_job = BashOperator(
        task_id="gold_job",
        bash_command="""
rm -f /opt/airflow/triggers/gold.done /opt/airflow/triggers/gold.failed
date -u +"%Y-%m-%dT%H:%M:%SZ" > /opt/airflow/triggers/gold.request

for i in $(seq 1 60); do
  if [ -f /opt/airflow/triggers/gold.done ]; then
    cat /opt/airflow/triggers/gold.done
    exit 0
  fi

  if [ -f /opt/airflow/triggers/gold.failed ]; then
    cat /opt/airflow/triggers/gold.failed
    exit 1
  fi

  sleep 10
done

echo "Timed out waiting for gold batch job"
exit 1
""",
    )

    generate_data >> wait_for_streaming >> gold_job
