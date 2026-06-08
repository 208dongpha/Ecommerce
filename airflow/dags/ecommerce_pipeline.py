from airflow import  DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG (
    dag_id = 'ecommerce_pipeline',
    start_date = datetime(2025,8,6),
    schedule="@hourly",
    catchup=False,
    tags=["ecommerce"]
) as dag:
    
    generate_date = BashOperator(
        task_id = "generate_data",
        bash_command = """
cd /opt/airflow/producer &&
NUM_EVENTS=1000 python producer.py
"""
    )

    gold_job = BashOperator(
        task_id = "gold_job",
        bash_command = """
docker exec spark-master bash -c '
mkdir -p /tmp/.ivy2/cache &&
mkdir -p /tmp/.ivy2/jars &&

SPARK_SUBMIT_OPTS="-Divy.cache.dir=/tmp/.ivy2/cache -Divy.home=/tmp/.ivy2" \
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.hadoop:hadoop-aws:3.3.4 \
/opt/spark-apps/batch_job.py
'
"""
    )

    load_to_postgres = BashOperator(
        task_id = "load_postgres",
        bash_command = """
docker exec spark-master bash -c '
mkdir -p /tmp/.ivy2/cache &&
mkdir -p /tmp/.ivy2/jars &&

SPARK_SUBMIT_OPTS="-Divy.cache.dir=/tmp/.ivy2/cache -Divy.home=/tmp/.ivy2" \
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:42.7.3 \
/opt/spark-apps/load_gold_to_postgres.py
'
"""
    )

    generate_data >> gold_job >> load_postgres
