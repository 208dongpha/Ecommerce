#!/usr/bin/env bash
set -u

TRIGGER_DIR="/opt/airflow-triggers"
REQUEST_FILE="$TRIGGER_DIR/gold.request"
DONE_FILE="$TRIGGER_DIR/gold.done"
FAILED_FILE="$TRIGGER_DIR/gold.failed"

mkdir -p "$TRIGGER_DIR" /tmp/.ivy2/cache /tmp/.ivy2/jars

while true; do
  if [ -f "$REQUEST_FILE" ]; then
    rm -f "$DONE_FILE" "$FAILED_FILE"
    REQUEST_ID="$(cat "$REQUEST_FILE")"
    rm -f "$REQUEST_FILE"

    echo "Starting gold batch for $REQUEST_ID"

    if /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
      /opt/spark-apps/batch_job.py; then
      echo "Gold batch completed for $REQUEST_ID" > "$DONE_FILE"
    else
      echo "Gold batch failed for $REQUEST_ID" > "$FAILED_FILE"
    fi
  fi

  sleep 5
done
