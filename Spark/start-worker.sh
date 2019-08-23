#!/bin/sh

/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    --webui-port $SPARK_WORKER_WEBUI_PORT \
    -c $SPARK_WORKER_CORES \
    -m $SPARK_WORKER_MEMORY \
    $SPARK_MASTER 