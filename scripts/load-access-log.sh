#!/bin/bash

# Path to spark (tested with spark-2.3.3-bin-hadoop2.7)
SPARK_HOME="/Users/vang4999/data-eng/spark-2.3.3-bin-hadoop2.7/"

ACCESS_LOG_TEXT_FILE="/Users/vang4999/data-eng/phdata/"

KAFKA_BOOTSTRAP_SERVERS="localhost:9092"

ACCESS_LOG_TOPIC="access-log"

CHECKPOINT="/Users/vang4999/data-eng/phdata/ddos/checkpoint-access-log/"

cd ../ \
&& sbt assembly \
&& $SPARK_HOME/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
--class LoadAccessLog \
--master local \
target/scala-2.11/ddos-assembly-0.1.jar $ACCESS_LOG_TEXT_FILE  $KAFKA_BOOTSTRAP_SERVERS $ACCESS_LOG_TOPIC $CHECKPOINT