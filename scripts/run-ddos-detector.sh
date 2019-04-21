#!/bin/bash

PROJECT_HOME="/Users/vang4999/data-eng/phdata/ddos/"

# Path to spark-2.3.3-bin-hadoop2.7
SPARK_HOME="/Users/vang4999/data-eng/spark-2.3.3-bin-hadoop2.7"

CHECKPOINT="/Users/vang4999/data-eng/phdata/ddos/checkpoint-ddos-attacks/"

KAFKA_BOOTSTRAP_SERVERS="localhost:9092"

ACCESS_LOG_TOPIC="access-log"

OUTPUT_DIRECTORY="/Users/vang4999/data-eng/phdata/ddos-attacks/"

rm -r $CHECKPOINT
rm -r $OUTPUT_DIRECTORY

sbt assembly \
&& $SPARK_HOME/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
--class DDosDetector \
--master local[4] \
--executor-memory 4GB \
--total-executor-cores 4 \
$PROJECT_HOME/target/scala-2.11/ddos-assembly-0.1.jar $KAFKA_BOOTSTRAP_SERVERS $ACCESS_LOG_TOPIC $OUTPUT_DIRECTORY $CHECKPOINT