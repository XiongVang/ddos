#!/bin/bash

# REQUIRED paths
PROJECT_HOME="<Path to project root>"
SPARK_HOME="<Path to spark-2.3.3-bin-hadoop2.7>"


# Defaults
CHECKPOINT="$PROJECT_HOME/checkpoint-ddos-attacks/"
KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
ACCESS_LOG_TOPIC="access-log"

rm -r ${CHECKPOINT}

sbt assembly \
&& $SPARK_HOME/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
--class DDosDetector \
--master local[2] \
$PROJECT_HOME/target/scala-2.11/ddos-assembly-0.1.jar ${KAFKA_BOOTSTRAP_SERVERS} ${ACCESS_LOG_TOPIC} ${CHECKPOINT}