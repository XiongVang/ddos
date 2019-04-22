#!/bin/bash

# Prerequesite: run create-topics-and-connectors.sh first

# REQUIRED paths
PROJECT_HOME="<Path to project root>"
ACCESS_LOG_DIRECTORY="<Path to access logs directory>"
SPARK_HOME="<Path to spark-2.3.3-bin-hadoop2.7>"

KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
ACCESS_LOG_TOPIC="access-log"
CHECKPOINT_DIRECTOTRY="$PROJECT_HOME/checkpoint-access-log/"

# Reset checkpoint
rm -r $CHECKPOINT_DIRECTOTRY

sbt assembly \
&& $SPARK_HOME/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
--class LoadAccessLog \
--master local \
target/scala-2.11/ddos-assembly-0.1.jar ${ACCESS_LOG_DIRECTORY}  ${KAFKA_BOOTSTRAP_SERVERS} ${ACCESS_LOG_TOPIC} ${CHECKPOINT_DIRECTOTRY}