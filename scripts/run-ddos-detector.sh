#!/bin/bash

# Path to spark-2.3.3-bin-hadoop2.7
SPARK_HOME="/Users/vang4999/data-eng/spark-2.3.3-bin-hadoop2.7"

KAFKA_BOOTSTRAP_SERVERS="localhost:9092"

ACCESS_LOG_TOPIC="access-log"

DDOS_ATTACKS_TOPIC="ddos-attacks"

CHECKPOINT="/Users/vang4999/data-eng/phdata/checkpoint-ddos-attacks/"

cd ../ \
&& sbt assembly \
&& $SPARK_HOME/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
--class me.xiongvang.ddos.DDosDetector \
--master local \
target/scala-2.11/ddos-assembly-0.1.jar $KAFKA_BOOTSTRAP_SERVERS $ACCESS_LOG_TOPIC $DDOS_ATTACKS_TOPIC $CHECKPOINT