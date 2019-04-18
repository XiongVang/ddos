#!/bin/bash

# Path to spark-2.3.3-bin-hadoop2.7
SPARK_HOME="/Users/vang4999/data-eng/spark-2.3.3-bin-hadoop2.7"

INPUT_PATH="/Users/vang4999/data-eng/phdata/access-logs/"

OUTPUT_DIRECTORY="/Users/vang4999/data-eng/phdata/ddos-attacks/"

# Reset output each run
rm -r $OUTPUT_DIRECTORY

cd ../ \
&& sbt assembly \
&& $SPARK_HOME/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
--class DDosDetector \
--master local[*] \
target/scala-2.11/ddos-assembly-0.1.jar $INPUT_PATH $OUTPUT_DIRECTORY