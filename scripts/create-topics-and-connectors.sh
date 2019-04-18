#!/bin/bash

SUCCESS_MESSAGE="** Go to http://localhost:9021 and verify that topics and connectors have been created **"

# Path to confluent-5.2.1
CONFLUENT_HOME="/Users/vang4999/data-eng/confluent-5.2.1"

ACCESS_LOG_TOPIC="access-log"

$CONFLUENT_HOME/bin/confluent destroy \

sleep 10s

$CONFLUENT_HOME/bin/confluent start \
&& $CONFLUENT_HOME/bin/kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic $ACCESS_LOG_TOPIC \
&& $CONFLUENT_HOME/bin/confluent load access-log-file-source -d access-log-file-source.json \
&& echo $SUCCESS_MESSAGE