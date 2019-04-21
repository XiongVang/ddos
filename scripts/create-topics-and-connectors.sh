#!/bin/bash

SUCCESS_MESSAGE="** Go to http://localhost:9021 and verify that topics and connectors have been created **"

# REQUIRED paths
CONFLUENT_HOME="<Path to confluent-5.2.1>"
PROJECT_HOME="<Path to project root>"

ACCESS_LOG_TOPIC="access-log"

ACCESS_LOG_CONNECTOR_CONFIG_FILE="$PROJECT_HOME/scripts/access-log-file-source.json"

$CONFLUENT_HOME/bin/confluent start \
&& $CONFLUENT_HOME/bin/kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic $ACCESS_LOG_TOPIC \
&& $CONFLUENT_HOME/bin/confluent load access-log-file-source -d access-log-file-source.json \
&& echo $SUCCESS_MESSAGE