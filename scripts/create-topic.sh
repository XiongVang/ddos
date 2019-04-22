#!/bin/bash

SUCCESS_MESSAGE="** Go to http://localhost:9021 and verify the topic **"

# REQUIRED paths
CONFLUENT_HOME="<Path to confluent-5.2.1>"

ACCESS_LOG_TOPIC="access-log"

$CONFLUENT_HOME/bin/confluent start \
&& $CONFLUENT_HOME/bin/kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic ${ACCESS_LOG_TOPIC} \
&& echo ${SUCCESS_MESSAGE}