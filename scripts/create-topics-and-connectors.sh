#!/bin/bash

SUCCESS_MESSAGE="****************************************************\nGo to http://localhost:9021 and verify that topics and connectors have been created\n***************************************"

# Path to confluent-5.2.1
CONFLUENT_HOME="/Users/vang4999/data-eng/confluent-5.2.1"

ACCESS_LOG_TOPIC="access-log"

DDOS_ATTACKS="ddos-attacks"

$CONFLUENT_HOME/bin/confluent start \
&& $CONFLUENT_HOME/bin/kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic $ACCESS_LOG_TOPIC \
&& $CONFLUENT_HOME/bin/kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic $DDOS_ATTACKS \
&& $CONFLUENT_HOME/bin/confluent load access-log-file-source -d access-log-file-source.json \
&& $CONFLUENT_HOME/bin/confluent load ddos-attacks-file-sink -d ddos-attacks-file-sink.json \
&& echo $SUCCESS_MESSAGE