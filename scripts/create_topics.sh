#!/bin/sh
# Create the project's Kafka topics. Safe to run repeatedly (--if-not-exists).
#
# Runs inside the kafka-init service on `docker compose up`, or by hand:
#     docker compose run --rm kafka-init
#
# --if-not-exists does not change an existing topic. To change it later:
#     retention:  kafka-configs.sh --alter --entity-type topics --entity-name <topic> --add-config retention.ms=<ms>
#     partitions: kafka-topics.sh --alter --topic <topic> --partitions <n>  (increase only)
set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:19092}"
TOPICS=/opt/kafka/bin/kafka-topics.sh

create_topic() {
    # $1 topic, $2 partitions, $3 retention.ms
    "$TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic "$1" --partitions "$2" --replication-factor 1 \
        --config "retention.ms=$3"
    "$TOPICS" --bootstrap-server "$BOOTSTRAP" --describe --topic "$1"
}

# Raw events, 3 days retention (long-term archive is SeaweedFS).
create_topic wikimedia.recentchange.raw 3 259200000
