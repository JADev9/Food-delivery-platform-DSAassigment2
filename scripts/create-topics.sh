#!/usr/bin/env bash
# Creates every Kafka topic the platform uses.
# Run automatically by the kafka-init container. Safe to re-run.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
REPLICATION="${REPLICATION:-1}"
BUSY_PARTITIONS="${BUSY_PARTITIONS:-3}"
QUIET_PARTITIONS="${QUIET_PARTITIONS:-1}"
RETENTION_MS="${RETENTION_MS:-604800000}"

echo "Waiting for Kafka at ${BOOTSTRAP} ..."
for attempt in $(seq 1 30); do
  if kafka-broker-api-versions --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; then
    echo "Kafka is up (attempt ${attempt})."
    break
  fi
  if [ "${attempt}" -eq 30 ]; then
    echo "Kafka did not become ready in time." >&2
    exit 1
  fi
  sleep 2
done

create() {
  local topic="$1"
  local partitions="$2"
  kafka-topics --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${REPLICATION}" \
    --config retention.ms="${RETENTION_MS}" \
    >/dev/null
  printf '  %-34s %s partition(s)\n' "${topic}" "${partitions}"
}

echo "Creating topics:"

create orders.created              "${BUSY_PARTITIONS}"
create orders.confirmed            "${BUSY_PARTITIONS}"
create orders.status.changed       "${BUSY_PARTITIONS}"
create orders.cancelled            "${QUIET_PARTITIONS}"

create payments.completed          "${BUSY_PARTITIONS}"
create payments.failed             "${QUIET_PARTITIONS}"

create restaurant.order.accepted   "${BUSY_PARTITIONS}"
create restaurant.order.ready      "${BUSY_PARTITIONS}"

create delivery.assigned           "${BUSY_PARTITIONS}"
create delivery.picked-up          "${BUSY_PARTITIONS}"
create delivery.completed          "${BUSY_PARTITIONS}"
create delivery.unassigned         "${QUIET_PARTITIONS}"

create notifications.dispatched    "${QUIET_PARTITIONS}"

echo
echo "Topics now on the broker:"
kafka-topics --bootstrap-server "${BOOTSTRAP}" --list | sed 's/^/  /'
echo
echo "Topic setup complete."
