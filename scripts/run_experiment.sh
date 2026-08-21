#!/usr/bin/env bash
#
# End-to-end run for the shared-watermark experiment. Both branches invoke this
# identically, so the watermark is the only difference between the two runs.
#
#   scripts/run_experiment.sh reports/main.json
#
set -euo pipefail

cd "$(dirname "$0")/.."

REPORT_PATH="${1:?usage: run_experiment.sh <report-path> [group-id]}"
GROUP="${2:-exp-$(date +%s)-$$}"
PYTHON="${PYTHON:-.venv/bin/python}"
# seconds to leave the consumer running past the last event, so the idle
# backstop (IDLE_THRESHOLD 0.8s, BACKSTOP_INTERVAL 0.2s) seals trailing windows
WAIT_AFTER="${WAIT_AFTER:-10}"

mkdir -p "$(dirname "$REPORT_PATH")" logs
CONSUMER_LOG="logs/experiment-consumer-$(basename "$REPORT_PATH" .json).log"

echo "=== branch: $(git rev-parse --abbrev-ref HEAD) @ $(git rev-parse --short HEAD)"
echo "=== report: $REPORT_PATH   group: $GROUP"

# ---------------------------------------------------------------- wipe kafka
# the topic otherwise still holds live-producer events timestamped ~63000s below
# the workload's T, and auto_offset_reset=earliest would replay them into every count
echo "=== wiping kafka state"
docker compose down -v
docker compose up -d

echo -n "=== waiting for broker health"
for _ in $(seq 1 60); do
  status="$(docker inspect --format='{{.State.Health.Status}}' kafka 2>/dev/null || echo none)"
  [ "$status" = "healthy" ] && break
  echo -n "."
  sleep 2
done
echo " $status"
[ "$status" = "healthy" ] || { echo "broker never became healthy"; exit 1; }

# kafka-init sits behind the 'apps' profile and KAFKA_AUTO_CREATE_TOPICS_ENABLE
# is false, so a bare `up` leaves market-events nonexistent. create it explicitly
echo "=== creating topics"
make topics-create >/dev/null

# ------------------------------------------------------- verify partition count
# not optional: a different partition count rehashes the symbols across
# partitions and silently invalidates the whole design with no error anywhere
PARTS="$(docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic market-events \
  | awk '/PartitionCount/ {for (i=1;i<NF;i++) if ($i=="PartitionCount:") print $(i+1)}')"
PARTS="${PARTS:-$(docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic market-events \
  | grep -c 'Partition:')}"
echo "=== market-events PartitionCount: $PARTS"
if [ "$PARTS" != "3" ]; then
  echo "ABORT: market-events has $PARTS partitions, expected exactly 3."
  exit 1
fi

# ------------------------------------------------------------------- consumer
echo "=== starting consumer -> $CONSUMER_LOG"
EXPERIMENT_REPORT="$REPORT_PATH" EXPERIMENT_GROUP_ID="$GROUP" \
  "$PYTHON" consumer/consumer.py >"$CONSUMER_LOG" 2>&1 &
CONSUMER_PID=$!
trap 'kill -INT "$CONSUMER_PID" 2>/dev/null || true' EXIT

# let it join the group and take its assignment before anything is produced
for _ in $(seq 1 30); do
  grep -q "consuming from" "$CONSUMER_LOG" 2>/dev/null && break
  sleep 1
done
sleep 3

# ------------------------------------------------------------------- producer
echo "=== replaying workload"
"$PYTHON" producer/producer.py --replay

echo "=== waiting ${WAIT_AFTER}s for the idle backstop to seal trailing windows"
sleep "$WAIT_AFTER"

# ------------------------------------------------- drain + report via SIGINT
echo "=== stopping consumer (SIGINT -> drain + report)"
trap - EXIT
kill -INT "$CONSUMER_PID"
wait "$CONSUMER_PID" || true

echo
sed -n '/experiment report/,/^====*$/p' "$CONSUMER_LOG"
echo
echo "=== full log: $CONSUMER_LOG"
