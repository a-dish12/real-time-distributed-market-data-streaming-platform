#!/usr/bin/env bash
#
# end to end run for the shared-watermark experiment, both branches invoke this
# identically so the watermark is the only difference between the two runs
#
#   scripts/run_experiment.sh reports/main.json
#
set -euo pipefail
# job control, without it a background job from a non-interactive shell inherits
# SIG_IGN for SIGINT and python never installs its KeyboardInterrupt handler
set -m

cd "$(dirname "$0")/.."

REPORT_PATH="${1:?usage: run_experiment.sh <report-path> [group-id]}"
GROUP="${2:-exp-$(date +%s)-$$}"
# -u, stdout goes to a file here and block buffering would hide the readiness
# line the wait loop greps for
PYTHON="${PYTHON:-.venv/bin/python} -u"
# seconds past the last event, long enough for the idle backstop to seal
# trailing windows
WAIT_AFTER="${WAIT_AFTER:-10}"

mkdir -p "$(dirname "$REPORT_PATH")" logs
CONSUMER_LOG="logs/experiment-consumer-$(basename "$REPORT_PATH" .json).log"

echo "=== branch: $(git rev-parse --abbrev-ref HEAD) @ $(git rev-parse --short HEAD)"
echo "=== report: $REPORT_PATH   group: $GROUP"

# wipe kafka
# the topic still holds live-producer events ~63000s below the workload's T and
# earliest would replay them into every count
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

# kafka-init sits behind the apps profile and auto create is off, so a bare up
# leaves market-events nonexistent
echo "=== creating topics"
make topics-create >/dev/null

# verify partition count
# not optional, a different count rehashes the symbols and invalidates the
# design with no error anywhere
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

# consumer
echo "=== starting consumer -> $CONSUMER_LOG"
# -m not a path, matches the compose commands and puts the repo root on sys.path
# so `from config import ...` resolves
EXPERIMENT_REPORT="$REPORT_PATH" EXPERIMENT_GROUP_ID="$GROUP" \
  $PYTHON -m consumer.consumer >"$CONSUMER_LOG" 2>&1 &
CONSUMER_PID=$!
trap 'kill -INT "$CONSUMER_PID" 2>/dev/null || true' EXIT

# let it join the group and take its assignment before anything is produced
for _ in $(seq 1 30); do
  grep -q "consuming from" "$CONSUMER_LOG" 2>/dev/null && break
  if ! kill -0 "$CONSUMER_PID" 2>/dev/null; then
    echo "ABORT: consumer exited before it started consuming"
    tail -20 "$CONSUMER_LOG"
    exit 1
  fi
  sleep 1
done
sleep 3

# ------------------------------------------------------------------- producer
echo "=== replaying workload"
$PYTHON -m producer.producer --replay

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
