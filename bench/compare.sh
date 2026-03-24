#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

SUBSCRIBERS="${BENCH_SUBSCRIBERS:-10}"
PUBLISHERS="${BENCH_PUBLISHERS:-1}"
MESSAGES="${BENCH_MESSAGES:-5000}"
PAYLOAD_BYTES="${BENCH_PAYLOAD_BYTES:-32}"
PORT="${BENCH_PORT:-18830}"

RUN_CMD=(mvn -pl jmqx-broker -Dtest=MqttLoadBenchmarkTest test \
  "-Dbench.subscribers=${SUBSCRIBERS}" \
  "-Dbench.publishers=${PUBLISHERS}" \
  "-Dbench.messages=${MESSAGES}" \
  "-Dbench.payloadBytes=${PAYLOAD_BYTES}" \
  "-Dbench.port=${PORT}")

echo "== MQTT Load Benchmark =="
echo "subscribers=${SUBSCRIBERS}, publishers=${PUBLISHERS}, messages=${MESSAGES}, payloadBytes=${PAYLOAD_BYTES}, port=${PORT}"
echo

cd "${ROOT_DIR}"

echo "== Run #1 =="
("${RUN_CMD[@]}") | tee "${SCRIPT_DIR}/run1.log"
echo

echo "== Run #2 =="
("${RUN_CMD[@]}") | tee "${SCRIPT_DIR}/run2.log"
echo

echo "== Summary =="
grep -E "MQTT load benchmark" -n "${SCRIPT_DIR}/run1.log" || true
grep -E "MQTT load benchmark" -n "${SCRIPT_DIR}/run2.log" || true
