#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
database_url="postgresql://stablerail:stablerail@localhost:15432/stablerail"
base_url="${STABLERAIL_E2E_BASE_URL:-http://localhost:8081}"
operator_token="${STABLERAIL_E2E_OPERATOR_TOKEN:-blindpay-e2e-operator}"
keep_stack="${STABLERAIL_E2E_KEEP_STACK:-0}"
server_log="${TMPDIR:-/tmp}/stablerail-blindpay-e2e.log"
server_binary="${TMPDIR:-/tmp}/stablerail-blindpay-e2e-$$"
server_pid_file="${TMPDIR:-/tmp}/stablerail-blindpay-e2e-$$.pid"
mock_binary="${TMPDIR:-/tmp}/stablerail-blindpay-mock-$$"
compose=(docker compose --env-file /dev/null -p stablerail-blindpay-e2e -f compose.e2e.yaml)
server_pid=""
original_server_pid=""
mock_pid=""

cleanup() {
  if [[ -f "$server_pid_file" ]]; then server_pid="$(<"$server_pid_file")"; fi
  if [[ -n "$original_server_pid" && "$original_server_pid" != "$server_pid" ]]; then wait "$original_server_pid" 2>/dev/null || true; fi
  if [[ -n "$server_pid" ]]; then kill "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true; fi
  if [[ -n "$mock_pid" ]]; then kill "$mock_pid" 2>/dev/null || true; wait "$mock_pid" 2>/dev/null || true; fi
  rm -f "$server_binary" "$server_pid_file" "$mock_binary"
  if [[ "$keep_stack" == "1" ]]; then
    echo "BlindPay E2E PostgreSQL and Kafka remain running for inspection."
  else
    "${compose[@]}" down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

cd "$project_dir"
"${compose[@]}" up -d postgres kafka
until "${compose[@]}" exec -T postgres pg_isready -U stablerail -d stablerail >/dev/null 2>&1; do sleep 1; done
until "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list >/dev/null 2>&1; do sleep 1; done
for topic in payment-events payment-commands; do
  "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1 >/dev/null
done
for migration in migrations/*.sql; do
  "${compose[@]}" exec -T postgres psql -v ON_ERROR_STOP=1 -U stablerail -d stablerail < "$migration"
done

go build -o "$mock_binary" ./e2e/blindpaymock
"$mock_binary" >"${TMPDIR:-/tmp}/stablerail-blindpay-mock.log" 2>&1 &
mock_pid=$!
until curl -fsS http://localhost:18081/__e2e/payout-calls/not-found >/dev/null; do sleep 0.1; done

go build -o "$server_binary" ./cmd/stablerail
STABLERAIL_DATABASE_URL="$database_url" \
STABLERAIL_KAFKA_BROKERS=localhost:19092 \
STABLERAIL_SETTLEMENT_PROVIDER=blindpay \
STABLERAIL_OPERATOR_TOKEN="$operator_token" \
STABLERAIL_BLINDPAY_API_KEY=blindpay-e2e-key \
STABLERAIL_BLINDPAY_INSTANCE_ID=in_e2e \
STABLERAIL_BLINDPAY_BASE_URL=http://localhost:18081/v1 \
STABLERAIL_BLINDPAY_WEBHOOK_SECRET=whsec_YmxpbmRwYXktZTJlLXNlY3JldA== \
STABLERAIL_BLINDPAY_NETWORK=base_sepolia \
STABLERAIL_BLINDPAY_TOKEN=USDB \
STABLERAIL_BLINDPAY_MANAGED_WALLET_ID=bl_e2e \
STABLERAIL_BLINDPAY_MANAGED_WALLET_ADDRESS=0xe2e \
STABLERAIL_MOCK_POLICY_REJECT_AMOUNT=4004 \
STABLERAIL_SAGA_COMPLIANCE_TIMEOUT=500ms \
STABLERAIL_SAGA_POLL_INTERVAL=100ms \
STABLERAIL_RECONCILIATION_INTERVAL=30s \
STABLERAIL_ALLOW_PRIVATE_WEBHOOK_URLS=1 \
STABLERAIL_HTTP_ADDRESS=:8081 \
"$server_binary" >"$server_log" 2>&1 &
server_pid=$!
original_server_pid="$server_pid"
printf '%s' "$server_pid" > "$server_pid_file"

STABLERAIL_E2E_BASE_URL="$base_url" \
STABLERAIL_E2E_DATABASE_URL="$database_url" \
STABLERAIL_E2E_OPERATOR_TOKEN="$operator_token" \
STABLERAIL_DATABASE_URL="$database_url" \
STABLERAIL_KAFKA_BROKERS=localhost:19092 \
STABLERAIL_SETTLEMENT_PROVIDER=blindpay \
STABLERAIL_OPERATOR_TOKEN="$operator_token" \
STABLERAIL_BLINDPAY_API_KEY=blindpay-e2e-key \
STABLERAIL_BLINDPAY_INSTANCE_ID=in_e2e \
STABLERAIL_BLINDPAY_BASE_URL=http://localhost:18081/v1 \
STABLERAIL_BLINDPAY_WEBHOOK_SECRET=whsec_YmxpbmRwYXktZTJlLXNlY3JldA== \
STABLERAIL_BLINDPAY_NETWORK=base_sepolia \
STABLERAIL_BLINDPAY_TOKEN=USDB \
STABLERAIL_BLINDPAY_MANAGED_WALLET_ID=bl_e2e \
STABLERAIL_BLINDPAY_MANAGED_WALLET_ADDRESS=0xe2e \
STABLERAIL_MOCK_POLICY_REJECT_AMOUNT=4004 \
STABLERAIL_SAGA_COMPLIANCE_TIMEOUT=500ms \
STABLERAIL_SAGA_POLL_INTERVAL=100ms \
STABLERAIL_RECONCILIATION_INTERVAL=30s \
STABLERAIL_ALLOW_PRIVATE_WEBHOOK_URLS=1 \
STABLERAIL_HTTP_ADDRESS=:8081 \
STABLERAIL_E2E_SERVER_BINARY="$server_binary" \
STABLERAIL_E2E_SERVER_PID="$server_pid" \
STABLERAIL_E2E_SERVER_PID_FILE="$server_pid_file" \
go test -tags=e2e -count=1 -v ./e2e/blindpay/... "$@"
