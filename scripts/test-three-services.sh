#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
COMPOSE=(docker compose -f docker-compose.yml)

for command in docker; do
  command -v "$command" >/dev/null 2>&1 || { echo "ERROR: Missing command: $command" >&2; exit 2; }
done
docker compose version >/dev/null

read_counter() {
  local container="$1" file="$2" offset="$3"
  docker compose exec -T "$container" sh -c \
    "od -An -tu8 -j ${offset} -N 8 '${file}' | tr -d '[:space:]'"
}

wait_healthy() {
  local service="$1" container="$2"
  for attempt in $(seq 1 90); do
    local status
    status="$(docker inspect -f '{{.State.Health.Status}}' "$container" 2>/dev/null || true)"
    [[ "$status" == "healthy" ]] && return 0
    if [[ "$attempt" -eq 90 ]]; then
      echo "ERROR: $service did not become healthy. Logs:" >&2
      "${COMPOSE[@]}" logs --tail=150 "$service" >&2 || true
      exit 1
    fi
    sleep 1
  done
}

echo "[1/7] Validating Compose configuration..."
"${COMPOSE[@]}" config >/dev/null

echo "[2/7] Building and starting Sequencer + PostgreSQL + Audit + Gateway..."
"${COMPOSE[@]}" up -d --build sequencer postgres audit gateway

echo "[3/7] Waiting for service health..."
wait_healthy sequencer g12-sequencer
wait_healthy postgres g12-postgres
wait_healthy audit g12-audit

JOURNAL=/dev/shm/g12-journal.ring
INBOUND=/dev/shm/g12-inbound.ring
ACK=/dev/shm/g12-ack.ring

journal_p_before="$(read_counter sequencer "$JOURNAL" 64)"
journal_c_before="$(read_counter audit "$JOURNAL" 128)"
inbound_c_before="$(read_counter sequencer "$INBOUND" 128)"
ack_p_before="$(read_counter audit "$ACK" 64)"
ack_c_before="$(read_counter gateway "$ACK" 128)"
stream_id="$(read_counter audit "$JOURNAL" 32)"

echo "[4/7] Verifying shared IPC namespace across all three services..."
for file in "$JOURNAL" "$ACK"; do
  seq_identity="$(docker compose exec -T sequencer stat -c '%d:%i' "$file")"
  audit_identity="$(docker compose exec -T audit stat -c '%d:%i' "$file")"
  gateway_identity="$(docker compose exec -T gateway stat -c '%d:%i' "$file")"
  if [[ "$seq_identity" != "$audit_identity" || "$seq_identity" != "$gateway_identity" ]]; then
    echo "ERROR: Services do not share the same inode for $file" >&2
    echo "sequencer=$seq_identity audit=$audit_identity gateway=$gateway_identity" >&2
    exit 1
  fi
done

inbound_seq_identity="$(docker compose exec -T sequencer stat -c '%d:%i' "$INBOUND")"
inbound_gateway_identity="$(docker compose exec -T gateway stat -c '%d:%i' "$INBOUND")"
[[ "$inbound_seq_identity" == "$inbound_gateway_identity" ]] || {
  echo "ERROR: Gateway and Sequencer do not share inbound ring" >&2; exit 1;
}

echo "[5/7] Running WebSocket -> Sequencer -> Audit -> ACK integration tests..."
"${COMPOSE[@]}" run --rm integration-test

echo "[6/7] Verifying all ring counters advanced..."
for attempt in $(seq 1 100); do
  journal_p_after="$(read_counter sequencer "$JOURNAL" 64)"
  journal_c_after="$(read_counter audit "$JOURNAL" 128)"
  inbound_c_after="$(read_counter sequencer "$INBOUND" 128)"
  ack_p_after="$(read_counter audit "$ACK" 64)"
  ack_c_after="$(read_counter gateway "$ACK" 128)"
  if (( journal_p_after > journal_p_before \
        && journal_c_after > journal_c_before \
        && inbound_c_after > inbound_c_before \
        && ack_p_after > ack_p_before \
        && ack_c_after > ack_c_before )); then
    break
  fi
  if [[ "$attempt" -eq 100 ]]; then
    echo "ERROR: Expected ring counters did not all advance." >&2
    printf 'journal producer: %s -> %s\n' "$journal_p_before" "$journal_p_after" >&2
    printf 'journal consumer: %s -> %s\n' "$journal_c_before" "$journal_c_after" >&2
    printf 'inbound consumer: %s -> %s\n' "$inbound_c_before" "$inbound_c_after" >&2
    printf 'ACK producer: %s -> %s\n' "$ack_p_before" "$ack_p_after" >&2
    printf 'ACK consumer: %s -> %s\n' "$ack_c_before" "$ack_c_after" >&2
    "${COMPOSE[@]}" logs --tail=150 sequencer audit gateway >&2
    exit 1
  fi
  sleep 0.1
done

echo "[7/7] Verifying durable PostgreSQL audit row..."
row_count="$(docker compose exec -T postgres psql -U g12 -d g12audit -tAc \
  "SELECT COUNT(*) FROM transaction_audit WHERE stream_id = ${stream_id} AND sequence_id = ${journal_p_before} AND client_txn_id = 1001 AND amount = 2500 AND from_account = 101 AND to_account = 202;")"
row_count="$(printf '%s' "$row_count" | tr -d '[:space:]')"
if [[ ! "$row_count" =~ ^[0-9]+$ ]] || (( row_count != 1 )); then
  echo "ERROR: Expected transaction 1001 was not found exactly once for stream ${stream_id}, sequence ${journal_p_before}." >&2
  exit 1
fi

printf '\nPASS: three-service integration verified.\n'
printf '  Inbound consumerSeq:  %s -> %s\n' "$inbound_c_before" "$inbound_c_after"
printf '  Journal producerSeq:  %s -> %s\n' "$journal_p_before" "$journal_p_after"
printf '  Journal consumerSeq:  %s -> %s\n' "$journal_c_before" "$journal_c_after"
printf '  ACK producerSeq:      %s -> %s\n' "$ack_p_before" "$ack_p_after"
printf '  ACK consumerSeq:      %s -> %s\n' "$ack_c_before" "$ack_c_after"
printf '  PostgreSQL matching rows: %s\n' "$row_count"
printf '\nTransaction was sequenced, persisted, acknowledged, and delivered to its WebSocket connection.\n'
