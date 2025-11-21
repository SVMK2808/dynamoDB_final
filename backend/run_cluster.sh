#!/usr/bin/env bash
# run_cluster.sh — start / stop the local Dynamo‑style KV‑store cluster
# =====================================================================
set -u          # abort on unset vars
IFS=$'\n\t'

# ───────────────────────────  ANSI colours  ──────────────────────────
ESC=$'\033'                     # real ESC char
RESET="${ESC}[0m"
BOLD="${ESC}[1m"
FG_RED="${ESC}[31m"
FG_GRN="${ESC}[32m"
FG_YLW="${ESC}[33m"
FG_CYN="${ESC}[36m"
FG_MAG="${ESC}[35m"

info()    { printf "${FG_CYN}ℹ %s${RESET}\n" "$*"; }
success() { printf "${FG_GRN}✓ %s${RESET}\n" "$*"; }
warn()    { printf "${FG_YLW}⚠ %s${RESET}\n" "$*"; }
error()   { printf "${FG_RED}✗ %s${RESET}\n" "$*"; }
step()    { printf "\n${BOLD}%s${RESET}\n" "$*"; }

# ────────────────  curl wrapper (hides libcurl banner)  ──────────────
curlx() {
  curl -sS "$@" \
       2> >(grep -v "no version information available (required by curl)" >&2)
}

# ────────────────────────────  constants  ────────────────────────────
# Allow overriding the number of nodes and base port
NUM_NODES=${NUM_NODES:-4}
BASE_PORT=${BASE_PORT:-8000}

NODES=()
PORTS=()
for ((i=0; i<NUM_NODES; i++)); do
  NODES+=("node$((i+1))")
  PORTS+=($((BASE_PORT + i)))
done

LOG_DIR="logs"
CFG_DIR="configs"
mkdir -p "$LOG_DIR" "$CFG_DIR"

# ──────────────────────────  stop_cluster  ───────────────────────────
stop_cluster() {
  step "Stopping any running nodes…"
  for port in "${PORTS[@]}"; do
    if pid=$(lsof -t -i:"$port" 2>/dev/null); then
      info "Stopping process on port $port (PID $pid)"
      kill "$pid" 2>/dev/null || true
    fi
  done
  sleep 2
  for port in "${PORTS[@]}"; do
    if pid=$(lsof -t -i:"$port" 2>/dev/null); then
      warn "Force‑killing stubborn process on port $port (PID $pid)"
      kill -9 "$pid" 2>/dev/null || true
    fi
  done
  pkill -f "go run .*" 2>/dev/null || true
  pkill -f "dynamo-node" 2>/dev/null || true
  success "All nodes stopped."
}

# ─────────────────────────  generate_configs  ────────────────────────
generate_configs() {
  step "Generating fresh config files…"
  
  # Default values if not set in environment
  local REPLICATION_FACTOR=${N:-3}
  local READ_QUORUM=${R:-2}
  local WRITE_QUORUM=${W:-2}
  
  info "Cluster Configuration:"
  info "  • Cluster Size (NUM_NODES):  $NUM_NODES (All nodes participate in the ring)"
  info "  • Replication Factor (N):    $REPLICATION_FACTOR (Copies of each data item)"
  info "  • Read Quorum (R):           $READ_QUORUM"
  info "  • Write Quorum (W):          $WRITE_QUORUM"

  for i in "${!NODES[@]}"; do
    node=${NODES[$i]}
    port=${PORTS[$i]}
    peers=()
    for j in "${!NODES[@]}"; do
      [[ $i == $j ]] && continue
      peers+=( "{\"node_id\":\"${NODES[$j]}\",\"host\":\"localhost\",\"port\":${PORTS[$j]}}" )
    done
    peers_json=$(IFS=,; echo "${peers[*]}")

    cat > "$CFG_DIR/$node.json" <<EOF
{
  "node_id": "$node",
  "host": "localhost",
  "port": $port,
  "peers": [ $peers_json ],
  "replication_factor": $REPLICATION_FACTOR,
  "read_quorum": $READ_QUORUM,
  "write_quorum": $WRITE_QUORUM,
  "gossip_interval_ms": 1000,
  "failure_check_interval_ms": 2000,
  "gossip_timeout_ms": 1000,
  "anti_entropy_interval_ms": 30000,
  "gossip_fanout": 2,
  "gossip_retries": 3,
  "gossip_retry_backoff_ms": 500,
  "failure_timeout_ms": 10000,
  "suspicion_timeout_ms": 5000
}
EOF
    success "Config for $node → $CFG_DIR/$node.json"
  done
}

# ──────────────────────────  start_cluster  ──────────────────────────
start_cluster() {
  step "Starting DynamoDB cluster…"
  stop_cluster
  
  # Clean up old logs and locks
  rm -f "$LOG_DIR"/*.log "$LOG_DIR"/*.txt "$LOG_DIR"/*.pid
  rm -f data/*/LOCK

  generate_configs

  step "Building binary..."
  if ! go build -o dynamo-node .; then
    error "Build failed!"
    exit 1
  fi
  success "Build successful."

  for i in "${!NODES[@]}"; do
    node=${NODES[$i]}
    port=${PORTS[$i]}
    cfg="$CFG_DIR/$node.json"

    info "Launching $node on :$port"
    ./dynamo-node -config="$cfg" >"$LOG_DIR/$node.log" 2>&1 &
    pid=$!
    echo "$pid" >"$LOG_DIR/$node.pid"
    sleep 1 # Short sleep to let it initialize
    
    if ps -p "$pid" >/dev/null; then
      success "$node running (PID $pid)"
    else
      error "$node failed to start — see ${LOG_DIR}/${node}.log"
    fi
  done

  # probe admin endpoint
  step "Verifying cluster responsiveness…"
  for attempt in {1..10}; do
    if curlx "http://localhost:${PORTS[0]}/admin/cluster" >/dev/null; then
      success "Cluster is up!"
      show_usage
      return
    fi
    info "Waiting for cluster… (attempt $attempt/10)"
    sleep 2
  done

  warn "Cluster did not respond in time. Logs follow:"
  for node in "${NODES[@]}"; do
    tail -n 20 "$LOG_DIR/$node.log" | sed "s/^/[${node}] /"
  done
}

# ────────────────────────────  banner  ───────────────────────────────
show_usage() {
  local BAR="───────────────────────────────────────────────────────────────"
  printf "${BOLD}%s${RESET}\n" "$BAR"
  printf "Cluster running with %d nodes.\n\n" "${#NODES[@]}"

  printf "%-10s %s\n" "${FG_MAG}PUT${RESET}"    "curl -X PUT http://localhost:8000/kv/mykey \\"
  printf "%-10s %s\n" ""                        "     -d '{\"value\":\"hello\"}' -H 'Content-Type: application/json'"
  printf "%-10s %s\n" "${FG_MAG}GET${RESET}"    "curl http://localhost:8000/kv/mykey"
  printf "%-10s %s\n" "${FG_MAG}STATUS${RESET}" "curl http://localhost:8000/admin/cluster"
  printf "%-10s %s\n" "${FG_MAG}STOP${RESET}"   "./run_cluster.sh stop"
  printf "\n"
  printf "You can customize N, R, W, and NUM_NODES by setting environment variables:\n"
  printf "  NUM_NODES=5 N=3 R=2 W=2 ./run_cluster.sh\n"

  printf "${BOLD}%s${RESET}\n" "$BAR"
}

# ──────────────────────────────  main  ───────────────────────────────
case "${1:-start}" in
  stop) stop_cluster ;;
  *)    start_cluster ;;
esac
