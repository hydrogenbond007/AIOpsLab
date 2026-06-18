#!/usr/bin/env bash
set -euo pipefail
cd /root/AIOpsLab
set -a; . /root/.env; set +a
export MESH_INTELLIGENCE_PATH=/root/mesh-main-bench
export MESH_AIOPSLAB_BENCHMARK_HINTS=0
export MESH_AIOPSLAB_PROBLEM_ANCHOR_ENABLED=1
export MESH_OBSERVER_ENABLED=1
export MESH_OBSERVER_PROVIDER=openai
export MESH_OBSERVER_BASE_URL=https://api.deepseek.com
export MESH_OBSERVER_API_KEY="${DEEPSEEK_API_KEY}"
export MESH_OBSERVER_MODEL=deepseek-v4-pro
export MESH_OBSERVER_TIMEOUT_SECONDS=90
export MESH_OBSERVER_MAX_TOKENS=8000
TS=$(date -u +%Y%m%dT%H%M%SZ)
RD="data/results/mesh-aiopslab-mitfix-${TS}"
LOG="logs/mesh-aiopslab-mitfix-${TS}.log"
mkdir -p "$RD" logs
printf "RD=%s\nLOG=%s\nMESH=%s\nSTARTED=%s\n" "/root/AIOpsLab/$RD" "/root/AIOpsLab/$LOG" "$(git -C /root/mesh-main-bench rev-parse --short HEAD)" "$TS" > /root/AIOpsLab/.last_mitfix_run.txt
echo "[launch] mitfix rerun results=$RD"
exec .venv/bin/python run_mesh_aiopslab.py \
  --problem-id auth_miss_mongodb-mitigation-1 \
  --problem-id revoke_auth_mongodb-mitigation-2 \
  --problem-id user_unregistered_mongodb-mitigation-2 \
  --max-steps 20 --results-dir "$RD" 2>&1 | tee "$LOG"
