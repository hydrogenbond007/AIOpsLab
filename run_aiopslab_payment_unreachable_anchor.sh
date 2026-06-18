#!/usr/bin/env bash
set -euo pipefail

cd /root/AIOpsLab
set -a
. /root/.env
set +a

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
export PYTHONUNBUFFERED=1

TS="$(date -u +%m%d_%H%M%S)"
RD="data/results/mesh-aiopslab-payment-unreachable-anchor-${TS}"
LOG="logs/mesh-aiopslab-payment-unreachable-anchor-${TS}.log"
mkdir -p "$RD" logs
printf 'RD=%s\nLOG=%s\nMESH=%s\nSTARTED=%s\nPROBLEM=%s\nANCHOR=%s\n' \
  "/root/AIOpsLab/$RD" \
  "/root/AIOpsLab/$LOG" \
  "$(git -C /root/mesh-main-bench rev-parse --short HEAD)" \
  "$(date -u +%FT%TZ)" \
  "astronomy_shop_payment_service_unreachable-localization-1" \
  "${MESH_AIOPSLAB_PROBLEM_ANCHOR_ENABLED}" \
  > /root/AIOpsLab/.last_payment_unreachable_anchor_run.txt

exec .venv/bin/python run_mesh_aiopslab.py \
  --problem-id astronomy_shop_payment_service_unreachable-localization-1 \
  --results-dir "$RD" 2>&1 | tee "$LOG"
