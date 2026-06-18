#!/usr/bin/env bash
# Full AIOpsLab bench on /root/mesh-main-bench (origin/main tip incl. PR #19
# log_search + PR #20 observation fidelity/spill), hints OFF, DeepSeek
# observer. Timestamped results/log paths; never clobbers earlier runs.
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

TS=$(date -u +%m%d_%H%M)
RD="data/results/mesh-aiopslab-mainbench-${TS}"
LOG="logs/mesh-aiopslab-mainbench-${TS}.log"
mkdir -p "$RD" logs
printf 'RD=%s\nLOG=%s\nMESH=%s\nSTARTED=%s\n' "/root/AIOpsLab/$RD" "/root/AIOpsLab/$LOG" "$(git -C /root/mesh-main-bench rev-parse --short HEAD)" "$(date -u +%FT%TZ)" > /root/AIOpsLab/.last_mainbench_run.txt
echo "[launch] mesh-main-bench@$(git -C /root/mesh-main-bench rev-parse --short HEAD) hints=OFF observer=deepseek-v4-pro results=$RD"
exec .venv/bin/python run_mesh_aiopslab.py --all-registry --results-dir "$RD" 2>&1 | tee "$LOG"
