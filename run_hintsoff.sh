#!/usr/bin/env bash
# Full AIOpsLab bench on /root/mesh (main, with core localization fix), hints
# OFF, DeepSeek observer. Stable results/log paths so the monitor + scheduled
# agent track it without guessing a timestamp. Each launch overwrites.
set -euo pipefail
cd /root/AIOpsLab

set -a; . /root/.env; set +a

export MESH_INTELLIGENCE_PATH=/root/mesh
export MESH_AIOPSLAB_BENCHMARK_HINTS=0
export MESH_OBSERVER_ENABLED=1
export MESH_OBSERVER_PROVIDER=openai
export MESH_OBSERVER_BASE_URL=https://api.deepseek.com
export MESH_OBSERVER_API_KEY="${DEEPSEEK_API_KEY}"
export MESH_OBSERVER_MODEL=deepseek-v4-pro
export MESH_OBSERVER_TIMEOUT_SECONDS=90
export MESH_OBSERVER_MAX_TOKENS=8000

RD="data/results/mesh-aiopslab-hintsoff-run"
LOG="logs/mesh-aiopslab-hintsoff-run.log"
rm -rf "$RD"; mkdir -p "$RD" logs
printf 'RD=%s\nLOG=%s\nSTARTED=%s\n' "/root/AIOpsLab/$RD" "/root/AIOpsLab/$LOG" "$(date -u +%FT%TZ)" > /root/AIOpsLab/.last_hintsoff_run.txt

echo "[launch] /root/mesh@main (localization fix) hints=OFF observer=deepseek-v4-pro results=$RD"
.venv/bin/python run_mesh_aiopslab.py --all-registry --results-dir "$RD" 2>&1 | tee "$LOG"
