#!/usr/bin/env bash
set -euo pipefail
cd /root/AIOpsLab
set -a; . /root/.env; set +a
export PATH=/root/.local/bin:$PATH
RD=data/results/claude-code-aiopslab-20260614T085338Z
TS=$(date -u +%Y%m%dT%H%M%SZ)
LOG="logs/claude-code-aiopslab-continue-${TS}.log"
echo "[continue] resuming $RD (skip-existing) at $TS"
exec .venv/bin/python run_claude_code_aiopslab.py \
  --all-registry --skip-existing --max-steps 8 \
  --results-dir "$RD" --model sonnet \
  --max-budget-usd 1.50 --action-timeout 180 --problem-timeout 900 \
  2>&1 | tee -a "$LOG"
