#!/usr/bin/env bash
set -euo pipefail
cd /root/AIOpsLab
set -a
. /root/.env
set +a
export PATH=/root/.local/bin:$PATH
export CLAUDE_AIOPSLAB_ACTION_TIMEOUT=${CLAUDE_AIOPSLAB_ACTION_TIMEOUT:-180}
export CLAUDE_AIOPSLAB_PROBLEM_TIMEOUT=${CLAUDE_AIOPSLAB_PROBLEM_TIMEOUT:-900}
export CLAUDE_AIOPSLAB_MAX_BUDGET_USD=${CLAUDE_AIOPSLAB_MAX_BUDGET_USD:-1.00}
MODEL=${CLAUDE_CODE_MODEL:-${ANTHROPIC_MODEL:-sonnet}}
TS=$(date -u +%Y%m%dT%H%M%SZ)
RD="data/results/claude-code-aiopslab-${TS}"
LOG="logs/claude-code-aiopslab-${TS}.log"
mkdir -p "$RD" logs
printf 'RD=%s
LOG=%s
MODEL=%s
STARTED=%s
MANIFEST=%s
'   "/root/AIOpsLab/$RD" "/root/AIOpsLab/$LOG" "$MODEL" "$(date -u +%FT%TZ)" "/root/AIOpsLab/$RD/claude_code_aiopslab_results.jsonl"   > /root/AIOpsLab/.last_claude_code_aiopslab_run.txt
echo "[launch] claude-code model=$MODEL results=$RD action_timeout=$CLAUDE_AIOPSLAB_ACTION_TIMEOUT problem_timeout=$CLAUDE_AIOPSLAB_PROBLEM_TIMEOUT"
exec .venv/bin/python run_claude_code_aiopslab.py   --all-registry   --max-steps 8   --results-dir "$RD"   --model "$MODEL"   --max-budget-usd "$CLAUDE_AIOPSLAB_MAX_BUDGET_USD"   --action-timeout "$CLAUDE_AIOPSLAB_ACTION_TIMEOUT"   --problem-timeout "$CLAUDE_AIOPSLAB_PROBLEM_TIMEOUT"   2>&1 | tee "$LOG"
